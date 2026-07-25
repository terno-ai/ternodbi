"""
discover_relationships — the measured-facts stage for join edges.

Given a datasource, it finds plausible column pairs (from declared FKs + name-token matches
among key-like columns), MEASURES their value overlap against the live database, applies the
deterministic D2 cardinality gate, and upserts the result into MeasuredRelationship.

Design notes (all validated against synthetic + real fixtures before landing here):
  * Detection is deterministic: the verdict is a pure function of measured numbers, so a
    re-run on unchanged data produces an unchanged content_hash (only last_confirmed_at bumps).
  * The cardinality gate keys on the smaller side's distinct_ratio WITHIN ITS OWN TABLE
    (repetition), NOT on raw domain size — a genuine key on a small table is not a trap.
  * human_asserted / is_protected edges are never silently overwritten; a contradicting
    measurement leaves the stored row intact and is reported for review.
"""
from typing import Any, Dict, List, Optional, Tuple
import logging
import re

import sqlalchemy
from sqlalchemy import MetaData, Table as SATable, select, func, distinct

from django.db.models import Q
from django.utils import timezone

from terno_dbi.core import models
from terno_dbi.connectors.factory import ConnectorFactory

logger = logging.getLogger(__name__)

# a column is "key-like" (join candidate) if its name looks like an identifier
_KEY_NAME_RE = re.compile(r'(^id$|_id$|_code$|_key$)', re.IGNORECASE)

# cardinality gate thresholds
_SMALL_DOMAIN = 50          # below this, a REPEATING column is a low-cardinality trap
_REPEAT_RATIO = 0.9         # distinct_ratio below this => the column repeats within its table
_RELIABLE = 0.9
_PARTIAL = 0.3

_CAMEL_BOUNDARY_RE = re.compile(r'(?<=[a-z0-9])(?=[A-Z])')


def _to_snake(name: str) -> str:
    """
    Normalize ANY naming convention to snake_case before pattern-matching, so every regex/
    token-matching rule below only has to know one convention.

    Found live: Chinook (a classic .NET/Entity-Framework-style schema) uses PascalCase FK
    names with NO underscores at all — TrackId, AlbumId, GenreId, MediaTypeId. The original
    `_KEY_NAME_RE`/`_name_token` logic only recognized snake_case (`_id` with an underscore),
    so it silently found ZERO candidate pairs on the entire schema — no error, just complete
    silent failure. PascalCase/camelCase is extremely common in real-world databases (any
    .NET/EF or Java/Hibernate-backed system), so this isn't a Chinook-specific quirk; it's
    normalizing at the one place all name-matching flows through, rather than hand-patching
    every individual regex to also accept a second convention.
    """
    return _CAMEL_BOUNDARY_RE.sub('_', name).lower()


def _name_token(col_name: str) -> str:
    """Normalise a column name to its join token, e.g. customer_id -> customer, AlbumId ->
    album, id -> id."""
    snake = _to_snake(col_name)
    stripped = re.sub(r'(_id|_code|_key)$', '', snake)
    return stripped or snake


def _classify(overlap: float, domain_size: int,
              from_ratio: float, to_ratio: float, id_named: bool) -> str:
    """
    Low-cardinality trap = a small-domain join key where NEITHER side is a genuine unique key
    (both columns repeat within their own tables) — the fact<->fact case (e.g. two tables both
    carrying an ``over_id`` 1..20). If EITHER side is a real key (distinct_ratio ~ 1.0), it's a
    normal FK->PK join to a small dimension (e.g. orders.status_id -> status.status_id), which
    is reliable, not a trap. This refinement was forced by a fact->small-lookup test case.
    """
    from_is_key = from_ratio is not None and from_ratio >= _REPEAT_RATIO
    to_is_key = to_ratio is not None and to_ratio >= _REPEAT_RATIO
    neither_is_key = not from_is_key and not to_is_key
    if domain_size < _SMALL_DOMAIN and neither_is_key:
        return models.MeasuredRelationship.Verdict.LOW_CARDINALITY_TRAP
    if overlap >= _RELIABLE:
        return models.MeasuredRelationship.Verdict.RELIABLE
    if overlap >= _PARTIAL:
        return models.MeasuredRelationship.Verdict.PARTIAL
    if id_named:
        return models.MeasuredRelationship.Verdict.SUSPICIOUS
    return models.MeasuredRelationship.Verdict.PARTIAL


# =========================================================================================
# D5: rollup/summary-table detection.
#
# A join can be perfectly `reliable` (D2) and STILL be the wrong table to aggregate from —
# e.g. results.driver_id <-> driver_standings.driver_id is a clean reliable join, but SUMming
# results.points per driver per year gives a DIFFERENT (and often wrong) answer than reading
# driver_standings.points, because driver_standings encodes business rules (drop-lowest-N
# scoring, disqualifications) invisible in results' columns. D5 flags this pattern so a
# downstream SQL-writing agent checks the summary table BEFORE aggregating the raw one.
#
# Row count is NOT a reliable signal here (verified live: driver_standings has MORE total rows
# than results in the f1 dataset, despite being the "summary" side) — the real signal is
# whether a shared, same-named measure column looks CUMULATIVE (monotonically non-decreasing,
# with occasional resets at natural period boundaries like season starts) when ordered by a
# time/sequence key, versus EPISODIC (resets every row) on the raw side.
# =========================================================================================

_NUMERIC_TYPE_MARKERS = ('INT', 'FLOAT', 'REAL', 'DECIMAL', 'NUMERIC', 'DOUBLE')


def _is_numeric_type(declared_type: str) -> bool:
    return any(m in (declared_type or '').upper() for m in _NUMERIC_TYPE_MARKERS)


_NON_MEASURE_TOKENS = {
    'position', 'rank', 'ranking', 'standing', 'place', 'number', 'num', 'no', 'lap', 'round',
    'grid', 'code', 'order',
}


def _is_non_measure_name(snake_name: str) -> bool:
    """Token-based, not whole-string: catches compound names like `position_order` (found
    live on f1 — a purely numeric-sort variant of `position`, same non-additive semantics),
    not just an exact 'position'. Operates on an already-`_to_snake`'d name."""
    return any(tok in _NON_MEASURE_TOKENS for tok in snake_name.split('_'))


def _shared_measure_columns(cols_a: Dict[str, models.TableColumn], cols_b: Dict[str, models.TableColumn],
                            exclude_names: set) -> List[str]:
    """Same-named (by normalized snake_case key), both-numeric columns present on both tables,
    excluding:
    - the key columns already used for the join/ordering (a key isn't a 'measure'),
    - any column flagged as part of the PRIMARY KEY on either side — found live on f1:
      lap_times/lap_times_ext share an identical schema (a D4 duplicate-table case, not a
      rollup) and their shared 'lap' column is part of the composite grain, not a measure;
      checking its monotonicity is meaningless,
    - rank/ordinal/identifier-shaped names (position, rank, number, code, ...) — also found
      live: 'position' triggered noisy 'uncertain' rollup signals across semantically
      unrelated table pairs (lap position vs championship position, different concepts that
      happen to share a column name). A rank is not an accumulating quantity; summing or
      monotonicity-checking it doesn't mean what D5 is trying to detect.
    `cols_a`/`cols_b` keys must already be `_to_snake`'d by the caller.
    """
    out = []
    for name, ca in cols_a.items():
        if name in exclude_names or _is_non_measure_name(name):
            continue
        cb = cols_b.get(name)
        if cb is None:
            continue
        if ca.primary_key or cb.primary_key:
            continue
        if _is_numeric_type(ca.data_type) and _is_numeric_type(cb.data_type):
            out.append(name)
    return out


def _monotonic_score(m: '_Measurer', table_name: str, entity_col: str, order_col: str,
                     measure_col: str, sample_size: int = 3) -> Optional[float]:
    """
    Sample a few entities, pull their (order_col, measure_col) rows in order, and score what
    fraction of consecutive steps are non-decreasing — SKIPPING steps that look like a natural
    period reset (value drops to under half the previous), since a real cumulative total still
    legitimately resets at a season/period boundary without that meaning it isn't cumulative.
    Returns None if there isn't enough data to judge (not a verdict of "not cumulative").
    """
    entities = m.sample_entities(table_name, entity_col, sample_size)
    if not entities:
        return None
    ratios = []
    for ev in entities:
        series = m.ordered_series(table_name, entity_col, ev, order_col, measure_col)
        vals = [row[1] for row in series if row[1] is not None]
        if len(vals) < 4:
            continue
        counted = 0
        non_decreasing = 0
        for prev, cur in zip(vals, vals[1:]):
            if prev == 0 or cur < prev * 0.5:
                continue  # treat as a reset (e.g. new season), not evidence against monotonicity
            counted += 1
            if cur >= prev:
                non_decreasing += 1
        if counted > 0:
            ratios.append(non_decreasing / counted)
    if not ratios:
        return None
    return sum(ratios) / len(ratios)


_ROLLUP_HIGH = 0.85  # score at/above this looks genuinely cumulative
_ROLLUP_LOW = 0.5    # score below this looks episodic (no better than a random walk)


def _classify_rollup(score_from: Optional[float], score_to: Optional[float]) -> Tuple[str, str]:
    Sig = models.MeasuredRelationship.RollupSignal
    if score_from is None or score_to is None:
        return Sig.NONE, "insufficient sample data to judge monotonicity on one or both sides"
    from_cumulative = score_from >= _ROLLUP_HIGH
    to_cumulative = score_to >= _ROLLUP_HIGH
    from_episodic = score_from < _ROLLUP_LOW
    to_episodic = score_to < _ROLLUP_LOW
    if to_cumulative and from_episodic:
        return Sig.TO_IS_ROLLUP, (
            f"'to' side's shared measure looks cumulative (monotonic score {score_to:.2f}) "
            f"while 'from' side looks episodic (score {score_from:.2f}) — prefer the 'to' "
            f"table over aggregating the 'from' table for ranking/total questions."
        )
    if from_cumulative and to_episodic:
        return Sig.FROM_IS_ROLLUP, (
            f"'from' side's shared measure looks cumulative (monotonic score {score_from:.2f}) "
            f"while 'to' side looks episodic (score {score_to:.2f}) — prefer the 'from' "
            f"table over aggregating the 'to' table for ranking/total questions."
        )
    if from_cumulative and to_cumulative:
        return Sig.UNCERTAIN, (
            f"both sides' shared measure look cumulative (scores {score_from:.2f}/{score_to:.2f}) "
            f"— unusual, verify which is authoritative before trusting either."
        )
    return Sig.NONE, (
        f"neither side's shared measure looks cumulative (scores {score_from:.2f}/{score_to:.2f}) "
        f"— no rollup relationship detected."
    )


def _cardinality(from_ratio: float, to_ratio: float) -> str:
    C = models.MeasuredRelationship.Cardinality
    from_key = from_ratio is not None and from_ratio >= _REPEAT_RATIO
    to_key = to_ratio is not None and to_ratio >= _REPEAT_RATIO
    if from_key and to_key:
        return C.ONE_TO_ONE
    if to_key and not from_key:
        return C.MANY_TO_ONE
    if from_key and not to_key:
        return C.ONE_TO_MANY
    return C.MANY_TO_MANY


def _confidence(verdict: str) -> str:
    Conf = models.MeasuredRelationship.Confidence
    V = models.MeasuredRelationship.Verdict
    if verdict == V.RELIABLE:
        return Conf.HIGH
    if verdict in (V.PARTIAL, V.LOW_CARDINALITY_TRAP):
        return Conf.MEDIUM
    return Conf.LOW


class _Measurer:
    """Reflects tables lazily and measures distinct/overlap via SQLAlchemy Core (dialect-safe)."""

    def __init__(self, conn: sqlalchemy.Connection):
        self.conn = conn
        self.meta = MetaData()
        self._cache: Dict[str, SATable] = {}

    def _table(self, name: str) -> SATable:
        if name not in self._cache:
            self._cache[name] = SATable(name, self.meta, autoload_with=self.conn)
        return self._cache[name]

    def col(self, table_name: str, col_name: str):
        return self._table(table_name).c[col_name]

    def distinct_and_rows(self, table_name: str, col_name: str) -> Tuple[int, int]:
        t = self._table(table_name)
        c = t.c[col_name]
        row = self.conn.execute(
            select(func.count().label('rows'), func.count(distinct(c)).label('distinct'))
            .select_from(t)
        ).fetchone()
        return int(row[0] or 0), int(row[1] or 0)

    def matched(self, a_table: str, a_col: str, b_table: str, b_col: str) -> int:
        """Distinct values of a_table.a_col that also appear in b_table.b_col."""
        a = self.col(a_table, a_col)
        b = self.col(b_table, b_col)
        return int(self.conn.execute(
            select(func.count(distinct(a))).where(a.in_(select(b)))
        ).scalar() or 0)

    def sample_entities(self, table_name: str, col_name: str, n: int) -> List:
        """A sample of distinct values of a column, for D5's per-entity monotonicity check."""
        c = self.col(table_name, col_name)
        rows = self.conn.execute(select(distinct(c)).limit(n)).fetchall()
        return [r[0] for r in rows if r[0] is not None]

    def ordered_series(self, table_name: str, entity_col: str, entity_val,
                       order_col: str, measure_col: str) -> List[Tuple]:
        """One entity's (order_val, measure_val) rows, ordered — the raw material for D5's
        monotonicity check (does the measure look cumulative when ordered by a time/sequence
        key, or episodic?)."""
        t = self._table(table_name)
        stmt = (
            select(t.c[order_col], t.c[measure_col])
            .where(t.c[entity_col] == entity_val)
            .order_by(t.c[order_col])
        )
        return self.conn.execute(stmt).fetchall()


_BARE_ID_RE = re.compile(r'^id$', re.IGNORECASE)

# common non-semantic table-name suffixes to strip before token-matching a bare `id` column's
# own table name against a candidate FK column's stem (e.g. "interest_map" -> tokens {"interest"})
_GENERIC_TABLE_SUFFIXES = {
    'map', 'table', 'dim', 'dimension', 'lookup', 'master', 'ref', 'reference', 'list', 'data'
}


def _table_tokens(table_name: str) -> set:
    snake = _to_snake(table_name)
    tokens = {t for t in snake.split('_') if t not in _GENERIC_TABLE_SUFFIXES}
    return tokens or {snake}


def _candidate_pairs(columns_by_table: Dict[int, List[models.TableColumn]],
                     tables: Dict[int, models.Table]) -> List[Tuple[models.TableColumn, models.TableColumn]]:
    """Key-like columns grouped by normalised name token; pair columns across different tables.

    Two candidate-generation rules:
    1. Same-stem match: customer_id <-> customer_id (both sides have the same normalised token).
    2. Bare-id, table-name-token match: a bare ``id`` column (the single most common PK
       convention) is paired against another table's ``*_id``/``*_code``/``*_key`` column ONLY
       when that column's stem shares a token with the bare id's OWN TABLE NAME — e.g.
       interest_metrics.interest_id (stem "interest") pairs with interest_map.id because
       "interest" is a token of "interest_map". This was found missing entirely (silently
       skipping the known-reliable interest_id<->id join) when discover_relationships first
       ran live; an initial fix that paired bare ids against EVERY other-table key column also
       ran live and was found to blow up candidate pairs 3x with pure combinatorial noise
       (junk `suspicious` edges between semantically unrelated tables, e.g.
       interest_map.id<->bitcoin_members.member_id) — a real cost/quality problem at the scale
       of a production schema with thousands of tables. Table-name-token matching is the bound:
       it still catches the realistic FK-naming convention without cartesian blowup, at the
       accepted cost of missing a relationship whose column name shares literally no token with
       either side's table name (an unusual naming case better suited to a human-asserted edge).
    """
    by_token: Dict[str, List[models.TableColumn]] = {}
    bare_ids: List[models.TableColumn] = []
    other_keys: List[models.TableColumn] = []
    for cols in columns_by_table.values():
        for c in cols:
            snake_name = _to_snake(c.name)
            if not _KEY_NAME_RE.search(snake_name):
                continue
            by_token.setdefault(_name_token(c.name), []).append(c)
            if _BARE_ID_RE.match(snake_name):
                bare_ids.append(c)
            else:
                other_keys.append(c)

    pairs: List[Tuple[models.TableColumn, models.TableColumn]] = []
    seen = set()

    def _add(a: models.TableColumn, b: models.TableColumn):
        if a.table_id == b.table_id:
            return
        key = tuple(sorted((a.id, b.id)))
        if key in seen:
            return
        seen.add(key)
        pairs.append((a, b))

    # rule 1: same normalised stem
    for group in by_token.values():
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                _add(group[i], group[j])

    # rule 2: bare `id`, bounded to candidates whose stem shares a token with its own table name
    for bare in bare_ids:
        bare_table_tokens = _table_tokens(tables[bare.table_id].name)
        for other in other_keys:
            if other.table_id == bare.table_id:
                continue
            if _name_token(other.name) in bare_table_tokens:
                _add(bare, other)

    return pairs


def _run_d5_pass(m: _Measurer, tables, columns_by_table, pairs, result: Dict[str, Any]) -> None:
    """
    For any two tables sharing >=2 candidate key-column-pairs (e.g. both driver_id AND
    race_id), try each pair as the "entity" (GROUP BY) dimension with another as the "time/
    sequence" (ORDER BY) dimension, and check any shared same-named numeric column for the
    cumulative-vs-episodic monotonicity signature. On a clear signal, write it onto the
    already-measured MeasuredRelationship row for that entity key pair.
    """
    grouped: Dict[frozenset, List[Tuple[models.TableColumn, models.TableColumn]]] = {}
    for col_a, col_b in pairs:
        grouped.setdefault(frozenset((col_a.table_id, col_b.table_id)), []).append((col_a, col_b))

    for tkey, key_pairs in grouped.items():
        if len(tkey) != 2 or len(key_pairs) < 2:
            continue
        t1_id, t2_id = tuple(tkey)
        t1, t2 = tables[t1_id], tables[t2_id]
        cols1 = {_to_snake(c.name): c for c in columns_by_table[t1_id]}
        cols2 = {_to_snake(c.name): c for c in columns_by_table[t2_id]}

        # normalize every key pair to (column-in-t1, column-in-t2)
        norm_pairs = []
        for ca, cb in key_pairs:
            norm_pairs.append((ca, cb) if ca.table_id == t1_id else (cb, ca))

        key_names_1 = {_to_snake(c1.name) for c1, _ in norm_pairs}
        measures = _shared_measure_columns(cols1, cols2, key_names_1)
        if not measures:
            continue

        found = False
        for gi, (g1, g2) in enumerate(norm_pairs):
            if found:
                break
            for oi, (o1, o2) in enumerate(norm_pairs):
                if gi == oi:
                    continue
                for measure in measures:
                    # `measure` is a normalized snake_case key; map back to each table's
                    # ACTUAL column name for SQL — they can differ in casing/spelling even
                    # when they normalize to the same key (e.g. PascalCase on one side).
                    measure_col_1 = cols1[measure].name
                    measure_col_2 = cols2[measure].name
                    try:
                        score_1 = _monotonic_score(m, t1.name, g1.name, o1.name, measure_col_1)
                        score_2 = _monotonic_score(m, t2.name, g2.name, o2.name, measure_col_2)
                    except Exception as e:
                        logger.warning(
                            f"D5 monotonic check failed for {t1.name}.{g1.name} <-> "
                            f"{t2.name}.{g2.name} measure={measure}: {e}"
                        )
                        continue
                    local_signal, evidence = _classify_rollup(score_1, score_2)
                    if local_signal == models.MeasuredRelationship.RollupSignal.NONE:
                        continue

                    existing = models.MeasuredRelationship.objects.filter(
                        Q(from_column=g1, to_column=g2) | Q(from_column=g2, to_column=g1)
                    ).first()
                    if existing is None or existing.is_protected:
                        continue

                    # local_signal was computed with t1="from", t2="to" — translate onto the
                    # row's ACTUAL from/to direction, which D2 may have stored either way.
                    Sig = models.MeasuredRelationship.RollupSignal
                    if local_signal in (Sig.TO_IS_ROLLUP, Sig.FROM_IS_ROLLUP):
                        t1_is_rollup = local_signal == Sig.FROM_IS_ROLLUP
                        row_t1_is_from = existing.from_column_id == g1.id
                        stored_signal = (
                            Sig.FROM_IS_ROLLUP if (t1_is_rollup == row_t1_is_from) else Sig.TO_IS_ROLLUP
                        )
                    else:
                        stored_signal = local_signal

                    full_evidence = (
                        f"shared measure '{measure}', ordered by {t1.name}.{o1.name} / "
                        f"{t2.name}.{o2.name}: {evidence}"
                    )
                    existing.rollup_signal = stored_signal
                    existing.rollup_evidence = full_evidence
                    existing.save(update_fields=['rollup_signal', 'rollup_evidence'])
                    result["rollup_signals"].append({
                        "entity_edge": f"{t1.name}.{g1.name} <-> {t2.name}.{g2.name}",
                        "measure": measure,
                        "order_key": f"{t1.name}.{o1.name} / {t2.name}.{o2.name}",
                        "signal": stored_signal,
                    })
                    found = True
                    break
                if found:
                    break


def discover_relationships(datasource_id: int) -> Dict[str, Any]:
    try:
        datasource = models.DataSource.objects.get(id=datasource_id, enabled=True)
    except models.DataSource.DoesNotExist:
        return {"error": f"Datasource {datasource_id} not found or not enabled"}

    tables = {t.id: t for t in models.Table.objects.filter(data_source=datasource)}
    columns_by_table: Dict[int, List[models.TableColumn]] = {}
    for col in models.TableColumn.objects.filter(table__data_source=datasource):
        columns_by_table.setdefault(col.table_id, []).append(col)

    pairs = _candidate_pairs(columns_by_table, tables)
    result = {
        "datasource_id": datasource_id,
        "candidate_pairs": len(pairs),
        "edges_written": 0,
        "edges_unchanged": 0,
        "edges_pruned": 0,
        "protected_conflicts": [],
        "errors": 0,
        "edges": [],
        "rollup_signals": [],
    }

    connector = ConnectorFactory.create_connector(
        datasource.type, datasource.connection_str, credentials=datasource.connection_json
    )
    with connector.get_connection() as conn:
        m = _Measurer(conn)
        for col_a, col_b in pairs:
            try:
                _measure_and_upsert(m, datasource, tables, col_a, col_b, result)
            except Exception as e:  # a bad pair must not abort the whole run
                logger.warning(f"relationship measure failed {col_a} <-> {col_b}: {e}")
                result["errors"] += 1

        try:
            _run_d5_pass(m, tables, columns_by_table, pairs, result)
        except Exception as e:
            logger.warning(f"D5 rollup detection pass failed: {e}")

    # Prune stale edges: a row is stale if its column pair is no longer even a CANDIDATE under
    # the current candidate-generation rules (e.g. a prior, since-corrected rule was too broad
    # and wrote junk edges between semantically unrelated tables). Only structural absence from
    # `pairs` prunes a row — a transient measurement error above does NOT, since the pair is
    # still a valid candidate and its previously-measured state should be left alone. Protected
    # (human-asserted) edges are never pruned automatically.
    candidate_id_pairs = {frozenset((a.id, b.id)) for a, b in pairs}
    stale_qs = models.MeasuredRelationship.objects.filter(
        data_source=datasource, is_protected=False
    )
    stale_ids = [
        r.id for r in stale_qs.only('id', 'from_column_id', 'to_column_id')
        if frozenset((r.from_column_id, r.to_column_id)) not in candidate_id_pairs
    ]
    if stale_ids:
        models.MeasuredRelationship.objects.filter(id__in=stale_ids).delete()
        result["edges_pruned"] = len(stale_ids)

    return result


def _measure_and_upsert(m: _Measurer, datasource, tables,
                        col_a: models.TableColumn, col_b: models.TableColumn,
                        result: Dict[str, Any]) -> None:
    ta, tb = tables[col_a.table_id], tables[col_b.table_id]
    a_rows, a_distinct = m.distinct_and_rows(ta.name, col_a.name)
    b_rows, b_distinct = m.distinct_and_rows(tb.name, col_b.name)

    a_ratio = (a_distinct / a_rows) if a_rows else 0.0
    b_ratio = (b_distinct / b_rows) if b_rows else 0.0

    # Direction: from = child (the side that REPEATS, lower distinct_ratio); to = parent (the
    # key side, higher distinct_ratio). Tie-break (BOTH sides are genuine keys, e.g. two
    # full-history tables like shipment/shipment_legacy): fewer distinct values is the child —
    # i.e. report the CONTAINMENT direction (does the smaller table's key set fully appear in
    # the larger one?), which is the informative fact ("shipment_legacy is fully superseded by
    # shipment"). BUG FOUND LIVE on d4_fixture: `-a_distinct <= -b_distinct` is mathematically
    # equivalent to `a_distinct >= b_distinct` — the OPPOSITE of the intended tie-break — so it
    # picked the LARGER table as child, measuring shipment(60)->shipment_legacy(36) instead of
    # shipment_legacy(36)->shipment(60), reporting a misleading 0.6 "partial" overlap for a
    # relationship that is actually a clean 1.0 "reliable" containment. Compare distinct counts
    # directly (ascending), not negated.
    a_is_child = (a_ratio, a_distinct) <= (b_ratio, b_distinct)
    if a_is_child:
        frm, to, frm_t, to_t, frm_ratio, to_ratio = col_a, col_b, ta, tb, a_ratio, b_ratio
        frm_distinct = a_distinct
    else:
        frm, to, frm_t, to_t, frm_ratio, to_ratio = col_b, col_a, tb, ta, b_ratio, a_ratio
        frm_distinct = b_distinct

    matched = m.matched(frm_t.name, frm.name, to_t.name, to.name)
    overlap = (matched / frm_distinct) if frm_distinct else 0.0
    orphan_count = frm_distinct - matched
    # domain_size MUST be the same basis overlap was computed against (frm_distinct), not a
    # separately-computed min(a_distinct, b_distinct) — those can diverge whenever frm isn't
    # numerically the smaller side, which is exactly what the tie-break bug above caused.
    smaller_domain = frm_distinct

    id_named = bool(_KEY_NAME_RE.search(_to_snake(frm.name)) and _KEY_NAME_RE.search(_to_snake(to.name)))
    verdict = _classify(overlap, smaller_domain, frm_ratio, to_ratio, id_named)
    smaller_ratio = frm_ratio
    cardinality = _cardinality(frm_ratio, to_ratio)
    confidence = _confidence(verdict)

    # Look up by the UNORDERED column pair (col_a, col_b), not by frm/to. Which of the two is
    # "from" vs "to" is computed fresh every run from measured ratios and can legitimately flip
    # between runs (e.g. once a direction-selection bug is fixed). Looking up by frm/to alone
    # would miss an existing row stored in the opposite direction and INSERT a duplicate instead
    # of updating it — found live on d4_fixture after fixing the tie-break bug: it left the old
    # wrong-direction row (shipment -> shipment_legacy, 0.6 partial) sitting next to the new
    # correct one (shipment_legacy -> shipment, 1.0 reliable) instead of replacing it.
    #
    # RECONCILE, don't assume at most one match: a datasource that already accumulated a
    # duplicate pair from that exact bug (one row per direction, both real rows in the DB) will
    # have TWO matches here. .first() on that would grab an arbitrary one and then attempt to
    # flip it onto the (from_column, to_column) tuple the OTHER row already owns — an
    # IntegrityError on the unique constraint, found live immediately after deploying the fix
    # above. Prefer the row already in the newly-computed direction as the keeper; delete any
    # others for this pair before proceeding, so there is exactly one row to update or confirm.
    matches = list(models.MeasuredRelationship.objects.filter(
        Q(from_column=col_a, to_column=col_b) | Q(from_column=col_b, to_column=col_a)
    ))
    if len(matches) > 1:
        # keeper preference: a protected row first (never auto-delete human-asserted data,
        # even a duplicate), then whichever already matches the freshly-computed direction.
        matches.sort(key=lambda r: (
            0 if r.is_protected else 1,
            0 if (r.from_column_id == frm.id and r.to_column_id == to.id) else 1,
        ))
        keeper = matches[0]
        deletable_extras = [r for r in matches[1:] if not r.is_protected]
        if deletable_extras:
            models.MeasuredRelationship.objects.filter(
                id__in=[r.id for r in deletable_extras]
            ).delete()
        existing = keeper
    else:
        existing = matches[0] if matches else None
    direction_changed = existing is not None and (
        existing.from_column_id != frm.id or existing.to_column_id != to.id
    )

    # build a transient instance to compute the material hash
    candidate = models.MeasuredRelationship(
        data_source=datasource, from_column=frm, to_column=to,
        overlap_ratio=overlap, smaller_domain_size=smaller_domain,
        smaller_distinct_ratio=smaller_ratio, verdict=verdict,
        cardinality=cardinality, orphan_count=orphan_count,
        composite_key_members=[], provenance=models.MeasuredRelationship.Provenance.MEASURED,
        confidence=confidence,
    )
    new_hash = candidate.compute_content_hash()

    if existing is None:
        candidate.content_hash = new_hash
        candidate.last_changed_at = timezone.now()
        candidate.save()
        result["edges_written"] += 1
    elif existing.is_protected and existing.verdict != verdict:
        # do NOT overwrite a protected/human-asserted edge that we now contradict
        result["protected_conflicts"].append({
            "edge": f"{frm} -> {to}",
            "stored_verdict": existing.verdict,
            "measured_verdict": verdict,
        })
        return
    elif not direction_changed and existing.content_hash == new_hash:
        existing.save(update_fields=['last_confirmed_at'])  # cheap: just touch timestamp
        result["edges_unchanged"] += 1
        return
    else:
        existing.from_column = frm
        existing.to_column = to
        existing.overlap_ratio = overlap
        existing.smaller_domain_size = smaller_domain
        existing.smaller_distinct_ratio = smaller_ratio
        existing.verdict = verdict
        existing.cardinality = cardinality
        existing.orphan_count = orphan_count
        existing.confidence = confidence
        existing.content_hash = new_hash
        existing.last_changed_at = timezone.now()
        existing.save()
        result["edges_written"] += 1

    result["edges"].append({
        "from": f"{frm_t.name}.{frm.name}",
        "to": f"{to_t.name}.{to.name}",
        "overlap_ratio": round(overlap, 4),
        "domain_size": smaller_domain,
        "verdict": verdict,
        "cardinality": cardinality,
        "orphan_count": orphan_count,
    })
