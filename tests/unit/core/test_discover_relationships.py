"""
Tests for the discover_relationships measured-facts stage.

Builds a real SQLite database on disk, registers it as a DataSource, syncs its schema, then
runs discovery and asserts the deterministic verdicts — including the two gate cases the
synthetic fixture taught us:
  * a genuine unique key on a SMALL table must be `reliable`, NOT a low_cardinality_trap
    (the gate keys on repetition within the table, not raw domain size);
  * a truly low-cardinality repeating column pair must be `low_cardinality_trap`.
Also checks idempotency (a second run records no changes) and protected-edge safety.
"""
import os
import sqlite3
import tempfile

import pytest
from django.contrib.auth.models import User
from django.db.models import Q

from terno_dbi.core.models import CoreOrganisation, DataSource, MeasuredRelationship
from terno_dbi.services.schema_utils import sync_metadata
from terno_dbi.services.discover_relationships import discover_relationships


@pytest.fixture
def sqlite_datasource(db):
    owner = User.objects.create_user('relowner', 'rel@example.com', 'pw')
    org = CoreOrganisation.objects.create(name='relorg', subdomain='relorg', owner=owner)
    fd, path = tempfile.mkstemp(suffix='.sqlite')
    os.close(fd)
    con = sqlite3.connect(path)
    c = con.cursor()
    # customers: 40 rows, customer_id unique (a genuine key on a smallish table)
    c.execute("CREATE TABLE customers (customer_id INTEGER, name TEXT)")
    for i in range(40):
        c.execute("INSERT INTO customers VALUES (?,?)", (i, f"c{i}"))
    # orders: customer_id repeats (many orders per customer) + a low-cardinality status_id
    c.execute("CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, status_id INTEGER)")
    for i in range(200):
        c.execute("INSERT INTO orders VALUES (?,?,?)", (i, i % 40, i % 3))
    # status: 3 rows — a genuine key lookup (status_id unique). orders.status_id -> status is a
    # legitimate many-to-one, NOT a trap (one side IS a key).
    c.execute("CREATE TABLE status (status_id INTEGER, label TEXT)")
    for i in range(3):
        c.execute("INSERT INTO status VALUES (?,?)", (i, f"s{i}"))
    # order_events: a SECOND fact table also carrying status_id that repeats (no key side).
    # orders.status_id <-> order_events.status_id is the true low-cardinality trap (fact<->fact,
    # domain 3, neither is a unique key).
    c.execute("CREATE TABLE order_events (event_id INTEGER, status_id INTEGER)")
    for i in range(150):
        c.execute("INSERT INTO order_events VALUES (?,?)", (i, i % 3))
    # tag_map: bare `id` PK, table name has a generic "_map" suffix (exactly the shape of the
    # real interest_map table). products.tag_id references it: the FK column's stem ("tag")
    # shares NO token with the bare id column's own name ("id") — same-stem matching alone would
    # never pair these — but DOES share a token with "tag_map" once the generic "_map" suffix is
    # stripped. This is the exact shape of the real interest_metrics.interest_id <->
    # interest_map.id miss found by running discover_relationships live.
    c.execute("CREATE TABLE tag_map (id INTEGER, label TEXT)")
    for i in range(20):
        c.execute("INSERT INTO tag_map VALUES (?,?)", (i, f"tag{i}"))
    c.execute("CREATE TABLE products (product_id INTEGER, tag_id INTEGER)")
    for i in range(80):
        c.execute("INSERT INTO products VALUES (?,?)", (i, i % 20))
    # accounts / accounts_archive: BOTH sides are genuine unique keys (tied distinct_ratio=1.0)
    # of DIFFERENT sizes, archive being a strict subset of the current table — mirrors the real
    # shipment/shipment_legacy shape found live on d4_fixture. Exercises the tie-break rule.
    c.execute("CREATE TABLE accounts (account_id INTEGER, name TEXT)")
    for i in range(30):
        c.execute("INSERT INTO accounts VALUES (?,?)", (i, f"acct{i}"))
    c.execute("CREATE TABLE accounts_archive (account_id INTEGER, name TEXT)")
    for i in range(15):  # first 15 ids only — a strict subset of accounts' 30
        c.execute("INSERT INTO accounts_archive VALUES (?,?)", (i, f"acct{i}"))
    # match_results (raw, episodic) / season_standings (cumulative-with-reset) — mirrors the
    # real f1 results/driver_standings shape: BOTH share player_id (entity) AND match_id
    # (sequence), plus a same-named numeric measure ("points"). match_results.points is
    # per-event and does NOT trend upward; season_standings.points is the running season total,
    # resetting once at match 11 (a "new season" boundary) — the D5 signature to detect.
    # `position` is a coincidental shared column name, not a real measure — mirrors the real
    # f1 case (lap_times/lap_times_ext and several other unrelated pairs all coincidentally
    # share a `position` column, meaning something different in each: track position vs
    # championship position). It must be excluded from measure candidacy so it can't produce
    # a false rollup signal, regardless of its actual value pattern.
    c.execute("CREATE TABLE match_results (player_id INTEGER, match_id INTEGER, points INTEGER, position INTEGER)")
    c.execute("CREATE TABLE season_standings (player_id INTEGER, match_id INTEGER, points INTEGER, position INTEGER)")
    # Episodic values: a majority-DECREASING sawtooth (9,7,6,5 repeating) bounded to a <2x
    # range so it never trips the reset-tolerance exception, giving a clearly low monotonic
    # score (~0.21). Two earlier attempts were rejected after computing their actual scores:
    # an ascending modulo sawtooth looked "cumulative" (its drops got excused as resets), and
    # a symmetric alternating high/low series landed right on the 0.5 threshold boundary
    # (~0.47) — too fragile. This one and its cumulative-with-one-reset counterpart (score 1.0)
    # give a robust, clearly-separated pair.
    episodic_pattern = ([9, 7, 6, 5] * 5)[:20]
    for player in range(3):
        cum = 0
        for match in range(1, 21):
            pts = episodic_pattern[match - 1]
            # position is deliberately monotonically increasing here (1..20) — if the
            # exclusion filter didn't work, this would ALSO trip a rollup signal, so the test
            # is a genuine check of the filter, not a coincidence of the sample data.
            c.execute("INSERT INTO match_results VALUES (?,?,?,?)", (player, match, pts, match))
            if match == 11:
                cum = 0  # season boundary reset
            cum += pts
            c.execute("INSERT INTO season_standings VALUES (?,?,?,?)", (player, match, cum, match))
    # PascalCase, no-underscore naming (mirrors Chinook / any .NET/Entity-Framework-style
    # schema) — found live: TrackId/AlbumId-style FK names produced ZERO candidate pairs on
    # the entire Chinook database, silently, because the original regexes only recognized
    # snake_case (`_id` with an underscore). This is not a Chinook-specific quirk; PascalCase
    # is extremely common in real-world databases.
    c.execute("CREATE TABLE Album (AlbumId INTEGER, Title TEXT)")
    for i in range(10):
        c.execute("INSERT INTO Album VALUES (?,?)", (i, f"album{i}"))
    c.execute("CREATE TABLE Track (TrackId INTEGER, AlbumId INTEGER, Name TEXT)")
    for i in range(50):
        c.execute("INSERT INTO Track VALUES (?,?,?)", (i, i % 10, f"track{i}"))
    con.commit()
    con.close()

    ds = DataSource.objects.create(
        display_name='rel_sqlite', type='sqlite',
        connection_str=f'sqlite:///{path}', organisation=org, enabled=True,
    )
    sync_metadata(ds.id)
    yield ds
    os.remove(path)


def _edge(res, frm, to):
    for e in res["edges"]:
        if e["from"] == frm and e["to"] == to:
            return e
    return None


@pytest.mark.django_db
def test_reliable_key_on_small_table_is_not_a_trap(sqlite_datasource):
    res = discover_relationships(sqlite_datasource.id)
    assert res["errors"] == 0

    # orders.customer_id (child, repeats) -> customers.customer_id (parent key, 40/40 rows).
    # 40 distinct is < 50, but the parent IS a unique key -> many-to-one, NOT a trap.
    edge = _edge(res, "orders.customer_id", "customers.customer_id")
    assert edge is not None, res["edges"]
    assert edge["verdict"] == MeasuredRelationship.Verdict.RELIABLE
    assert edge["cardinality"] == MeasuredRelationship.Cardinality.MANY_TO_ONE
    assert edge["orphan_count"] == 0

    # orders.status_id -> status.status_id: tiny domain (3) BUT status_id is a real key on
    # status -> legitimate many-to-one lookup, still reliable, still not a trap.
    lookup = _edge(res, "orders.status_id", "status.status_id")
    assert lookup is not None, res["edges"]
    assert lookup["verdict"] == MeasuredRelationship.Verdict.RELIABLE


@pytest.mark.django_db
def test_bare_id_matches_via_table_name_token(sqlite_datasource):
    """products.tag_id <-> tag_map.id: no shared column-name stem with "id" itself, only
    discoverable via bounded bare-id matching against the target table's own name ("tag_map"
    minus the generic "_map" suffix -> "tag"). This is the exact miss found when
    discover_relationships ran live and silently skipped interest_id <-> id — and the fix must
    stay BOUNDED (table-name-token match), not "pair bare id with every other table's key
    column", which a live run showed creates massive combinatorial noise at schema scale."""
    res = discover_relationships(sqlite_datasource.id)
    edge = _edge(res, "products.tag_id", "tag_map.id")
    assert edge is not None, res["edges"]
    assert edge["verdict"] == MeasuredRelationship.Verdict.RELIABLE
    assert edge["cardinality"] == MeasuredRelationship.Cardinality.MANY_TO_ONE
    assert edge["orphan_count"] == 0

    # bounded: bare id must NOT be paired against wholly unrelated tables' key columns
    # (e.g. customers.customer_id shares no token with "tag_map")
    unrelated = _edge(res, "customers.customer_id", "tag_map.id") or _edge(res, "tag_map.id", "customers.customer_id")
    assert unrelated is None, "bare-id matching must be bounded to table-name-token overlap"


@pytest.mark.django_db
def test_tied_ratio_containment_reports_smaller_as_child(sqlite_datasource):
    """When BOTH sides are genuine unique keys (tied distinct_ratio ~ 1.0) but of different
    sizes, the SMALLER side (the subset) must be reported as the child, so the edge reads as
    the informative containment fact: "100% of accounts_archive is present in accounts."

    Found live on d4_fixture: `-a_distinct <= -b_distinct` is mathematically equivalent to
    `a_distinct >= b_distinct`, the OPPOSITE of the intended tie-break, so it picked the
    LARGER table as child and reported a misleading 0.6 partial overlap for
    shipment/shipment_legacy instead of the true 1.0 reliable containment. Also asserts
    domain_size and overlap_ratio are computed on the SAME basis (both must reflect the
    child side's distinct count) — the two were found to diverge under the buggy tie-break.
    """
    res = discover_relationships(sqlite_datasource.id)
    edge = _edge(res, "accounts_archive.account_id", "accounts.account_id")
    assert edge is not None, res["edges"]
    assert edge["verdict"] == MeasuredRelationship.Verdict.RELIABLE
    assert edge["overlap_ratio"] == 1.0
    assert edge["orphan_count"] == 0
    assert edge["domain_size"] == 15  # the child (smaller/subset) side's distinct count

    # the reverse direction must NOT also be reported (each pair yields exactly one edge)
    reverse = _edge(res, "accounts.account_id", "accounts_archive.account_id")
    assert reverse is None


@pytest.mark.django_db
def test_direction_flip_updates_in_place_not_duplicated(sqlite_datasource):
    """If a row already exists in the WRONG direction (e.g. left over from a
    direction-selection bug fixed in a later run), the next run must UPDATE that row in place,
    not insert a second row alongside it.

    Found live on d4_fixture after fixing the tie-break bug: the old wrong-direction row
    (shipment -> shipment_legacy, stale) was left sitting next to the newly-inserted correct
    row (shipment_legacy -> shipment) instead of being replaced, because the upsert lookup
    matched on the exact (from_column, to_column) direction rather than the unordered pair.
    """
    from terno_dbi.core.models import TableColumn
    accounts_col = TableColumn.objects.get(table__name='accounts', name='account_id')
    archive_col = TableColumn.objects.get(table__name='accounts_archive', name='account_id')

    # manually seed a row in the WRONG direction, as if from a buggy prior run
    wrong = MeasuredRelationship.objects.create(
        data_source=sqlite_datasource, from_column=accounts_col, to_column=archive_col,
        overlap_ratio=0.6, smaller_domain_size=30, smaller_distinct_ratio=1.0,
        verdict=MeasuredRelationship.Verdict.PARTIAL,
        provenance=MeasuredRelationship.Provenance.MEASURED,
        confidence=MeasuredRelationship.Confidence.MEDIUM,
    )

    res = discover_relationships(sqlite_datasource.id)

    all_rows = MeasuredRelationship.objects.filter(
        from_column__in=[accounts_col, archive_col], to_column__in=[accounts_col, archive_col]
    )
    assert all_rows.count() == 1, "must update the existing row, not insert a duplicate"
    row = all_rows.first()
    assert row.id == wrong.id  # same row, updated in place
    assert row.from_column_id == archive_col.id and row.to_column_id == accounts_col.id
    assert row.verdict == MeasuredRelationship.Verdict.RELIABLE
    assert row.overlap_ratio == 1.0

    edge = _edge(res, "accounts_archive.account_id", "accounts.account_id")
    assert edge is not None
    assert edge["verdict"] == MeasuredRelationship.Verdict.RELIABLE


@pytest.mark.django_db
def test_existing_duplicate_pair_is_reconciled_not_errored(sqlite_datasource):
    """If BOTH directions of a pair already exist as separate rows (a duplicate left over from
    the direction-flip bug, before that bug's own fix was deployed), the next run must
    reconcile them to exactly one row rather than raising an IntegrityError trying to flip
    one row onto the (from_column, to_column) tuple the other one already owns.

    Found live: immediately after deploying the direction-flip fix, a datasource that had
    already accumulated the duplicate (one row per direction) hit exactly this error.
    """
    from terno_dbi.core.models import TableColumn
    accounts_col = TableColumn.objects.get(table__name='accounts', name='account_id')
    archive_col = TableColumn.objects.get(table__name='accounts_archive', name='account_id')

    wrong = MeasuredRelationship.objects.create(
        data_source=sqlite_datasource, from_column=accounts_col, to_column=archive_col,
        overlap_ratio=0.6, smaller_domain_size=30, smaller_distinct_ratio=1.0,
        verdict=MeasuredRelationship.Verdict.PARTIAL,
        provenance=MeasuredRelationship.Provenance.MEASURED,
        confidence=MeasuredRelationship.Confidence.MEDIUM,
    )
    correct = MeasuredRelationship.objects.create(
        data_source=sqlite_datasource, from_column=archive_col, to_column=accounts_col,
        overlap_ratio=1.0, smaller_domain_size=15, smaller_distinct_ratio=1.0,
        verdict=MeasuredRelationship.Verdict.RELIABLE,
        provenance=MeasuredRelationship.Provenance.MEASURED,
        confidence=MeasuredRelationship.Confidence.HIGH,
    )

    res = discover_relationships(sqlite_datasource.id)
    assert res["errors"] == 0

    all_rows = MeasuredRelationship.objects.filter(
        from_column__in=[accounts_col, archive_col], to_column__in=[accounts_col, archive_col]
    )
    assert all_rows.count() == 1
    row = all_rows.first()
    assert row.id == correct.id  # the already-correct-direction row is kept
    assert not MeasuredRelationship.objects.filter(id=wrong.id).exists()
    assert row.verdict == MeasuredRelationship.Verdict.RELIABLE


@pytest.mark.django_db
def test_duplicate_reconciliation_never_deletes_a_protected_row(sqlite_datasource):
    """Among duplicate rows for the same pair, a protected/human-asserted one must survive
    reconciliation even if it isn't in the freshly-computed direction."""
    from terno_dbi.core.models import TableColumn
    accounts_col = TableColumn.objects.get(table__name='accounts', name='account_id')
    archive_col = TableColumn.objects.get(table__name='accounts_archive', name='account_id')

    protected_wrong_direction = MeasuredRelationship.objects.create(
        data_source=sqlite_datasource, from_column=accounts_col, to_column=archive_col,
        overlap_ratio=0.6, smaller_domain_size=30, smaller_distinct_ratio=1.0,
        verdict=MeasuredRelationship.Verdict.PARTIAL,
        provenance=MeasuredRelationship.Provenance.HUMAN_ASSERTED,
        confidence=MeasuredRelationship.Confidence.MEDIUM,
        is_protected=True,
    )
    stale_measured = MeasuredRelationship.objects.create(
        data_source=sqlite_datasource, from_column=archive_col, to_column=accounts_col,
        overlap_ratio=0.5, smaller_domain_size=15, smaller_distinct_ratio=1.0,
        verdict=MeasuredRelationship.Verdict.PARTIAL,
        provenance=MeasuredRelationship.Provenance.MEASURED,
        confidence=MeasuredRelationship.Confidence.MEDIUM,
    )

    discover_relationships(sqlite_datasource.id)

    assert MeasuredRelationship.objects.filter(id=protected_wrong_direction.id).exists()


@pytest.mark.django_db
def test_d5_detects_rollup_pair_by_monotonicity_not_row_count(sqlite_datasource):
    """match_results (episodic points per event) vs season_standings (cumulative points,
    resetting once at a season boundary) share BOTH player_id and match_id, plus a same-named
    'points' measure. D5 must flag season_standings as the rollup side using the monotonicity
    signature — NOT row count, which is equal on both sides here (and was found LIVE on f1 to
    be an unreliable signal: driver_standings actually has MORE total rows than results)."""
    res = discover_relationships(sqlite_datasource.id)
    assert res["errors"] == 0
    assert len(res["rollup_signals"]) >= 1

    from terno_dbi.core.models import TableColumn
    player_a = TableColumn.objects.get(table__name='match_results', name='player_id')
    player_b = TableColumn.objects.get(table__name='season_standings', name='player_id')
    row = MeasuredRelationship.objects.filter(
        Q(from_column=player_a, to_column=player_b) | Q(from_column=player_b, to_column=player_a)
    ).first()
    assert row is not None
    assert row.rollup_signal in (
        MeasuredRelationship.RollupSignal.TO_IS_ROLLUP,
        MeasuredRelationship.RollupSignal.FROM_IS_ROLLUP,
    )
    rollup_col = (
        row.to_column if row.rollup_signal == MeasuredRelationship.RollupSignal.TO_IS_ROLLUP
        else row.from_column
    )
    assert rollup_col.table.name == 'season_standings'
    assert row.rollup_evidence  # evidence text is populated, not just the enum
    # rank/ordinal-shaped column names (position, rank, number, lap, ...) must NEVER be treated
    # as a measure, even if their values happen to be monotonic — found live on f1, where a
    # coincidentally-shared `position` column produced noisy false rollup signals across
    # semantically unrelated table pairs.
    assert not any('position' in s["measure"] for s in res["rollup_signals"])


@pytest.mark.django_db
def test_pascal_case_fk_naming_is_detected(sqlite_datasource):
    """Track.AlbumId <-> Album.AlbumId (PascalCase, no underscores) must be found as a
    candidate pair and measured as reliable, exactly like snake_case customer_id would be.

    Found live on the real Chinook database: EVERY column matching this convention
    (TrackId, AlbumId, GenreId, MediaTypeId, ...) was invisible to the original
    _KEY_NAME_RE/_name_token logic, which only recognized an underscore before "id" — the
    entire schema silently produced ZERO candidate pairs, with no error at all.
    """
    res = discover_relationships(sqlite_datasource.id)
    assert res["errors"] == 0
    edge = _edge(res, "Track.AlbumId", "Album.AlbumId")
    assert edge is not None, res["edges"]
    assert edge["verdict"] == MeasuredRelationship.Verdict.RELIABLE
    assert edge["cardinality"] == MeasuredRelationship.Cardinality.MANY_TO_ONE
    assert edge["orphan_count"] == 0


@pytest.mark.django_db
def test_fact_to_fact_low_cardinality_pair_is_trap(sqlite_datasource):
    discover_relationships(sqlite_datasource.id)
    # orders.status_id <-> order_events.status_id: domain 3, BOTH sides repeat (no key) -> trap.
    trap = MeasuredRelationship.objects.filter(
        verdict=MeasuredRelationship.Verdict.LOW_CARDINALITY_TRAP
    )
    assert trap.exists()
    pairs = {frozenset((r.from_column.table.name, r.to_column.table.name)) for r in trap}
    assert frozenset(('orders', 'order_events')) in pairs


@pytest.mark.django_db
def test_idempotent_second_run(sqlite_datasource):
    first = discover_relationships(sqlite_datasource.id)
    assert first["edges_written"] > 0
    second = discover_relationships(sqlite_datasource.id)
    # nothing changed in the data => no edges rewritten, all confirmed unchanged
    assert second["edges_written"] == 0
    assert second["edges_unchanged"] == first["edges_written"]


@pytest.mark.django_db
def test_stale_edges_are_pruned_when_no_longer_a_candidate(sqlite_datasource):
    """A row whose column pair is no longer generated as a candidate at all (e.g. left over
    from a prior, since-corrected candidate-generation rule) must be pruned on the next run —
    found live: an earlier fix that paired bare `id` against every other table's key column
    wrote 20+ junk edges between unrelated tables; those rows would sit in the store forever
    without this prune step, since upsert alone only ever touches THIS run's candidates."""
    discover_relationships(sqlite_datasource.id)

    # a pair that is genuinely never a candidate under either rule: neither column is key-like
    # (customers.name is a plain text column, status.label likewise) and there's no table-name
    # token overlap either — simulating a leftover row from a prior candidate-generation rule.
    from terno_dbi.core.models import TableColumn
    unrelated_col_a = TableColumn.objects.get(table__name='customers', name='name')
    unrelated_col_b = TableColumn.objects.get(table__name='status', name='label')
    junk = MeasuredRelationship.objects.create(
        data_source=sqlite_datasource, from_column=unrelated_col_a, to_column=unrelated_col_b,
        overlap_ratio=0.0, smaller_domain_size=3, smaller_distinct_ratio=1.0,
        verdict=MeasuredRelationship.Verdict.SUSPICIOUS,
        provenance=MeasuredRelationship.Provenance.MEASURED,
        confidence=MeasuredRelationship.Confidence.LOW,
    )

    res = discover_relationships(sqlite_datasource.id)
    assert res["edges_pruned"] >= 1
    assert not MeasuredRelationship.objects.filter(id=junk.id).exists()


@pytest.mark.django_db
def test_protected_stale_edge_is_never_pruned(sqlite_datasource):
    """A human-asserted/protected edge is never auto-pruned, even if it's no longer a
    candidate under the current rules — a human said this join matters."""
    discover_relationships(sqlite_datasource.id)
    from terno_dbi.core.models import TableColumn
    unrelated_col_a = TableColumn.objects.get(table__name='customers', name='name')
    unrelated_col_b = TableColumn.objects.get(table__name='status', name='label')
    protected = MeasuredRelationship.objects.create(
        data_source=sqlite_datasource, from_column=unrelated_col_a, to_column=unrelated_col_b,
        overlap_ratio=0.0, smaller_domain_size=3, smaller_distinct_ratio=1.0,
        verdict=MeasuredRelationship.Verdict.SUSPICIOUS,
        provenance=MeasuredRelationship.Provenance.HUMAN_ASSERTED,
        confidence=MeasuredRelationship.Confidence.LOW,
        is_protected=True,
    )

    discover_relationships(sqlite_datasource.id)
    assert MeasuredRelationship.objects.filter(id=protected.id).exists()


@pytest.mark.django_db
def test_protected_edge_not_overwritten(sqlite_datasource):
    discover_relationships(sqlite_datasource.id)
    # filter specifically for the orders/order_events status_id trap — the fixture now also
    # produces a separate match_id low_cardinality_trap edge (from the D5 fixture tables), so
    # a bare .first() on the verdict alone is no longer guaranteed to pick this one.
    edge = MeasuredRelationship.objects.filter(
        verdict=MeasuredRelationship.Verdict.LOW_CARDINALITY_TRAP,
        from_column__table__name__in=['orders', 'order_events'],
        to_column__table__name__in=['orders', 'order_events'],
    ).first()
    # a human overrides the verdict and protects it
    edge.verdict = MeasuredRelationship.Verdict.RELIABLE
    edge.is_protected = True
    edge.provenance = MeasuredRelationship.Provenance.HUMAN_ASSERTED
    edge.save()

    res = discover_relationships(sqlite_datasource.id)
    edge.refresh_from_db()
    # the protected human verdict survives; the contradiction is reported, not applied
    assert edge.verdict == MeasuredRelationship.Verdict.RELIABLE
    assert any('status_id' in c["edge"] for c in res["protected_conflicts"])
