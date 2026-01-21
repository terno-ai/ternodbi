import time
import os
import sys
from sqlalchemy import create_engine, text
import django

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'server'))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dbi_server.settings")
django.setup()

from terno_dbi.services.pagination import (
    PaginationConfig,
    PaginationMode,
    OrderColumn,
    create_pagination_service
)

DB_PATH = "benchmark.db"
NUM_ROWS = 100_000
PAGE_SIZE = 100


class SimpleConnector:
    def __init__(self, engine):
        self.engine = engine

    def get_connection(self):
        return self.engine.connect()


def setup_database():
    """Create DB and populate with data."""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    print(f"Creating database with {NUM_ROWS:,} rows...")
    engine = create_engine(f"sqlite:///{DB_PATH}")

    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, created_at TEXT)"))
        conn.commit()

        batch_size = 10000
        for i in range(0, NUM_ROWS, batch_size):
            params = [
                {"id": x, "name": f"item_{x}", "created_at": "2024-01-01"} 
                for x in range(i, i + batch_size)
            ]
            conn.execute(
                text("INSERT INTO items (id, name, created_at) VALUES (:id, :name, :created_at)"),
                params
            )
            conn.commit()
            sys.stdout.write(f"\r   Inserted {i + batch_size:,} rosw")
            sys.stdout.flush()

    print("Database ready.")
    return engine


def run_benchmark(engine):
    connector = SimpleConnector(engine)
    service = create_pagination_service(connector, "sqlite", "bench-secret")

    depths = [
        1,
        NUM_ROWS // 10,
        NUM_ROWS // 2,
        NUM_ROWS - 100
    ]

    results = []

    print("Running Benchmarks...")
    print(f"{'Depth (Rows)':<15} | {'Mode':<10} | {'Time (ms)':<10} | {'Speedup':<10}")
    print("-" * 55)

    for row_offset in depths:
        page_num = (row_offset // PAGE_SIZE) + 1

        # OFFSET Pagination
        config_offset = PaginationConfig(
            mode=PaginationMode.OFFSET,
            page=page_num,
            per_page=PAGE_SIZE
        )

        start = time.perf_counter()
        service.paginate("SELECT * FROM items ORDER BY id ASC", config_offset)
        dur_offset = (time.perf_counter() - start) * 1000

        # CURSOR Pagination
        with engine.connect() as conn:
            target_id = row_offset
            cursor = None
            if target_id > 0:
                prev_id = target_id - 1
                row = conn.execute(text(f"SELECT id, name, created_at FROM items WHERE id = {prev_id}")).fetchone()
                if row:
                    vals = {"id": row[0]}
                    cursor = service.cursor_codec.encode(vals, [OrderColumn("id", "ASC")])

        config_cursor = PaginationConfig(
            mode=PaginationMode.CURSOR,
            cursor=cursor,
            per_page=PAGE_SIZE,
            order_by=[OrderColumn("id", "ASC")]
        )

        start = time.perf_counter()
        service.paginate("SELECT * FROM items ORDER BY id ASC", config_cursor)
        dur_cursor = (time.perf_counter() - start) * 1000

        speedup = dur_offset / dur_cursor if dur_cursor > 0 else 0

        print(f"{row_offset:<15,} | Offset     | {dur_offset:6.2f} ms | 1.0x")
        print(f"{'':<15} | Cursor     | {dur_cursor:6.2f} ms | {speedup:.1f}x")
        print("-" * 55)

        results.append({
            "depth": row_offset,
            "offset_ms": dur_offset,
            "cursor_ms": dur_cursor
        })

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)


if __name__ == "__main__":
    eng = setup_database()
    run_benchmark(eng)
