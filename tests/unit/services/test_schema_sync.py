
import pytest
import sqlite3
import os
from terno_dbi.core.models import DataSource, Table, TableColumn
from terno_dbi.services.schema_utils import sync_metadata


@pytest.fixture
def sqlite_datasource(tmp_path):
    """Creates a temporary SQLite database and a corresponding DataSource model."""
    db_path = tmp_path / "test_db.sqlite"
    conn_str = f"sqlite:///{db_path}"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    cursor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
    conn.commit()
    conn.close()

    ds = DataSource.objects.create(
        display_name="Test SQLite",
        type="sqlite",
        connection_str=conn_str,
        enabled=True
    )
    return ds, db_path


@pytest.mark.django_db
def test_sync_new_tables_and_columns(sqlite_datasource):
    """Scenario 1: New Tables or Columns Added"""
    ds, db_path = sqlite_datasource

    sync_metadata(ds.id)

    assert Table.objects.filter(data_source=ds, name="users").exists()
    table = Table.objects.get(data_source=ds, name="users")
    assert TableColumn.objects.filter(table=table, name="name").exists()

    # Modify DB: Add new table and column
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, title TEXT)")
    cursor.execute("ALTER TABLE users ADD COLUMN age INTEGER")
    conn.commit()
    conn.close()

    sync_metadata(ds.id)

    # Verify new table detected
    assert Table.objects.filter(data_source=ds, name="products").exists()

    # Verify new column detected
    assert TableColumn.objects.filter(table=table, name="age").exists()


@pytest.mark.django_db
def test_sync_renames(sqlite_datasource):
    """Scenario 2: Physical Table/Column Renamed in Database"""
    ds, db_path = sqlite_datasource
    sync_metadata(ds.id)

    # Set a description to verify it gets "lost" (stays with old table)
    table = Table.objects.get(data_source=ds, name="users")
    table.description = "User data"
    table.save()

    # Rename table in DB
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("ALTER TABLE users RENAME TO customers")
    conn.commit()
    conn.close()

    sync_metadata(ds.id)

    # With Smart Deletion, the old table should be DELETED
    assert not Table.objects.filter(data_source=ds, name="users").exists()

    # And the new table should be CREATED
    new_table = Table.objects.get(data_source=ds, name="customers")
    assert new_table is not None
    # Note: Description is lost because it was attached to the deleted "users" table
    assert new_table.description is None or new_table.description == ""


@pytest.mark.django_db
def test_sync_deletions(sqlite_datasource):
    """Scenario 3: Table or Column Deleted from Database"""
    ds, db_path = sqlite_datasource
    sync_metadata(ds.id)

    # Delete table in DB
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE users")
    conn.commit()
    conn.close()

    sync_metadata(ds.id)

    # Table metadata should be DELETED now
    assert not Table.objects.filter(data_source=ds, name="users").exists()


@pytest.mark.django_db
def test_sync_data_changes(sqlite_datasource):
    """Scenario 4: Rows Added/Modified (Data Changes)"""
    ds, db_path = sqlite_datasource
    sync_metadata(ds.id)

    table = Table.objects.get(data_source=ds, name="users")
    initial_desc = table.description

    # Add rows
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
    conn.commit()
    conn.close()

    sync_metadata(ds.id)

    # Metadata should remain unchanged
    table.refresh_from_db()
    assert table.description == initial_desc


@pytest.mark.django_db
def test_sync_preserves_descriptions(sqlite_datasource):
    """Scenario 5: Existing Descriptions & Metadata"""
    ds, db_path = sqlite_datasource
    sync_metadata(ds.id)

    # Add descriptions
    table = Table.objects.get(data_source=ds, name="users")
    table.description = "Important user table"
    table.save()

    col = TableColumn.objects.get(table=table, name="name")
    col.description = "User's full name"
    col.save()

    # Sync again (even with overwrite=True for types)
    sync_metadata(ds.id, overwrite=True)

    table.refresh_from_db()
    col.refresh_from_db()

    assert table.description == "Important user table"
    assert col.description == "User's full name"


@pytest.mark.django_db
def test_sync_creates_revisions(sqlite_datasource):
    """Scenario 6: Verify Reversion History is Created"""
    from reversion.models import Version

    ds, db_path = sqlite_datasource

    sync_metadata(ds.id)

    table = Table.objects.get(data_source=ds, name="users")

    # Verify version created for table
    versions = Version.objects.get_for_object(table)
    assert len(versions) > 0
    assert versions[0].revision.comment == "Automatic Metadata Sync"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("ALTER TABLE users ADD COLUMN email TEXT")
    conn.commit()
    conn.close()

    sync_metadata(ds.id)

    versions = Version.objects.get_for_object(table)

    col = TableColumn.objects.get(table=table, name="email")
    col_versions = Version.objects.get_for_object(col)
    assert len(col_versions) > 0
    assert col_versions[0].revision.comment == "Automatic Metadata Sync"
