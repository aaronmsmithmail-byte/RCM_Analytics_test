"""Unit tests for src/backlog_page.py — CRUD operations on feature_backlog table.

Tests the database helper functions (not the Streamlit UI rendering).
"""

import duckdb
import pytest

from src.database import _seed_backlog_examples, create_tables


@pytest.fixture
def db(tmp_path):
    """Temporary DuckDB database with feature_backlog table and seed data."""
    db_path = str(tmp_path / "test.db")
    conn = duckdb.connect(db_path)
    create_tables(conn)
    _seed_backlog_examples(conn)
    conn.close()
    return db_path


@pytest.fixture
def empty_db(tmp_path):
    """Temporary DuckDB database with feature_backlog table but no data."""
    db_path = str(tmp_path / "test.db")
    conn = duckdb.connect(db_path)
    create_tables(conn)
    conn.close()
    return db_path


class TestLoadBacklog:
    def test_returns_dataframe_with_seed_data(self, db, monkeypatch):
        monkeypatch.setattr("src.database.DB_PATH", db)
        from src.backlog_page import _load_backlog
        df = _load_backlog()
        assert len(df) == 3  # 3 seed examples

    def test_returns_empty_dataframe_when_no_data(self, empty_db, monkeypatch):
        monkeypatch.setattr("src.database.DB_PATH", empty_db)
        from src.backlog_page import _load_backlog
        df = _load_backlog()
        assert df.empty

    def test_ordered_by_priority(self, db, monkeypatch):
        monkeypatch.setattr("src.database.DB_PATH", db)
        from src.backlog_page import _load_backlog
        df = _load_backlog()
        priorities = df["priority"].tolist()
        # Critical/High should come before Medium/Low
        priority_rank = {"Critical": 1, "High": 2, "Medium": 3, "Low": 4}
        ranks = [priority_rank.get(p, 5) for p in priorities]
        assert ranks == sorted(ranks)


class TestInsertItem:
    def test_inserts_new_item(self, empty_db, monkeypatch):
        monkeypatch.setattr("src.database.DB_PATH", empty_db)
        from src.backlog_page import _insert_item, _load_backlog
        _insert_item("Test Feature", "A test description", "High", "Must work", "Better UX")
        df = _load_backlog()
        assert len(df) == 1
        assert df.iloc[0]["title"] == "Test Feature"
        assert df.iloc[0]["priority"] == "High"
        assert df.iloc[0]["status"] == "Not Started"

    def test_inserts_multiple_items(self, empty_db, monkeypatch):
        monkeypatch.setattr("src.database.DB_PATH", empty_db)
        from src.backlog_page import _insert_item, _load_backlog
        _insert_item("Feature 1", "Desc 1", "High", "", "")
        _insert_item("Feature 2", "Desc 2", "Low", "", "")
        df = _load_backlog()
        assert len(df) == 2


class TestUpdateStatus:
    def test_updates_status(self, db, monkeypatch):
        monkeypatch.setattr("src.database.DB_PATH", db)
        from src.backlog_page import _load_backlog, _update_status
        df = _load_backlog()
        first_id = int(df.iloc[0]["id"])
        _update_status(first_id, "In Progress")
        df2 = _load_backlog()
        updated_row = df2[df2["id"] == first_id].iloc[0]
        assert updated_row["status"] == "In Progress"


class TestDeleteItem:
    def test_deletes_item(self, db, monkeypatch):
        monkeypatch.setattr("src.database.DB_PATH", db)
        from src.backlog_page import _delete_item, _load_backlog
        df = _load_backlog()
        first_id = int(df.iloc[0]["id"])
        original_count = len(df)
        _delete_item(first_id)
        df2 = _load_backlog()
        assert len(df2) == original_count - 1
        assert first_id not in df2["id"].values


class TestSeedBacklogExamples:
    def test_seeds_three_examples(self, empty_db):
        conn = duckdb.connect(empty_db)
        _seed_backlog_examples(conn)
        count = conn.execute("SELECT COUNT(*) FROM feature_backlog").fetchone()[0]
        conn.close()
        assert count == 3

    def test_idempotent_does_not_duplicate(self, empty_db):
        conn = duckdb.connect(empty_db)
        _seed_backlog_examples(conn)
        _seed_backlog_examples(conn)
        count = conn.execute("SELECT COUNT(*) FROM feature_backlog").fetchone()[0]
        conn.close()
        assert count == 3
