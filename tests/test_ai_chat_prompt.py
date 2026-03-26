"""Unit tests for build_system_prompt() and _get_meta_context() in src/ai_chat.py.

Covers meta-table reading, fallback behaviour when tables are empty,
live KPI snapshot injection, and system prompt structure.
"""

import duckdb
import pytest

from src.ai_chat import _get_meta_context, build_system_prompt
from src.database import create_tables, persist_metadata

# ===========================================================================
# Shared fixtures
# ===========================================================================

@pytest.fixture
def db_with_meta(tmp_path):
    """Database with schema + metadata tables populated."""
    db_path = str(tmp_path / "test.db")
    conn = duckdb.connect(db_path)
    create_tables(conn)
    persist_metadata(conn)
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def db_empty(tmp_path):
    """Database with schema only — no metadata rows."""
    db_path = str(tmp_path / "empty.db")
    conn = duckdb.connect(db_path)
    create_tables(conn)
    conn.commit()
    conn.close()
    return db_path


# ===========================================================================
# _get_meta_context
# ===========================================================================

class TestGetMetaContext:
    def test_returns_string(self, db_with_meta):
        result = _get_meta_context(db_path=db_with_meta)
        assert isinstance(result, str)

    def test_contains_table_names(self, db_with_meta):
        result = _get_meta_context(db_path=db_with_meta)
        assert "silver_claims" in result
        assert "silver_payments" in result

    def test_contains_join_paths(self, db_with_meta):
        result = _get_meta_context(db_path=db_with_meta)
        # Should mention FK relationships from meta_kg_edges
        assert "claim_id" in result or "encounter_id" in result

    def test_contains_kpi_definitions(self, db_with_meta):
        result = _get_meta_context(db_path=db_with_meta)
        # meta_kpi_catalog should have KPI names
        assert "Days in A/R" in result or "Collection Rate" in result or "Denial Rate" in result

    def test_empty_meta_falls_back_to_sqlite_master(self, db_empty):
        result = _get_meta_context(db_path=db_empty)
        # Should still return a string with silver table names from sqlite_master
        assert isinstance(result, str)
        assert "silver_claims" in result

    def test_non_empty_result(self, db_with_meta):
        result = _get_meta_context(db_path=db_with_meta)
        assert len(result) > 100  # Should have substantial content


# ===========================================================================
# build_system_prompt
# ===========================================================================

class TestBuildSystemPrompt:
    def test_returns_string(self, db_with_meta):
        result = build_system_prompt(db_path=db_with_meta)
        assert isinstance(result, str)

    def test_contains_role_description(self, db_with_meta):
        result = build_system_prompt(db_path=db_with_meta)
        assert "RCM" in result or "Revenue Cycle" in result

    def test_contains_meta_context(self, db_with_meta):
        result = build_system_prompt(db_path=db_with_meta)
        # Meta context should be embedded
        assert "silver_claims" in result

    def test_contains_benchmarks(self, db_with_meta):
        result = build_system_prompt(db_path=db_with_meta)
        assert "DAR" in result or "NCR" in result

    def test_live_kpis_included(self, db_with_meta):
        kpis = {"Days in A/R": "32.5 days", "Net Collection Rate": "96.2%"}
        result = build_system_prompt(live_kpis=kpis, db_path=db_with_meta)
        assert "32.5 days" in result
        assert "96.2%" in result
        assert "Live KPI Snapshot" in result

    def test_no_live_kpis_no_snapshot(self, db_with_meta):
        result = build_system_prompt(db_path=db_with_meta)
        assert "Live KPI Snapshot" not in result

    def test_empty_live_kpis_no_snapshot(self, db_with_meta):
        result = build_system_prompt(live_kpis={}, db_path=db_with_meta)
        assert "Live KPI Snapshot" not in result

    def test_works_with_empty_meta(self, db_empty):
        # Should still produce a usable prompt even without meta tables populated
        result = build_system_prompt(db_path=db_empty)
        assert isinstance(result, str)
        assert "run_sql" in result or "query" in result.lower()

    def test_contains_tool_usage_guidelines(self, db_with_meta):
        result = build_system_prompt(db_path=db_with_meta)
        assert "GROUP BY" in result or "aggregation" in result.lower()
