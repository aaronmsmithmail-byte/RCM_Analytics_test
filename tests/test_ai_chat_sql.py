"""Unit tests for execute_sql_tool() and _format_result_for_llm() in src/ai_chat.py.

Covers SQL validation (read-only enforcement), row truncation, NaN handling,
error paths, and the LLM result formatter.
"""

import duckdb
import math
import pytest

from src.database import create_tables
from src.ai_chat import execute_sql_tool, _format_result_for_llm


# ===========================================================================
# Shared fixtures
# ===========================================================================

@pytest.fixture
def db(tmp_path):
    """Temporary SQLite database with a few Silver-layer rows."""
    db_path = str(tmp_path / "test.db")
    conn = duckdb.connect(db_path)
    create_tables(conn)
    conn.execute("""
        INSERT INTO silver_payers VALUES
            ('PYR001','Aetna','Commercial',0.85,'C001'),
            ('PYR002','Medicaid','Government',0.70,'G001');
        INSERT INTO silver_patients VALUES
            ('PAT001','Alice','Smith','1980-01-01','F','PYR001','M001','10001'),
            ('PAT002','Bob','Jones','1975-05-15','M','PYR001','M002','10002'),
            ('PAT003','Carol','Lee','1990-03-20','F','PYR002','M003','10003');
        INSERT INTO silver_providers VALUES
            ('PRV001','Dr. A','1111111111','Cardiology','Internal Medicine');
        INSERT INTO silver_encounters VALUES
            ('ENC010','PAT001','PRV001','2024-01-15','2024-01-15','Outpatient','Cardiology'),
            ('ENC020','PAT002','PRV001','2024-01-20','2024-01-20','Outpatient','Cardiology'),
            ('ENC030','PAT003','PRV001','2024-02-10','2024-02-11','Inpatient','Cardiology');
        INSERT INTO silver_claims VALUES
            ('CLM001','ENC010','PAT001','PYR001','2024-01-15','2024-01-17',1000.0,'Paid',1,'Electronic',NULL),
            ('CLM002','ENC020','PAT002','PYR001','2024-01-20','2024-01-22',2000.0,'Denied',0,'Electronic','CODING_ERROR'),
            ('CLM003','ENC030','PAT003','PYR002','2024-02-10','2024-02-12',1500.0,'Paid',1,'Electronic',NULL);
    """)
    conn.commit()
    conn.close()
    return db_path


# ===========================================================================
# SQL validation — read-only enforcement
# ===========================================================================

class TestExecuteSqlToolValidation:
    """Only SELECT and WITH queries should be allowed."""

    def test_select_query_succeeds(self, db):
        result = execute_sql_tool("SELECT COUNT(*) AS n FROM silver_claims", db_path=db)
        assert "error" not in result
        assert result["rows"][0][0] == 3

    def test_with_cte_query_succeeds(self, db):
        sql = "WITH x AS (SELECT * FROM silver_claims) SELECT COUNT(*) AS n FROM x"
        result = execute_sql_tool(sql, db_path=db)
        assert "error" not in result
        assert result["rows"][0][0] == 3

    def test_insert_rejected(self, db):
        result = execute_sql_tool(
            "INSERT INTO silver_payers VALUES ('X','X','X',0,'X')", db_path=db
        )
        assert "error" in result
        assert "SELECT" in result["error"]

    def test_update_rejected(self, db):
        result = execute_sql_tool(
            "UPDATE silver_claims SET billed_amount=0", db_path=db
        )
        assert "error" in result

    def test_delete_rejected(self, db):
        result = execute_sql_tool(
            "DELETE FROM silver_claims", db_path=db
        )
        assert "error" in result

    def test_drop_rejected(self, db):
        result = execute_sql_tool(
            "DROP TABLE silver_claims", db_path=db
        )
        assert "error" in result

    def test_case_insensitive_select(self, db):
        result = execute_sql_tool("select count(*) as n from silver_claims", db_path=db)
        assert "error" not in result

    def test_leading_whitespace_allowed(self, db):
        result = execute_sql_tool("   SELECT 1 AS x", db_path=db)
        assert "error" not in result
        assert result["rows"][0][0] == 1

    def test_leading_paren_select_allowed(self, db):
        result = execute_sql_tool("(SELECT 1 AS x)", db_path=db)
        # The safety check strips leading '(' — query may still fail at DB
        # level but should NOT be rejected by the validation gate
        assert result.get("error", "") != "Only SELECT and WITH (CTE) queries are permitted."

    def test_empty_query_rejected(self, db):
        result = execute_sql_tool("", db_path=db)
        assert "error" in result


# ===========================================================================
# Result shape and row truncation
# ===========================================================================

class TestExecuteSqlToolResults:
    """Verify result dict structure, truncation, and NaN handling."""

    def test_result_has_required_keys(self, db):
        result = execute_sql_tool("SELECT claim_id FROM silver_claims", db_path=db)
        for key in ("columns", "rows", "row_count", "total_rows", "truncated"):
            assert key in result

    def test_columns_match_query(self, db):
        result = execute_sql_tool(
            "SELECT claim_id, total_charge_amount FROM silver_claims", db_path=db
        )
        assert result["columns"] == ["claim_id", "total_charge_amount"]

    def test_row_count_matches_rows(self, db):
        result = execute_sql_tool("SELECT * FROM silver_claims", db_path=db)
        assert result["row_count"] == len(result["rows"])
        assert result["total_rows"] == 3
        assert result["truncated"] is False

    def test_nan_converted_to_none(self, db):
        # fail_reason is NULL for CLM001 — read_sql_query returns NaN for text NULLs
        result = execute_sql_tool(
            "SELECT fail_reason FROM silver_claims WHERE claim_id='CLM001'",
            db_path=db,
        )
        val = result["rows"][0][0]
        assert val is None  # NaN should be converted to None

    def test_malformed_sql_returns_error(self, db):
        result = execute_sql_tool("SELECT FROM WHERE", db_path=db)
        assert "error" in result

    def test_nonexistent_table_returns_error(self, db):
        result = execute_sql_tool("SELECT * FROM no_such_table", db_path=db)
        assert "error" in result

    def test_empty_result_set(self, db):
        result = execute_sql_tool(
            "SELECT * FROM silver_claims WHERE 1=0", db_path=db
        )
        assert result["row_count"] == 0
        assert result["rows"] == []
        assert result["truncated"] is False


# ===========================================================================
# _format_result_for_llm
# ===========================================================================

class TestFormatResultForLlm:
    """Verify the CSV-style formatting sent back to the LLM."""

    def test_error_result(self):
        result = {"error": "bad query"}
        formatted = _format_result_for_llm(result)
        assert "Query error:" in formatted
        assert "bad query" in formatted

    def test_empty_result(self):
        result = {"row_count": 0, "columns": ["x"], "rows": [], "total_rows": 0, "truncated": False}
        assert _format_result_for_llm(result) == "Query returned 0 rows."

    def test_normal_result_csv(self):
        result = {
            "columns": ["id", "amount"],
            "rows": [["CLM001", 100.0], ["CLM002", 200.0]],
            "row_count": 2,
            "total_rows": 2,
            "truncated": False,
        }
        formatted = _format_result_for_llm(result)
        lines = formatted.strip().split("\n")
        assert lines[0] == "id,amount"
        assert lines[1] == "CLM001,100.0"

    def test_truncated_result_has_note(self):
        result = {
            "columns": ["x"],
            "rows": [[1]],
            "row_count": 1,
            "total_rows": 500,
            "truncated": True,
        }
        formatted = _format_result_for_llm(result)
        assert "Showing 1 of 500 total rows" in formatted

    def test_none_values_formatted_as_null(self):
        result = {
            "columns": ["x"],
            "rows": [[None]],
            "row_count": 1,
            "total_rows": 1,
            "truncated": False,
        }
        formatted = _format_result_for_llm(result)
        assert "NULL" in formatted
