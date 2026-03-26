"""Unit tests for src/data_loader.py.

Covers _parse_dates, _parse_booleans, _validate_columns, and
load_all_data / load_gold_data against a real test database.
"""

import duckdb
import pytest
import pandas as pd
import numpy as np

from src.database import create_tables, _etl_bronze_to_silver
from src.data_loader import (
    _parse_dates,
    _parse_booleans,
    _validate_columns,
    REQUIRED_COLUMNS,
    load_all_data,
    load_gold_data,
)


# ===========================================================================
# _parse_dates
# ===========================================================================

class TestParseDates:
    def test_converts_date_strings_to_datetime(self):
        df = pd.DataFrame({"dt": ["2024-01-15", "2024-06-30"]})
        result = _parse_dates(df, ["dt"])
        assert pd.api.types.is_datetime64_any_dtype(result["dt"])

    def test_unparseable_dates_become_nat(self):
        df = pd.DataFrame({"dt": ["2024-01-15", "not_a_date", None]})
        result = _parse_dates(df, ["dt"])
        assert pd.isna(result["dt"].iloc[1])
        assert pd.isna(result["dt"].iloc[2])

    def test_missing_column_is_ignored(self):
        df = pd.DataFrame({"other": [1, 2]})
        result = _parse_dates(df, ["nonexistent_col"])
        assert "other" in result.columns  # no error

    def test_empty_dataframe(self):
        df = pd.DataFrame({"dt": pd.Series([], dtype=str)})
        result = _parse_dates(df, ["dt"])
        assert result.empty

    def test_multiple_columns(self):
        df = pd.DataFrame({
            "start": ["2024-01-01"],
            "end": ["2024-12-31"],
        })
        result = _parse_dates(df, ["start", "end"])
        assert pd.api.types.is_datetime64_any_dtype(result["start"])
        assert pd.api.types.is_datetime64_any_dtype(result["end"])


# ===========================================================================
# _parse_booleans
# ===========================================================================

class TestParseBooleans:
    def test_converts_1_to_true(self):
        df = pd.DataFrame({"flag": [1, 0, 1]})
        result = _parse_booleans(df, ["flag"])
        assert result["flag"].dtype == bool
        assert result["flag"].iloc[0] is np.bool_(True)
        assert result["flag"].iloc[1] is np.bool_(False)

    def test_missing_column_is_ignored(self):
        df = pd.DataFrame({"other": [1]})
        result = _parse_booleans(df, ["nonexistent"])
        assert "other" in result.columns

    def test_empty_dataframe(self):
        df = pd.DataFrame({"flag": pd.Series([], dtype=int)})
        result = _parse_booleans(df, ["flag"])
        assert result.empty


# ===========================================================================
# _validate_columns
# ===========================================================================

class TestValidateColumns:
    def test_valid_columns_no_error(self):
        df = pd.DataFrame({"payer_id": [], "payer_name": [], "payer_type": []})
        _validate_columns(df, "payers", "test_query")  # should not raise

    def test_missing_columns_raises_value_error(self):
        df = pd.DataFrame({"payer_id": []})
        with pytest.raises(ValueError, match="missing required columns"):
            _validate_columns(df, "payers", "test_query")

    def test_unknown_key_skips_validation(self):
        df = pd.DataFrame({"x": []})
        _validate_columns(df, "unknown_table", "test_query")  # no error

    def test_all_required_column_sets_defined(self):
        expected_tables = [
            "payers", "patients", "providers", "encounters", "charges",
            "claims", "payments", "denials", "adjustments", "operating_costs",
        ]
        for t in expected_tables:
            assert t in REQUIRED_COLUMNS, f"Missing REQUIRED_COLUMNS entry for {t}"


# ===========================================================================
# load_all_data (integration test)
# ===========================================================================

class TestLoadAllData:
    @pytest.fixture
    def populated_db(self, tmp_path, monkeypatch):
        """Database with Bronze + Silver data, patched as default DB."""
        db_path = str(tmp_path / "test.db")
        conn = duckdb.connect(db_path)
        create_tables(conn)
        conn.execute("""
            INSERT INTO bronze_payers VALUES
                ('PYR001','Aetna','Commercial','0.85','C001',CURRENT_TIMESTAMP);
            INSERT INTO bronze_patients VALUES
                ('PAT001','Alice','Smith','1980-01-01','F','PYR001','M001','10001',CURRENT_TIMESTAMP);
            INSERT INTO bronze_providers VALUES
                ('PRV001','Dr. A','1111111111','Cardiology','Internal Medicine',CURRENT_TIMESTAMP);
            INSERT INTO bronze_encounters VALUES
                ('ENC001','PAT001','PRV001','2024-01-15','2024-01-15','Outpatient','Cardiology',CURRENT_TIMESTAMP);
            INSERT INTO bronze_charges VALUES
                ('CHG001','ENC001','99213','Office Visit','1','200.00','2024-01-15','2024-01-17','Z00.00',CURRENT_TIMESTAMP);
            INSERT INTO bronze_claims VALUES
                ('CLM001','ENC001','PAT001','PYR001','2024-01-15','2024-01-17','1000','Paid','True','Electronic','',CURRENT_TIMESTAMP);
            INSERT INTO bronze_payments VALUES
                ('PAY001','CLM001','PYR001','900','950','2024-02-01','EFT','1',CURRENT_TIMESTAMP);
            INSERT INTO bronze_denials VALUES
                ('DEN001','CLM001','CO-4','Not covered','2024-02-01','500','Won','2024-03-01','450',CURRENT_TIMESTAMP);
            INSERT INTO bronze_adjustments VALUES
                ('ADJ001','CLM001','CONTRACTUAL','Contractual','50','2024-02-01',CURRENT_TIMESTAMP);
            INSERT INTO bronze_operating_costs VALUES
                ('2024-01','10000','2000','5000','500','17500',CURRENT_TIMESTAMP);
        """)
        _etl_bronze_to_silver(conn)
        conn.commit()
        conn.close()
        monkeypatch.setattr("src.data_loader.DB_PATH", db_path)
        monkeypatch.setattr("src.database.DB_PATH", db_path)
        return db_path

    def test_returns_dict_with_all_tables(self, populated_db):
        data = load_all_data()
        expected = {"payers", "patients", "providers", "encounters", "charges",
                    "claims", "payments", "denials", "adjustments", "operating_costs"}
        assert set(data.keys()) == expected

    def test_all_values_are_dataframes(self, populated_db):
        data = load_all_data()
        for key, df in data.items():
            assert isinstance(df, pd.DataFrame), f"{key} is not a DataFrame"

    def test_dates_are_parsed(self, populated_db):
        data = load_all_data()
        assert pd.api.types.is_datetime64_any_dtype(data["claims"]["date_of_service"])
        assert pd.api.types.is_datetime64_any_dtype(data["encounters"]["date_of_service"])

    def test_booleans_are_parsed(self, populated_db):
        data = load_all_data()
        assert data["claims"]["is_clean_claim"].dtype == bool

    def test_each_table_has_rows(self, populated_db):
        data = load_all_data()
        for key, df in data.items():
            assert len(df) >= 1, f"{key} has no rows"


# ===========================================================================
# load_gold_data (integration test)
# ===========================================================================

class TestLoadGoldData:
    @pytest.fixture
    def populated_db(self, tmp_path, monkeypatch):
        """Same as above — need Silver data for Gold views."""
        db_path = str(tmp_path / "test.db")
        conn = duckdb.connect(db_path)
        create_tables(conn)
        conn.execute("""
            INSERT INTO bronze_payers VALUES
                ('PYR001','Aetna','Commercial','0.85','C001',CURRENT_TIMESTAMP);
            INSERT INTO bronze_patients VALUES
                ('PAT001','Alice','Smith','1980-01-01','F','PYR001','M001','10001',CURRENT_TIMESTAMP);
            INSERT INTO bronze_providers VALUES
                ('PRV001','Dr. A','1111111111','Cardiology','Internal Medicine',CURRENT_TIMESTAMP);
            INSERT INTO bronze_encounters VALUES
                ('ENC001','PAT001','PRV001','2024-01-15','2024-01-15','Outpatient','Cardiology',CURRENT_TIMESTAMP);
            INSERT INTO bronze_claims VALUES
                ('CLM001','ENC001','PAT001','PYR001','2024-01-15','2024-01-17','1000','Paid','True','Electronic','',CURRENT_TIMESTAMP);
            INSERT INTO bronze_payments VALUES
                ('PAY001','CLM001','PYR001','900','950','2024-02-01','EFT','1',CURRENT_TIMESTAMP);
            INSERT INTO bronze_denials VALUES
                ('DEN001','CLM001','CO-4','Not covered','2024-02-01','500','Won','2024-03-01','450',CURRENT_TIMESTAMP);
        """)
        _etl_bronze_to_silver(conn)
        conn.commit()
        conn.close()
        monkeypatch.setattr("src.data_loader.DB_PATH", db_path)
        monkeypatch.setattr("src.database.DB_PATH", db_path)
        return db_path

    def test_returns_dict_with_all_views(self, populated_db):
        gold = load_gold_data()
        expected = {"monthly_kpis", "payer_performance", "department_performance",
                    "ar_aging", "denial_analysis"}
        assert set(gold.keys()) == expected

    def test_all_values_are_dataframes(self, populated_db):
        gold = load_gold_data()
        for key, df in gold.items():
            assert isinstance(df, pd.DataFrame), f"{key} is not a DataFrame"
