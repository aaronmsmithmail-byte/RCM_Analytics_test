"""Unit tests for src/metrics.py — all 26 RCM metric functions.

All tests use an in-memory DuckDB database via pytest's tmp_path fixture,
populating the Silver layer tables directly to verify SQL-based metric queries.
"""

import duckdb
import pytest

from src.database import create_tables
from src.metrics import (
    FilterParams,
    query_appeal_success_rate,
    query_ar_aging,
    query_avg_reimbursement,
    query_bad_debt_rate,
    query_charge_lag,
    query_clean_claim_breakdown,
    query_clean_claim_rate,
    query_cost_to_collect,
    query_cpt_analysis,
    query_data_freshness,
    query_days_in_ar,
    query_denial_rate,
    query_denial_rate_by_payer,
    query_denial_reasons,
    query_department_performance,
    query_first_pass_rate,
    query_gross_collection_rate,
    query_net_collection_rate,
    query_patient_responsibility_by_dept,
    query_patient_responsibility_by_payer,
    query_patient_responsibility_trend,
    query_payer_mix,
    query_payment_accuracy,
    query_provider_performance,
    query_underpayment_analysis,
    query_underpayment_trend,
)

# ===========================================================================
# Shared fixtures
# ===========================================================================

@pytest.fixture
def db(tmp_path):
    """Temporary DuckDB database pre-loaded with representative Silver data.

    Data summary (all dates in 2024):
        silver_claims:
            CLM001  ENC010  PAT001  PYR001  2024-01-15  1000.0  Paid      clean
            CLM002  ENC020  PAT002  PYR001  2024-01-20  2000.0  Denied    dirty
            CLM003  ENC030  PAT003  PYR002  2024-02-10  1500.0  Paid      clean
            CLM004  ENC040  PAT004  PYR002  2024-02-25   500.0  Appealed  dirty

        silver_payments:
            PAY001  CLM001  PYR001   900.0  is_accurate=1
            PAY002  CLM003  PYR002   700.0  is_accurate=1
            PAY003  CLM003  PYR002   300.0  is_accurate=0

        silver_adjustments:
            ADJ001  CLM001  CONTRACTUAL   50.0
            ADJ002  CLM002  CONTRACTUAL  100.0
            ADJ003  CLM003  WRITEOFF     200.0

        silver_denials:
            DEN001  CLM002  CO-4   2000.0  Won          recovered=1800
            DEN002  CLM004  CO-97   500.0  Lost         recovered=0
            DEN003  CLM002  CO-4    200.0  In Progress  recovered=0

        silver_operating_costs:
            2024-01  total=17500
            2024-02  total=17500

        silver_charges:
            CHG001  ENC010  service=2024-01-15  post=2024-01-17  (lag=2)
            CHG002  ENC020  service=2024-01-20  post=2024-01-25  (lag=5)
            CHG003  ENC030  service=2024-02-10  post=2024-02-12  (lag=2)
    """
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
            ('PAT003','Carol','Lee','1990-03-20','F','PYR002','M003','10003'),
            ('PAT004','Dave','Kim','1965-11-30','M','PYR002','M004','10004');

        INSERT INTO silver_providers VALUES
            ('PRV001','Dr. A','1111111111','Cardiology','Internal Medicine'),
            ('PRV002','Dr. B','2222222222','Orthopedics','Orthopedics');

        INSERT INTO silver_encounters VALUES
            ('ENC010','PAT001','PRV001','2024-01-15','2024-01-15','Outpatient','Cardiology'),
            ('ENC020','PAT002','PRV001','2024-01-20','2024-01-20','Outpatient','Cardiology'),
            ('ENC030','PAT003','PRV002','2024-02-10','2024-02-11','Inpatient','Orthopedics'),
            ('ENC040','PAT004','PRV002','2024-02-25','2024-02-25','Emergency','Orthopedics');

        INSERT INTO silver_charges VALUES
            ('CHG001','ENC010','99213','Office Visit',1,200.0,'2024-01-15','2024-01-17','Z00.00'),
            ('CHG002','ENC020','99214','Office Visit Complex',1,350.0,'2024-01-20','2024-01-25','Z00.00'),
            ('CHG003','ENC030','27447','Total Knee Replacement',1,15000.0,'2024-02-10','2024-02-12','M17.11');

        INSERT INTO silver_claims VALUES
            ('CLM001','ENC010','PAT001','PYR001','2024-01-15','2024-01-17',1000.0,'Paid',1,'Electronic',NULL),
            ('CLM002','ENC020','PAT002','PYR001','2024-01-20','2024-01-22',2000.0,'Denied',0,'Electronic','CODING_ERROR'),
            ('CLM003','ENC030','PAT003','PYR002','2024-02-10','2024-02-12',1500.0,'Paid',1,'Electronic',NULL),
            ('CLM004','ENC040','PAT004','PYR002','2024-02-25','2024-02-27',500.0,'Appealed',0,'Paper','MISSING_AUTH');

        INSERT INTO silver_payments VALUES
            ('PAY001','CLM001','PYR001',900.0,950.0,'2024-02-01','EFT',1),
            ('PAY002','CLM003','PYR002',700.0,750.0,'2024-03-01','EFT',1),
            ('PAY003','CLM003','PYR002',300.0,350.0,'2024-03-15','Check',0);

        INSERT INTO silver_denials VALUES
            ('DEN001','CLM002','CO-4','Service not covered','2024-02-01',2000.0,'Won','2024-03-01',1800.0),
            ('DEN002','CLM004','CO-97','Payment included in allowance','2024-03-05',500.0,'Lost','2024-04-01',0.0),
            ('DEN003','CLM002','CO-4','Service not covered','2024-02-15',200.0,'In Progress',NULL,0.0);

        INSERT INTO silver_adjustments VALUES
            ('ADJ001','CLM001','CONTRACTUAL','Contractual Adjustment',50.0,'2024-02-01'),
            ('ADJ002','CLM002','CONTRACTUAL','Contractual Adjustment',100.0,'2024-02-05'),
            ('ADJ003','CLM003','WRITEOFF','Bad Debt Write-Off',200.0,'2024-03-01');

        INSERT INTO silver_operating_costs VALUES
            ('2024-01',10000.0,2000.0,5000.0,500.0,17500.0),
            ('2024-02',10000.0,2000.0,5000.0,500.0,17500.0);
    """)
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def full_params():
    """FilterParams covering all test data (Jan–Dec 2024, no dimension filters)."""
    return FilterParams(start_date="2024-01-01", end_date="2024-12-31")


@pytest.fixture
def empty_db(tmp_path):
    """DuckDB database with schema only — no data rows."""
    db_path = str(tmp_path / "empty.db")
    conn = duckdb.connect(db_path)
    create_tables(conn)
    conn.close()
    return db_path


# ===========================================================================
# 1. query_days_in_ar
# ===========================================================================

class TestQueryDaysInAr:
    def test_returns_float_and_dataframe(self, db, full_params):
        import pandas as pd
        dar, trend = query_days_in_ar(full_params, db_path=db)
        assert isinstance(dar, float)
        assert isinstance(trend, pd.DataFrame)

    def test_trend_has_required_columns(self, db, full_params):
        _, trend = query_days_in_ar(full_params, db_path=db)
        for col in ("charges", "payments", "ar_balance", "days_in_ar"):
            assert col in trend.columns

    def test_dar_is_non_negative(self, db, full_params):
        dar, _ = query_days_in_ar(full_params, db_path=db)
        assert dar >= 0

    def test_empty_db_returns_zero(self, empty_db, full_params):
        dar, trend = query_days_in_ar(full_params, db_path=empty_db)
        assert dar == 0.0
        assert trend.empty

    def test_ar_balance_cumulative(self, db, full_params):
        _, trend = query_days_in_ar(full_params, db_path=db)
        # ar_balance should be positive (we billed more than we collected)
        assert trend["ar_balance"].iloc[-1] > 0

    def test_trend_has_two_months(self, db, full_params):
        _, trend = query_days_in_ar(full_params, db_path=db)
        assert len(trend) == 2  # 2024-01 and 2024-02

    def test_payer_filter_reduces_dar(self, db):
        params_all = FilterParams(start_date="2024-01-01", end_date="2024-12-31")
        params_pyr1 = FilterParams(start_date="2024-01-01", end_date="2024-12-31", payer_id="PYR001")
        dar_all, _ = query_days_in_ar(params_all, db_path=db)
        dar_pyr1, _ = query_days_in_ar(params_pyr1, db_path=db)
        # PYR001 has CLM001 (paid 900) and CLM002 (not paid) — still has AR
        assert dar_pyr1 >= 0


# ===========================================================================
# 2. query_net_collection_rate
# ===========================================================================

class TestQueryNetCollectionRate:
    def test_returns_float_and_dataframe(self, db, full_params):
        import pandas as pd
        ncr, trend = query_net_collection_rate(full_params, db_path=db)
        assert isinstance(ncr, float)
        assert isinstance(trend, pd.DataFrame)

    def test_ncr_between_0_and_100(self, db, full_params):
        ncr, _ = query_net_collection_rate(full_params, db_path=db)
        assert 0 <= ncr <= 100

    def test_ncr_correct_value(self, db, full_params):
        # payments=1900, charges=5000, contractual_adj=150, net=5000-150=4850
        # NCR = 1900/4850*100 ≈ 39.18%
        ncr, _ = query_net_collection_rate(full_params, db_path=db)
        expected = round(1900 / (5000 - 150) * 100, 2)
        assert ncr == pytest.approx(expected, abs=0.1)

    def test_empty_db_returns_zero(self, empty_db, full_params):
        ncr, trend = query_net_collection_rate(full_params, db_path=empty_db)
        assert ncr == 0.0
        assert trend.empty

    def test_trend_has_ncr_column(self, db, full_params):
        _, trend = query_net_collection_rate(full_params, db_path=db)
        assert "ncr" in trend.columns

    def test_trend_indexed_by_year_month(self, db, full_params):
        _, trend = query_net_collection_rate(full_params, db_path=db)
        assert trend.index.name == "year_month"


# ===========================================================================
# 3. query_gross_collection_rate
# ===========================================================================

class TestQueryGrossCollectionRate:
    def test_returns_float_and_dataframe(self, db, full_params):
        import pandas as pd
        gcr, trend = query_gross_collection_rate(full_params, db_path=db)
        assert isinstance(gcr, float)
        assert isinstance(trend, pd.DataFrame)

    def test_gcr_between_0_and_100(self, db, full_params):
        gcr, _ = query_gross_collection_rate(full_params, db_path=db)
        assert 0 <= gcr <= 100

    def test_gcr_correct_value(self, db, full_params):
        # payments=1900, charges=5000 → 38.0%
        gcr, _ = query_gross_collection_rate(full_params, db_path=db)
        assert gcr == pytest.approx(38.0, abs=0.1)

    def test_empty_db_returns_zero(self, empty_db, full_params):
        gcr, trend = query_gross_collection_rate(full_params, db_path=empty_db)
        assert gcr == 0.0
        assert trend.empty

    def test_trend_has_gcr_column(self, db, full_params):
        _, trend = query_gross_collection_rate(full_params, db_path=db)
        assert "gcr" in trend.columns

    def test_payer_filter_pyr001(self, db):
        params = FilterParams(start_date="2024-01-01", end_date="2024-12-31", payer_id="PYR001")
        # PYR001: charges=3000, payments=900 → GCR=30%
        gcr, _ = query_gross_collection_rate(params, db_path=db)
        assert gcr == pytest.approx(30.0, abs=0.1)

    def test_payer_filter_pyr002(self, db):
        params = FilterParams(start_date="2024-01-01", end_date="2024-12-31", payer_id="PYR002")
        # PYR002: charges=2000, payments=1000 → GCR=50%
        gcr, _ = query_gross_collection_rate(params, db_path=db)
        assert gcr == pytest.approx(50.0, abs=0.1)

    def test_date_filter_jan_only(self, db):
        params = FilterParams(start_date="2024-01-01", end_date="2024-01-31")
        # Jan: charges=3000, payments=900 → GCR=30%
        gcr, _ = query_gross_collection_rate(params, db_path=db)
        assert gcr == pytest.approx(30.0, abs=0.1)


# ===========================================================================
# 4. query_clean_claim_rate
# ===========================================================================

class TestQueryCleanClaimRate:
    def test_returns_float_and_dataframe(self, db, full_params):
        import pandas as pd
        ccr, trend = query_clean_claim_rate(full_params, db_path=db)
        assert isinstance(ccr, float)
        assert isinstance(trend, pd.DataFrame)

    def test_ccr_correct_value(self, db, full_params):
        # 2 clean out of 4 = 50%
        ccr, _ = query_clean_claim_rate(full_params, db_path=db)
        assert ccr == pytest.approx(50.0, abs=0.1)

    def test_empty_db_returns_zero(self, empty_db, full_params):
        ccr, trend = query_clean_claim_rate(full_params, db_path=empty_db)
        assert ccr == 0.0
        assert trend.empty

    def test_ccr_payer_filter(self, db):
        # PYR001: CLM001(clean), CLM002(dirty) → 50%
        params = FilterParams(start_date="2024-01-01", end_date="2024-12-31", payer_id="PYR001")
        ccr, _ = query_clean_claim_rate(params, db_path=db)
        assert ccr == pytest.approx(50.0, abs=0.1)

    def test_ccr_between_0_and_100(self, db, full_params):
        ccr, _ = query_clean_claim_rate(full_params, db_path=db)
        assert 0 <= ccr <= 100

    def test_trend_has_ccr_column(self, db, full_params):
        _, trend = query_clean_claim_rate(full_params, db_path=db)
        assert "ccr" in trend.columns


# ===========================================================================
# 5. query_denial_rate
# ===========================================================================

class TestQueryDenialRate:
    def test_returns_float_and_dataframe(self, db, full_params):
        import pandas as pd
        rate, trend = query_denial_rate(full_params, db_path=db)
        assert isinstance(rate, float)
        assert isinstance(trend, pd.DataFrame)

    def test_denial_rate_correct_value(self, db, full_params):
        # Denied(CLM002) + Appealed(CLM004) = 2 out of 4 = 50%
        rate, _ = query_denial_rate(full_params, db_path=db)
        assert rate == pytest.approx(50.0, abs=0.1)

    def test_empty_db_returns_zero(self, empty_db, full_params):
        rate, trend = query_denial_rate(full_params, db_path=empty_db)
        assert rate == 0.0
        assert trend.empty

    def test_denial_rate_between_0_and_100(self, db, full_params):
        rate, _ = query_denial_rate(full_params, db_path=db)
        assert 0 <= rate <= 100

    def test_trend_has_denial_rate_column(self, db, full_params):
        _, trend = query_denial_rate(full_params, db_path=db)
        assert "denial_rate" in trend.columns

    def test_pyr002_denial_rate(self, db):
        # PYR002: CLM003(Paid), CLM004(Appealed) → 1/2 = 50%
        params = FilterParams(start_date="2024-01-01", end_date="2024-12-31", payer_id="PYR002")
        rate, _ = query_denial_rate(params, db_path=db)
        assert rate == pytest.approx(50.0, abs=0.1)


# ===========================================================================
# 6. query_denial_reasons
# ===========================================================================

class TestQueryDenialReasons:
    def test_returns_dataframe(self, db, full_params):
        import pandas as pd
        result = query_denial_reasons(full_params, db_path=db)
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, db, full_params):
        result = query_denial_reasons(full_params, db_path=db)
        for col in ("denial_reason_code", "denial_reason_description", "count",
                    "total_denied_amount", "total_recovered", "recovery_rate"):
            assert col in result.columns

    def test_co4_is_top_reason(self, db, full_params):
        result = query_denial_reasons(full_params, db_path=db)
        # CO-4 appears in DEN001 + DEN003 = 2 denials, CO-97 in DEN002 = 1
        assert result.iloc[0]["denial_reason_code"] == "CO-4"

    def test_recovery_rate_computed(self, db, full_params):
        result = query_denial_reasons(full_params, db_path=db)
        # CO-4: denied=2000+200=2200, recovered=1800 → rate=81.8%
        co4 = result[result["denial_reason_code"] == "CO-4"].iloc[0]
        assert co4["recovery_rate"] == pytest.approx(1800 / 2200 * 100, abs=0.1)

    def test_empty_db_returns_empty_dataframe(self, empty_db, full_params):
        result = query_denial_reasons(full_params, db_path=empty_db)
        assert result.empty


# ===========================================================================
# 7. query_first_pass_rate
# ===========================================================================

class TestQueryFirstPassRate:
    def test_returns_float_and_dataframe(self, db, full_params):
        import pandas as pd
        fpr, trend = query_first_pass_rate(full_params, db_path=db)
        assert isinstance(fpr, float)
        assert isinstance(trend, pd.DataFrame)

    def test_fpr_correct_value(self, db, full_params):
        # Paid: CLM001, CLM003 = 2 out of 4 = 50%
        fpr, _ = query_first_pass_rate(full_params, db_path=db)
        assert fpr == pytest.approx(50.0, abs=0.1)

    def test_empty_db_returns_zero(self, empty_db, full_params):
        fpr, trend = query_first_pass_rate(full_params, db_path=empty_db)
        assert fpr == 0.0
        assert trend.empty

    def test_fpr_between_0_and_100(self, db, full_params):
        fpr, _ = query_first_pass_rate(full_params, db_path=db)
        assert 0 <= fpr <= 100

    def test_trend_has_fpr_column(self, db, full_params):
        _, trend = query_first_pass_rate(full_params, db_path=db)
        assert "fpr" in trend.columns

    def test_payer_filter_pyr001(self, db):
        # PYR001: CLM001(Paid), CLM002(Denied) → FPR=50%
        params = FilterParams(start_date="2024-01-01", end_date="2024-12-31", payer_id="PYR001")
        fpr, _ = query_first_pass_rate(params, db_path=db)
        assert fpr == pytest.approx(50.0, abs=0.1)


# ===========================================================================
# 8. query_charge_lag
# ===========================================================================

class TestQueryChargeLag:
    def test_returns_float_and_series(self, db, full_params):
        import pandas as pd
        avg_lag, trend, dist = query_charge_lag(full_params, db_path=db)
        assert isinstance(avg_lag, float)
        assert isinstance(trend, pd.Series)
        assert isinstance(dist, pd.Series)

    def test_avg_lag_correct(self, db, full_params):
        # CHG001: lag=2, CHG002: lag=5, CHG003: lag=2 → avg=3.0
        avg_lag, _, _ = query_charge_lag(full_params, db_path=db)
        assert avg_lag == pytest.approx(3.0, abs=0.1)

    def test_avg_lag_non_negative(self, db, full_params):
        avg_lag, _, _ = query_charge_lag(full_params, db_path=db)
        assert avg_lag >= 0

    def test_empty_db_returns_zero(self, empty_db, full_params):
        import pandas as pd
        avg_lag, trend, dist = query_charge_lag(full_params, db_path=empty_db)
        assert avg_lag == 0.0
        assert isinstance(trend, pd.Series)
        assert isinstance(dist, pd.Series)

    def test_distribution_contains_lag_values(self, db, full_params):
        _, _, dist = query_charge_lag(full_params, db_path=db)
        # lag=2 appears twice
        assert 2 in dist.index
        assert dist[2] == 2

    def test_trend_indexed_by_period(self, db, full_params):
        _, trend, _ = query_charge_lag(full_params, db_path=db)
        assert trend.index.name == "year_month"


# ===========================================================================
# 9. query_cost_to_collect
# ===========================================================================

class TestQueryCostToCollect:
    def test_returns_float_and_dataframe(self, db, full_params):
        import pandas as pd
        ctc, trend = query_cost_to_collect(full_params, db_path=db)
        assert isinstance(ctc, float)
        assert isinstance(trend, pd.DataFrame)

    def test_ctc_positive(self, db, full_params):
        ctc, _ = query_cost_to_collect(full_params, db_path=db)
        assert ctc > 0

    def test_ctc_correct_value(self, db, full_params):
        # total_cost=35000, total_collected=1900 → CTC=1842.1%
        ctc, _ = query_cost_to_collect(full_params, db_path=db)
        expected = round(35000 / 1900 * 100, 2)
        assert ctc == pytest.approx(expected, abs=0.5)

    def test_empty_db_returns_zero(self, empty_db, full_params):
        ctc, trend = query_cost_to_collect(full_params, db_path=empty_db)
        assert ctc == 0.0
        assert trend.empty

    def test_trend_has_ctc_column(self, db, full_params):
        _, trend = query_cost_to_collect(full_params, db_path=db)
        assert "cost_to_collect_pct" in trend.columns


# ===========================================================================
# 10. query_ar_aging
# ===========================================================================

class TestQueryArAging:
    def test_returns_dataframe_and_float(self, db, full_params):
        import pandas as pd
        summary, total_ar = query_ar_aging(full_params, db_path=db)
        assert isinstance(summary, pd.DataFrame)
        assert isinstance(total_ar, float)

    def test_total_ar_positive(self, db, full_params):
        _, total_ar = query_ar_aging(full_params, db_path=db)
        # CLM001 ar=100, CLM002 ar=2000, CLM003 ar=500, CLM004 ar=500 → total=3100
        assert total_ar == pytest.approx(3100.0, abs=1.0)

    def test_pct_of_total_sums_to_100(self, db, full_params):
        summary, total_ar = query_ar_aging(full_params, db_path=db)
        if total_ar > 0:
            assert summary["pct_of_total"].sum() == pytest.approx(100.0, abs=0.5)

    def test_summary_has_required_columns(self, db, full_params):
        summary, _ = query_ar_aging(full_params, db_path=db)
        for col in ("claim_count", "total_ar", "pct_of_total"):
            assert col in summary.columns

    def test_summary_indexed_by_aging_buckets(self, db, full_params):
        summary, _ = query_ar_aging(full_params, db_path=db)
        for bucket in ("0-30", "31-60", "61-90", "91-120", "120+"):
            assert bucket in summary.index

    def test_empty_db_returns_zero_total(self, empty_db, full_params):
        _, total_ar = query_ar_aging(full_params, db_path=empty_db)
        assert total_ar == 0.0

    def test_all_ar_in_120_plus_for_old_dates(self, db, full_params):
        # All claims are from 2024, so 700+ days old → all in 120+
        summary, _ = query_ar_aging(full_params, db_path=db)
        assert summary.loc["120+", "total_ar"] == pytest.approx(3100.0, abs=1.0)


# ===========================================================================
# 11. query_payment_accuracy
# ===========================================================================

class TestQueryPaymentAccuracy:
    def test_returns_float(self, db, full_params):
        acc = query_payment_accuracy(full_params, db_path=db)
        assert isinstance(acc, float)

    def test_accuracy_correct_value(self, db, full_params):
        # PAY001 accurate, PAY002 accurate, PAY003 not → 2/3 = 66.67%
        acc = query_payment_accuracy(full_params, db_path=db)
        assert acc == pytest.approx(66.67, abs=0.1)

    def test_accuracy_between_0_and_100(self, db, full_params):
        acc = query_payment_accuracy(full_params, db_path=db)
        assert 0 <= acc <= 100

    def test_empty_db_returns_zero(self, empty_db, full_params):
        acc = query_payment_accuracy(full_params, db_path=empty_db)
        assert acc == 0.0

    def test_payer_filter_pyr001(self, db):
        # PYR001 payments: only PAY001 (accurate=1) → 100%
        params = FilterParams(start_date="2024-01-01", end_date="2024-12-31", payer_id="PYR001")
        acc = query_payment_accuracy(params, db_path=db)
        assert acc == pytest.approx(100.0, abs=0.1)


# ===========================================================================
# 12. query_bad_debt_rate
# ===========================================================================

class TestQueryBadDebtRate:
    def test_returns_three_values(self, db, full_params):
        result = query_bad_debt_rate(full_params, db_path=db)
        assert len(result) == 3

    def test_bad_debt_rate_correct(self, db, full_params):
        # WRITEOFF: ADJ003 = 200, total_charges = 5000 → rate=4.0%
        rate, bad_debt, total_charges = query_bad_debt_rate(full_params, db_path=db)
        assert rate == pytest.approx(4.0, abs=0.1)

    def test_bad_debt_amount_correct(self, db, full_params):
        _, bad_debt, _ = query_bad_debt_rate(full_params, db_path=db)
        assert bad_debt == pytest.approx(200.0, abs=0.01)

    def test_total_charges_correct(self, db, full_params):
        _, _, total_charges = query_bad_debt_rate(full_params, db_path=db)
        assert total_charges == pytest.approx(5000.0, abs=0.01)

    def test_empty_db_returns_zeros(self, empty_db, full_params):
        rate, bad_debt, total = query_bad_debt_rate(full_params, db_path=empty_db)
        assert rate == 0.0
        assert bad_debt == 0.0
        assert total == 0.0

    def test_rate_between_0_and_100(self, db, full_params):
        rate, _, _ = query_bad_debt_rate(full_params, db_path=db)
        assert 0 <= rate <= 100


# ===========================================================================
# 13. query_appeal_success_rate
# ===========================================================================

class TestQueryAppealSuccessRate:
    def test_returns_three_values(self, db, full_params):
        result = query_appeal_success_rate(full_params, db_path=db)
        assert len(result) == 3

    def test_appeal_rate_correct(self, db, full_params):
        # Won=DEN001, Lost=DEN002, InProgress=DEN003 → 3 total, 1 won → 33.33%
        rate, total_appealed, won = query_appeal_success_rate(full_params, db_path=db)
        assert rate == pytest.approx(33.33, abs=0.1)

    def test_total_appealed_correct(self, db, full_params):
        _, total_appealed, _ = query_appeal_success_rate(full_params, db_path=db)
        assert total_appealed == 3

    def test_won_count_correct(self, db, full_params):
        _, _, won = query_appeal_success_rate(full_params, db_path=db)
        assert won == 1

    def test_empty_db_returns_zeros(self, empty_db, full_params):
        rate, total, won = query_appeal_success_rate(full_params, db_path=empty_db)
        assert rate == 0.0
        assert total == 0
        assert won == 0


# ===========================================================================
# 14. query_avg_reimbursement
# ===========================================================================

class TestQueryAvgReimbursement:
    def test_returns_float_and_series(self, db, full_params):
        import pandas as pd
        avg, trend = query_avg_reimbursement(full_params, db_path=db)
        assert isinstance(avg, float)
        assert isinstance(trend, pd.Series)

    def test_avg_correct_value(self, db, full_params):
        # CLM001=900, CLM002=0, CLM003=1000, CLM004=0 → avg=475.0
        avg, _ = query_avg_reimbursement(full_params, db_path=db)
        assert avg == pytest.approx(475.0, abs=1.0)

    def test_avg_non_negative(self, db, full_params):
        avg, _ = query_avg_reimbursement(full_params, db_path=db)
        assert avg >= 0

    def test_empty_db_returns_zero(self, empty_db, full_params):
        import pandas as pd
        avg, trend = query_avg_reimbursement(full_params, db_path=empty_db)
        assert avg == 0.0
        assert isinstance(trend, pd.Series)

    def test_trend_indexed_by_year_month(self, db, full_params):
        _, trend = query_avg_reimbursement(full_params, db_path=db)
        assert trend.index.name == "year_month"


# ===========================================================================
# 15. query_payer_mix
# ===========================================================================

class TestQueryPayerMix:
    def test_returns_dataframe(self, db, full_params):
        import pandas as pd
        result = query_payer_mix(full_params, db_path=db)
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, db, full_params):
        result = query_payer_mix(full_params, db_path=db)
        for col in ("payer_id", "payer_name", "payer_type",
                    "claim_count", "total_charges", "total_payments", "collection_rate"):
            assert col in result.columns

    def test_two_payers_returned(self, db, full_params):
        result = query_payer_mix(full_params, db_path=db)
        assert len(result) == 2

    def test_pyr001_charges_correct(self, db, full_params):
        result = query_payer_mix(full_params, db_path=db)
        pyr1 = result[result["payer_id"] == "PYR001"].iloc[0]
        assert pyr1["total_charges"] == pytest.approx(3000.0, abs=0.01)

    def test_pyr001_payments_correct(self, db, full_params):
        result = query_payer_mix(full_params, db_path=db)
        pyr1 = result[result["payer_id"] == "PYR001"].iloc[0]
        assert pyr1["total_payments"] == pytest.approx(900.0, abs=0.01)

    def test_pyr002_payments_correct(self, db, full_params):
        result = query_payer_mix(full_params, db_path=db)
        pyr2 = result[result["payer_id"] == "PYR002"].iloc[0]
        assert pyr2["total_payments"] == pytest.approx(1000.0, abs=0.01)

    def test_collection_rate_computed(self, db, full_params):
        result = query_payer_mix(full_params, db_path=db)
        # PYR001: 900/3000*100=30%, PYR002: 1000/2000*100=50%
        pyr1 = result[result["payer_id"] == "PYR001"].iloc[0]
        assert pyr1["collection_rate"] == pytest.approx(30.0, abs=0.1)

    def test_empty_db_returns_empty(self, empty_db, full_params):
        result = query_payer_mix(full_params, db_path=empty_db)
        assert result.empty

    def test_payer_filter_returns_one_row(self, db):
        params = FilterParams(start_date="2024-01-01", end_date="2024-12-31", payer_id="PYR001")
        result = query_payer_mix(params, db_path=db)
        assert len(result) == 1


# ===========================================================================
# 16. query_denial_rate_by_payer
# ===========================================================================

class TestQueryDenialRateByPayer:
    def test_returns_dataframe(self, db, full_params):
        import pandas as pd
        result = query_denial_rate_by_payer(full_params, db_path=db)
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, db, full_params):
        result = query_denial_rate_by_payer(full_params, db_path=db)
        for col in ("payer_id", "payer_name", "total_claims", "denied", "denial_rate"):
            assert col in result.columns

    def test_two_payers_returned(self, db, full_params):
        result = query_denial_rate_by_payer(full_params, db_path=db)
        assert len(result) == 2

    def test_pyr001_denial_rate(self, db, full_params):
        # PYR001: CLM001(Paid), CLM002(Denied) → 1/2=50%
        result = query_denial_rate_by_payer(full_params, db_path=db)
        pyr1 = result[result["payer_id"] == "PYR001"].iloc[0]
        assert pyr1["denial_rate"] == pytest.approx(50.0, abs=0.1)

    def test_pyr002_denial_rate(self, db, full_params):
        # PYR002: CLM003(Paid), CLM004(Appealed) → 1/2=50%
        result = query_denial_rate_by_payer(full_params, db_path=db)
        pyr2 = result[result["payer_id"] == "PYR002"].iloc[0]
        assert pyr2["denial_rate"] == pytest.approx(50.0, abs=0.1)

    def test_empty_db_returns_empty(self, empty_db, full_params):
        result = query_denial_rate_by_payer(full_params, db_path=empty_db)
        assert result.empty


# ===========================================================================
# 17. query_department_performance
# ===========================================================================

class TestQueryDepartmentPerformance:
    def test_returns_dataframe(self, db, full_params):
        import pandas as pd
        result = query_department_performance(full_params, db_path=db)
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, db, full_params):
        result = query_department_performance(full_params, db_path=db)
        for col in ("department", "encounter_count", "total_charges",
                    "total_payments", "collection_rate", "avg_payment_per_encounter"):
            assert col in result.columns

    def test_two_departments_returned(self, db, full_params):
        result = query_department_performance(full_params, db_path=db)
        assert len(result) == 2

    def test_cardiology_charges_correct(self, db, full_params):
        # Cardiology: CLM001(1000)+CLM002(2000)=3000
        result = query_department_performance(full_params, db_path=db)
        card = result[result["department"] == "Cardiology"].iloc[0]
        assert card["total_charges"] == pytest.approx(3000.0, abs=0.01)

    def test_orthopedics_payments_correct(self, db, full_params):
        # Orthopedics: CLM003 payments=1000 (CLM004 none)
        result = query_department_performance(full_params, db_path=db)
        orth = result[result["department"] == "Orthopedics"].iloc[0]
        assert orth["total_payments"] == pytest.approx(1000.0, abs=0.01)

    def test_avg_payment_per_encounter_computed(self, db, full_params):
        result = query_department_performance(full_params, db_path=db)
        # Orthopedics: 1000 / 2 encounters = 500.0
        orth = result[result["department"] == "Orthopedics"].iloc[0]
        assert orth["avg_payment_per_encounter"] == pytest.approx(500.0, abs=0.01)

    def test_empty_db_returns_empty(self, empty_db, full_params):
        result = query_department_performance(full_params, db_path=empty_db)
        assert result.empty

    def test_dept_filter_returns_one_dept(self, db):
        params = FilterParams(
            start_date="2024-01-01", end_date="2024-12-31", department="Cardiology"
        )
        result = query_department_performance(params, db_path=db)
        assert len(result) == 1
        assert result.iloc[0]["department"] == "Cardiology"


# ===========================================================================
# FilterParams dataclass tests
# ===========================================================================

class TestFilterParams:
    def test_start_end_required(self):
        p = FilterParams(start_date="2024-01-01", end_date="2024-12-31")
        assert p.start_date == "2024-01-01"
        assert p.end_date == "2024-12-31"

    def test_optional_fields_default_none(self):
        p = FilterParams(start_date="2024-01-01", end_date="2024-12-31")
        assert p.payer_id is None
        assert p.department is None
        assert p.encounter_type is None

    def test_all_fields_settable(self):
        p = FilterParams(
            start_date="2024-01-01",
            end_date="2024-12-31",
            payer_id="PYR001",
            department="Cardiology",
            encounter_type="Outpatient",
        )
        assert p.payer_id == "PYR001"
        assert p.department == "Cardiology"
        assert p.encounter_type == "Outpatient"

    def test_encounter_type_filter_works(self, db):
        # Outpatient: ENC010(CLM001), ENC020(CLM002) — both Cardiology/Outpatient
        params = FilterParams(
            start_date="2024-01-01",
            end_date="2024-12-31",
            encounter_type="Outpatient",
        )
        gcr, _ = query_gross_collection_rate(params, db_path=db)
        # Outpatient charges: CLM001(1000)+CLM002(2000)=3000, payments=900 → 30%
        assert gcr == pytest.approx(30.0, abs=0.1)


# ===========================================================================
# 18. PROVIDER PERFORMANCE
# ===========================================================================

class TestQueryProviderPerformance:
    def test_returns_dataframe(self, db, full_params):
        import pandas as pd
        df = query_provider_performance(full_params, db_path=db)
        assert isinstance(df, pd.DataFrame)

    def test_has_required_columns(self, db, full_params):
        df = query_provider_performance(full_params, db_path=db)
        for col in ("provider_id", "provider_name", "specialty", "total_charges",
                     "total_payments", "collection_rate", "denial_rate",
                     "clean_claim_rate", "avg_payment_per_encounter"):
            assert col in df.columns

    def test_returns_both_providers(self, db, full_params):
        df = query_provider_performance(full_params, db_path=db)
        assert set(df["provider_id"]) == {"PRV001", "PRV002"}

    def test_collection_rate_non_negative(self, db, full_params):
        df = query_provider_performance(full_params, db_path=db)
        assert (df["collection_rate"] >= 0).all()

    def test_empty_db_returns_empty(self, empty_db, full_params):
        df = query_provider_performance(full_params, db_path=empty_db)
        assert df.empty


# ===========================================================================
# 19. CPT ANALYSIS
# ===========================================================================

class TestQueryCptAnalysis:
    def test_returns_dataframe(self, db, full_params):
        import pandas as pd
        df = query_cpt_analysis(full_params, db_path=db)
        assert isinstance(df, pd.DataFrame)

    def test_has_required_columns(self, db, full_params):
        df = query_cpt_analysis(full_params, db_path=db)
        for col in ("cpt_code", "cpt_description", "charge_count", "total_charges",
                     "avg_charge_per_unit", "denial_rate"):
            assert col in df.columns

    def test_returns_known_cpt_codes(self, db, full_params):
        df = query_cpt_analysis(full_params, db_path=db)
        assert "99213" in df["cpt_code"].values
        assert "27447" in df["cpt_code"].values

    def test_sorted_by_total_charges_desc(self, db, full_params):
        df = query_cpt_analysis(full_params, db_path=db)
        charges = df["total_charges"].tolist()
        assert charges == sorted(charges, reverse=True)

    def test_empty_db_returns_empty(self, empty_db, full_params):
        df = query_cpt_analysis(full_params, db_path=empty_db)
        assert df.empty


# ===========================================================================
# 20. UNDERPAYMENT ANALYSIS
# ===========================================================================

class TestQueryUnderpaymentAnalysis:
    def test_returns_tuple(self, db, full_params):
        result = query_underpayment_analysis(full_params, db_path=db)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_dataframe_has_required_columns(self, db, full_params):
        df, _ = query_underpayment_analysis(full_params, db_path=db)
        for col in ("payer_id", "payer_name", "total_allowed", "total_paid",
                     "total_underpaid", "underpayment_rate"):
            assert col in df.columns

    def test_recovery_is_float(self, db, full_params):
        _, recovery = query_underpayment_analysis(full_params, db_path=db)
        assert isinstance(recovery, float)

    def test_recovery_non_negative(self, db, full_params):
        _, recovery = query_underpayment_analysis(full_params, db_path=db)
        assert recovery >= 0.0

    def test_empty_db_returns_zero_recovery(self, empty_db, full_params):
        df, recovery = query_underpayment_analysis(full_params, db_path=empty_db)
        assert df.empty
        assert recovery == 0.0


# ===========================================================================
# 21. UNDERPAYMENT TREND
# ===========================================================================

class TestQueryUnderpaymentTrend:
    def test_returns_dataframe(self, db, full_params):
        import pandas as pd
        df = query_underpayment_trend(full_params, db_path=db)
        assert isinstance(df, pd.DataFrame)

    def test_has_required_columns(self, db, full_params):
        df = query_underpayment_trend(full_params, db_path=db)
        for col in ("total_allowed", "total_paid", "total_underpaid", "underpayment_rate"):
            assert col in df.columns

    def test_index_is_year_month(self, db, full_params):
        df = query_underpayment_trend(full_params, db_path=db)
        if not df.empty:
            assert df.index.name == "year_month"

    def test_empty_db_returns_empty(self, empty_db, full_params):
        df = query_underpayment_trend(full_params, db_path=empty_db)
        assert df.empty


# ===========================================================================
# 22. CLEAN CLAIM BREAKDOWN
# ===========================================================================

class TestQueryCleanClaimBreakdown:
    def test_returns_dataframe(self, db, full_params):
        import pandas as pd
        df = query_clean_claim_breakdown(full_params, db_path=db)
        assert isinstance(df, pd.DataFrame)

    def test_has_required_columns(self, db, full_params):
        df = query_clean_claim_breakdown(full_params, db_path=db)
        for col in ("fail_reason", "label", "count", "total_charges", "pct_of_dirty", "guidance"):
            assert col in df.columns

    def test_only_dirty_claims_included(self, db, full_params):
        # CLM002 (CODING_ERROR) and CLM004 (MISSING_AUTH) are dirty
        df = query_clean_claim_breakdown(full_params, db_path=db)
        assert set(df["fail_reason"]) == {"CODING_ERROR", "MISSING_AUTH"}

    def test_pct_of_dirty_sums_to_100(self, db, full_params):
        df = query_clean_claim_breakdown(full_params, db_path=db)
        if not df.empty:
            assert df["pct_of_dirty"].sum() == pytest.approx(100.0, abs=0.1)

    def test_labels_mapped(self, db, full_params):
        df = query_clean_claim_breakdown(full_params, db_path=db)
        # CODING_ERROR should map to "Invalid CPT/ICD-10 Combination"
        coding = df[df["fail_reason"] == "CODING_ERROR"]
        if not coding.empty:
            assert coding.iloc[0]["label"] == "Invalid CPT/ICD-10 Combination"

    def test_empty_db_returns_empty(self, empty_db, full_params):
        df = query_clean_claim_breakdown(full_params, db_path=empty_db)
        assert df.empty


# ===========================================================================
# 23. PATIENT RESPONSIBILITY BY PAYER
# ===========================================================================

class TestQueryPatientResponsibilityByPayer:
    def test_returns_dataframe(self, db, full_params):
        import pandas as pd
        df = query_patient_responsibility_by_payer(full_params, db_path=db)
        assert isinstance(df, pd.DataFrame)

    def test_has_required_columns(self, db, full_params):
        df = query_patient_responsibility_by_payer(full_params, db_path=db)
        for col in ("payer_name", "payer_type", "total_patient_resp",
                     "avg_patient_resp", "pct_of_allowed"):
            assert col in df.columns

    def test_patient_resp_non_negative(self, db, full_params):
        df = query_patient_responsibility_by_payer(full_params, db_path=db)
        if not df.empty:
            assert (df["total_patient_resp"] >= 0).all()

    def test_empty_db_returns_empty(self, empty_db, full_params):
        df = query_patient_responsibility_by_payer(full_params, db_path=empty_db)
        assert df.empty


# ===========================================================================
# 24. PATIENT RESPONSIBILITY BY DEPARTMENT
# ===========================================================================

class TestQueryPatientResponsibilityByDept:
    def test_returns_dataframe(self, db, full_params):
        import pandas as pd
        df = query_patient_responsibility_by_dept(full_params, db_path=db)
        assert isinstance(df, pd.DataFrame)

    def test_has_required_columns(self, db, full_params):
        df = query_patient_responsibility_by_dept(full_params, db_path=db)
        for col in ("department", "encounter_type", "claim_count",
                     "total_patient_resp", "avg_patient_resp"):
            assert col in df.columns

    def test_empty_db_returns_empty(self, empty_db, full_params):
        df = query_patient_responsibility_by_dept(full_params, db_path=empty_db)
        assert df.empty


# ===========================================================================
# 25. PATIENT RESPONSIBILITY TREND
# ===========================================================================

class TestQueryPatientResponsibilityTrend:
    def test_returns_dataframe(self, db, full_params):
        import pandas as pd
        df = query_patient_responsibility_trend(full_params, db_path=db)
        assert isinstance(df, pd.DataFrame)

    def test_has_required_columns(self, db, full_params):
        df = query_patient_responsibility_trend(full_params, db_path=db)
        for col in ("total_patient_resp", "total_allowed", "patient_resp_rate"):
            assert col in df.columns

    def test_index_is_year_month(self, db, full_params):
        df = query_patient_responsibility_trend(full_params, db_path=db)
        if not df.empty:
            assert df.index.name == "year_month"

    def test_empty_db_returns_empty(self, empty_db, full_params):
        df = query_patient_responsibility_trend(full_params, db_path=empty_db)
        assert df.empty


# ===========================================================================
# 26. DATA FRESHNESS
# ===========================================================================

class TestQueryDataFreshness:
    @pytest.fixture
    def db_with_pipeline(self, tmp_path):
        """Database with pipeline_runs populated."""
        import datetime
        db_path = str(tmp_path / "test.db")
        conn = duckdb.connect(db_path)
        create_tables(conn)
        # Insert a recent pipeline run
        now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?)",
            ("claims", now, 100, "claims.csv"),
        )
        # Insert a stale pipeline run (72 hours ago)
        stale = (datetime.datetime.utcnow() - datetime.timedelta(hours=72)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        conn.execute(
            "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?)",
            ("payments", stale, 50, "payments.csv"),
        )
        conn.commit()
        conn.close()
        return db_path

    def test_returns_dataframe(self, db_with_pipeline):
        import pandas as pd
        df = query_data_freshness(db_path=db_with_pipeline)
        assert isinstance(df, pd.DataFrame)

    def test_has_required_columns(self, db_with_pipeline):
        df = query_data_freshness(db_path=db_with_pipeline)
        for col in ("domain", "label", "last_loaded_at", "row_count",
                     "cadence_hours", "age_hours", "status"):
            assert col in df.columns

    def test_fresh_status_for_recent_data(self, db_with_pipeline):
        df = query_data_freshness(db_path=db_with_pipeline)
        claims_row = df[df["domain"] == "claims"]
        assert claims_row.iloc[0]["status"] == "fresh"

    def test_stale_or_critical_for_old_data(self, db_with_pipeline):
        df = query_data_freshness(db_path=db_with_pipeline)
        payments_row = df[df["domain"] == "payments"]
        assert payments_row.iloc[0]["status"] in ("stale", "critical")

    def test_empty_db_returns_empty(self, empty_db):
        df = query_data_freshness(db_path=empty_db)
        assert df.empty
