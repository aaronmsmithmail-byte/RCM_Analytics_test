"""Unit tests for src/metrics.py — all 17 RCM metric functions."""

import pandas as pd
import numpy as np
import pytest

from src.metrics import (
    calc_days_in_ar,
    calc_net_collection_rate,
    calc_gross_collection_rate,
    calc_clean_claim_rate,
    calc_denial_rate,
    calc_denial_reasons,
    calc_first_pass_rate,
    calc_charge_lag,
    calc_cost_to_collect,
    calc_ar_aging,
    calc_payment_accuracy,
    calc_bad_debt_rate,
    calc_appeal_success_rate,
    calc_avg_reimbursement,
    calc_payer_mix,
    calc_denial_rate_by_payer,
    calc_department_performance,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def claims():
    return pd.DataFrame({
        "claim_id": [1, 2, 3, 4],
        "encounter_id": [10, 20, 30, 40],
        "patient_id": [100, 200, 300, 400],
        "payer_id": [1, 1, 2, 2],
        "date_of_service": pd.to_datetime(["2024-01-15", "2024-01-20", "2024-02-10", "2024-02-25"]),
        "submission_date": pd.to_datetime(["2024-01-17", "2024-01-22", "2024-02-12", "2024-02-27"]),
        "total_charge_amount": [1000.0, 2000.0, 1500.0, 500.0],
        "claim_status": ["Paid", "Denied", "Paid", "Appealed"],
        "is_clean_claim": [True, False, True, False],
        "submission_method": ["Electronic", "Electronic", "Electronic", "Paper"],
    })


@pytest.fixture
def payments():
    return pd.DataFrame({
        "payment_id": [1, 2, 3],
        "claim_id": [1, 3, 3],
        "payer_id": [1, 2, 2],
        "payment_amount": [900.0, 700.0, 300.0],
        "allowed_amount": [950.0, 750.0, 350.0],
        "payment_date": pd.to_datetime(["2024-02-01", "2024-03-01", "2024-03-15"]),
        "payment_method": ["EFT", "EFT", "Check"],
        "is_accurate_payment": [True, True, False],
    })


@pytest.fixture
def adjustments():
    return pd.DataFrame({
        "adjustment_id": [1, 2, 3],
        "claim_id": [1, 2, 3],
        "adjustment_type_code": ["CONTRACTUAL", "CONTRACTUAL", "WRITEOFF"],
        "adjustment_type_description": ["Contractual", "Contractual", "Write-Off"],
        "adjustment_amount": [50.0, 100.0, 200.0],
        "adjustment_date": pd.to_datetime(["2024-02-01", "2024-02-05", "2024-03-01"]),
    })


@pytest.fixture
def denials():
    return pd.DataFrame({
        "denial_id": [1, 2, 3],
        "claim_id": [2, 4, 2],
        "denial_reason_code": ["CO-4", "CO-97", "CO-4"],
        "denial_reason_description": ["Service not covered", "Payment included in allowance", "Service not covered"],
        "denial_date": pd.to_datetime(["2024-02-01", "2024-03-05", "2024-02-15"]),
        "denied_amount": [2000.0, 500.0, 200.0],
        "appeal_status": ["Won", "Lost", "In Progress"],
        "appeal_date": pd.to_datetime(["2024-03-01", "2024-04-01", None]),
        "recovered_amount": [1800.0, 0.0, 0.0],
    })


@pytest.fixture
def operating_costs():
    return pd.DataFrame({
        "period": pd.to_datetime(["2024-01", "2024-02"], format="%Y-%m"),
        "billing_staff_cost": [10000.0, 10000.0],
        "software_cost": [2000.0, 2000.0],
        "outsourcing_cost": [5000.0, 5000.0],
        "supplies_overhead": [500.0, 500.0],
        "total_rcm_cost": [17500.0, 17500.0],
    })


@pytest.fixture
def payers():
    return pd.DataFrame({
        "payer_id": [1, 2],
        "payer_name": ["Aetna", "Medicare"],
        "payer_type": ["Commercial", "Government"],
        "avg_reimbursement_pct": [0.85, 0.80],
        "contract_id": ["C001", "G001"],
    })


@pytest.fixture
def encounters():
    return pd.DataFrame({
        "encounter_id": [10, 20, 30, 40],
        "patient_id": [100, 200, 300, 400],
        "provider_id": [1, 1, 2, 2],
        "date_of_service": pd.to_datetime(["2024-01-15", "2024-01-20", "2024-02-10", "2024-02-25"]),
        "discharge_date": pd.to_datetime(["2024-01-15", "2024-01-20", "2024-02-11", "2024-02-25"]),
        "encounter_type": ["Outpatient", "Outpatient", "Inpatient", "Emergency"],
        "department": ["Cardiology", "Cardiology", "Orthopedics", "Emergency"],
    })


@pytest.fixture
def charges():
    return pd.DataFrame({
        "charge_id": [1, 2, 3, 4],
        "encounter_id": [10, 20, 30, 40],
        "cpt_code": ["99213", "99214", "27447", "99285"],
        "cpt_description": ["Office Visit", "Office Visit Complex", "Total Knee Replacement", "ED Visit"],
        "units": [1, 1, 1, 1],
        "charge_amount": [200.0, 350.0, 15000.0, 800.0],
        "service_date": pd.to_datetime(["2024-01-15", "2024-01-20", "2024-02-10", "2024-02-25"]),
        "post_date": pd.to_datetime(["2024-01-16", "2024-01-21", "2024-02-12", "2024-02-26"]),
        "icd10_code": ["Z00.00", "Z00.00", "M17.11", "R07.9"],
    })


# ── Helper: empty DataFrames ──────────────────────────────────────────────────

def empty_claims():
    return pd.DataFrame(columns=["claim_id", "encounter_id", "patient_id", "payer_id",
                                  "date_of_service", "submission_date", "total_charge_amount",
                                  "claim_status", "is_clean_claim"])


def empty_payments():
    return pd.DataFrame(columns=["payment_id", "claim_id", "payer_id", "payment_amount",
                                  "payment_date", "is_accurate_payment"])


def empty_denials():
    return pd.DataFrame(columns=["denial_id", "claim_id", "denial_reason_code",
                                  "denial_reason_description", "denied_amount",
                                  "appeal_status", "recovered_amount"])


# ── Tests: calc_days_in_ar ─────────────────────────────────────────────────────

class TestCalcDaysInAr:
    def test_returns_float_and_dataframe(self, claims, payments):
        dar, trend = calc_days_in_ar(claims, payments)
        assert isinstance(dar, float)
        assert isinstance(trend, pd.DataFrame)

    def test_trend_has_required_columns(self, claims, payments):
        _, trend = calc_days_in_ar(claims, payments)
        assert "days_in_ar" in trend.columns
        assert "ar_balance" in trend.columns

    def test_dar_is_non_negative(self, claims, payments):
        dar, _ = calc_days_in_ar(claims, payments)
        assert dar >= 0

    def test_empty_claims_returns_zero(self):
        dar, trend = calc_days_in_ar(empty_claims(), empty_payments())
        assert dar == 0.0
        assert trend.empty


# ── Tests: calc_net_collection_rate ───────────────────────────────────────────

class TestCalcNetCollectionRate:
    def test_returns_float_and_dataframe(self, claims, payments, adjustments):
        ncr, trend = calc_net_collection_rate(claims, payments, adjustments)
        assert isinstance(ncr, float)
        assert isinstance(trend, pd.DataFrame)

    def test_ncr_between_0_and_100(self, claims, payments, adjustments):
        # Total payments=1900, charges=5000, contractual_adj=150 → ncr = 1900/4850 * 100 ≈ 39.2%
        ncr, _ = calc_net_collection_rate(claims, payments, adjustments)
        assert 0 <= ncr <= 100

    def test_ncr_correct_value(self, claims, payments, adjustments):
        # payments: 900+700+300=1900, charges: 5000, contractual adj: 50+100=150
        ncr, _ = calc_net_collection_rate(claims, payments, adjustments)
        expected = round(1900 / (5000 - 150) * 100, 2)
        assert ncr == pytest.approx(expected, abs=0.01)

    def test_empty_claims_returns_zero(self, adjustments):
        ncr, trend = calc_net_collection_rate(empty_claims(), empty_payments(), adjustments)
        assert ncr == 0.0
        assert trend.empty

    def test_ncr_100_when_payments_equal_net_charges(self):
        claims = pd.DataFrame({
            "claim_id": [1],
            "date_of_service": pd.to_datetime(["2024-01-01"]),
            "submission_date": pd.to_datetime(["2024-01-02"]),
            "total_charge_amount": [1000.0],
        })
        payments = pd.DataFrame({
            "claim_id": [1],
            "payment_amount": [800.0],
        })
        adjustments = pd.DataFrame({
            "claim_id": [1],
            "adjustment_type_code": ["CONTRACTUAL"],
            "adjustment_amount": [200.0],
        })
        ncr, _ = calc_net_collection_rate(claims, payments, adjustments)
        assert ncr == pytest.approx(100.0, abs=0.01)


# ── Tests: calc_gross_collection_rate ─────────────────────────────────────────

class TestCalcGrossCollectionRate:
    def test_returns_float_and_dataframe(self, claims, payments):
        gcr, trend = calc_gross_collection_rate(claims, payments)
        assert isinstance(gcr, float)
        assert isinstance(trend, pd.DataFrame)

    def test_gcr_between_0_and_100(self, claims, payments):
        gcr, _ = calc_gross_collection_rate(claims, payments)
        assert 0 <= gcr <= 100

    def test_gcr_correct_value(self, claims, payments):
        # payments=1900, charges=5000 → 38%
        gcr, _ = calc_gross_collection_rate(claims, payments)
        assert gcr == pytest.approx(38.0, abs=0.01)

    def test_empty_claims_returns_zero(self):
        gcr, trend = calc_gross_collection_rate(empty_claims(), empty_payments())
        assert gcr == 0.0
        assert trend.empty


# ── Tests: calc_clean_claim_rate ──────────────────────────────────────────────

class TestCalcCleanClaimRate:
    def test_returns_float_and_dataframe(self, claims):
        ccr, trend = calc_clean_claim_rate(claims)
        assert isinstance(ccr, float)
        assert isinstance(trend, pd.DataFrame)

    def test_ccr_correct_value(self, claims):
        # 2 clean out of 4 = 50%
        ccr, _ = calc_clean_claim_rate(claims)
        assert ccr == pytest.approx(50.0, abs=0.01)

    def test_ccr_100_when_all_clean(self):
        claims = pd.DataFrame({
            "claim_id": [1, 2],
            "submission_date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "is_clean_claim": [True, True],
        })
        ccr, _ = calc_clean_claim_rate(claims)
        assert ccr == pytest.approx(100.0)

    def test_ccr_0_when_none_clean(self):
        claims = pd.DataFrame({
            "claim_id": [1, 2],
            "submission_date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "is_clean_claim": [False, False],
        })
        ccr, _ = calc_clean_claim_rate(claims)
        assert ccr == pytest.approx(0.0)

    def test_empty_claims_returns_zero(self):
        ccr, trend = calc_clean_claim_rate(empty_claims())
        assert ccr == 0.0
        assert trend.empty


# ── Tests: calc_denial_rate ───────────────────────────────────────────────────

class TestCalcDenialRate:
    def test_returns_float_and_dataframe(self, claims):
        rate, trend = calc_denial_rate(claims)
        assert isinstance(rate, float)
        assert isinstance(trend, pd.DataFrame)

    def test_denial_rate_correct_value(self, claims):
        # 2 denied/appealed out of 4 = 50%
        rate, _ = calc_denial_rate(claims)
        assert rate == pytest.approx(50.0, abs=0.01)

    def test_denial_rate_zero_when_no_denials(self):
        claims = pd.DataFrame({
            "claim_id": [1, 2],
            "submission_date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "claim_status": ["Paid", "Paid"],
        })
        rate, _ = calc_denial_rate(claims)
        assert rate == 0.0

    def test_empty_claims_returns_zero(self):
        rate, trend = calc_denial_rate(empty_claims())
        assert rate == 0.0
        assert trend.empty


# ── Tests: calc_denial_reasons ────────────────────────────────────────────────

class TestCalcDenialReasons:
    def test_returns_dataframe(self, denials):
        result = calc_denial_reasons(denials)
        assert isinstance(result, pd.DataFrame)

    def test_groups_by_reason_code(self, denials):
        result = calc_denial_reasons(denials)
        # CO-4 appears twice in fixture
        co4_row = result[result["denial_reason_code"] == "CO-4"]
        assert len(co4_row) == 1
        assert co4_row["count"].values[0] == 2

    def test_recovery_rate_calculated(self, denials):
        result = calc_denial_reasons(denials)
        co4 = result[result["denial_reason_code"] == "CO-4"]
        # total_denied=2200, recovered=1800 → ~81.8%
        assert co4["recovery_rate"].values[0] == pytest.approx(1800 / 2200 * 100, abs=0.1)

    def test_empty_denials_returns_empty_dataframe(self):
        result = calc_denial_reasons(empty_denials())
        assert isinstance(result, pd.DataFrame)
        assert result.empty


# ── Tests: calc_first_pass_rate ───────────────────────────────────────────────

class TestCalcFirstPassRate:
    def test_returns_float_and_dataframe(self, claims):
        rate, trend = calc_first_pass_rate(claims)
        assert isinstance(rate, float)
        assert isinstance(trend, pd.DataFrame)

    def test_first_pass_correct_value(self, claims):
        # 2 paid out of 4 = 50%
        rate, _ = calc_first_pass_rate(claims)
        assert rate == pytest.approx(50.0, abs=0.01)

    def test_empty_claims_returns_zero(self):
        rate, trend = calc_first_pass_rate(empty_claims())
        assert rate == 0.0
        assert trend.empty


# ── Tests: calc_charge_lag ────────────────────────────────────────────────────

class TestCalcChargeLag:
    def test_returns_float_series_series(self, charges):
        avg_lag, trend, distribution = calc_charge_lag(charges)
        assert isinstance(avg_lag, float)
        assert isinstance(trend, pd.Series)
        assert isinstance(distribution, pd.Series)

    def test_avg_lag_correct_value(self, charges):
        # Lags: 1, 1, 2, 1 days → mean=1.25, round(1.25, 1)=1.2
        avg_lag, _, _ = calc_charge_lag(charges)
        assert avg_lag == pytest.approx(1.2, abs=0.01)

    def test_avg_lag_non_negative(self, charges):
        avg_lag, _, _ = calc_charge_lag(charges)
        assert avg_lag >= 0

    def test_empty_charges_returns_zero(self):
        empty = pd.DataFrame(columns=["charge_id", "encounter_id", "charge_amount",
                                       "service_date", "post_date"])
        avg_lag, trend, dist = calc_charge_lag(empty)
        assert avg_lag == 0.0


# ── Tests: calc_cost_to_collect ───────────────────────────────────────────────

class TestCalcCostToCollect:
    def test_returns_float_and_dataframe(self, operating_costs, claims, payments):
        ctc, trend = calc_cost_to_collect(operating_costs, claims, payments)
        assert isinstance(ctc, float)
        assert isinstance(trend, pd.DataFrame)

    def test_cost_to_collect_correct_value(self, operating_costs, claims, payments):
        # total_cost=35000, total_collected=1900 → ~1842%
        ctc, _ = calc_cost_to_collect(operating_costs, claims, payments)
        expected = round(35000 / 1900 * 100, 2)
        assert ctc == pytest.approx(expected, abs=0.01)

    def test_empty_payments_returns_zero(self, operating_costs, claims):
        ctc, trend = calc_cost_to_collect(operating_costs, claims, empty_payments())
        assert ctc == 0.0
        assert trend.empty


# ── Tests: calc_ar_aging ──────────────────────────────────────────────────────

class TestCalcArAging:
    def test_returns_dataframe_and_float(self, claims, payments):
        summary, total_ar = calc_ar_aging(claims, payments)
        assert isinstance(summary, pd.DataFrame)
        assert isinstance(total_ar, (float, np.floating, int))

    def test_summary_has_five_buckets(self, claims, payments):
        summary, _ = calc_ar_aging(claims, payments)
        assert set(summary.index) == {"0-30", "31-60", "61-90", "91-120", "120+"}

    def test_total_ar_non_negative(self, claims, payments):
        _, total_ar = calc_ar_aging(claims, payments)
        assert total_ar >= 0

    def test_pct_sums_to_100_or_zero(self, claims, payments):
        summary, total_ar = calc_ar_aging(claims, payments)
        if total_ar > 0:
            assert summary["pct_of_total"].sum() == pytest.approx(100.0, abs=0.01)

    def test_empty_claims_returns_zero_totals(self):
        summary, total_ar = calc_ar_aging(empty_claims(), empty_payments())
        assert total_ar == 0.0
        assert summary["total_ar"].sum() == 0


# ── Tests: calc_payment_accuracy ─────────────────────────────────────────────

class TestCalcPaymentAccuracy:
    def test_returns_float(self, payments):
        rate = calc_payment_accuracy(payments)
        assert isinstance(rate, float)

    def test_accuracy_correct_value(self, payments):
        # 2 accurate out of 3 = 66.67%
        rate = calc_payment_accuracy(payments)
        assert rate == pytest.approx(66.67, abs=0.01)

    def test_accuracy_100_when_all_accurate(self):
        payments = pd.DataFrame({
            "payment_id": [1, 2],
            "is_accurate_payment": [True, True],
        })
        assert calc_payment_accuracy(payments) == pytest.approx(100.0)

    def test_empty_payments_returns_zero(self):
        assert calc_payment_accuracy(empty_payments()) == 0.0


# ── Tests: calc_bad_debt_rate ─────────────────────────────────────────────────

class TestCalcBadDebtRate:
    def test_returns_three_values(self, claims, adjustments):
        result = calc_bad_debt_rate(claims, adjustments)
        assert len(result) == 3

    def test_bad_debt_correct_value(self, claims, adjustments):
        # WRITEOFF=200, total_charges=5000 → 4%
        rate, bad_debt, total = calc_bad_debt_rate(claims, adjustments)
        assert rate == pytest.approx(4.0, abs=0.01)
        assert bad_debt == pytest.approx(200.0)
        assert total == pytest.approx(5000.0)

    def test_empty_claims_returns_zeros(self, adjustments):
        rate, bad_debt, total = calc_bad_debt_rate(empty_claims(), adjustments)
        assert rate == 0.0
        assert bad_debt == 0.0
        assert total == 0.0


# ── Tests: calc_appeal_success_rate ──────────────────────────────────────────

class TestCalcAppealSuccessRate:
    def test_returns_three_values(self, denials):
        result = calc_appeal_success_rate(denials)
        assert len(result) == 3

    def test_appeal_success_correct_value(self, denials):
        # Won=1, Lost=1, In Progress=1 → total_appealed=3, won=1 → 33.33%
        rate, total_appealed, won = calc_appeal_success_rate(denials)
        assert rate == pytest.approx(33.33, abs=0.01)
        assert total_appealed == 3
        assert won == 1

    def test_empty_denials_returns_zeros(self):
        rate, total_appealed, won = calc_appeal_success_rate(empty_denials())
        assert rate == 0.0
        assert total_appealed == 0
        assert won == 0


# ── Tests: calc_avg_reimbursement ─────────────────────────────────────────────

class TestCalcAvgReimbursement:
    def test_returns_float_and_series(self, claims, payments):
        avg, trend = calc_avg_reimbursement(claims, payments)
        assert isinstance(avg, float)
        assert isinstance(trend, pd.Series)

    def test_avg_reimbursement_non_negative(self, claims, payments):
        avg, _ = calc_avg_reimbursement(claims, payments)
        assert avg >= 0

    def test_avg_reimbursement_correct_value(self, claims, payments):
        # claim 1: 900, claim 3: 1000, claims 2 & 4: 0 → avg = (900+0+1000+0)/4 = 475
        avg, _ = calc_avg_reimbursement(claims, payments)
        assert avg == pytest.approx(475.0, abs=0.01)

    def test_empty_claims_returns_zero(self):
        avg, trend = calc_avg_reimbursement(empty_claims(), empty_payments())
        assert avg == 0.0
        assert trend.empty


# ── Tests: calc_payer_mix ─────────────────────────────────────────────────────

class TestCalcPayerMix:
    def test_returns_dataframe(self, claims, payments, payers):
        result = calc_payer_mix(claims, payments, payers)
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, claims, payments, payers):
        result = calc_payer_mix(claims, payments, payers)
        for col in ["payer_name", "payer_type", "claim_count", "total_charges",
                    "total_payments", "collection_rate"]:
            assert col in result.columns

    def test_collection_rate_between_0_and_100(self, claims, payments, payers):
        result = calc_payer_mix(claims, payments, payers)
        assert (result["collection_rate"] >= 0).all()
        assert (result["collection_rate"] <= 100).all()

    def test_empty_claims_returns_empty_dataframe(self, payers):
        result = calc_payer_mix(empty_claims(), empty_payments(), payers)
        assert result.empty


# ── Tests: calc_denial_rate_by_payer ─────────────────────────────────────────

class TestCalcDenialRateByPayer:
    def test_returns_dataframe(self, claims, payers):
        result = calc_denial_rate_by_payer(claims, payers)
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, claims, payers):
        result = calc_denial_rate_by_payer(claims, payers)
        for col in ["payer_name", "total_claims", "denied", "denial_rate"]:
            assert col in result.columns

    def test_denial_rate_between_0_and_100(self, claims, payers):
        result = calc_denial_rate_by_payer(claims, payers)
        assert (result["denial_rate"] >= 0).all()
        assert (result["denial_rate"] <= 100).all()

    def test_correct_denial_counts(self, claims, payers):
        result = calc_denial_rate_by_payer(claims, payers)
        # Payer 1 (Aetna): claims 1(Paid) and 2(Denied) → 1 denied out of 2 = 50%
        aetna = result[result["payer_name"] == "Aetna"]
        assert aetna["denied"].values[0] == 1
        assert aetna["denial_rate"].values[0] == pytest.approx(50.0)

    def test_empty_claims_returns_empty_dataframe(self, payers):
        result = calc_denial_rate_by_payer(empty_claims(), payers)
        assert result.empty


# ── Tests: calc_department_performance ───────────────────────────────────────

class TestCalcDepartmentPerformance:
    def test_returns_dataframe(self, encounters, claims, payments):
        result = calc_department_performance(encounters, claims, payments)
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, encounters, claims, payments):
        result = calc_department_performance(encounters, claims, payments)
        for col in ["department", "encounter_count", "total_charges",
                    "total_payments", "collection_rate", "avg_payment_per_encounter"]:
            assert col in result.columns

    def test_collection_rate_between_0_and_100(self, encounters, claims, payments):
        result = calc_department_performance(encounters, claims, payments)
        assert (result["collection_rate"] >= 0).all()
        assert (result["collection_rate"] <= 100).all()

    def test_departments_present(self, encounters, claims, payments):
        result = calc_department_performance(encounters, claims, payments)
        assert "Cardiology" in result["department"].values

    def test_empty_encounters_returns_empty_dataframe(self, claims, payments):
        empty_enc = pd.DataFrame(columns=["encounter_id", "patient_id", "provider_id",
                                           "date_of_service", "department", "encounter_type"])
        result = calc_department_performance(empty_enc, claims, payments)
        assert result.empty
