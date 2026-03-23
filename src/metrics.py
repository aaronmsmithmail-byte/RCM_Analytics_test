"""
RCM Performance Metrics — Full SQL Pipeline
============================================

This module implements all 17 Revenue Cycle Management KPIs as parameterized
SQL queries executing directly against the Silver layer of the medallion
architecture.  Moving computation to the database layer means:

  - The Gold layer is the authoritative computation layer (medallion purity).
  - No DataFrames are loaded for filtering — SQL WHERE clauses do the work.
  - The database engine can optimise joins and aggregations.
  - The code scales to larger datasets without memory pressure.

Core pattern
------------
Every metric function accepts a :class:`FilterParams` instance that captures
the four interactive sidebar filter dimensions (date range, payer, department,
encounter type).  The :func:`_cte` helper converts FilterParams into a
``WITH filtered_claims AS (...)`` CTE that joins silver_claims to
silver_encounters so all four dimensions can be applied in one place.
Downstream metric SQL then selects from ``filtered_claims``.

Metrics Implemented (17 total)
-------------------------------
Financial Performance:
    1.  Days in A/R (DAR)
    2.  Net Collection Rate (NCR)
    3.  Gross Collection Rate (GCR)
    4.  Cost to Collect (CTC)
    5.  Bad Debt Rate
    6.  Avg Reimbursement per Encounter

Claims Quality:
    7.  Clean Claim Rate (CCR)
    8.  Claim Denial Rate
    9.  First-Pass Resolution Rate (FPRR)
    10. Charge Lag
    11. Denial Reasons Breakdown

Recovery & Appeals:
    12. Appeal Success Rate
    13. A/R Aging Buckets
    14. Payment Accuracy Rate

Segmentation:
    15. Payer Mix Analysis
    16. Denial Rate by Payer
    17. Department Performance

Return-type contract
--------------------
Each function returns the same types as the previous calc_* equivalents so
that app.py chart code needs minimal changes:

    query_days_in_ar        -> (float, DataFrame[period,charges,payments,ar_balance,days_in_ar])
    query_net_collection_rate -> (float, DataFrame[period,charges,payments,contractual_adj,ncr])
    query_gross_collection_rate -> (float, DataFrame[period,charges,payments,gcr])
    query_clean_claim_rate  -> (float, DataFrame[period,total_claims,clean_claims,ccr])
    query_denial_rate       -> (float, DataFrame[period,total_claims,denied_claims,denial_rate])
    query_denial_reasons    -> DataFrame[denial_reason_code,denial_reason_description,count,
                                        total_denied_amount,total_recovered,recovery_rate]
    query_first_pass_rate   -> (float, DataFrame[period,total,paid,fpr])
    query_charge_lag        -> (float, Series[monthly_avg_lag], Series[lag_distribution])
    query_cost_to_collect   -> (float, DataFrame[period,rcm_cost,collections,cost_to_collect_pct])
    query_ar_aging          -> (DataFrame[claim_count,total_ar,pct_of_total], float)
    query_payment_accuracy  -> float
    query_bad_debt_rate     -> (float, float, float)   # rate, bad_debt_amount, total_charges
    query_appeal_success_rate -> (float, int, int)     # rate, total_appealed, won
    query_avg_reimbursement -> (float, Series[monthly_avg])
    query_payer_mix         -> DataFrame[payer_id,payer_name,payer_type,
                                        claim_count,total_charges,total_payments,collection_rate]
    query_denial_rate_by_payer -> DataFrame[payer_id,payer_name,total_claims,denied,denial_rate]
    query_department_performance -> DataFrame[department,encounter_count,total_charges,
                                             total_payments,collection_rate,avg_payment_per_encounter]
"""

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from src.database import build_filter_cte, get_connection, query_to_dataframe


# ===========================================================================
# Filter parameter container
# ===========================================================================

@dataclass
class FilterParams:
    """All four sidebar filter dimensions in one object.

    Args:
        start_date:     Lower bound for date_of_service ('YYYY-MM-DD', inclusive).
        end_date:       Upper bound for date_of_service ('YYYY-MM-DD', inclusive).
        payer_id:       Payer ID to restrict to, or None for all payers.
        department:     Department name to restrict to, or None for all.
        encounter_type: Encounter type to restrict to, or None for all.
    """
    start_date: str
    end_date: str
    payer_id: Optional[str] = None
    department: Optional[str] = None
    encounter_type: Optional[str] = None


# ===========================================================================
# Internal helpers
# ===========================================================================

def _cte(p: FilterParams):
    """Return (cte_sql, params) for the filtered_claims CTE."""
    return build_filter_cte(
        p.start_date, p.end_date,
        payer_id=p.payer_id,
        department=p.department,
        encounter_type=p.encounter_type,
    )


def _empty_trend(*columns):
    """Return an empty DataFrame with the given columns."""
    return pd.DataFrame(columns=list(columns))


def _set_period_index(df: pd.DataFrame) -> pd.DataFrame:
    """Set 'period' column as the index and rename it to 'year_month'."""
    df = df.set_index("period")
    df.index.name = "year_month"
    return df


# ===========================================================================
# 1. DAYS IN ACCOUNTS RECEIVABLE (DAR)
# ===========================================================================

def query_days_in_ar(p: FilterParams, db_path=None):
    """Calculate Days in Accounts Receivable (DAR).

    Returns:
        tuple: (overall_dar, trend_dataframe)
            overall_dar: most recent month's DAR value (float).
            trend_dataframe: DataFrame indexed by year_month with columns
                [charges, payments, ar_balance, days_in_ar].
    """
    cte, params = _cte(p)
    # Charges and payments are aggregated in separate CTEs to avoid
    # row duplication when a claim has multiple payments (a LEFT JOIN
    # to silver_payments would produce N rows per claim with N payments,
    # causing SUM(charge_amount) to be overcounted by a factor of N).
    sql = cte + """
, monthly_charges AS (
    SELECT strftime('%Y-%m', date_of_service) AS period,
           SUM(total_charge_amount)           AS charges
    FROM filtered_claims
    GROUP BY strftime('%Y-%m', date_of_service)
), monthly_payments AS (
    SELECT strftime('%Y-%m', fc.date_of_service) AS period,
           COALESCE(SUM(p.payment_amount), 0)    AS payments
    FROM filtered_claims fc
    LEFT JOIN silver_payments p ON fc.claim_id = p.claim_id
    GROUP BY strftime('%Y-%m', fc.date_of_service)
)
SELECT c.period, c.charges, COALESCE(mp.payments, 0) AS payments
FROM monthly_charges c
LEFT JOIN monthly_payments mp ON c.period = mp.period
ORDER BY c.period
"""
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return 0.0, _empty_trend("charges", "payments", "ar_balance", "days_in_ar")

    df["ar_balance"] = df["charges"].cumsum() - df["payments"].cumsum()
    df["avg_daily_charges"] = df["charges"] / 30
    df["days_in_ar"] = np.where(
        df["avg_daily_charges"] > 0,
        df["ar_balance"] / df["avg_daily_charges"],
        0,
    )
    df = _set_period_index(df)
    overall_dar = df["days_in_ar"].iloc[-1] if len(df) > 0 else 0.0
    return round(float(overall_dar), 1), df[["charges", "payments", "ar_balance", "days_in_ar"]]


# ===========================================================================
# 2. NET COLLECTION RATE (NCR)
# ===========================================================================

def query_net_collection_rate(p: FilterParams, db_path=None):
    """Calculate Net Collection Rate (NCR).

    NCR = Payments / (Charges - Contractual Adjustments) * 100

    Returns:
        tuple: (ncr_percentage, trend_dataframe)
    """
    cte, params = _cte(p)
    # Use three separate monthly CTEs to prevent charge duplication when a
    # claim has multiple payments or multiple adjustments.
    sql = cte + """
, monthly_charges AS (
    SELECT strftime('%Y-%m', date_of_service) AS period,
           SUM(total_charge_amount)           AS charges
    FROM filtered_claims
    GROUP BY strftime('%Y-%m', date_of_service)
), monthly_payments AS (
    SELECT strftime('%Y-%m', fc.date_of_service) AS period,
           COALESCE(SUM(p.payment_amount), 0)    AS payments
    FROM filtered_claims fc
    LEFT JOIN silver_payments p ON fc.claim_id = p.claim_id
    GROUP BY strftime('%Y-%m', fc.date_of_service)
), monthly_contractual AS (
    SELECT strftime('%Y-%m', fc.date_of_service) AS period,
           COALESCE(SUM(CASE WHEN a.adjustment_type_code = 'CONTRACTUAL'
                             THEN a.adjustment_amount ELSE 0 END), 0) AS contractual_adj
    FROM filtered_claims fc
    LEFT JOIN silver_adjustments a ON fc.claim_id = a.claim_id
    GROUP BY strftime('%Y-%m', fc.date_of_service)
)
SELECT c.period,
       c.charges,
       COALESCE(mp.payments, 0)       AS payments,
       COALESCE(mc.contractual_adj, 0) AS contractual_adj
FROM monthly_charges c
LEFT JOIN monthly_payments    mp ON c.period = mp.period
LEFT JOIN monthly_contractual mc ON c.period = mc.period
ORDER BY c.period
"""
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return 0.0, _empty_trend("charges", "payments", "contractual_adj", "ncr")

    total_charges = df["charges"].sum()
    total_payments = df["payments"].sum()
    total_contractual = df["contractual_adj"].sum()
    denominator = total_charges - total_contractual
    ncr = (total_payments / denominator * 100) if denominator > 0 else 0.0

    df["ncr"] = np.where(
        (df["charges"] - df["contractual_adj"]) > 0,
        df["payments"] / (df["charges"] - df["contractual_adj"]) * 100,
        0,
    )
    df = _set_period_index(df)
    return round(float(ncr), 2), df


# ===========================================================================
# 3. GROSS COLLECTION RATE (GCR)
# ===========================================================================

def query_gross_collection_rate(p: FilterParams, db_path=None):
    """Calculate Gross Collection Rate (GCR).

    GCR = Payments / Charges * 100

    Returns:
        tuple: (gcr_percentage, trend_dataframe)
    """
    cte, params = _cte(p)
    sql = cte + """
, monthly_charges AS (
    SELECT strftime('%Y-%m', date_of_service) AS period,
           SUM(total_charge_amount)           AS charges
    FROM filtered_claims
    GROUP BY strftime('%Y-%m', date_of_service)
), monthly_payments AS (
    SELECT strftime('%Y-%m', fc.date_of_service) AS period,
           COALESCE(SUM(p.payment_amount), 0)    AS payments
    FROM filtered_claims fc
    LEFT JOIN silver_payments p ON fc.claim_id = p.claim_id
    GROUP BY strftime('%Y-%m', fc.date_of_service)
)
SELECT c.period, c.charges, COALESCE(mp.payments, 0) AS payments
FROM monthly_charges c
LEFT JOIN monthly_payments mp ON c.period = mp.period
ORDER BY c.period
"""
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return 0.0, _empty_trend("charges", "payments", "gcr")

    total_charges = df["charges"].sum()
    total_payments = df["payments"].sum()
    gcr = (total_payments / total_charges * 100) if total_charges > 0 else 0.0

    df["gcr"] = np.where(df["charges"] > 0, df["payments"] / df["charges"] * 100, 0)
    df = _set_period_index(df)
    return round(float(gcr), 2), df


# ===========================================================================
# 4. CLEAN CLAIM RATE (CCR)
# ===========================================================================

def query_clean_claim_rate(p: FilterParams, db_path=None):
    """Calculate Clean Claim Rate (CCR).

    CCR = Clean Claims / Total Claims * 100

    Returns:
        tuple: (ccr_percentage, trend_dataframe)
    """
    cte, params = _cte(p)
    sql = cte + """
SELECT strftime('%Y-%m', submission_date)       AS period,
       COUNT(*)                                 AS total_claims,
       SUM(is_clean_claim)                      AS clean_claims
FROM filtered_claims
GROUP BY strftime('%Y-%m', submission_date)
ORDER BY period
"""
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return 0.0, _empty_trend("total_claims", "clean_claims", "ccr")

    total = df["total_claims"].sum()
    clean = df["clean_claims"].sum()
    ccr = (clean / total * 100) if total > 0 else 0.0

    df["ccr"] = np.where(df["total_claims"] > 0, df["clean_claims"] / df["total_claims"] * 100, 0)
    df = _set_period_index(df)
    return round(float(ccr), 2), df


# ===========================================================================
# 5. CLAIM DENIAL RATE
# ===========================================================================

def query_denial_rate(p: FilterParams, db_path=None):
    """Calculate Claim Denial Rate.

    Denial Rate = (Denied + Appealed Claims) / Total Claims * 100

    Returns:
        tuple: (denial_rate_percentage, trend_dataframe)
    """
    cte, params = _cte(p)
    sql = cte + """
SELECT strftime('%Y-%m', submission_date) AS period,
       COUNT(*)                           AS total_claims,
       SUM(CASE WHEN claim_status IN ('Denied', 'Appealed')
               THEN 1 ELSE 0 END)         AS denied_claims
FROM filtered_claims
GROUP BY strftime('%Y-%m', submission_date)
ORDER BY period
"""
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return 0.0, _empty_trend("total_claims", "denied_claims", "denial_rate")

    total = df["total_claims"].sum()
    denied = df["denied_claims"].sum()
    rate = (denied / total * 100) if total > 0 else 0.0

    df["denial_rate"] = np.where(
        df["total_claims"] > 0, df["denied_claims"] / df["total_claims"] * 100, 0
    )
    df = _set_period_index(df)
    return round(float(rate), 2), df


# ===========================================================================
# 6. DENIAL REASONS BREAKDOWN
# ===========================================================================

def query_denial_reasons(p: FilterParams, db_path=None):
    """Aggregate denials by reason code to identify root causes.

    Returns:
        DataFrame with columns [denial_reason_code, denial_reason_description,
        count, total_denied_amount, total_recovered, recovery_rate],
        sorted by count descending.
    """
    cte, params = _cte(p)
    sql = cte + """
SELECT d.denial_reason_code,
       d.denial_reason_description,
       COUNT(*)                              AS count,
       SUM(d.denied_amount)                 AS total_denied_amount,
       COALESCE(SUM(d.recovered_amount), 0) AS total_recovered
FROM filtered_claims fc
JOIN silver_denials d ON fc.claim_id = d.claim_id
GROUP BY d.denial_reason_code, d.denial_reason_description
ORDER BY count DESC
"""
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return pd.DataFrame(columns=[
            "denial_reason_code", "denial_reason_description",
            "count", "total_denied_amount", "total_recovered", "recovery_rate",
        ])

    df["recovery_rate"] = np.where(
        df["total_denied_amount"] > 0,
        df["total_recovered"] / df["total_denied_amount"] * 100,
        0,
    )
    return df


# ===========================================================================
# 7. FIRST-PASS RESOLUTION RATE (FPRR)
# ===========================================================================

def query_first_pass_rate(p: FilterParams, db_path=None):
    """Calculate First-Pass Resolution Rate (FPRR).

    FPRR = Claims paid on first submission / Total Claims * 100

    Returns:
        tuple: (fpr_percentage, trend_dataframe)
    """
    cte, params = _cte(p)
    sql = cte + """
SELECT strftime('%Y-%m', submission_date) AS period,
       COUNT(*)                           AS total,
       SUM(CASE WHEN claim_status = 'Paid' THEN 1 ELSE 0 END) AS paid
FROM filtered_claims
GROUP BY strftime('%Y-%m', submission_date)
ORDER BY period
"""
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return 0.0, _empty_trend("total", "paid", "fpr")

    total = df["total"].sum()
    paid = df["paid"].sum()
    rate = (paid / total * 100) if total > 0 else 0.0

    df["fpr"] = np.where(df["total"] > 0, df["paid"] / df["total"] * 100, 0)
    df = _set_period_index(df)
    return round(float(rate), 2), df


# ===========================================================================
# 8. AVERAGE CHARGE LAG (Days)
# ===========================================================================

def query_charge_lag(p: FilterParams, db_path=None):
    """Calculate Average Charge Lag.

    Charge Lag = Post Date - Service Date (days)

    Returns:
        tuple: (avg_lag_days, monthly_trend_series, lag_distribution_series)
    """
    cte, params = _cte(p)
    # Charges for encounters that appear in filtered claims
    sql = cte + """
SELECT strftime('%Y-%m', ch.service_date)                              AS period,
       CAST(julianday(ch.post_date) - julianday(ch.service_date)
            AS INTEGER)                                                AS lag_days
FROM silver_charges ch
WHERE ch.encounter_id IN (SELECT DISTINCT encounter_id FROM filtered_claims)
  AND ch.post_date   IS NOT NULL
  AND ch.service_date IS NOT NULL
"""
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return 0.0, pd.Series(dtype=float), pd.Series(dtype=float)

    avg_lag = df["lag_days"].mean()

    trend = df.groupby("period")["lag_days"].mean()
    trend.index.name = "year_month"

    distribution = df["lag_days"].value_counts().sort_index()
    return round(float(avg_lag), 1), trend, distribution


# ===========================================================================
# 9. COST TO COLLECT (CTC)
# ===========================================================================

def query_cost_to_collect(p: FilterParams, db_path=None):
    """Calculate Cost to Collect.

    CTC = Total RCM Operating Costs / Total Collections * 100

    Returns:
        tuple: (ctc_percentage, trend_dataframe)
    """
    cte, params = _cte(p)
    # Monthly collections from filtered claims
    sql = cte + """
SELECT strftime('%Y-%m', fc.date_of_service) AS period,
       COALESCE(SUM(p.payment_amount), 0)    AS collections
FROM filtered_claims fc
LEFT JOIN silver_payments p ON fc.claim_id = p.claim_id
GROUP BY strftime('%Y-%m', fc.date_of_service)
ORDER BY period
"""
    collections_df = query_to_dataframe(sql, params=params, db_path=db_path)
    if collections_df.empty:
        return 0.0, _empty_trend("rcm_cost", "collections", "cost_to_collect_pct")

    costs_df = query_to_dataframe(
        "SELECT period, total_rcm_cost AS rcm_cost FROM silver_operating_costs",
        db_path=db_path,
    )

    trend = collections_df.merge(costs_df, on="period", how="left").fillna(0)
    trend["cost_to_collect_pct"] = np.where(
        trend["collections"] > 0,
        trend["rcm_cost"] / trend["collections"] * 100,
        0,
    )

    total_cost = costs_df["rcm_cost"].sum()
    total_collected = collections_df["collections"].sum()
    ctc = (total_cost / total_collected * 100) if total_collected > 0 else 0.0

    trend = _set_period_index(trend)
    return round(float(ctc), 2), trend


# ===========================================================================
# 10. A/R AGING BUCKETS
# ===========================================================================

def query_ar_aging(p: FilterParams, db_path=None):
    """Categorize outstanding A/R into aging buckets.

    Returns:
        tuple: (aging_summary_dataframe, total_ar_balance)
            aging_summary: DataFrame indexed by bucket name
                           with columns [claim_count, total_ar, pct_of_total].
    """
    empty_summary = pd.DataFrame(
        {"claim_count": 0, "total_ar": 0.0, "pct_of_total": 0.0},
        index=["0-30", "31-60", "61-90", "91-120", "120+"],
    )

    cte, params = _cte(p)
    sql = cte + """
SELECT fc.claim_id,
       fc.date_of_service,
       fc.total_charge_amount - COALESCE(SUM(p.payment_amount), 0) AS ar_balance,
       CAST(julianday('now') - julianday(fc.date_of_service) AS INTEGER) AS days_outstanding
FROM filtered_claims fc
LEFT JOIN silver_payments p ON fc.claim_id = p.claim_id
GROUP BY fc.claim_id, fc.date_of_service, fc.total_charge_amount
HAVING ar_balance > 0
"""
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return empty_summary, 0.0

    def _bucket(days):
        if days <= 30:
            return "0-30"
        elif days <= 60:
            return "31-60"
        elif days <= 90:
            return "61-90"
        elif days <= 120:
            return "91-120"
        return "120+"

    df["aging_bucket"] = df["days_outstanding"].apply(_bucket)
    summary = df.groupby("aging_bucket").agg(
        claim_count=("claim_id", "count"),
        total_ar=("ar_balance", "sum"),
    ).reindex(["0-30", "31-60", "61-90", "91-120", "120+"]).fillna(0)

    total_ar = summary["total_ar"].sum()
    summary["pct_of_total"] = np.where(total_ar > 0, summary["total_ar"] / total_ar * 100, 0)
    return summary, float(total_ar)


# ===========================================================================
# 11. PAYMENT ACCURACY RATE
# ===========================================================================

def query_payment_accuracy(p: FilterParams, db_path=None):
    """Calculate Payment Accuracy Rate.

    Returns:
        float: Accuracy rate as a percentage.
    """
    cte, params = _cte(p)
    sql = cte + """
SELECT COUNT(*)                     AS total,
       SUM(p.is_accurate_payment)   AS accurate
FROM filtered_claims fc
JOIN silver_payments p ON fc.claim_id = p.claim_id
"""
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty or df["total"].iloc[0] == 0:
        return 0.0
    total = df["total"].iloc[0]
    accurate = df["accurate"].iloc[0] or 0
    return round(float(accurate / total * 100), 2)


# ===========================================================================
# 12. BAD DEBT RATE
# ===========================================================================

def query_bad_debt_rate(p: FilterParams, db_path=None):
    """Calculate Bad Debt Rate.

    Bad Debt Rate = WRITEOFF adjustments / Total Charges * 100

    Returns:
        tuple: (bad_debt_rate, bad_debt_amount, total_charges)
    """
    cte, params = _cte(p)
    # Compute charges and write-offs separately to avoid row duplication.
    sql = cte + """
SELECT
    (SELECT COALESCE(SUM(total_charge_amount), 0) FROM filtered_claims) AS total_charges,
    COALESCE((
        SELECT SUM(a.adjustment_amount)
        FROM silver_adjustments a
        WHERE a.adjustment_type_code = 'WRITEOFF'
          AND a.claim_id IN (SELECT claim_id FROM filtered_claims)
    ), 0) AS bad_debt
"""
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return 0.0, 0.0, 0.0
    total_charges = df["total_charges"].iloc[0] or 0.0
    bad_debt = df["bad_debt"].iloc[0] or 0.0
    rate = (bad_debt / total_charges * 100) if total_charges > 0 else 0.0
    return round(float(rate), 2), float(bad_debt), float(total_charges)


# ===========================================================================
# 13. APPEAL SUCCESS RATE
# ===========================================================================

def query_appeal_success_rate(p: FilterParams, db_path=None):
    """Calculate Appeal Success Rate.

    Returns:
        tuple: (success_rate_pct, total_appeals_filed, appeals_won_count)
    """
    cte, params = _cte(p)
    sql = cte + """
SELECT d.appeal_status,
       COUNT(*) AS n
FROM filtered_claims fc
JOIN silver_denials d ON fc.claim_id = d.claim_id
WHERE d.appeal_status IN ('Won', 'Lost', 'In Progress')
GROUP BY d.appeal_status
"""
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return 0.0, 0, 0
    total_appealed = int(df["n"].sum())
    won = int(df.loc[df["appeal_status"] == "Won", "n"].sum())
    rate = (won / total_appealed * 100) if total_appealed > 0 else 0.0
    return round(float(rate), 2), total_appealed, won


# ===========================================================================
# 14. AVERAGE REIMBURSEMENT PER ENCOUNTER
# ===========================================================================

def query_avg_reimbursement(p: FilterParams, db_path=None):
    """Calculate Average Reimbursement per Encounter.

    Returns:
        tuple: (avg_reimbursement_dollars, monthly_trend_series)
            monthly_trend_series: Series indexed by year_month with
            column name 'payment_amount'.
    """
    cte, params = _cte(p)
    sql = cte + """
SELECT strftime('%Y-%m', fc.date_of_service) AS period,
       COALESCE(SUM(p.payment_amount), 0)    AS payment_amount
FROM filtered_claims fc
LEFT JOIN silver_payments p ON fc.claim_id = p.claim_id
GROUP BY fc.claim_id, fc.date_of_service
ORDER BY period
"""
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return 0.0, pd.Series(dtype=float)

    avg = df["payment_amount"].mean()
    trend = df.groupby("period")["payment_amount"].mean()
    trend.index.name = "year_month"
    return round(float(avg), 2), trend


# ===========================================================================
# 15. PAYER MIX ANALYSIS
# ===========================================================================

def query_payer_mix(p: FilterParams, db_path=None):
    """Analyze revenue and volume by payer.

    Returns:
        DataFrame with columns [payer_id, payer_name, payer_type,
        claim_count, total_charges, total_payments, collection_rate],
        sorted by total_payments descending.
    """
    cte, params = _cte(p)
    sql = cte + """
SELECT fc.payer_id,
       py.payer_name,
       py.payer_type,
       COUNT(DISTINCT fc.claim_id)           AS claim_count,
       SUM(fc.total_charge_amount)           AS total_charges,
       COALESCE(SUM(p.payment_amount), 0)    AS total_payments
FROM filtered_claims fc
JOIN  silver_payers   py ON fc.payer_id  = py.payer_id
LEFT JOIN silver_payments p ON fc.claim_id  = p.claim_id
GROUP BY fc.payer_id, py.payer_name, py.payer_type
ORDER BY total_payments DESC
"""
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return pd.DataFrame(columns=[
            "payer_id", "payer_name", "payer_type",
            "claim_count", "total_charges", "total_payments", "collection_rate",
        ])
    df["collection_rate"] = np.where(
        df["total_charges"] > 0,
        df["total_payments"] / df["total_charges"] * 100,
        0,
    )
    return df


# ===========================================================================
# 16. DENIAL RATE BY PAYER
# ===========================================================================

def query_denial_rate_by_payer(p: FilterParams, db_path=None):
    """Calculate denial rate for each payer.

    Returns:
        DataFrame with [payer_id, payer_name, total_claims, denied, denial_rate],
        sorted by denial_rate descending.
    """
    cte, params = _cte(p)
    sql = cte + """
SELECT fc.payer_id,
       py.payer_name,
       COUNT(*)                                                        AS total_claims,
       SUM(CASE WHEN fc.claim_status IN ('Denied','Appealed')
               THEN 1 ELSE 0 END)                                     AS denied
FROM filtered_claims fc
JOIN silver_payers py ON fc.payer_id = py.payer_id
GROUP BY fc.payer_id, py.payer_name
ORDER BY denied * 1.0 / COUNT(*) DESC
"""
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return pd.DataFrame(columns=["payer_id", "payer_name", "total_claims", "denied", "denial_rate"])
    df["denial_rate"] = np.where(
        df["total_claims"] > 0,
        df["denied"] / df["total_claims"] * 100,
        0,
    )
    return df


# ===========================================================================
# 17. DEPARTMENT PERFORMANCE
# ===========================================================================

def query_department_performance(p: FilterParams, db_path=None):
    """Calculate revenue performance metrics by clinical department.

    Returns:
        DataFrame with [department, encounter_count, total_charges,
        total_payments, collection_rate, avg_payment_per_encounter],
        sorted by total_payments descending.
    """
    cte, params = _cte(p)
    sql = cte + """
SELECT e.department,
       COUNT(DISTINCT e.encounter_id)         AS encounter_count,
       COALESCE(SUM(fc.total_charge_amount), 0) AS total_charges,
       COALESCE(SUM(p.payment_amount), 0)     AS total_payments
FROM filtered_claims fc
JOIN  silver_encounters e  ON fc.encounter_id = e.encounter_id
LEFT JOIN silver_payments p ON fc.claim_id    = p.claim_id
GROUP BY e.department
ORDER BY total_payments DESC
"""
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return pd.DataFrame(columns=[
            "department", "encounter_count", "total_charges",
            "total_payments", "collection_rate", "avg_payment_per_encounter",
        ])
    df["collection_rate"] = np.where(
        df["total_charges"] > 0,
        df["total_payments"] / df["total_charges"] * 100,
        0,
    )
    df["avg_payment_per_encounter"] = np.where(
        df["encounter_count"] > 0,
        df["total_payments"] / df["encounter_count"],
        0,
    )
    return df
