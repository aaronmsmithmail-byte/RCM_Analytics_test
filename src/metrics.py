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

from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.database import build_filter_cte, query_to_dataframe

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
    payer_id: str | None = None
    department: str | None = None
    encounter_type: str | None = None


# ===========================================================================
# Internal helpers
# ===========================================================================


def _cte(p: FilterParams):
    """Return (cte_sql, params) for the filtered_claims CTE."""
    return build_filter_cte(
        p.start_date,
        p.end_date,
        payer_id=p.payer_id,
        department=p.department,
        encounter_type=p.encounter_type,
    )


def _try_cube_query(measures, dimensions=None, p: FilterParams = None):
    """
    Attempt a metric query through the Cube semantic layer.

    Returns a pandas DataFrame on success, or None if Cube is unavailable
    (so the caller falls back to raw SQL).
    """
    try:
        from src.cube_client import build_cube_filters, is_cube_available, query_cube

        if not is_cube_available():
            return None
        filters, time_dims = (
            build_cube_filters(
                p.start_date,
                p.end_date,
                payer_id=p.payer_id,
                department=p.department,
                encounter_type=p.encounter_type,
            )
            if p
            else ([], [])
        )
        return query_cube(measures, dimensions=dimensions, filters=filters, time_dimensions=time_dims)
    except Exception:
        return None


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
    # ── Try Cube semantic layer ──────────────────────────────────────
    cube_df = _try_cube_query(
        measures=["claims.total_charges", "payments.total_payments"],
        dimensions=["claims.period"],
        p=p,
    )
    if cube_df is not None and not cube_df.empty:
        cube_df.columns = ["period", "charges", "payments"]
        cube_df["ar_balance"] = cube_df["charges"].cumsum() - cube_df["payments"].cumsum()
        cube_df["avg_daily_charges"] = cube_df["charges"] / 30
        cube_df["days_in_ar"] = np.where(
            cube_df["avg_daily_charges"] > 0,
            cube_df["ar_balance"] / cube_df["avg_daily_charges"],
            0,
        )
        cube_df = _set_period_index(cube_df)
        overall_dar = cube_df["days_in_ar"].iloc[-1] if len(cube_df) > 0 else 0.0
        return round(float(overall_dar), 1), cube_df[["charges", "payments", "ar_balance", "days_in_ar"]]

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    # Charges and payments are aggregated in separate CTEs to avoid
    # row duplication when a claim has multiple payments (a LEFT JOIN
    # to silver_payments would produce N rows per claim with N payments,
    # causing SUM(charge_amount) to be overcounted by a factor of N).
    sql = (
        cte
        + """
, monthly_charges AS (
    SELECT strftime(CAST(date_of_service AS DATE), '%Y-%m') AS period,
           SUM(total_charge_amount)           AS charges
    FROM filtered_claims
    GROUP BY strftime(CAST(date_of_service AS DATE), '%Y-%m')
), monthly_payments AS (
    SELECT strftime(CAST(fc.date_of_service AS DATE), '%Y-%m') AS period,
           COALESCE(SUM(p.payment_amount), 0)    AS payments
    FROM filtered_claims fc
    LEFT JOIN silver_payments p ON fc.claim_id = p.claim_id
    GROUP BY strftime(CAST(fc.date_of_service AS DATE), '%Y-%m')
)
SELECT c.period, c.charges, COALESCE(mp.payments, 0) AS payments
FROM monthly_charges c
LEFT JOIN monthly_payments mp ON c.period = mp.period
ORDER BY c.period
"""
    )
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
    # ── Try Cube semantic layer ──────────────────────────────────────
    cube_df = _try_cube_query(
        measures=["claims.total_charges", "payments.total_payments", "adjustments.contractual_total"],
        dimensions=["claims.period"],
        p=p,
    )
    if cube_df is not None and not cube_df.empty:
        cube_df.columns = ["period", "charges", "payments", "contractual_adj"]
        total_charges = cube_df["charges"].sum()
        total_payments = cube_df["payments"].sum()
        total_contractual = cube_df["contractual_adj"].sum()
        denominator = total_charges - total_contractual
        ncr = (total_payments / denominator * 100) if denominator > 0 else 0.0
        cube_df["ncr"] = np.where(
            (cube_df["charges"] - cube_df["contractual_adj"]) > 0,
            cube_df["payments"] / (cube_df["charges"] - cube_df["contractual_adj"]) * 100,
            0,
        )
        cube_df = _set_period_index(cube_df)
        return round(float(ncr), 2), cube_df

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    # Use three separate monthly CTEs to prevent charge duplication when a
    # claim has multiple payments or multiple adjustments.
    sql = (
        cte
        + """
, monthly_charges AS (
    SELECT strftime(CAST(date_of_service AS DATE), '%Y-%m') AS period,
           SUM(total_charge_amount)           AS charges
    FROM filtered_claims
    GROUP BY strftime(CAST(date_of_service AS DATE), '%Y-%m')
), monthly_payments AS (
    SELECT strftime(CAST(fc.date_of_service AS DATE), '%Y-%m') AS period,
           COALESCE(SUM(p.payment_amount), 0)    AS payments
    FROM filtered_claims fc
    LEFT JOIN silver_payments p ON fc.claim_id = p.claim_id
    GROUP BY strftime(CAST(fc.date_of_service AS DATE), '%Y-%m')
), monthly_contractual AS (
    SELECT strftime(CAST(fc.date_of_service AS DATE), '%Y-%m') AS period,
           COALESCE(SUM(CASE WHEN a.adjustment_type_code = 'CONTRACTUAL'
                             THEN a.adjustment_amount ELSE 0 END), 0) AS contractual_adj
    FROM filtered_claims fc
    LEFT JOIN silver_adjustments a ON fc.claim_id = a.claim_id
    GROUP BY strftime(CAST(fc.date_of_service AS DATE), '%Y-%m')
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
    )
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
    # ── Try Cube semantic layer ──────────────────────────────────────
    cube_df = _try_cube_query(
        measures=["claims.total_charges", "payments.total_payments"],
        dimensions=["claims.period"],
        p=p,
    )
    if cube_df is not None and not cube_df.empty:
        cube_df.columns = ["period", "charges", "payments"]
        total_charges = cube_df["charges"].sum()
        total_payments = cube_df["payments"].sum()
        gcr = (total_payments / total_charges * 100) if total_charges > 0 else 0.0
        cube_df["gcr"] = np.where(cube_df["charges"] > 0, cube_df["payments"] / cube_df["charges"] * 100, 0)
        cube_df = _set_period_index(cube_df)
        return round(float(gcr), 2), cube_df

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
, monthly_charges AS (
    SELECT strftime(CAST(date_of_service AS DATE), '%Y-%m') AS period,
           SUM(total_charge_amount)           AS charges
    FROM filtered_claims
    GROUP BY strftime(CAST(date_of_service AS DATE), '%Y-%m')
), monthly_payments AS (
    SELECT strftime(CAST(fc.date_of_service AS DATE), '%Y-%m') AS period,
           COALESCE(SUM(p.payment_amount), 0)    AS payments
    FROM filtered_claims fc
    LEFT JOIN silver_payments p ON fc.claim_id = p.claim_id
    GROUP BY strftime(CAST(fc.date_of_service AS DATE), '%Y-%m')
)
SELECT c.period, c.charges, COALESCE(mp.payments, 0) AS payments
FROM monthly_charges c
LEFT JOIN monthly_payments mp ON c.period = mp.period
ORDER BY c.period
"""
    )
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
    # ── Try Cube semantic layer ──────────────────────────────────────
    cube_df = _try_cube_query(
        measures=["claims.count", "claims.clean_count"],
        dimensions=["claims.period"],
        p=p,
    )
    if cube_df is not None and not cube_df.empty:
        cube_df.columns = ["period", "total_claims", "clean_claims"]
        total = cube_df["total_claims"].sum()
        clean = cube_df["clean_claims"].sum()
        ccr = (clean / total * 100) if total > 0 else 0.0
        cube_df["ccr"] = np.where(
            cube_df["total_claims"] > 0, cube_df["clean_claims"] / cube_df["total_claims"] * 100, 0
        )
        cube_df = _set_period_index(cube_df)
        return round(float(ccr), 2), cube_df

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
SELECT strftime(CAST(submission_date AS DATE), '%Y-%m')       AS period,
       COUNT(*)                                 AS total_claims,
       SUM(is_clean_claim)                      AS clean_claims
FROM filtered_claims
GROUP BY strftime(CAST(submission_date AS DATE), '%Y-%m')
ORDER BY period
"""
    )
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
    Tries Cube semantic layer first, falls back to raw SQL.

    Returns:
        tuple: (denial_rate_percentage, trend_dataframe)
    """
    # ── Try Cube semantic layer ──────────────────────────────────────
    cube_df = _try_cube_query(
        measures=["claims.count", "claims.denied_count"],
        dimensions=["claims.period"],
        p=p,
    )
    if cube_df is not None and not cube_df.empty:
        cube_df.columns = ["period", "total_claims", "denied_claims"]
        total = cube_df["total_claims"].sum()
        denied = cube_df["denied_claims"].sum()
        rate = (denied / total * 100) if total > 0 else 0.0
        cube_df["denial_rate"] = np.where(
            cube_df["total_claims"] > 0, cube_df["denied_claims"] / cube_df["total_claims"] * 100, 0
        )
        cube_df = _set_period_index(cube_df)
        return round(float(rate), 2), cube_df

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
SELECT strftime(CAST(submission_date AS DATE), '%Y-%m') AS period,
       COUNT(*)                           AS total_claims,
       SUM(CASE WHEN claim_status IN ('Denied', 'Appealed')
               THEN 1 ELSE 0 END)         AS denied_claims
FROM filtered_claims
GROUP BY strftime(CAST(submission_date AS DATE), '%Y-%m')
ORDER BY period
"""
    )
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return 0.0, _empty_trend("total_claims", "denied_claims", "denial_rate")

    total = df["total_claims"].sum()
    denied = df["denied_claims"].sum()
    rate = (denied / total * 100) if total > 0 else 0.0

    df["denial_rate"] = np.where(df["total_claims"] > 0, df["denied_claims"] / df["total_claims"] * 100, 0)
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
    # Cube integration: complex multi-table query with denial reason
    # dimensions not available in Cube — falls through to SQL

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
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
    )
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "denial_reason_code",
                "denial_reason_description",
                "count",
                "total_denied_amount",
                "total_recovered",
                "recovery_rate",
            ]
        )

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
    # ── Try Cube semantic layer ──────────────────────────────────────
    # First-pass rate uses claim_status='Paid' count vs total — approximate
    # with claims.count (total) and a separate query is not straightforward
    # since there is no dedicated "paid_count" measure. Falls through to SQL.
    # Cube integration: no dedicated first-pass paid measure — falls through to SQL

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
SELECT strftime(CAST(submission_date AS DATE), '%Y-%m') AS period,
       COUNT(*)                           AS total,
       SUM(CASE WHEN claim_status = 'Paid' THEN 1 ELSE 0 END) AS paid
FROM filtered_claims
GROUP BY strftime(CAST(submission_date AS DATE), '%Y-%m')
ORDER BY period
"""
    )
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
    # ── Try Cube semantic layer ──────────────────────────────────────
    cube_df = _try_cube_query(
        measures=["charges.avg_charge_lag"],
        dimensions=["charges.period"],
        p=p,
    )
    if cube_df is not None and not cube_df.empty:
        cube_df.columns = ["period", "avg_lag"]
        avg_lag = cube_df["avg_lag"].mean()
        trend = cube_df.set_index("period")["avg_lag"]
        trend.index.name = "year_month"
        # Distribution not available from Cube; return empty Series
        return round(float(avg_lag), 1), trend, pd.Series(dtype=float)

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    # Charges for encounters that appear in filtered claims
    sql = (
        cte
        + """
SELECT strftime(CAST(ch.service_date AS DATE), '%Y-%m')                              AS period,
       date_diff('day', CAST(ch.service_date AS DATE),
                       CAST(ch.post_date AS DATE))                     AS lag_days
FROM silver_charges ch
WHERE ch.encounter_id IN (SELECT DISTINCT encounter_id FROM filtered_claims)
  AND ch.post_date   IS NOT NULL
  AND ch.service_date IS NOT NULL
"""
    )
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
    # ── Try Cube semantic layer ──────────────────────────────────────
    cube_df = _try_cube_query(
        measures=["payments.total_payments", "operating_costs.total_rcm_cost"],
        dimensions=["payments.period"],
        p=p,
    )
    if cube_df is not None and not cube_df.empty:
        cube_df.columns = ["period", "collections", "rcm_cost"]
        total_cost = cube_df["rcm_cost"].sum()
        total_collected = cube_df["collections"].sum()
        ctc = (total_cost / total_collected * 100) if total_collected > 0 else 0.0
        cube_df["cost_to_collect_pct"] = np.where(
            cube_df["collections"] > 0,
            cube_df["rcm_cost"] / cube_df["collections"] * 100,
            0,
        )
        cube_df = _set_period_index(cube_df)
        return round(float(ctc), 2), cube_df[["rcm_cost", "collections", "cost_to_collect_pct"]]

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    # Monthly collections from filtered claims
    sql = (
        cte
        + """
SELECT strftime(CAST(fc.date_of_service AS DATE), '%Y-%m') AS period,
       COALESCE(SUM(p.payment_amount), 0)    AS collections
FROM filtered_claims fc
LEFT JOIN silver_payments p ON fc.claim_id = p.claim_id
GROUP BY strftime(CAST(fc.date_of_service AS DATE), '%Y-%m')
ORDER BY period
"""
    )
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

    # Cube integration: complex per-claim aging bucket logic with HAVING
    # and date_diff not expressible as Cube measures — falls through to SQL

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
SELECT fc.claim_id,
       fc.date_of_service,
       fc.total_charge_amount - COALESCE(SUM(p.payment_amount), 0) AS ar_balance,
       date_diff('day', CAST(fc.date_of_service AS DATE), CURRENT_DATE) AS days_outstanding
FROM filtered_claims fc
LEFT JOIN silver_payments p ON fc.claim_id = p.claim_id
GROUP BY fc.claim_id, fc.date_of_service, fc.total_charge_amount
HAVING ar_balance > 0
"""
    )
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
    summary = (
        df.groupby("aging_bucket")
        .agg(
            claim_count=("claim_id", "count"),
            total_ar=("ar_balance", "sum"),
        )
        .reindex(["0-30", "31-60", "61-90", "91-120", "120+"])
        .fillna(0)
    )

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
    # ── Try Cube semantic layer ──────────────────────────────────────
    cube_df = _try_cube_query(
        measures=["payments.count", "payments.accurate_count"],
        p=p,
    )
    if cube_df is not None and not cube_df.empty:
        total = cube_df.iloc[0, 0]
        accurate = cube_df.iloc[0, 1] or 0
        if total and total > 0:
            return round(float(accurate / total * 100), 2)
        return 0.0

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
SELECT COUNT(*)                     AS total,
       SUM(p.is_accurate_payment)   AS accurate
FROM filtered_claims fc
JOIN silver_payments p ON fc.claim_id = p.claim_id
"""
    )
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
    # ── Try Cube semantic layer ──────────────────────────────────────
    cube_df = _try_cube_query(
        measures=["claims.total_charges", "adjustments.bad_debt_total"],
        p=p,
    )
    if cube_df is not None and not cube_df.empty:
        total_charges = float(cube_df.iloc[0, 0] or 0)
        bad_debt = float(cube_df.iloc[0, 1] or 0)
        rate = (bad_debt / total_charges * 100) if total_charges > 0 else 0.0
        return round(float(rate), 2), bad_debt, total_charges

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    # Compute charges and write-offs separately to avoid row duplication.
    sql = (
        cte
        + """
SELECT
    (SELECT COALESCE(SUM(total_charge_amount), 0) FROM filtered_claims) AS total_charges,
    COALESCE((
        SELECT SUM(a.adjustment_amount)
        FROM silver_adjustments a
        WHERE a.adjustment_type_code = 'WRITEOFF'
          AND a.claim_id IN (SELECT claim_id FROM filtered_claims)
    ), 0) AS bad_debt
"""
    )
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
    # ── Try Cube semantic layer ──────────────────────────────────────
    cube_df = _try_cube_query(
        measures=["denials.appealed_count", "denials.won_count"],
        p=p,
    )
    if cube_df is not None and not cube_df.empty:
        total_appealed = int(cube_df.iloc[0, 0] or 0)
        won = int(cube_df.iloc[0, 1] or 0)
        rate = (won / total_appealed * 100) if total_appealed > 0 else 0.0
        return round(float(rate), 2), total_appealed, won

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
SELECT d.appeal_status,
       COUNT(*) AS n
FROM filtered_claims fc
JOIN silver_denials d ON fc.claim_id = d.claim_id
WHERE d.appeal_status IN ('Won', 'Lost', 'In Progress')
GROUP BY d.appeal_status
"""
    )
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
    # ── Try Cube semantic layer ──────────────────────────────────────
    cube_df = _try_cube_query(
        measures=["payments.total_payments", "encounters.count"],
        dimensions=["claims.period"],
        p=p,
    )
    if cube_df is not None and not cube_df.empty:
        cube_df.columns = ["period", "total_payments", "encounter_count"]
        total_pay = cube_df["total_payments"].sum()
        total_enc = cube_df["encounter_count"].sum()
        avg = (total_pay / total_enc) if total_enc > 0 else 0.0
        trend = cube_df.set_index("period")["total_payments"] / cube_df.set_index("period")["encounter_count"].replace(
            0, np.nan
        )
        trend = trend.fillna(0)
        trend.index.name = "year_month"
        return round(float(avg), 2), trend

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
SELECT strftime(CAST(fc.date_of_service AS DATE), '%Y-%m') AS period,
       COALESCE(SUM(p.payment_amount), 0)    AS payment_amount
FROM filtered_claims fc
LEFT JOIN silver_payments p ON fc.claim_id = p.claim_id
GROUP BY fc.claim_id, fc.date_of_service
ORDER BY period
"""
    )
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
    # Cube integration: complex multi-table JOIN with payer dimensions
    # not directly expressible as single Cube query — falls through to SQL

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
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
    )
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "payer_id",
                "payer_name",
                "payer_type",
                "claim_count",
                "total_charges",
                "total_payments",
                "collection_rate",
            ]
        )
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
    # Cube integration: complex multi-table JOIN with payer dimensions
    # and denial status filtering — falls through to SQL

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
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
    )
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
    # Cube integration: complex multi-table JOIN with encounter/department
    # dimensions and distinct encounter counts — falls through to SQL

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
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
    )
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "department",
                "encounter_count",
                "total_charges",
                "total_payments",
                "collection_rate",
                "avg_payment_per_encounter",
            ]
        )
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


# ===========================================================================
# 18. PROVIDER PERFORMANCE
# ===========================================================================


def query_provider_performance(p: FilterParams, db_path=None):
    """Calculate revenue cycle KPIs by individual provider.

    Joins encounters → providers so every claim is attributed to the rendering
    provider.  Metrics include collection rate, denial rate, clean claim rate,
    and average payment per encounter — the standard set used for provider
    scorecards in enterprise RCM systems.

    Returns:
        DataFrame with columns [provider_id, provider_name, specialty,
        department, encounter_count, claim_count, total_charges, total_payments,
        collection_rate, denial_rate, clean_claim_rate, avg_payment_per_encounter],
        sorted by total_payments descending.
    """
    # Cube integration: complex multi-table query with provider dimensions,
    # multiple CTEs, and computed rates — falls through to SQL

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
, provider_claims AS (
    SELECT pr.provider_id,
           pr.provider_name,
           pr.specialty,
           pr.department,
           COUNT(DISTINCT fc.encounter_id)                          AS encounter_count,
           COUNT(DISTINCT fc.claim_id)                              AS claim_count,
           SUM(fc.total_charge_amount)                              AS total_charges,
           SUM(fc.is_clean_claim)                                   AS clean_claims,
           SUM(CASE WHEN fc.claim_status IN ('Denied','Appealed')
                    THEN 1 ELSE 0 END)                              AS denied_claims
    FROM filtered_claims fc
    JOIN silver_encounters e  ON fc.encounter_id = e.encounter_id
    JOIN silver_providers  pr ON e.provider_id   = pr.provider_id
    GROUP BY pr.provider_id, pr.provider_name, pr.specialty, pr.department
), provider_payments AS (
    SELECT e.provider_id,
           COALESCE(SUM(p.payment_amount), 0) AS total_payments
    FROM filtered_claims fc
    JOIN silver_encounters e ON fc.encounter_id = e.encounter_id
    LEFT JOIN silver_payments p ON fc.claim_id  = p.claim_id
    GROUP BY e.provider_id
)
SELECT pc.provider_id,
       pc.provider_name,
       pc.specialty,
       pc.department,
       pc.encounter_count,
       pc.claim_count,
       pc.total_charges,
       COALESCE(pp.total_payments, 0) AS total_payments,
       pc.clean_claims,
       pc.denied_claims
FROM provider_claims pc
LEFT JOIN provider_payments pp ON pc.provider_id = pp.provider_id
ORDER BY total_payments DESC
"""
    )
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "provider_id",
                "provider_name",
                "specialty",
                "department",
                "encounter_count",
                "claim_count",
                "total_charges",
                "total_payments",
                "collection_rate",
                "denial_rate",
                "clean_claim_rate",
                "avg_payment_per_encounter",
            ]
        )
    df["collection_rate"] = np.where(df["total_charges"] > 0, df["total_payments"] / df["total_charges"] * 100, 0)
    df["denial_rate"] = np.where(df["claim_count"] > 0, df["denied_claims"] / df["claim_count"] * 100, 0)
    df["clean_claim_rate"] = np.where(df["claim_count"] > 0, df["clean_claims"] / df["claim_count"] * 100, 0)
    df["avg_payment_per_encounter"] = np.where(
        df["encounter_count"] > 0, df["total_payments"] / df["encounter_count"], 0
    )
    return df


# ===========================================================================
# 19. CPT CODE ANALYSIS
# ===========================================================================


def query_cpt_analysis(p: FilterParams, db_path=None):
    """Analyse revenue and denial patterns at the CPT procedure-code level.

    Charges are directly at CPT granularity.  Denial rate is approximated as
    the share of distinct claims (for encounters containing this CPT) that were
    denied — standard practice when line-level remittance data is unavailable.

    Returns:
        DataFrame with columns [cpt_code, cpt_description, charge_count,
        total_units, total_charges, avg_charge_per_unit, claim_count,
        denied_claims, denial_rate], sorted by total_charges descending.
    """
    # Cube integration: complex multi-CTE query with CPT-level charge
    # stats and claim-level denial joins — falls through to SQL

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
, encounter_ids AS (
    SELECT DISTINCT encounter_id FROM filtered_claims
), charge_stats AS (
    SELECT ch.cpt_code,
           ch.cpt_description,
           COUNT(ch.charge_id)  AS charge_count,
           SUM(ch.units)        AS total_units,
           SUM(ch.charge_amount) AS total_charges
    FROM silver_charges ch
    WHERE ch.encounter_id IN (SELECT encounter_id FROM encounter_ids)
    GROUP BY ch.cpt_code, ch.cpt_description
), cpt_claim_pairs AS (
    -- One row per (cpt_code, claim_id) — deduplicates multi-charge encounters
    SELECT DISTINCT ch.cpt_code, fc.claim_id, fc.claim_status
    FROM silver_charges ch
    JOIN filtered_claims fc ON ch.encounter_id = fc.encounter_id
), claim_stats AS (
    SELECT cpt_code,
           COUNT(DISTINCT claim_id)                                          AS claim_count,
           SUM(CASE WHEN claim_status IN ('Denied','Appealed') THEN 1 ELSE 0 END) AS denied_claims
    FROM cpt_claim_pairs
    GROUP BY cpt_code
)
SELECT cs.cpt_code,
       cs.cpt_description,
       cs.charge_count,
       cs.total_units,
       cs.total_charges,
       COALESCE(cls.claim_count,   0) AS claim_count,
       COALESCE(cls.denied_claims, 0) AS denied_claims
FROM charge_stats cs
LEFT JOIN claim_stats cls ON cs.cpt_code = cls.cpt_code
ORDER BY cs.total_charges DESC
"""
    )
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "cpt_code",
                "cpt_description",
                "charge_count",
                "total_units",
                "total_charges",
                "avg_charge_per_unit",
                "claim_count",
                "denied_claims",
                "denial_rate",
            ]
        )
    df["avg_charge_per_unit"] = np.where(df["total_units"] > 0, df["total_charges"] / df["total_units"], 0)
    df["denial_rate"] = np.where(df["claim_count"] > 0, df["denied_claims"] / df["claim_count"] * 100, 0)
    return df


# ===========================================================================
# 20. UNDERPAYMENT ANALYSIS
# ===========================================================================


def query_underpayment_analysis(p: FilterParams, db_path=None):
    """Identify payer underpayments by comparing payment_amount to allowed_amount.

    In healthcare billing the ERA (835) carries both the allowed amount
    (what the contract permits) and the actual payment.  When
    payment_amount < allowed_amount the difference is an underpayment —
    money owed under contract that was not remitted.

    Returns:
        tuple: (summary_dataframe, total_recovery_opportunity)
            summary_dataframe: DataFrame with columns [payer_id, payer_name,
                payer_type, payment_count, total_allowed, total_paid,
                total_underpaid, underpaid_count, underpayment_rate],
                sorted by total_underpaid descending.
            total_recovery_opportunity: scalar sum of all underpayments (float).
    """
    # Cube integration: complex multi-table JOIN with payer dimensions
    # and conditional underpayment logic — falls through to SQL

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
SELECT fc.payer_id,
       py.payer_name,
       py.payer_type,
       COUNT(p.payment_id)                                            AS payment_count,
       SUM(p.allowed_amount)                                          AS total_allowed,
       SUM(p.payment_amount)                                          AS total_paid,
       SUM(CASE WHEN p.allowed_amount > p.payment_amount
                THEN p.allowed_amount - p.payment_amount
                ELSE 0 END)                                           AS total_underpaid,
       COUNT(CASE WHEN p.allowed_amount > p.payment_amount
                  THEN 1 END)                                         AS underpaid_count
FROM filtered_claims fc
JOIN silver_payers   py ON fc.payer_id  = py.payer_id
JOIN silver_payments p  ON fc.claim_id  = p.claim_id
WHERE p.allowed_amount IS NOT NULL AND p.allowed_amount > 0
GROUP BY fc.payer_id, py.payer_name, py.payer_type
ORDER BY total_underpaid DESC
"""
    )
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    empty_cols = [
        "payer_id",
        "payer_name",
        "payer_type",
        "payment_count",
        "total_allowed",
        "total_paid",
        "total_underpaid",
        "underpaid_count",
        "underpayment_rate",
    ]
    if df.empty:
        return pd.DataFrame(columns=empty_cols), 0.0
    df["underpayment_rate"] = np.where(df["total_allowed"] > 0, df["total_underpaid"] / df["total_allowed"] * 100, 0)
    total_recovery = float(df["total_underpaid"].sum())
    return df, total_recovery


def query_underpayment_trend(p: FilterParams, db_path=None):
    """Monthly underpayment amounts for trend analysis.

    Returns:
        DataFrame indexed by year_month with columns
        [total_allowed, total_paid, total_underpaid, underpayment_rate].
    """
    # ── Try Cube semantic layer ──────────────────────────────────────
    cube_df = _try_cube_query(
        measures=["payments.total_allowed", "payments.total_payments", "payments.total_underpaid"],
        dimensions=["payments.period"],
        p=p,
    )
    if cube_df is not None and not cube_df.empty:
        cube_df.columns = ["period", "total_allowed", "total_paid", "total_underpaid"]
        cube_df["underpayment_rate"] = np.where(
            cube_df["total_allowed"] > 0,
            cube_df["total_underpaid"] / cube_df["total_allowed"] * 100,
            0,
        )
        cube_df = cube_df.set_index("period")
        cube_df.index.name = "year_month"
        return cube_df

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
SELECT strftime(CAST(p.payment_date AS DATE), '%Y-%m')                              AS period,
       SUM(p.allowed_amount)                                          AS total_allowed,
       SUM(p.payment_amount)                                          AS total_paid,
       SUM(CASE WHEN p.allowed_amount > p.payment_amount
                THEN p.allowed_amount - p.payment_amount
                ELSE 0 END)                                           AS total_underpaid
FROM filtered_claims fc
JOIN silver_payments p ON fc.claim_id = p.claim_id
WHERE p.allowed_amount IS NOT NULL AND p.allowed_amount > 0
GROUP BY strftime(CAST(p.payment_date AS DATE), '%Y-%m')
ORDER BY period
"""
    )
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return pd.DataFrame(columns=["total_allowed", "total_paid", "total_underpaid", "underpayment_rate"])
    df["underpayment_rate"] = np.where(df["total_allowed"] > 0, df["total_underpaid"] / df["total_allowed"] * 100, 0)
    df = df.set_index("period")
    df.index.name = "year_month"
    return df


# ===========================================================================
# 21. CLEAN CLAIM SCRUBBING BREAKDOWN
# ===========================================================================

# Human-readable labels and resolution guidance for each fail reason code.
_FAIL_REASON_LABELS = {
    "MISSING_AUTH": "Missing Prior Authorization",
    "ELIGIBILITY_FAIL": "Patient Eligibility Not Verified",
    "CODING_ERROR": "Invalid CPT/ICD-10 Combination",
    "DUPLICATE_SUBMISSION": "Duplicate Claim Submission",
    "TIMELY_FILING": "Outside Timely Filing Window",
    "MISSING_INFO": "Missing Required Information",
}
_FAIL_REASON_GUIDANCE = {
    "MISSING_AUTH": "Automate auth check at scheduling; obtain PA before service date.",
    "ELIGIBILITY_FAIL": "Verify eligibility 24-48h before appointment via real-time check.",
    "CODING_ERROR": "Add CPT/ICD-10 edit rules to charge capture; schedule coder training.",
    "DUPLICATE_SUBMISSION": "Enable duplicate detection in clearinghouse scrubber settings.",
    "TIMELY_FILING": "Set automated alerts when claims approach payer filing deadlines.",
    "MISSING_INFO": "Implement front-desk registration checklists with required-field validation.",
}


def query_clean_claim_breakdown(p: FilterParams, db_path=None):
    """Break down dirty (failed scrubbing) claims by specific fail reason.

    Returns:
        DataFrame with columns [fail_reason, label, count, total_charges,
        pct_of_dirty, guidance], sorted by count descending.
    """
    # Cube integration: fail_reason dimension with dirty-claim filter
    # not available as Cube dimension — falls through to SQL

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
SELECT fail_reason,
       COUNT(*)                    AS count,
       SUM(total_charge_amount)    AS total_charges
FROM filtered_claims
WHERE is_clean_claim = 0
  AND fail_reason IS NOT NULL
GROUP BY fail_reason
ORDER BY count DESC
"""
    )
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "fail_reason",
                "label",
                "count",
                "total_charges",
                "pct_of_dirty",
                "guidance",
            ]
        )
    total_dirty = df["count"].sum()
    df["pct_of_dirty"] = np.where(total_dirty > 0, df["count"] / total_dirty * 100, 0)
    df["label"] = df["fail_reason"].map(_FAIL_REASON_LABELS).fillna(df["fail_reason"])
    df["guidance"] = df["fail_reason"].map(_FAIL_REASON_GUIDANCE).fillna("")
    return df


# ===========================================================================
# 22. PATIENT FINANCIAL RESPONSIBILITY
# ===========================================================================


def query_patient_responsibility_by_payer(p: FilterParams, db_path=None):
    """Patient financial responsibility (patient portion) grouped by payer.

    Patient responsibility = max(allowed_amount - payment_amount, 0).
    This represents the co-pay, deductible, and coinsurance owed by the
    patient after insurance adjudicates the claim.

    Returns:
        DataFrame with columns [payer_name, payer_type, payment_count,
        total_patient_resp, avg_patient_resp, pct_of_allowed],
        sorted by total_patient_resp descending.
    """
    # Cube integration: complex multi-table JOIN with payer dimensions
    # and conditional patient responsibility logic — falls through to SQL

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
SELECT py.payer_name,
       py.payer_type,
       COUNT(p.payment_id)                                                    AS payment_count,
       SUM(CASE WHEN p.allowed_amount > p.payment_amount
                THEN p.allowed_amount - p.payment_amount ELSE 0 END)          AS total_patient_resp,
       AVG(CASE WHEN p.allowed_amount > p.payment_amount
                THEN p.allowed_amount - p.payment_amount ELSE 0 END)          AS avg_patient_resp,
       SUM(p.allowed_amount)                                                   AS total_allowed
FROM filtered_claims fc
JOIN silver_payers   py ON fc.payer_id = py.payer_id
JOIN silver_payments p  ON fc.claim_id = p.claim_id
WHERE p.allowed_amount IS NOT NULL AND p.allowed_amount > 0
GROUP BY py.payer_name, py.payer_type
ORDER BY total_patient_resp DESC
"""
    )
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "payer_name",
                "payer_type",
                "payment_count",
                "total_patient_resp",
                "avg_patient_resp",
                "pct_of_allowed",
            ]
        )
    df["pct_of_allowed"] = np.where(df["total_allowed"] > 0, df["total_patient_resp"] / df["total_allowed"] * 100, 0)
    return df


def query_patient_responsibility_by_dept(p: FilterParams, db_path=None):
    """Patient financial responsibility grouped by department and encounter type.

    Returns:
        DataFrame with columns [department, encounter_type, claim_count,
        total_patient_resp, avg_patient_resp], sorted by total_patient_resp desc.
    """
    # Cube integration: complex multi-table JOIN with department/encounter
    # dimensions and conditional patient responsibility — falls through to SQL

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
SELECT e.department,
       e.encounter_type,
       COUNT(DISTINCT fc.claim_id)                                            AS claim_count,
       SUM(CASE WHEN p.allowed_amount > p.payment_amount
                THEN p.allowed_amount - p.payment_amount ELSE 0 END)          AS total_patient_resp,
       AVG(CASE WHEN p.allowed_amount > p.payment_amount
                THEN p.allowed_amount - p.payment_amount ELSE 0 END)          AS avg_patient_resp
FROM filtered_claims fc
JOIN silver_encounters e ON fc.encounter_id = e.encounter_id
JOIN silver_payments   p ON fc.claim_id     = p.claim_id
WHERE p.allowed_amount IS NOT NULL AND p.allowed_amount > 0
GROUP BY e.department, e.encounter_type
ORDER BY total_patient_resp DESC
"""
    )
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "department",
                "encounter_type",
                "claim_count",
                "total_patient_resp",
                "avg_patient_resp",
            ]
        )
    return df


def query_patient_responsibility_trend(p: FilterParams, db_path=None):
    """Monthly trend of patient financial responsibility.

    Returns:
        DataFrame indexed by year_month with columns
        [total_patient_resp, total_allowed, patient_resp_rate].
    """
    # Cube integration: conditional patient responsibility logic
    # (allowed - payment when underpaid) not available as Cube measure —
    # falls through to SQL

    # ── Fallback: raw SQL via DuckDB ─────────────────────────────────
    cte, params = _cte(p)
    sql = (
        cte
        + """
SELECT strftime(CAST(fc.date_of_service AS DATE), '%Y-%m')                                  AS period,
       SUM(CASE WHEN p.allowed_amount > p.payment_amount
                THEN p.allowed_amount - p.payment_amount ELSE 0 END)          AS total_patient_resp,
       SUM(p.allowed_amount)                                                   AS total_allowed,
       COUNT(DISTINCT fc.claim_id)                                             AS claim_count
FROM filtered_claims fc
JOIN silver_payments p ON fc.claim_id = p.claim_id
WHERE p.allowed_amount IS NOT NULL AND p.allowed_amount > 0
GROUP BY strftime(CAST(fc.date_of_service AS DATE), '%Y-%m')
ORDER BY period
"""
    )
    df = query_to_dataframe(sql, params=params, db_path=db_path)
    if df.empty:
        return pd.DataFrame(columns=["total_patient_resp", "total_allowed", "patient_resp_rate"])
    df["patient_resp_rate"] = np.where(df["total_allowed"] > 0, df["total_patient_resp"] / df["total_allowed"] * 100, 0)
    df = df.set_index("period")
    df.index.name = "year_month"
    return df


# ===========================================================================
# 23. DATA FRESHNESS
# ===========================================================================

# Expected refresh cadence per domain (hours) — used to compute staleness.
_DOMAIN_CADENCE_HOURS = {
    "claims": 4,
    "payments": 6,
    "encounters": 4,
    "charges": 4,
    "denials": 12,
    "adjustments": 8,
    "payers": 24,
    "patients": 24,
    "providers": 24,
    "operating_costs": 720,  # monthly
}

_DOMAIN_LABELS = {
    "claims": "Claims",
    "payments": "Payments / ERA",
    "encounters": "Encounters / ADT",
    "charges": "Charges / CDM",
    "denials": "Denials",
    "adjustments": "Adjustments",
    "payers": "Payer Master",
    "patients": "Patient Demographics",
    "providers": "Provider Roster",
    "operating_costs": "Operating Costs",
}


def query_data_freshness(db_path=None):
    """Return pipeline run metadata for the data freshness sidebar panel.

    Returns:
        DataFrame with columns [domain, label, last_loaded_at, row_count,
        source_file, cadence_hours, age_hours, status],
        where status is 'fresh', 'stale', or 'critical'.
    """
    sql = "SELECT domain, last_loaded_at, row_count, source_file FROM pipeline_runs ORDER BY domain"
    df = query_to_dataframe(sql, db_path=db_path)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "domain",
                "label",
                "last_loaded_at",
                "row_count",
                "source_file",
                "cadence_hours",
                "age_hours",
                "status",
            ]
        )
    now = pd.Timestamp.utcnow().replace(tzinfo=None)
    df["label"] = df["domain"].map(_DOMAIN_LABELS).fillna(df["domain"])
    df["cadence_hours"] = df["domain"].map(_DOMAIN_CADENCE_HOURS).fillna(24)
    df["last_loaded_at_dt"] = pd.to_datetime(df["last_loaded_at"], errors="coerce", utc=True).dt.tz_localize(None)
    df["age_hours"] = (now - df["last_loaded_at_dt"]).dt.total_seconds() / 3600
    df["status"] = "fresh"
    df.loc[df["age_hours"] > df["cadence_hours"], "status"] = "stale"
    df.loc[df["age_hours"] > df["cadence_hours"] * 3, "status"] = "critical"
    return df[["domain", "label", "last_loaded_at", "row_count", "source_file", "cadence_hours", "age_hours", "status"]]
