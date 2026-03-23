"""

RCM Performance Metrics Calculation Engine
==========================================

This module contains functions that calculate all 17 Revenue Cycle Management
(RCM) Key Performance Indicators (KPIs) used in the dashboard.

What is the Revenue Cycle?
    The healthcare revenue cycle is the entire financial process of a patient
    encounter, from scheduling through final payment:

    1. PRE-SERVICE:   Patient scheduling, insurance verification, prior auth
    2. SERVICE:       Patient visit, clinical documentation
    3. CHARGE CAPTURE: Translating services into CPT/ICD-10 codes and charges
    4. CLAIM SUBMISSION: Sending claims to insurance payers electronically
    5. PAYMENT:       Receiving payer remittance and patient payments
    6. DENIALS:       Managing rejected claims, filing appeals
    7. A/R FOLLOW-UP: Pursuing outstanding balances

    Each metric in this module measures the health of one or more of these steps.

Metrics Implemented (grouped by category):
    Financial Performance:
        1.  Days in Accounts Receivable (DAR)  — How fast we collect
        2.  Net Collection Rate (NCR)          — How much we collect vs. what we're owed
        3.  Gross Collection Rate (GCR)        — How much we collect vs. what we billed
        4.  Cost to Collect (CTC)              — How much it costs to collect $1
        5.  Bad Debt Rate                      — How much we write off as uncollectable
        6.  Avg Reimbursement per Encounter    — Average revenue per patient visit

    Claims Quality:
        7.  Clean Claim Rate (CCR)             — % of claims submitted without errors
        8.  Claim Denial Rate                  — % of claims rejected by payers
        9.  First-Pass Resolution Rate (FPRR)  — % of claims paid on first submission
        10. Charge Lag                         — Days between service and charge posting
        11. Denial Reasons Breakdown           — Why claims are being denied

    Recovery & Appeals:
        12. Appeal Success Rate                — % of appealed denials that are won
        13. A/R Aging Buckets                  — Age distribution of outstanding balances
        14. Payment Accuracy Rate              — % of payments at correct amount

    Segmentation:
        15. Payer Mix Analysis                 — Revenue breakdown by insurance company
        16. Denial Rate by Payer               — Which payers deny most often
        17. Department Performance             — Revenue metrics by clinical department

Design Pattern:
    Most functions follow the same pattern:
    1. Calculate the overall KPI value (a single number for the KPI card).
    2. Calculate a monthly trend (a DataFrame for the line/bar chart).
    3. Return both as a tuple: (overall_value, trend_dataframe).

    This design lets the dashboard show both the current state (KPI card)
    and the trajectory over time (trend chart) from a single function call.

Dependencies:
    - pandas: DataFrame operations, groupby aggregations, date handling
    - numpy:  Vectorized conditional logic (np.where) for safe division
"""


import pandas as pd
import numpy as np


def add_year_month(df, date_col):
    """

    Add a 'year_month' column derived from a date column.

    This helper is used by nearly every metric function to group data by month
    for trend analysis. pandas Period objects (e.g., "2024-06") are ideal for
    this because they sort correctly and display cleanly on chart axes.

    Args:
        df:       DataFrame containing the date column.
        date_col: Name of the column to extract year-month from.

    Returns:
        A copy of the DataFrame with a new 'year_month' column (Period type).

    Note:
        We return a copy (.copy()) to avoid SettingWithCopyWarning and to
        prevent accidentally modifying the original filtered DataFrame.
    """

    df = df.copy()
    df["year_month"] = df[date_col].dt.to_period("M")
    return df


def _empty_trend(*columns):
    """Return an empty DataFrame with the given columns for use as a fallback trend."""

    return pd.DataFrame(columns=list(columns))


# =====================================================================
# 1. DAYS IN ACCOUNTS RECEIVABLE (DAR)
# =====================================================================
def calc_days_in_ar(claims, payments):
    """

    Calculate Days in Accounts Receivable (DAR).

    What it measures:
        How many days' worth of charges are sitting unpaid in A/R. This is the
        single most important cash-flow metric in healthcare RCM. A high DAR
        means the organization is slow to collect, tying up cash that could be
        used for operations, payroll, or capital improvements.

    Formula:
        DAR = Total A/R Balance / Average Daily Charges
        Where:
            A/R Balance     = Cumulative Charges - Cumulative Payments
            Avg Daily Chg   = Monthly Charges / 30 (approximation)

    Industry benchmarks:
        - Best practice: < 30 days
        - Good:          30-40 days
        - Needs work:    40-50 days
        - Critical:      > 50 days

    Args:
        claims:   DataFrame with columns [claim_id, date_of_service, total_charge_amount]
        payments: DataFrame with columns [claim_id, payment_amount]

    Returns:
        tuple: (overall_dar, trend_dataframe)
            - overall_dar: float, the most recent month's DAR value
            - trend_dataframe: DataFrame indexed by year_month with columns
              [charges, payments, ar_balance, days_in_ar]
    """
    if claims.empty:
        return 0.0, _empty_trend("charges", "payments", "ar_balance", "days_in_ar")
    claims = add_year_month(claims, "date_of_service")

    # Step 1: Aggregate charges by month
    monthly_charges = claims.groupby("year_month")["total_charge_amount"].sum()

    # Step 2: Map each payment back to the month its claim was for,
    # then aggregate payments by month
    payments_by_claim = payments.merge(
        claims[["claim_id", "year_month"]], on="claim_id", how="left"
    )
    monthly_payments = payments_by_claim.groupby("year_month")["payment_amount"].sum()

    # Step 3: Build a combined monthly view
    combined = pd.DataFrame({
        "charges": monthly_charges,
        "payments": monthly_payments
    }).fillna(0)

    # Step 4: Calculate running A/R balance using cumulative sums.
    # A/R grows when charges exceed payments, shrinks when payments catch up.
    combined["ar_balance"] = combined["charges"].cumsum() - combined["payments"].cumsum()

    # Step 5: Estimate average daily charges (using 30-day month approximation)
    combined["avg_daily_charges"] = combined["charges"] / 30

    # Step 6: Calculate DAR for each month.
    # np.where prevents division-by-zero when a month has no charges.
    combined["days_in_ar"] = np.where(
        combined["avg_daily_charges"] > 0,
        combined["ar_balance"] / combined["avg_daily_charges"],
        0
    )

    # Convert index to strings for Plotly chart compatibility
    combined.index = combined.index.astype(str)

    # The "current" DAR is the most recent month's value
    overall_dar = combined["days_in_ar"].iloc[-1] if len(combined) > 0 else 0
    return round(overall_dar, 1), combined[["charges", "payments", "ar_balance", "days_in_ar"]]


# =====================================================================
# 2. NET COLLECTION RATE (NCR)
# =====================================================================
def calc_net_collection_rate(claims, payments, adjustments):
    """

    Calculate Net Collection Rate (NCR).

    What it measures:
        The percentage of money collected out of what the organization was
        actually *entitled* to collect. Unlike Gross Collection Rate, NCR
        excludes contractual adjustments (negotiated discounts with payers)
        from the denominator, giving a truer picture of collection effectiveness.

    Formula:
        NCR = Total Payments / (Total Charges - Contractual Adjustments) * 100

    Why it matters:
        NCR is considered the most accurate measure of billing effectiveness.
        A low NCR means you're leaving money on the table — money that payers
        have agreed to pay but you're failing to collect.

    Industry benchmarks:
        - Best practice: > 96%
        - Good:          95-96%
        - Needs work:    90-95%
        - Critical:      < 90%

    Args:
        claims:      DataFrame with [claim_id, date_of_service, total_charge_amount]
        payments:    DataFrame with [claim_id, payment_amount]
        adjustments: DataFrame with [claim_id, adjustment_type_code, adjustment_amount]

    Returns:
        tuple: (ncr_percentage, trend_dataframe)
    """
    if claims.empty or payments.empty:
        return 0.0, _empty_trend("charges", "payments", "contractual_adj", "ncr")
    total_charges = claims["total_charge_amount"].sum()
    total_payments = payments["payment_amount"].sum()
    contractual_adj = adjustments[
        adjustments["adjustment_type_code"] == "CONTRACTUAL"
    ]["adjustment_amount"].sum()
    denominator = total_charges - contractual_adj
    ncr = (total_payments / denominator * 100) if denominator > 0 else 0

    # Monthly trend
    claims_m = add_year_month(claims, "date_of_service")
    pay_m = payments.merge(claims_m[["claim_id", "year_month"]], on="claim_id", how="left")
    adj_m = adjustments.merge(claims_m[["claim_id", "year_month"]], on="claim_id", how="left")

    monthly_charges = claims_m.groupby("year_month")["total_charge_amount"].sum()
    monthly_payments = pay_m.groupby("year_month")["payment_amount"].sum()
    monthly_contractual = adj_m[adj_m["adjustment_type_code"] == "CONTRACTUAL"].groupby("year_month")["adjustment_amount"].sum()

    trend = pd.DataFrame({
        "charges": monthly_charges,
        "payments": monthly_payments,
        "contractual_adj": monthly_contractual
    }).fillna(0)
    trend["ncr"] = np.where(
        (trend["charges"] - trend["contractual_adj"]) > 0,
        trend["payments"] / (trend["charges"] - trend["contractual_adj"]) * 100,
        0
    )
    trend.index = trend.index.astype(str)
    return round(ncr, 2), trend


# =====================================================================
# 3. GROSS COLLECTION RATE (GCR)
# =====================================================================
def calc_gross_collection_rate(claims, payments):
    """

    Calculate Gross Collection Rate (GCR).

    What it measures:
        The percentage of total billed charges that were actually collected.
        This is a simpler (but less precise) version of NCR because it doesn't
        account for contractual adjustments. A provider billing at chargemaster
        rates will always have a low GCR because no payer pays full charges.

    Formula:
        GCR = Total Payments / Total Charges * 100

    Industry benchmarks:
        - Typical range: 30-70% (varies widely by specialty and payer mix)
        - GCR is less useful as a standalone metric; it's best used alongside NCR.

    Args:
        claims:   DataFrame with [claim_id, date_of_service, total_charge_amount]
        payments: DataFrame with [claim_id, payment_amount]

    Returns:
        tuple: (gcr_percentage, trend_dataframe)
    """
    if claims.empty:
        return 0.0, _empty_trend("charges", "payments", "gcr")
    total_charges = claims["total_charge_amount"].sum()
    total_payments = payments["payment_amount"].sum()
    gcr = (total_payments / total_charges * 100) if total_charges > 0 else 0

    claims_m = add_year_month(claims, "date_of_service")
    pay_m = payments.merge(claims_m[["claim_id", "year_month"]], on="claim_id", how="left")
    monthly_charges = claims_m.groupby("year_month")["total_charge_amount"].sum()
    monthly_payments = pay_m.groupby("year_month")["payment_amount"].sum()
    trend = pd.DataFrame({"charges": monthly_charges, "payments": monthly_payments}).fillna(0)
    trend["gcr"] = np.where(trend["charges"] > 0, trend["payments"] / trend["charges"] * 100, 0)
    trend.index = trend.index.astype(str)
    return round(gcr, 2), trend


# =====================================================================
# 4. CLEAN CLAIM RATE (CCR)
# =====================================================================
def calc_clean_claim_rate(claims):
    """

    Calculate Clean Claim Rate (CCR).

    What it measures:
        The percentage of claims that pass through the billing system without
        requiring manual intervention, corrections, or resubmission. A "clean"
        claim has correct patient info, valid codes, proper modifiers, and
        matching authorization — it can be auto-adjudicated by the payer.

    Formula:
        CCR = Clean Claims / Total Claims * 100

    Why it matters:
        Dirty claims cause rework, delays, and denials. Every claim that needs
        manual correction costs $25-$30 in staff time. A 1% improvement in CCR
        can save a large health system hundreds of thousands of dollars annually.

    Industry benchmarks:
        - Best practice: > 95%
        - Good:          90-95%
        - Needs work:    80-90%
        - Critical:      < 80%

    Args:
        claims: DataFrame with [claim_id, submission_date, is_clean_claim]

    Returns:
        tuple: (ccr_percentage, trend_dataframe)
    """
    if claims.empty:
        return 0.0, _empty_trend("ccr")
    total = len(claims)
    clean = claims["is_clean_claim"].sum()
    ccr = (clean / total * 100) if total > 0 else 0

    claims_m = add_year_month(claims, "submission_date")
    trend = claims_m.groupby("year_month").agg(
        total_claims=("claim_id", "count"),
        clean_claims=("is_clean_claim", "sum")
    )
    trend["ccr"] = np.where(trend["total_claims"] > 0, trend["clean_claims"] / trend["total_claims"] * 100, 0)
    trend.index = trend.index.astype(str)
    return round(ccr, 2), trend


# =====================================================================
# 5. CLAIM DENIAL RATE
# =====================================================================
def calc_denial_rate(claims):
    """

    Calculate Claim Denial Rate.

    What it measures:
        The percentage of submitted claims that are rejected (denied) by payers.
        Denials are one of the biggest sources of revenue leakage in healthcare —
        the industry loses ~$19.7 billion/year to denials, and 65% of denied
        claims are never reworked or appealed.

    Formula:
        Denial Rate = (Denied + Appealed Claims) / Total Claims * 100

    Note: We include "Appealed" claims because they were initially denied.
    The appeal is an attempt to overturn the denial.

    Industry benchmarks:
        - Best practice: < 5%
        - Good:          5-10%
        - Average:       10-15%
        - Critical:      > 15%

    Args:
        claims: DataFrame with [claim_id, submission_date, claim_status]

    Returns:
        tuple: (denial_rate_percentage, trend_dataframe)
    """
    if claims.empty:
        return 0.0, _empty_trend("denial_rate")
    total = len(claims)
    denied = len(claims[claims["claim_status"].isin(["Denied", "Appealed"])])
    rate = (denied / total * 100) if total > 0 else 0

    claims_m = add_year_month(claims, "submission_date")
    trend = claims_m.groupby("year_month").agg(
        total_claims=("claim_id", "count"),
        denied_claims=("claim_status", lambda x: x.isin(["Denied", "Appealed"]).sum())
    )
    trend["denial_rate"] = np.where(trend["total_claims"] > 0, trend["denied_claims"] / trend["total_claims"] * 100, 0)
    trend.index = trend.index.astype(str)
    return round(rate, 2), trend


# =====================================================================
# 6. DENIAL REASONS BREAKDOWN
# =====================================================================
def calc_denial_reasons(denials):
    """

    Aggregate denials by reason code to identify root causes.

    What it measures:
        A Pareto analysis of WHY claims are being denied. This is critical
        for targeted process improvement — if 40% of denials are "Prior Auth
        Required," the fix is to improve the pre-service authorization workflow.

    Common denial reason codes:
        AUTH   — Prior authorization was missing or expired
        ELIG   — Patient was not eligible for coverage on date of service
        COD    — Coding error (wrong CPT/ICD-10, missing modifier)
        DUP    — Duplicate claim submission
        TMF    — Timely filing limit exceeded (claim submitted too late)
        INFO   — Missing or invalid information on the claim
        MED    — Medical necessity not established
        COORD  — Coordination of benefits issue (multiple insurers)

    Args:
        denials: DataFrame with [denial_id, denial_reason_code,
                 denial_reason_description, denied_amount, recovered_amount]

    Returns:
        DataFrame with columns: [denial_reason_code, denial_reason_description,
        count, total_denied_amount, total_recovered, recovery_rate],
        sorted by count descending (most common reasons first).
    """
    breakdown = denials.groupby(["denial_reason_code", "denial_reason_description"]).agg(
        count=("denial_id", "count"),
        total_denied_amount=("denied_amount", "sum"),
        total_recovered=("recovered_amount", "sum")
    ).reset_index()
    breakdown["recovery_rate"] = np.where(
        breakdown["total_denied_amount"] > 0,
        breakdown["total_recovered"] / breakdown["total_denied_amount"] * 100,
        0
    )
    return breakdown.sort_values("count", ascending=False)


# =====================================================================
# 7. FIRST-PASS RESOLUTION RATE (FPRR)
# =====================================================================
def calc_first_pass_rate(claims):
    """

    Calculate First-Pass Resolution Rate (FPRR).

    What it measures:
        The percentage of claims that are paid on their first submission,
        without needing resubmission, appeal, or manual intervention. This
        is the ultimate measure of "getting it right the first time."

    Formula:
        FPRR = Claims with status "Paid" / Total Claims * 100

    Why it matters:
        Every claim that doesn't resolve on first pass costs $25+ in rework.
        Claims that require resubmission also delay payment by 30-60 days,
        directly increasing Days in A/R.

    Industry benchmarks:
        - Best practice: > 90%
        - Good:          85-90%
        - Needs work:    75-85%
        - Critical:      < 75%

    Args:
        claims: DataFrame with [claim_id, submission_date, claim_status]

    Returns:
        tuple: (fpr_percentage, trend_dataframe)
    """
    if claims.empty:
        return 0.0, _empty_trend("fpr")
    total = len(claims)
    first_pass = len(claims[claims["claim_status"] == "Paid"])
    rate = (first_pass / total * 100) if total > 0 else 0

    claims_m = add_year_month(claims, "submission_date")
    trend = claims_m.groupby("year_month").agg(
        total=("claim_id", "count"),
        paid=("claim_status", lambda x: (x == "Paid").sum())
    )
    trend["fpr"] = np.where(trend["total"] > 0, trend["paid"] / trend["total"] * 100, 0)
    trend.index = trend.index.astype(str)
    return round(rate, 2), trend


# =====================================================================
# 8. AVERAGE CHARGE LAG (Days)
# =====================================================================
def calc_charge_lag(charges):
    """

    Calculate Average Charge Lag.

    What it measures:
        The average number of days between when a service is performed and
        when the charge is posted to the billing system. Delays in charge
        capture push back the entire revenue cycle — if it takes 5 days
        to post a charge, the claim goes out 5 days later, and payment
        arrives 5 days later.

    Formula:
        Charge Lag = Post Date - Service Date (in days)

    Why it matters:
        Only 32% of providers capture charges within 24 hours. Every day
        of charge lag directly adds a day to your payment timeline. For a
        provider billing $10M/year, each day of lag ties up ~$27,000 in
        delayed cash flow.

    Industry benchmarks:
        - Best practice: < 2 days
        - Good:          2-3 days
        - Needs work:    3-5 days
        - Critical:      > 5 days

    Args:
        charges: DataFrame with [service_date, post_date]

    Returns:
        tuple: (avg_lag_days, monthly_trend_series, lag_distribution_series)
    """
    if charges.empty:
        return 0.0, pd.Series(dtype=float), pd.Series(dtype=float)
    charges = charges.copy()
    charges["service_date"] = pd.to_datetime(charges["service_date"])
    charges["post_date"] = pd.to_datetime(charges["post_date"])
    charges["lag_days"] = (charges["post_date"] - charges["service_date"]).dt.days
    avg_lag = charges["lag_days"].mean()

    charges = add_year_month(charges, "service_date")
    trend = charges.groupby("year_month")["lag_days"].mean()
    trend.index = trend.index.astype(str)

    distribution = charges["lag_days"].value_counts().sort_index()
    return round(avg_lag, 1), trend, distribution


# =====================================================================
# 9. COST TO COLLECT (CTC)
# =====================================================================
def calc_cost_to_collect(operating_costs, claims, payments):
    """

    Calculate Cost to Collect.

    What it measures:
        How many cents it costs to collect each dollar of revenue. This includes
        billing staff salaries, software licenses, clearinghouse fees, outsourced
        billing services, and overhead. A high cost to collect means inefficiency
        in the revenue cycle operation.

    Formula:
        CTC = Total RCM Operating Costs / Total Collections * 100

    Industry benchmarks:
        - Best practice: < 3%
        - Good:          3-5%
        - Average:       5-8%
        - Critical:      > 8%

    Args:
        operating_costs: DataFrame with [period, total_rcm_cost]
        claims:          DataFrame with [claim_id, date_of_service]
        payments:        DataFrame with [claim_id, payment_amount]

    Returns:
        tuple: (ctc_percentage, trend_dataframe)
    """
    if payments.empty:
        return 0.0, _empty_trend("cost_to_collect_pct")
    total_cost = operating_costs["total_rcm_cost"].sum()
    total_collected = payments["payment_amount"].sum()
    ctc = (total_cost / total_collected * 100) if total_collected > 0 else 0

    # Monthly trend
    claims_m = add_year_month(claims, "date_of_service")
    pay_m = payments.merge(claims_m[["claim_id", "year_month"]], on="claim_id", how="left")
    monthly_collections = pay_m.groupby("year_month")["payment_amount"].sum()

    oc = operating_costs.copy()
    oc["year_month"] = oc["period"].dt.to_period("M")
    monthly_costs = oc.set_index("year_month")["total_rcm_cost"]

    trend = pd.DataFrame({
        "rcm_cost": monthly_costs,
        "collections": monthly_collections
    }).fillna(0)
    trend["cost_to_collect_pct"] = np.where(
        trend["collections"] > 0,
        trend["rcm_cost"] / trend["collections"] * 100,
        0
    )
    trend.index = trend.index.astype(str)
    return round(ctc, 2), trend


# =====================================================================
# 10. A/R AGING BUCKETS
# =====================================================================
def calc_ar_aging(claims, payments):
    """

    Categorize outstanding Accounts Receivable into aging buckets.

    What it measures:
        The age distribution of unpaid balances. In healthcare, the older a
        balance gets, the less likely it is to be collected. Industry data
        shows that collection probability drops dramatically after 90 days:
            0-30 days:  ~95% collectible
            31-60 days: ~85% collectible
            61-90 days: ~70% collectible
            91-120 days: ~50% collectible
            120+ days:   ~30% collectible

    How it works:
        1. Calculate how much has been paid on each claim.
        2. Subtract payments from charges to get the remaining A/R balance.
        3. Calculate how old each unpaid balance is (days since service).
        4. Sort balances into standard aging buckets.

    Industry benchmarks:
        - At least 70% of A/R should be in the 0-60 day buckets.
        - Less than 15% should be in the 120+ bucket.

    Args:
        claims:   DataFrame with [claim_id, date_of_service, total_charge_amount]
        payments: DataFrame with [claim_id, payment_amount]

    Returns:
        tuple: (aging_summary_dataframe, total_ar_balance)
            - aging_summary has columns: [claim_count, total_ar, pct_of_total]
            - indexed by bucket: ["0-30", "31-60", "61-90", "91-120", "120+"]
    """
    # Step 1: Sum all payments per claim
    if claims.empty:
        empty_summary = pd.DataFrame(
            {"claim_count": 0, "total_ar": 0.0, "pct_of_total": 0.0},
            index=["0-30", "31-60", "61-90", "91-120", "120+"]
        )
        return empty_summary, 0.0
    paid_per_claim = payments.groupby("claim_id")["payment_amount"].sum().reset_index()
    paid_per_claim.columns = ["claim_id", "total_paid"]

    # Step 2: Join payments to claims and calculate remaining balance
    ar = claims.merge(paid_per_claim, on="claim_id", how="left")
    ar["total_paid"] = ar["total_paid"].fillna(0)  # Claims with no payments = $0 paid
    ar["ar_balance"] = ar["total_charge_amount"] - ar["total_paid"]

    # Step 3: Keep only claims with a positive outstanding balance
    ar = ar[ar["ar_balance"] > 0].copy()

    # Step 4: Calculate days outstanding from service date to today
    today = pd.Timestamp.now()
    ar["days_outstanding"] = (today - ar["date_of_service"]).dt.days

    # Step 5: Assign each claim to an aging bucket
    def bucket(days):
        if days <= 30:
            return "0-30"
        elif days <= 60:
            return "31-60"
        elif days <= 90:
            return "61-90"
        elif days <= 120:
            return "91-120"
        else:
            return "120+"

    ar["aging_bucket"] = ar["days_outstanding"].apply(bucket)

    # Step 6: Aggregate by bucket
    summary = ar.groupby("aging_bucket").agg(
        claim_count=("claim_id", "count"),
        total_ar=("ar_balance", "sum")
    ).reindex(["0-30", "31-60", "61-90", "91-120", "120+"])  # Ensure correct order

    total_ar = summary["total_ar"].sum()
    summary["pct_of_total"] = np.where(total_ar > 0, summary["total_ar"] / total_ar * 100, 0)
    return summary, total_ar


# =====================================================================
# 11. PAYMENT ACCURACY RATE
# =====================================================================
def calc_payment_accuracy(payments):
    """

    Calculate Payment Accuracy Rate.

    What it measures:
        The percentage of payments received at the correct amount per the
        payer contract. Inaccurate payments include underpayments (payer
        paid less than contracted rate) and overpayments (payer paid more).

    Formula:
        Payment Accuracy = Accurate Payments / Total Payments * 100

    Why it matters:
        Underpayments are essentially hidden revenue leakage. If a payer
        contract says they'll reimburse $150 for CPT 99213 but they only
        pay $120, that $30 underpayment often goes undetected without
        payment accuracy tracking.

    Industry benchmarks:
        - Target: > 95%

    Args:
        payments: DataFrame with [is_accurate_payment]

    Returns:
        float: Accuracy rate as a percentage.
    """
    total = len(payments)
    accurate = payments["is_accurate_payment"].sum()
    rate = (accurate / total * 100) if total > 0 else 0
    return round(rate, 2)


# =====================================================================
# 12. BAD DEBT RATE
# =====================================================================
def calc_bad_debt_rate(claims, adjustments):
    """

    Calculate Bad Debt Rate.

    What it measures:
        The percentage of total charges that are written off as uncollectable.
        Bad debt occurs when a patient or payer balance is deemed unrecoverable
        after exhausting collection efforts.

    Formula:
        Bad Debt Rate = Bad Debt Write-offs / Total Charges * 100

    Industry benchmarks:
        - Best practice: < 2%
        - Average:       2-5%
        - Critical:      > 5%

    Args:
        claims:      DataFrame with [total_charge_amount]
        adjustments: DataFrame with [adjustment_type_code, adjustment_amount]

    Returns:
        tuple: (bad_debt_rate, bad_debt_amount, total_charges)
    """
    if claims.empty:
        return 0.0, 0.0, 0.0
    total_charges = claims["total_charge_amount"].sum()
    bad_debt = adjustments[
        adjustments["adjustment_type_code"] == "WRITEOFF"
    ]["adjustment_amount"].sum()
    rate = (bad_debt / total_charges * 100) if total_charges > 0 else 0
    return round(rate, 2), bad_debt, total_charges


# =====================================================================
# 13. APPEAL SUCCESS RATE
# =====================================================================
def calc_appeal_success_rate(denials):
    """

    Calculate Appeal Success Rate.

    What it measures:
        The percentage of appealed denials that are overturned (won). About
        67% of denied claims are eligible for appeal, and successful appeals
        can recover significant revenue.

    Formula:
        Appeal Success Rate = Won Appeals / Total Appeals Filed * 100

    Why it matters:
        If your appeal success rate is high (> 50%), it suggests that many
        denials were incorrect and could potentially be prevented upstream.
        If it's low, either the denials are legitimate or the appeals process
        needs improvement.

    Args:
        denials: DataFrame with [appeal_status] where values are
                 "Won", "Lost", "In Progress", or "Not Appealed"

    Returns:
        tuple: (success_rate_pct, total_appeals_filed, appeals_won_count)
    """
    appealed = denials[denials["appeal_status"].isin(["Won", "Lost", "In Progress"])]
    total_appealed = len(appealed)
    won = len(appealed[appealed["appeal_status"] == "Won"])
    rate = (won / total_appealed * 100) if total_appealed > 0 else 0
    return round(rate, 2), total_appealed, won


# =====================================================================
# 14. AVERAGE REIMBURSEMENT PER ENCOUNTER
# =====================================================================
def calc_avg_reimbursement(claims, payments):
    """

    Calculate Average Reimbursement per Encounter.

    What it measures:
        The average dollar amount collected per claim/encounter. This helps
        gauge overall revenue intensity and can reveal trends like declining
        reimbursement rates or shifts in service mix (e.g., fewer high-value
        procedures).

    Formula:
        Avg Reimbursement = Sum of all payments linked to claims / Number of claims

    Args:
        claims:   DataFrame with [claim_id, date_of_service]
        payments: DataFrame with [claim_id, payment_amount]

    Returns:
        tuple: (avg_reimbursement_dollars, monthly_trend_series)
    """
    if claims.empty or payments.empty:
        return 0.0, pd.Series(dtype=float)
    pay_per_claim = payments.groupby("claim_id")["payment_amount"].sum().reset_index()
    merged = claims.merge(pay_per_claim, on="claim_id", how="left")
    merged["payment_amount"] = merged["payment_amount"].fillna(0)
    avg = merged["payment_amount"].mean()

    merged = add_year_month(merged, "date_of_service")
    trend = merged.groupby("year_month")["payment_amount"].mean()
    trend.index = trend.index.astype(str)
    return round(avg, 2), trend


# =====================================================================
# 15. PAYER MIX ANALYSIS
# =====================================================================
def calc_payer_mix(claims, payments, payers):
    """

    Analyze revenue and volume by payer (insurance company).

    What it measures:
        The distribution of claims and revenue across different payers.
        Payer mix directly impacts profitability because reimbursement
        rates vary significantly:
            - Commercial payers: highest rates (70-92% of charges)
            - Medicare:          moderate (80% of charges)
            - Medicaid:          lowest (50-65% of charges)
            - Self-Pay:          variable, often low collection rates

    Why it matters:
        Understanding payer mix helps with:
        - Contract negotiations (which payers need renegotiation?)
        - Resource allocation (which payers need more follow-up?)
        - Financial forecasting (how will payer mix shifts affect revenue?)

    Args:
        claims:   DataFrame with [claim_id, payer_id, total_charge_amount]
        payments: DataFrame with [claim_id, payment_amount]
        payers:   DataFrame with [payer_id, payer_name, payer_type]

    Returns:
        DataFrame with columns: [payer_id, payer_name, payer_type,
        claim_count, total_charges, total_payments, collection_rate]
    """
    pay_per_claim = payments.groupby("claim_id")["payment_amount"].sum().reset_index()
    merged = claims.merge(pay_per_claim, on="claim_id", how="left")
    merged["payment_amount"] = merged["payment_amount"].fillna(0)
    merged = merged.merge(payers[["payer_id", "payer_name", "payer_type"]], on="payer_id", how="left")

    payer_summary = merged.groupby(["payer_id", "payer_name", "payer_type"]).agg(
        claim_count=("claim_id", "count"),
        total_charges=("total_charge_amount", "sum"),
        total_payments=("payment_amount", "sum")
    ).reset_index()
    payer_summary["collection_rate"] = np.where(
        payer_summary["total_charges"] > 0,
        payer_summary["total_payments"] / payer_summary["total_charges"] * 100,
        0
    )
    return payer_summary.sort_values("total_payments", ascending=False)


# =====================================================================
# 16. DENIAL RATE BY PAYER
# =====================================================================
def calc_denial_rate_by_payer(claims, payers):
    """

    Calculate denial rate for each payer.

    What it measures:
        Which insurance companies deny claims most frequently. High denial
        rates from a specific payer may indicate:
        - Eligibility verification issues with that payer
        - Coding requirements specific to that payer
        - Contract/authorization problems
        - Aggressive claim review practices by the payer

    Args:
        claims: DataFrame with [claim_id, payer_id, claim_status]
        payers: DataFrame with [payer_id, payer_name]

    Returns:
        DataFrame with [payer_id, payer_name, total_claims, denied, denial_rate]
    """
    merged = claims.merge(payers[["payer_id", "payer_name"]], on="payer_id", how="left")
    summary = merged.groupby(["payer_id", "payer_name"]).agg(
        total_claims=("claim_id", "count"),
        denied=("claim_status", lambda x: x.isin(["Denied", "Appealed"]).sum())
    ).reset_index()
    summary["denial_rate"] = np.where(
        summary["total_claims"] > 0,
        summary["denied"] / summary["total_claims"] * 100,
        0
    )
    return summary.sort_values("denial_rate", ascending=False)


# =====================================================================
# 17. DEPARTMENT PERFORMANCE
# =====================================================================
def calc_department_performance(encounters, claims, payments):
    """

    Calculate revenue performance metrics by clinical department.

    What it measures:
        How effectively each department (Cardiology, Orthopedics, etc.)
        converts encounters into collected revenue. Helps identify:
        - High-performing departments to model best practices from
        - Underperforming departments that need process improvement
        - Revenue concentration risk (too dependent on one department?)

    Metrics calculated per department:
        - encounter_count:          Number of unique patient visits
        - total_charges:            Total billed charges
        - total_payments:           Total payments received
        - collection_rate:          Payments / Charges * 100
        - avg_payment_per_encounter: Average revenue per visit

    Args:
        encounters: DataFrame with [encounter_id, department]
        claims:     DataFrame with [claim_id, encounter_id, total_charge_amount]
        payments:   DataFrame with [claim_id, payment_amount]

    Returns:
        DataFrame sorted by total_payments descending.
    """
    enc_claims = encounters[["encounter_id", "department"]].merge(
        claims[["claim_id", "encounter_id", "total_charge_amount"]], on="encounter_id", how="inner"
    )
    pay_per_claim = payments.groupby("claim_id")["payment_amount"].sum().reset_index()
    merged = enc_claims.merge(pay_per_claim, on="claim_id", how="left")
    merged["payment_amount"] = merged["payment_amount"].fillna(0)

    dept = merged.groupby("department").agg(
        encounter_count=("encounter_id", "nunique"),
        total_charges=("total_charge_amount", "sum"),
        total_payments=("payment_amount", "sum")
    ).reset_index()
    dept["collection_rate"] = np.where(
        dept["total_charges"] > 0,
        dept["total_payments"] / dept["total_charges"] * 100,
        0
    )
    dept["avg_payment_per_encounter"] = np.where(
        dept["encounter_count"] > 0,
        dept["total_payments"] / dept["encounter_count"],
        0
    )
    return dept.sort_values("total_payments", ascending=False)
