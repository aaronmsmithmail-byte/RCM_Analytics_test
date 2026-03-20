"""Calculate all RCM performance metrics."""

import pandas as pd
import numpy as np


def add_year_month(df, date_col):
    """Add year_month column from a date column."""
    df = df.copy()
    df["year_month"] = df[date_col].dt.to_period("M")
    return df


# =====================================================================
# 1. DAYS IN ACCOUNTS RECEIVABLE (DAR)
# =====================================================================
def calc_days_in_ar(claims, payments):
    """DAR = Total AR / (Average Daily Charges).
    Also compute monthly trend."""
    claims = add_year_month(claims, "date_of_service")
    # Total charges by month
    monthly_charges = claims.groupby("year_month")["total_charge_amount"].sum()

    # Total payments by month
    payments_by_claim = payments.merge(
        claims[["claim_id", "year_month"]], on="claim_id", how="left"
    )
    monthly_payments = payments_by_claim.groupby("year_month")["payment_amount"].sum()

    # AR balance = cumulative charges - cumulative payments
    combined = pd.DataFrame({
        "charges": monthly_charges,
        "payments": monthly_payments
    }).fillna(0)
    combined["ar_balance"] = combined["charges"].cumsum() - combined["payments"].cumsum()
    combined["avg_daily_charges"] = combined["charges"] / 30
    combined["days_in_ar"] = np.where(
        combined["avg_daily_charges"] > 0,
        combined["ar_balance"] / combined["avg_daily_charges"],
        0
    )
    combined.index = combined.index.astype(str)
    overall_dar = combined["days_in_ar"].iloc[-1] if len(combined) > 0 else 0
    return round(overall_dar, 1), combined[["charges", "payments", "ar_balance", "days_in_ar"]]


# =====================================================================
# 2. NET COLLECTION RATE
# =====================================================================
def calc_net_collection_rate(claims, payments, adjustments):
    """NCR = Payments / (Charges - Contractual Adjustments)."""
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
# 3. GROSS COLLECTION RATE
# =====================================================================
def calc_gross_collection_rate(claims, payments):
    """GCR = Total Payments / Total Charges."""
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
# 4. CLEAN CLAIM RATE
# =====================================================================
def calc_clean_claim_rate(claims):
    """CCR = Clean Claims / Total Claims."""
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
    """Denial Rate = Denied Claims / Total Claims."""
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
    """Group denials by reason code."""
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
# 7. FIRST-PASS RESOLUTION RATE
# =====================================================================
def calc_first_pass_rate(claims):
    """FPRR = Claims Paid on First Submission / Total Claims."""
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
    """Charge Lag = Post Date - Service Date."""
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
# 9. COST TO COLLECT
# =====================================================================
def calc_cost_to_collect(operating_costs, claims, payments):
    """Cost to Collect = Total RCM Costs / Total Collections."""
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
    """Categorize outstanding AR into aging buckets."""
    # Calculate total paid per claim
    paid_per_claim = payments.groupby("claim_id")["payment_amount"].sum().reset_index()
    paid_per_claim.columns = ["claim_id", "total_paid"]

    ar = claims.merge(paid_per_claim, on="claim_id", how="left")
    ar["total_paid"] = ar["total_paid"].fillna(0)
    ar["ar_balance"] = ar["total_charge_amount"] - ar["total_paid"]
    ar = ar[ar["ar_balance"] > 0].copy()

    today = pd.Timestamp.now()
    ar["days_outstanding"] = (today - ar["date_of_service"]).dt.days

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
    summary = ar.groupby("aging_bucket").agg(
        claim_count=("claim_id", "count"),
        total_ar=("ar_balance", "sum")
    ).reindex(["0-30", "31-60", "61-90", "91-120", "120+"])

    total_ar = summary["total_ar"].sum()
    summary["pct_of_total"] = np.where(total_ar > 0, summary["total_ar"] / total_ar * 100, 0)
    return summary, total_ar


# =====================================================================
# 11. PAYMENT ACCURACY RATE
# =====================================================================
def calc_payment_accuracy(payments):
    """Payment Accuracy = Accurate Payments / Total Payments."""
    total = len(payments)
    accurate = payments["is_accurate_payment"].sum()
    rate = (accurate / total * 100) if total > 0 else 0
    return round(rate, 2)


# =====================================================================
# 12. BAD DEBT RATE
# =====================================================================
def calc_bad_debt_rate(claims, adjustments):
    """Bad Debt Rate = Bad Debt Write-offs / Total Charges."""
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
    """Appeal Success = Won Appeals / Total Appeals."""
    appealed = denials[denials["appeal_status"].isin(["Won", "Lost", "In Progress"])]
    total_appealed = len(appealed)
    won = len(appealed[appealed["appeal_status"] == "Won"])
    rate = (won / total_appealed * 100) if total_appealed > 0 else 0
    return round(rate, 2), total_appealed, won


# =====================================================================
# 14. AVERAGE REIMBURSEMENT PER ENCOUNTER
# =====================================================================
def calc_avg_reimbursement(claims, payments):
    """Average $ reimbursed per encounter."""
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
    """Revenue and volume breakdown by payer."""
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
    """Denial rate broken down by payer."""
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
    """Revenue metrics by department."""
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
