"""
Generate realistic sample data for Healthcare RCM Analytics.
Creates 10 CSV files representing common revenue cycle data sources.
"""

import csv
import random
import os
from datetime import datetime, timedelta

random.seed(42)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

# --- Constants ---
NUM_PATIENTS = 500
NUM_PROVIDERS = 25
NUM_ENCOUNTERS = 3000
NUM_CLAIMS = 2800
NUM_PAYMENTS = 2200
NUM_DENIALS = 420
NUM_ADJUSTMENTS = 600

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 12, 31)

PAYER_NAMES = [
    "Blue Cross Blue Shield", "Aetna", "Cigna", "UnitedHealthcare",
    "Humana", "Medicare", "Medicaid", "Tricare", "Self-Pay", "Kaiser Permanente"
]
PAYER_IDS = [f"PYR{str(i+1).zfill(3)}" for i in range(len(PAYER_NAMES))]

DEPARTMENTS = [
    "Cardiology", "Orthopedics", "Internal Medicine", "Emergency",
    "Pediatrics", "Neurology", "Oncology", "Radiology", "General Surgery", "Family Medicine"
]

CPT_CODES = [
    ("99213", "Office Visit - Est. Patient, Level 3", 150.00),
    ("99214", "Office Visit - Est. Patient, Level 4", 210.00),
    ("99215", "Office Visit - Est. Patient, Level 5", 300.00),
    ("99203", "Office Visit - New Patient, Level 3", 200.00),
    ("99204", "Office Visit - New Patient, Level 4", 290.00),
    ("99283", "ED Visit - Level 3", 350.00),
    ("99284", "ED Visit - Level 4", 550.00),
    ("99285", "ED Visit - Level 5", 850.00),
    ("93000", "ECG - 12 Lead", 75.00),
    ("71046", "Chest X-Ray - 2 Views", 120.00),
    ("80053", "Comprehensive Metabolic Panel", 95.00),
    ("85025", "Complete Blood Count", 45.00),
    ("36415", "Venipuncture", 25.00),
    ("99232", "Inpatient Visit - Level 2", 180.00),
    ("99233", "Inpatient Visit - Level 3", 250.00),
    ("27447", "Total Knee Replacement", 4500.00),
    ("43239", "Upper GI Endoscopy w/ Biopsy", 1200.00),
    ("70553", "Brain MRI w/ and w/o Contrast", 2800.00),
    ("29881", "Knee Arthroscopy", 3200.00),
    ("10060", "I&D Abscess", 350.00),
]

ICD10_CODES = [
    "I10", "E11.9", "J06.9", "M54.5", "Z00.00", "J18.9", "K21.0",
    "E78.5", "N39.0", "M79.3", "R10.9", "G43.909", "J45.20",
    "F41.1", "I25.10", "E03.9", "M17.11", "K80.20", "D64.9", "L03.90"
]

DENIAL_REASONS = [
    ("AUTH", "Prior Authorization Required"),
    ("DUP", "Duplicate Claim"),
    ("ELIG", "Patient Not Eligible"),
    ("COD", "Coding Error"),
    ("TMF", "Timely Filing Limit Exceeded"),
    ("MED", "Medical Necessity Not Met"),
    ("INFO", "Missing/Invalid Information"),
    ("MOD", "Invalid Modifier"),
    ("COORD", "Coordination of Benefits Issue"),
    ("BUNDLE", "Bundling/Unbundling Issue"),
]

ADJUSTMENT_TYPES = [
    ("CONTRACTUAL", "Contractual Adjustment"),
    ("WRITEOFF", "Bad Debt Write-Off"),
    ("CHARITY", "Charity Care"),
    ("ADMIN", "Administrative Adjustment"),
    ("PROMPT_PAY", "Prompt Pay Discount"),
    ("SMALL_BAL", "Small Balance Write-Off"),
]

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Steven", "Ashley",
    "Paul", "Dorothy", "Andrew", "Kimberly", "Joshua", "Emily", "Kenneth", "Donna"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson"
]

PROVIDER_NAMES = [
    "Dr. Sarah Chen", "Dr. James Wilson", "Dr. Maria Rodriguez", "Dr. Robert Kim",
    "Dr. Emily Johnson", "Dr. David Patel", "Dr. Lisa Thompson", "Dr. Michael Brown",
    "Dr. Jennifer Davis", "Dr. William Garcia", "Dr. Amanda White", "Dr. Christopher Lee",
    "Dr. Stephanie Martinez", "Dr. Brian Anderson", "Dr. Nicole Taylor",
    "Dr. Kevin Moore", "Dr. Rachel Clark", "Dr. Steven Harris", "Dr. Laura Lewis",
    "Dr. Andrew Robinson", "Dr. Michelle Walker", "Dr. Jason Hall", "Dr. Angela Young",
    "Dr. Ryan Allen", "Dr. Samantha King"
]


def rand_date(start=START_DATE, end=END_DATE):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def write_csv(filename, headers, rows):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"  Created {path} ({len(rows)} rows)")


# ---- 1. PAYERS ----
def generate_payers():
    rows = []
    for i, (pid, name) in enumerate(zip(PAYER_IDS, PAYER_NAMES)):
        payer_type = "Government" if name in ("Medicare", "Medicaid", "Tricare") else (
            "Self-Pay" if name == "Self-Pay" else "Commercial"
        )
        avg_reimburse_pct = {
            "Medicare": 0.80, "Medicaid": 0.65, "Tricare": 0.75, "Self-Pay": 0.40
        }.get(name, round(random.uniform(0.70, 0.92), 2))
        rows.append([pid, name, payer_type, avg_reimburse_pct, f"CON-{pid}-2024"])
    write_csv("payers.csv", ["payer_id", "payer_name", "payer_type", "avg_reimbursement_pct", "contract_id"], rows)
    return rows


# ---- 2. PATIENTS ----
def generate_patients():
    rows = []
    for i in range(NUM_PATIENTS):
        pid = f"PAT{str(i+1).zfill(5)}"
        fname = random.choice(FIRST_NAMES)
        lname = random.choice(LAST_NAMES)
        dob = datetime(random.randint(1940, 2010), random.randint(1, 12), random.randint(1, 28))
        gender = random.choice(["M", "F"])
        payer_id = random.choice(PAYER_IDS)
        member_id = f"MEM{random.randint(100000, 999999)}"
        zip_code = f"{random.randint(10000, 99999)}"
        rows.append([pid, fname, lname, dob.strftime("%Y-%m-%d"), gender, payer_id, member_id, zip_code])
    write_csv("patients.csv",
              ["patient_id", "first_name", "last_name", "date_of_birth", "gender", "primary_payer_id", "member_id", "zip_code"],
              rows)
    return rows


# ---- 3. PROVIDERS ----
def generate_providers():
    rows = []
    for i in range(NUM_PROVIDERS):
        prov_id = f"PROV{str(i+1).zfill(3)}"
        name = PROVIDER_NAMES[i]
        npi = f"{random.randint(1000000000, 9999999999)}"
        dept = DEPARTMENTS[i % len(DEPARTMENTS)]
        specialty = dept
        rows.append([prov_id, name, npi, dept, specialty])
    write_csv("providers.csv", ["provider_id", "provider_name", "npi", "department", "specialty"], rows)
    return rows


# ---- 4. ENCOUNTERS ----
def generate_encounters(patients, providers):
    rows = []
    for i in range(NUM_ENCOUNTERS):
        enc_id = f"ENC{str(i+1).zfill(6)}"
        pat = random.choice(patients)
        prov = random.choice(providers)
        dos = rand_date()
        enc_type = random.choices(
            ["Outpatient", "Inpatient", "Emergency", "Telehealth"],
            weights=[50, 15, 20, 15]
        )[0]
        dept = prov[3]
        discharge_date = dos + timedelta(days=random.randint(0, 5)) if enc_type == "Inpatient" else dos
        rows.append([enc_id, pat[0], prov[0], dos.strftime("%Y-%m-%d"),
                      discharge_date.strftime("%Y-%m-%d"), enc_type, dept])
    rows.sort(key=lambda r: r[3])
    write_csv("encounters.csv",
              ["encounter_id", "patient_id", "provider_id", "date_of_service", "discharge_date", "encounter_type", "department"],
              rows)
    return rows


# ---- 5. CHARGES ----
def generate_charges(encounters):
    rows = []
    charge_id = 1
    for enc in encounters:
        num_charges = random.choices([1, 2, 3, 4], weights=[40, 35, 15, 10])[0]
        selected_cpts = random.sample(CPT_CODES, min(num_charges, len(CPT_CODES)))
        for cpt_code, cpt_desc, base_charge in selected_cpts:
            chg_id = f"CHG{str(charge_id).zfill(7)}"
            units = random.choices([1, 2, 3], weights=[80, 15, 5])[0]
            charge_amt = round(base_charge * units * random.uniform(0.9, 1.1), 2)
            service_date = enc[3]
            # Charge lag: most within 1-2 days, some longer
            lag_days = random.choices(
                [0, 1, 2, 3, 5, 7, 14, 30],
                weights=[25, 30, 20, 10, 5, 5, 3, 2]
            )[0]
            post_date = (datetime.strptime(service_date, "%Y-%m-%d") + timedelta(days=lag_days)).strftime("%Y-%m-%d")
            icd10 = random.choice(ICD10_CODES)
            rows.append([chg_id, enc[0], cpt_code, cpt_desc, units, charge_amt, service_date, post_date, icd10])
            charge_id += 1
    write_csv("charges.csv",
              ["charge_id", "encounter_id", "cpt_code", "cpt_description", "units", "charge_amount", "service_date", "post_date", "icd10_code"],
              rows)
    return rows


# ---- 6. CLAIMS ----
def generate_claims(encounters, patients):
    rows = []
    pat_payer = {p[0]: p[5] for p in patients}
    enc_subset = random.sample(encounters, min(NUM_CLAIMS, len(encounters)))
    for i, enc in enumerate(enc_subset):
        claim_id = f"CLM{str(i+1).zfill(7)}"
        payer_id = pat_payer.get(enc[1], random.choice(PAYER_IDS))
        dos = enc[3]
        submission_lag = random.choices([1, 2, 3, 5, 7, 14, 21, 30, 45],
                                        weights=[10, 20, 20, 15, 10, 10, 5, 5, 5])[0]
        submission_date = (datetime.strptime(dos, "%Y-%m-%d") + timedelta(days=submission_lag)).strftime("%Y-%m-%d")
        total_charge = round(random.uniform(50, 5000), 2)
        # Claim status distribution (realistic: ~85% paid, ~8% denied)
        status = random.choices(
            ["Paid", "Denied", "Pending", "Partially Paid", "Appealed"],
            weights=[68, 8, 8, 12, 4]
        )[0]
        is_clean = random.choices([True, False], weights=[92, 8])[0] if status != "Denied" else random.choices([True, False], weights=[40, 60])[0]
        rows.append([claim_id, enc[0], enc[1], payer_id, dos, submission_date,
                      total_charge, status, is_clean, "Electronic"])
    rows.sort(key=lambda r: r[5])
    write_csv("claims.csv",
              ["claim_id", "encounter_id", "patient_id", "payer_id", "date_of_service",
               "submission_date", "total_charge_amount", "claim_status", "is_clean_claim", "submission_method"],
              rows)
    return rows


# ---- 7. PAYMENTS / REMITTANCES ----
def generate_payments(claims):
    rows = []
    payment_id = 1
    for claim in claims:
        if claim[7] in ("Paid", "Partially Paid", "Appealed"):
            pay_id = f"PAY{str(payment_id).zfill(7)}"
            claim_id = claim[0]
            payer_id = claim[3]
            charge = claim[6]
            if claim[7] == "Paid":
                paid_pct = random.uniform(0.75, 0.98)
            elif claim[7] == "Partially Paid":
                paid_pct = random.uniform(0.30, 0.60)
            else:
                paid_pct = random.uniform(0.50, 0.85)
            payment_amount = round(charge * paid_pct, 2)
            allowed_amount = round(charge * random.uniform(0.70, 0.98), 2)
            # Payment timing
            sub_date = datetime.strptime(claim[5], "%Y-%m-%d")
            pay_lag = random.randint(14, 90)
            payment_date = (sub_date + timedelta(days=pay_lag)).strftime("%Y-%m-%d")
            payment_method = random.choice(["EFT", "Check", "Virtual Card"])
            # Was payment accurate?
            is_accurate = random.choices([True, False], weights=[92, 8])[0]
            rows.append([pay_id, claim_id, payer_id, payment_amount, allowed_amount,
                          payment_date, payment_method, is_accurate])
            payment_id += 1

            # Some claims also have patient responsibility payment
            if random.random() < 0.35:
                pay_id = f"PAY{str(payment_id).zfill(7)}"
                pat_amount = round(charge * random.uniform(0.05, 0.25), 2)
                pat_pay_lag = random.randint(7, 120)
                pat_pay_date = (sub_date + timedelta(days=pat_pay_lag)).strftime("%Y-%m-%d")
                rows.append([pay_id, claim_id, "PATIENT", pat_amount, pat_amount,
                              pat_pay_date, random.choice(["Credit Card", "Cash", "Check", "Online Portal"]),
                              True])
                payment_id += 1

    write_csv("payments.csv",
              ["payment_id", "claim_id", "payer_id", "payment_amount", "allowed_amount",
               "payment_date", "payment_method", "is_accurate_payment"],
              rows)
    return rows


# ---- 8. DENIALS ----
def generate_denials(claims):
    rows = []
    denied_claims = [c for c in claims if c[7] in ("Denied", "Appealed")]
    # Also add some denials for partially paid claims
    partial_denied = random.sample(
        [c for c in claims if c[7] == "Partially Paid"],
        min(50, len([c for c in claims if c[7] == "Partially Paid"]))
    )
    all_denied = denied_claims + partial_denied
    for i, claim in enumerate(all_denied):
        denial_id = f"DEN{str(i+1).zfill(6)}"
        reason_code, reason_desc = random.choice(DENIAL_REASONS)
        sub_date = datetime.strptime(claim[5], "%Y-%m-%d")
        denial_date = (sub_date + timedelta(days=random.randint(10, 45))).strftime("%Y-%m-%d")
        denied_amount = round(claim[6] * random.uniform(0.3, 1.0), 2)
        if claim[7] == "Appealed":
            appeal_status = random.choice(["Won", "Lost", "In Progress"])
        else:
            appeal_status = random.choices(["Not Appealed", "Won", "Lost", "In Progress"],
                                            weights=[50, 15, 20, 15])[0]
        appeal_date = ""
        if appeal_status != "Not Appealed":
            appeal_date = (datetime.strptime(denial_date, "%Y-%m-%d") + timedelta(days=random.randint(5, 30))).strftime("%Y-%m-%d")
        recovered_amount = 0.0
        if appeal_status == "Won":
            recovered_amount = round(denied_amount * random.uniform(0.5, 1.0), 2)
        rows.append([denial_id, claim[0], reason_code, reason_desc, denial_date,
                      denied_amount, appeal_status, appeal_date, recovered_amount])
    write_csv("denials.csv",
              ["denial_id", "claim_id", "denial_reason_code", "denial_reason_description",
               "denial_date", "denied_amount", "appeal_status", "appeal_date", "recovered_amount"],
              rows)
    return rows


# ---- 9. ADJUSTMENTS ----
def generate_adjustments(claims):
    rows = []
    adj_claims = random.sample(claims, min(NUM_ADJUSTMENTS, len(claims)))
    for i, claim in enumerate(adj_claims):
        adj_id = f"ADJ{str(i+1).zfill(6)}"
        adj_type_code, adj_type_desc = random.choices(
            ADJUSTMENT_TYPES,
            weights=[45, 20, 5, 15, 10, 5]
        )[0]
        adj_amount = round(claim[6] * random.uniform(0.05, 0.50), 2)
        sub_date = datetime.strptime(claim[5], "%Y-%m-%d")
        adj_date = (sub_date + timedelta(days=random.randint(14, 90))).strftime("%Y-%m-%d")
        rows.append([adj_id, claim[0], adj_type_code, adj_type_desc, adj_amount, adj_date])
    write_csv("adjustments.csv",
              ["adjustment_id", "claim_id", "adjustment_type_code", "adjustment_type_description",
               "adjustment_amount", "adjustment_date"],
              rows)
    return rows


# ---- 10. OPERATING COSTS (for Cost to Collect) ----
def generate_operating_costs():
    rows = []
    months = []
    d = START_DATE
    while d <= END_DATE:
        months.append(d)
        if d.month == 12:
            d = d.replace(year=d.year + 1, month=1)
        else:
            d = d.replace(month=d.month + 1)
    for m in months:
        period = m.strftime("%Y-%m")
        billing_staff_cost = round(random.uniform(35000, 55000), 2)
        software_cost = round(random.uniform(5000, 12000), 2)
        outsourcing_cost = round(random.uniform(2000, 8000), 2)
        supplies_overhead = round(random.uniform(1000, 3000), 2)
        total = round(billing_staff_cost + software_cost + outsourcing_cost + supplies_overhead, 2)
        rows.append([period, billing_staff_cost, software_cost, outsourcing_cost, supplies_overhead, total])
    write_csv("operating_costs.csv",
              ["period", "billing_staff_cost", "software_cost", "outsourcing_cost", "supplies_overhead", "total_rcm_cost"],
              rows)
    return rows


# ---- MAIN ----
if __name__ == "__main__":
    print("Generating Healthcare RCM sample data...")
    print()
    payers = generate_payers()
    patients = generate_patients()
    providers = generate_providers()
    encounters = generate_encounters(patients, providers)
    charges = generate_charges(encounters)
    claims = generate_claims(encounters, patients)
    payments = generate_payments(claims)
    denials = generate_denials(claims)
    adjustments = generate_adjustments(claims)
    operating_costs = generate_operating_costs()
    print()
    print("Done! All sample data files created in ./data/")
