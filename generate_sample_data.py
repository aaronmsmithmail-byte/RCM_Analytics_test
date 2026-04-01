"""
Sample Data Generator for Healthcare RCM Analytics
====================================================

This script generates realistic synthetic data that simulates the data sources
found in a real hospital or medical practice billing system. It creates 10 CSV
files that mirror the output of common healthcare IT systems:

    Source System Simulation:
    ────────────────────────────────────────────────────────────────────
    CSV File              Real-World Source System
    ────────────────────────────────────────────────────────────────────
    payers.csv            Payer/Insurance Master File (contract management)
    patients.csv          Patient Registration / EHR demographics
    providers.csv         Provider Credentialing / HR system
    encounters.csv        EHR Scheduling + Admission/Discharge/Transfer (ADT)
    charges.csv           Charge Capture / Charge Description Master (CDM)
    claims.csv            Claims Management / Clearinghouse
    payments.csv          Payment Posting / ERA (Electronic Remittance Advice)
    denials.csv           Denial Management / Worklist system
    adjustments.csv       Adjustment/Write-off Posting (part of billing system)
    operating_costs.csv   Finance/Accounting (GL cost center reports)
    ────────────────────────────────────────────────────────────────────

Data Relationships (Entity-Relationship model):
    Payers ──< Patients ──< Encounters ──< Charges
                              │
                              └──< Claims ──< Payments
                                     │
                                     ├──< Denials
                                     └──< Adjustments

    (──< means "one to many")

Running This Script:
    python generate_sample_data.py

    This will create/overwrite all 10 CSV files in the ./data/ directory.
    After generating CSVs, run `python -m src.database` to load them into DuckDB.

Reproducibility:
    We use random.seed(42) so the same data is generated every time.
    Change the seed or remove it for different random data.
"""

import csv
import os
import random
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Seed the random number generator for reproducible output.
# Using seed(42) means running this script twice produces identical CSVs.
# This is important for testing — you can verify metrics against known values.
# ---------------------------------------------------------------------------
random.seed(42)

# Load .env so RCM_DATA_DIR override is respected when running this script
# directly (the same variable used by src/database.py ETL pipeline).
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

_DEFAULT_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DATA_DIR = os.environ.get("RCM_DATA_DIR", _DEFAULT_DATA_DIR)
os.makedirs(DATA_DIR, exist_ok=True)

# ===========================================================================
# Volume Constants
# ===========================================================================
# These control how much data is generated. The values below simulate a
# mid-sized medical practice over a 2-year period. You can increase these
# to stress-test the DuckDB database and dashboard performance.
#
# Relationships between volumes:
#   - Not every encounter becomes a claim (some are self-pay or unbilled)
#   - Not every claim gets a payment (some are denied or pending)
#   - Denials are a subset of claims (typically 5-15%)
#   - Adjustments apply to a subset of claims
# ===========================================================================
NUM_PATIENTS = 500  # Unique patients in the system
NUM_PROVIDERS = 25  # Physicians/clinicians on staff
NUM_ENCOUNTERS = 3000  # Total patient visits over 2 years
NUM_CLAIMS = 2800  # ~93% of encounters generate a claim
NUM_PAYMENTS = 2200  # Base count (actual count varies by claim status)
NUM_DENIALS = 420  # ~15% of claims get denied
NUM_ADJUSTMENTS = 600  # ~21% of claims have adjustments

# Date range for generated data (2 full calendar years)
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 12, 31)

# ===========================================================================
# Payer (Insurance) Reference Data
# ===========================================================================
# These represent the most common insurance payers in the US healthcare system.
# The mix includes:
#   - Commercial payers (private insurance): BCBS, Aetna, Cigna, UHC, Humana, Kaiser
#   - Government payers: Medicare (age 65+), Medicaid (low-income), Tricare (military)
#   - Self-Pay: Uninsured patients paying out of pocket
# ===========================================================================
PAYER_NAMES = [
    "Blue Cross Blue Shield",
    "Aetna",
    "Cigna",
    "UnitedHealthcare",
    "Humana",
    "Medicare",
    "Medicaid",
    "Tricare",
    "Self-Pay",
    "Kaiser Permanente",
]
PAYER_IDS = [f"PYR{str(i + 1).zfill(3)}" for i in range(len(PAYER_NAMES))]

# ===========================================================================
# Clinical Departments
# ===========================================================================
# These represent the main service lines in a multi-specialty medical group.
# Each department has different charge profiles and payer mixes.
# ===========================================================================
DEPARTMENTS = [
    "Cardiology",
    "Orthopedics",
    "Internal Medicine",
    "Emergency",
    "Pediatrics",
    "Neurology",
    "Oncology",
    "Radiology",
    "General Surgery",
    "Family Medicine",
]

# ===========================================================================
# CPT Codes (Current Procedural Terminology)
# ===========================================================================
# CPT codes are the standard coding system used to describe medical procedures
# and services. Each code maps to a specific service with a standard charge.
# Format: (code, description, base_charge_amount)
#
# The codes below represent a realistic mix of:
#   - Evaluation & Management (E/M) visits: 99203-99285 (most common)
#   - Diagnostic tests: 93000 (ECG), 71046 (X-ray), 80053/85025 (lab)
#   - Procedures: 36415 (blood draw), 10060 (I&D)
#   - Surgeries: 27447 (knee replacement), 29881 (arthroscopy)
#   - Imaging: 70553 (MRI)
#   - Endoscopy: 43239 (upper GI)
# ===========================================================================
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

# ===========================================================================
# ICD-10 Diagnosis Codes
# ===========================================================================
# ICD-10-CM codes identify the medical reason (diagnosis) for a service.
# Every claim must have at least one diagnosis code to justify medical necessity.
# These represent common outpatient diagnoses:
#   I10    = Essential hypertension (high blood pressure)
#   E11.9  = Type 2 diabetes mellitus
#   J06.9  = Upper respiratory infection
#   M54.5  = Low back pain
#   Z00.00 = General adult medical exam (wellness visit)
#   J18.9  = Pneumonia
#   K21.0  = GERD (acid reflux)
#   E78.5  = Hyperlipidemia (high cholesterol)
#   N39.0  = Urinary tract infection
#   M79.3  = Panniculitis
#   R10.9  = Abdominal pain
#   G43.909 = Migraine
#   J45.20 = Mild persistent asthma
#   F41.1  = Generalized anxiety disorder
#   I25.10 = Coronary artery disease
#   E03.9  = Hypothyroidism
#   M17.11 = Primary osteoarthritis, right knee
#   K80.20 = Gallstones
#   D64.9  = Anemia
#   L03.90 = Cellulitis
# ===========================================================================
ICD10_CODES = [
    "I10",
    "E11.9",
    "J06.9",
    "M54.5",
    "Z00.00",
    "J18.9",
    "K21.0",
    "E78.5",
    "N39.0",
    "M79.3",
    "R10.9",
    "G43.909",
    "J45.20",
    "F41.1",
    "I25.10",
    "E03.9",
    "M17.11",
    "K80.20",
    "D64.9",
    "L03.90",
]

# ===========================================================================
# Denial Reason Codes
# ===========================================================================
# These represent the most common reasons insurance companies deny claims.
# Understanding denial patterns is critical for process improvement.
# ===========================================================================
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

# ===========================================================================
# Adjustment Types
# ===========================================================================
# Adjustments reduce the amount owed on a claim. They represent the gap
# between what was billed and what can actually be collected.
#   CONTRACTUAL: The biggest category — the negotiated discount between the
#                provider and the payer (e.g., billed $200, contract says $140,
#                so $60 is a contractual adjustment).
#   WRITEOFF:    Bad debt — patient balance deemed uncollectable after
#                collection efforts.
#   CHARITY:     Services provided free to qualifying low-income patients.
#   ADMIN:       Corrections for billing errors (e.g., duplicate charge removal).
#   PROMPT_PAY:  Discount offered for early payment (sometimes for self-pay).
#   SMALL_BAL:   Balances too small to justify the cost of collection
#                (e.g., writing off a $3.50 balance).
# ===========================================================================
ADJUSTMENT_TYPES = [
    ("CONTRACTUAL", "Contractual Adjustment"),
    ("WRITEOFF", "Bad Debt Write-Off"),
    ("CHARITY", "Charity Care"),
    ("ADMIN", "Administrative Adjustment"),
    ("PROMPT_PAY", "Prompt Pay Discount"),
    ("SMALL_BAL", "Small Balance Write-Off"),
]

FIRST_NAMES = [
    "James",
    "Mary",
    "Robert",
    "Patricia",
    "John",
    "Jennifer",
    "Michael",
    "Linda",
    "David",
    "Elizabeth",
    "William",
    "Barbara",
    "Richard",
    "Susan",
    "Joseph",
    "Jessica",
    "Thomas",
    "Sarah",
    "Charles",
    "Karen",
    "Christopher",
    "Lisa",
    "Daniel",
    "Nancy",
    "Matthew",
    "Betty",
    "Anthony",
    "Margaret",
    "Mark",
    "Sandra",
    "Steven",
    "Ashley",
    "Paul",
    "Dorothy",
    "Andrew",
    "Kimberly",
    "Joshua",
    "Emily",
    "Kenneth",
    "Donna",
]

LAST_NAMES = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
    "Lee",
    "Perez",
    "Thompson",
    "White",
    "Harris",
    "Sanchez",
    "Clark",
    "Ramirez",
    "Lewis",
    "Robinson",
]

PROVIDER_NAMES = [
    "Dr. Sarah Chen",
    "Dr. James Wilson",
    "Dr. Maria Rodriguez",
    "Dr. Robert Kim",
    "Dr. Emily Johnson",
    "Dr. David Patel",
    "Dr. Lisa Thompson",
    "Dr. Michael Brown",
    "Dr. Jennifer Davis",
    "Dr. William Garcia",
    "Dr. Amanda White",
    "Dr. Christopher Lee",
    "Dr. Stephanie Martinez",
    "Dr. Brian Anderson",
    "Dr. Nicole Taylor",
    "Dr. Kevin Moore",
    "Dr. Rachel Clark",
    "Dr. Steven Harris",
    "Dr. Laura Lewis",
    "Dr. Andrew Robinson",
    "Dr. Michelle Walker",
    "Dr. Jason Hall",
    "Dr. Angela Young",
    "Dr. Ryan Allen",
    "Dr. Samantha King",
]


def rand_date(start=START_DATE, end=END_DATE):
    """Generate a random date between start and end (inclusive)."""
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def write_csv(filename, headers, rows):
    """Write a list of rows to a CSV file in the data directory."""
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"  Created {path} ({len(rows)} rows)")


# =========================================================================
# DATA GENERATION FUNCTIONS
# =========================================================================
# Each function below generates one CSV file. They are called in order
# because later tables depend on IDs from earlier tables (e.g., claims
# need encounter_ids, payments need claim_ids).
# =========================================================================


# ---- 1. PAYERS ----
def generate_payers():
    """
    Generate the payers (insurance companies) reference table.

    Simulates: Payer Master File / Contract Management System

    In a real hospital, this data comes from the managed care/contracting
    department. Each payer has a negotiated contract that defines:
    - Fee schedules (how much they pay for each CPT code)
    - Timely filing limits (how long you have to submit a claim)
    - Prior authorization requirements
    - Appeal processes and deadlines
    """
    rows = []
    for i, (pid, name) in enumerate(zip(PAYER_IDS, PAYER_NAMES)):
        payer_type = (
            "Government"
            if name in ("Medicare", "Medicaid", "Tricare")
            else ("Self-Pay" if name == "Self-Pay" else "Commercial")
        )
        avg_reimburse_pct = {"Medicare": 0.80, "Medicaid": 0.65, "Tricare": 0.75, "Self-Pay": 0.40}.get(
            name, round(random.uniform(0.70, 0.92), 2)
        )
        rows.append([pid, name, payer_type, avg_reimburse_pct, f"CON-{pid}-2024"])
    write_csv("payers.csv", ["payer_id", "payer_name", "payer_type", "avg_reimbursement_pct", "contract_id"], rows)
    return rows


# ---- 2. PATIENTS ----
def generate_patients():
    """
    Generate patient demographics and insurance information.

    Simulates: Patient Registration / EHR Master Patient Index (MPI)

    In a real hospital, this data is collected at registration (front desk)
    and includes insurance verification. Accurate patient data is critical
    because errors here cascade into claim denials downstream.
    """
    rows = []
    for i in range(NUM_PATIENTS):
        pid = f"PAT{str(i + 1).zfill(5)}"
        fname = random.choice(FIRST_NAMES)
        lname = random.choice(LAST_NAMES)
        dob = datetime(random.randint(1940, 2010), random.randint(1, 12), random.randint(1, 28))
        gender = random.choice(["M", "F"])
        payer_id = random.choice(PAYER_IDS)
        member_id = f"MEM{random.randint(100000, 999999)}"
        zip_code = f"{random.randint(10000, 99999)}"
        rows.append([pid, fname, lname, dob.strftime("%Y-%m-%d"), gender, payer_id, member_id, zip_code])
    write_csv(
        "patients.csv",
        [
            "patient_id",
            "first_name",
            "last_name",
            "date_of_birth",
            "gender",
            "primary_payer_id",
            "member_id",
            "zip_code",
        ],
        rows,
    )
    return rows


# ---- 3. PROVIDERS ----
def generate_providers():
    """
    Generate provider (physician/clinician) reference data.

    Simulates: Provider Credentialing / HR System

    Each provider has an NPI (National Provider Identifier), a unique 10-digit
    number assigned by CMS. The NPI is required on every claim submission.
    """
    rows = []
    for i in range(NUM_PROVIDERS):
        prov_id = f"PROV{str(i + 1).zfill(3)}"
        name = PROVIDER_NAMES[i]
        npi = f"{random.randint(1000000000, 9999999999)}"
        dept = DEPARTMENTS[i % len(DEPARTMENTS)]
        specialty = dept
        rows.append([prov_id, name, npi, dept, specialty])
    write_csv("providers.csv", ["provider_id", "provider_name", "npi", "department", "specialty"], rows)
    return rows


# ---- 4. ENCOUNTERS ----
def generate_encounters(patients, providers):
    """
    Generate patient encounters (visits).

    Simulates: EHR Scheduling + ADT (Admission/Discharge/Transfer) system

    An encounter is the fundamental unit of the revenue cycle — it represents
    one patient interaction that will eventually generate charges, a claim,
    and (hopefully) a payment. The encounter type distribution reflects a
    typical multi-specialty practice:
        - Outpatient (50%): Scheduled office visits
        - Emergency (20%):  ED visits
        - Inpatient (15%):  Hospital admissions (have a discharge date)
        - Telehealth (15%): Virtual visits (growing post-COVID)
    """
    rows = []
    for i in range(NUM_ENCOUNTERS):
        enc_id = f"ENC{str(i + 1).zfill(6)}"
        pat = random.choice(patients)
        prov = random.choice(providers)
        dos = rand_date()
        enc_type = random.choices(["Outpatient", "Inpatient", "Emergency", "Telehealth"], weights=[50, 15, 20, 15])[0]
        dept = prov[3]
        discharge_date = dos + timedelta(days=random.randint(0, 5)) if enc_type == "Inpatient" else dos
        rows.append(
            [enc_id, pat[0], prov[0], dos.strftime("%Y-%m-%d"), discharge_date.strftime("%Y-%m-%d"), enc_type, dept]
        )
    rows.sort(key=lambda r: r[3])
    write_csv(
        "encounters.csv",
        [
            "encounter_id",
            "patient_id",
            "provider_id",
            "date_of_service",
            "discharge_date",
            "encounter_type",
            "department",
        ],
        rows,
    )
    return rows


# ---- 5. CHARGES ----
def generate_charges(encounters):
    """
    Generate charge line items for each encounter.

    Simulates: Charge Capture / Charge Description Master (CDM)

    Each encounter generates 1-4 charges (weighted toward 1-2). For example,
    a typical office visit might generate:
        - 99214 (Office visit, Level 4):           $210
        - 85025 (Complete Blood Count):              $45
        - 36415 (Venipuncture/blood draw):           $25
        Total charges: $280

    Charge lag (post_date - service_date) is the delay between the service
    and when it appears in the billing system. Most are posted within 0-2
    days, but some take up to 30 days (e.g., complex surgical encounters
    waiting for operative notes and coding).
    """
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
            lag_days = random.choices([0, 1, 2, 3, 5, 7, 14, 30], weights=[25, 30, 20, 10, 5, 5, 3, 2])[0]
            post_date = (datetime.strptime(service_date, "%Y-%m-%d") + timedelta(days=lag_days)).strftime("%Y-%m-%d")
            icd10 = random.choice(ICD10_CODES)
            rows.append([chg_id, enc[0], cpt_code, cpt_desc, units, charge_amt, service_date, post_date, icd10])
            charge_id += 1
    write_csv(
        "charges.csv",
        [
            "charge_id",
            "encounter_id",
            "cpt_code",
            "cpt_description",
            "units",
            "charge_amount",
            "service_date",
            "post_date",
            "icd10_code",
        ],
        rows,
    )
    return rows


# ---- 6. CLAIMS ----
def generate_claims(encounters, patients):
    """
    Generate insurance claims.

    Simulates: Claims Management System / Clearinghouse

    A claim is a formal request for payment sent to an insurance payer.
    The claim lifecycle:
        1. Provider renders service (encounter)
        2. Charges are captured and coded (charges)
        3. Claim is generated and scrubbed for errors
        4. Claim is submitted electronically via a clearinghouse
        5. Payer adjudicates: Paid / Denied / Partially Paid / Pending

    Submission lag (days from service to submission) varies:
        - Same day to 3 days: Well-run practices (50% of claims)
        - 5-14 days: Typical (25% of claims)
        - 15-45 days: Delayed, often due to missing documentation (25%)

    Status distribution (realistic for a well-run practice):
        - Paid (68%):           Claim accepted and fully paid
        - Partially Paid (12%): Payer paid less than billed
        - Denied (8%):          Claim rejected entirely
        - Pending (8%):         Still being processed by payer
        - Appealed (4%):        Initially denied, appeal filed
    """
    rows = []
    pat_payer = {p[0]: p[5] for p in patients}
    enc_subset = random.sample(encounters, min(NUM_CLAIMS, len(encounters)))
    for i, enc in enumerate(enc_subset):
        claim_id = f"CLM{str(i + 1).zfill(7)}"
        payer_id = pat_payer.get(enc[1], random.choice(PAYER_IDS))
        dos = enc[3]
        submission_lag = random.choices([1, 2, 3, 5, 7, 14, 21, 30, 45], weights=[10, 20, 20, 15, 10, 10, 5, 5, 5])[0]
        submission_date = (datetime.strptime(dos, "%Y-%m-%d") + timedelta(days=submission_lag)).strftime("%Y-%m-%d")
        total_charge = round(random.uniform(50, 5000), 2)
        # Claim status distribution (realistic: ~85% paid, ~8% denied)
        status = random.choices(["Paid", "Denied", "Pending", "Partially Paid", "Appealed"], weights=[68, 8, 8, 12, 4])[
            0
        ]
        is_clean = (
            random.choices([True, False], weights=[92, 8])[0]
            if status != "Denied"
            else random.choices([True, False], weights=[40, 60])[0]
        )
        # Assign a specific scrubbing fail reason to every dirty claim.
        # Reason distribution reflects typical clearinghouse edit failure patterns.
        if is_clean:
            fail_reason = ""
        else:
            fail_reason = random.choices(
                [
                    "MISSING_AUTH",
                    "ELIGIBILITY_FAIL",
                    "CODING_ERROR",
                    "DUPLICATE_SUBMISSION",
                    "TIMELY_FILING",
                    "MISSING_INFO",
                ],
                weights=[25, 20, 25, 10, 10, 10],
            )[0]
        rows.append(
            [
                claim_id,
                enc[0],
                enc[1],
                payer_id,
                dos,
                submission_date,
                total_charge,
                status,
                is_clean,
                "Electronic",
                fail_reason,
            ]
        )
    rows.sort(key=lambda r: r[5])
    write_csv(
        "claims.csv",
        [
            "claim_id",
            "encounter_id",
            "patient_id",
            "payer_id",
            "date_of_service",
            "submission_date",
            "total_charge_amount",
            "claim_status",
            "is_clean_claim",
            "submission_method",
            "fail_reason",
        ],
        rows,
    )
    return rows


# ---- 7. PAYMENTS / REMITTANCES ----
def generate_payments(claims):
    """
    Generate payment/remittance records.

    Simulates: Payment Posting / Electronic Remittance Advice (ERA/835)

    When a payer processes a claim, they send:
        - An ERA (Electronic Remittance Advice, ANSI 835 transaction) explaining
          what was paid, denied, and adjusted
        - An EFT (Electronic Funds Transfer), check, or virtual card payment

    A single claim can have multiple payments:
        1. Payer pays their portion (e.g., 80% of allowed amount)
        2. Patient pays their responsibility (copay, coinsurance, deductible)

    Payment lag (days from submission to payment) typically ranges from
    14-90 days, depending on the payer and claim complexity.

    We also track payment accuracy — whether the payer paid the correct
    amount per their contract. ~8% of payments have inaccuracies.
    """
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
            rows.append(
                [pay_id, claim_id, payer_id, payment_amount, allowed_amount, payment_date, payment_method, is_accurate]
            )
            payment_id += 1

            # Some claims also have patient responsibility payment
            if random.random() < 0.35:
                pay_id = f"PAY{str(payment_id).zfill(7)}"
                pat_amount = round(charge * random.uniform(0.05, 0.25), 2)
                pat_pay_lag = random.randint(7, 120)
                pat_pay_date = (sub_date + timedelta(days=pat_pay_lag)).strftime("%Y-%m-%d")
                rows.append(
                    [
                        pay_id,
                        claim_id,
                        "PATIENT",
                        pat_amount,
                        pat_amount,
                        pat_pay_date,
                        random.choice(["Credit Card", "Cash", "Check", "Online Portal"]),
                        True,
                    ]
                )
                payment_id += 1

    write_csv(
        "payments.csv",
        [
            "payment_id",
            "claim_id",
            "payer_id",
            "payment_amount",
            "allowed_amount",
            "payment_date",
            "payment_method",
            "is_accurate_payment",
        ],
        rows,
    )
    return rows


# ---- 8. DENIALS ----
def generate_denials(claims):
    """
    Generate claim denial records.

    Simulates: Denial Management / Payer Correspondence

    When a payer denies a claim, they send a denial reason code explaining why.
    The provider can then:
        - Correct and resubmit (if it was a billing error)
        - File an appeal with supporting documentation
        - Write off the balance (if the denial is valid)

    Appeal outcomes:
        - Not Appealed (50%):  Staff determined the denial was valid
        - Won (15%):           Appeal overturned the denial, payment received
        - Lost (20%):          Appeal upheld the denial
        - In Progress (15%):   Appeal filed, awaiting payer response

    Denials come from:
        - Claims with status "Denied" or "Appealed"
        - Some "Partially Paid" claims (partial denials)
    """
    rows = []
    denied_claims = [c for c in claims if c[7] in ("Denied", "Appealed")]
    # Also add some denials for partially paid claims
    partial_denied = random.sample(
        [c for c in claims if c[7] == "Partially Paid"], min(50, len([c for c in claims if c[7] == "Partially Paid"]))
    )
    all_denied = denied_claims + partial_denied
    for i, claim in enumerate(all_denied):
        denial_id = f"DEN{str(i + 1).zfill(6)}"
        reason_code, reason_desc = random.choice(DENIAL_REASONS)
        sub_date = datetime.strptime(claim[5], "%Y-%m-%d")
        denial_date = (sub_date + timedelta(days=random.randint(10, 45))).strftime("%Y-%m-%d")
        denied_amount = round(claim[6] * random.uniform(0.3, 1.0), 2)
        if claim[7] == "Appealed":
            appeal_status = random.choice(["Won", "Lost", "In Progress"])
        else:
            appeal_status = random.choices(["Not Appealed", "Won", "Lost", "In Progress"], weights=[50, 15, 20, 15])[0]
        appeal_date = ""
        if appeal_status != "Not Appealed":
            appeal_date = (datetime.strptime(denial_date, "%Y-%m-%d") + timedelta(days=random.randint(5, 30))).strftime(
                "%Y-%m-%d"
            )
        recovered_amount = 0.0
        if appeal_status == "Won":
            recovered_amount = round(denied_amount * random.uniform(0.5, 1.0), 2)
        rows.append(
            [
                denial_id,
                claim[0],
                reason_code,
                reason_desc,
                denial_date,
                denied_amount,
                appeal_status,
                appeal_date,
                recovered_amount,
            ]
        )
    write_csv(
        "denials.csv",
        [
            "denial_id",
            "claim_id",
            "denial_reason_code",
            "denial_reason_description",
            "denial_date",
            "denied_amount",
            "appeal_status",
            "appeal_date",
            "recovered_amount",
        ],
        rows,
    )
    return rows


# ---- 9. ADJUSTMENTS ----
def generate_adjustments(claims):
    """
    Generate financial adjustments.

    Simulates: Adjustment/Write-off Posting in the billing system

    Adjustments represent the difference between what was billed and what
    can actually be collected. Distribution of adjustment types:
        - Contractual (45%): The largest category — negotiated payer discounts
        - Write-off (20%):   Bad debt — uncollectable patient balances
        - Admin (15%):       Corrections, billing errors
        - Prompt Pay (10%):  Early payment discounts
        - Charity (5%):      Free care for qualifying patients
        - Small Balance (5%): Balances too small to pursue
    """
    rows = []
    adj_claims = random.sample(claims, min(NUM_ADJUSTMENTS, len(claims)))
    for i, claim in enumerate(adj_claims):
        adj_id = f"ADJ{str(i + 1).zfill(6)}"
        adj_type_code, adj_type_desc = random.choices(ADJUSTMENT_TYPES, weights=[45, 20, 5, 15, 10, 5])[0]
        adj_amount = round(claim[6] * random.uniform(0.05, 0.50), 2)
        sub_date = datetime.strptime(claim[5], "%Y-%m-%d")
        adj_date = (sub_date + timedelta(days=random.randint(14, 90))).strftime("%Y-%m-%d")
        rows.append([adj_id, claim[0], adj_type_code, adj_type_desc, adj_amount, adj_date])
    write_csv(
        "adjustments.csv",
        [
            "adjustment_id",
            "claim_id",
            "adjustment_type_code",
            "adjustment_type_description",
            "adjustment_amount",
            "adjustment_date",
        ],
        rows,
    )
    return rows


# ---- 10. OPERATING COSTS (for Cost to Collect) ----
def generate_operating_costs():
    """
    Generate monthly RCM department operating costs.

    Simulates: Finance/Accounting General Ledger (GL) reports

    This data feeds the "Cost to Collect" KPI. It represents the total
    cost of running the billing/revenue cycle operation:
        - Billing staff salaries:  $35K-55K/month (largest cost)
        - Software costs:          $5K-12K/month (EHR, billing, clearinghouse)
        - Outsourcing costs:       $2K-8K/month (third-party billing services)
        - Supplies/overhead:       $1K-3K/month (printing, postage, office)

    The industry benchmark for cost to collect is 3-8% of total collections.
    """
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
    write_csv(
        "operating_costs.csv",
        ["period", "billing_staff_cost", "software_cost", "outsourcing_cost", "supplies_overhead", "total_rcm_cost"],
        rows,
    )
    return rows


# ===========================================================================
# MAIN ENTRY POINT
# ===========================================================================
# The generation order matters because of data dependencies:
#   1. Payers (no dependencies)
#   2. Patients (references payers)
#   3. Providers (no dependencies)
#   4. Encounters (references patients + providers)
#   5. Charges (references encounters)
#   6. Claims (references encounters + patients)
#   7. Payments (references claims)
#   8. Denials (references claims)
#   9. Adjustments (references claims)
#   10. Operating Costs (standalone)
# ===========================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("Healthcare RCM Analytics - Sample Data Generator")
    print("=" * 60)
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
    print("=" * 60)
    print(f"Done! All 10 CSV files created in {DATA_DIR}/")
    print("Next step: Run 'python -m src.database' to load into DuckDB.")
    print("=" * 60)
