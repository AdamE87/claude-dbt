"""
Generate ERP seed data for the duck_ap dbt project.

Usage:
    python generate_seeds.py [--seed 42] [--output-dir seeds/]

Produces reproducible CSVs for:
  ref_currency, ref_payment_terms, ref_gl_account, ref_cost_center,
  ref_payment_method, ap_vendor, ap_invoice, ap_payment,
  ap_payment_application
"""

import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TODAY = date(2025, 6, 30)
DATE_START = date(2022, 1, 1)
DATE_END = date(2025, 6, 30)

# ---------------------------------------------------------------------------
# Reference table data
# ---------------------------------------------------------------------------

CURRENCIES = [
    ("USD", "US Dollar", "$", True, 2),
    ("EUR", "Euro", "€", False, 2),
    ("GBP", "British Pound", "£", False, 2),
    ("CAD", "Canadian Dollar", "CA$", False, 2),
    ("MXN", "Mexican Peso", "MX$", False, 2),
    ("JPY", "Japanese Yen", "¥", False, 0),
    ("CNY", "Chinese Yuan", "¥", False, 2),
    ("INR", "Indian Rupee", "₹", False, 2),
    ("AUD", "Australian Dollar", "A$", False, 2),
    ("SGD", "Singapore Dollar", "S$", False, 2),
    ("CHF", "Swiss Franc", "CHF", False, 2),
    ("SEK", "Swedish Krona", "kr", False, 2),
    ("NOK", "Norwegian Krone", "kr", False, 2),
    ("DKK", "Danish Krone", "kr", False, 2),
    ("BRL", "Brazilian Real", "R$", False, 2),
    ("KRW", "South Korean Won", "₩", False, 0),
    ("HKD", "Hong Kong Dollar", "HK$", False, 2),
    ("NZD", "New Zealand Dollar", "NZ$", False, 2),
    ("ZAR", "South African Rand", "R", False, 2),
    ("ILS", "Israeli Shekel", "₪", False, 2),
]

# Static FX mid-points (USD base, 2023-24 approximate)
FX_RATES = {
    "USD": 1.0000, "EUR": 1.0850, "GBP": 1.2700, "CAD": 0.7400,
    "MXN": 0.0590, "JPY": 0.0067, "CNY": 0.1380, "INR": 0.0120,
    "AUD": 0.6550, "SGD": 0.7500, "CHF": 1.1200, "SEK": 0.0960,
    "NOK": 0.0950, "DKK": 0.1455, "BRL": 0.2030, "KRW": 0.00075,
    "HKD": 0.1280, "NZD": 0.6150, "ZAR": 0.0540, "ILS": 0.2740,
}

PAYMENT_TERMS = [
    ("IMMEDIATE", "Immediate Payment", 0, 0, 0.00, "Due upon receipt"),
    ("NET10", "Net 10", 10, 0, 0.00, "Payment due within 10 days"),
    ("NET15", "Net 15", 15, 0, 0.00, "Payment due within 15 days"),
    ("NET30", "Net 30", 30, 0, 0.00, "Payment due within 30 days"),
    ("NET45", "Net 45", 45, 0, 0.00, "Payment due within 45 days"),
    ("NET60", "Net 60", 60, 0, 0.00, "Payment due within 60 days"),
    ("NET90", "Net 90", 90, 0, 0.00, "Payment due within 90 days"),
    ("2_10_NET30", "2/10 Net 30", 30, 10, 2.00, "2% discount if paid within 10 days, otherwise net 30"),
    ("1_10_NET30", "1/10 Net 30", 30, 10, 1.00, "1% discount if paid within 10 days, otherwise net 30"),
    ("1_10_NET60", "1/10 Net 60", 60, 10, 1.00, "1% discount if paid within 10 days, otherwise net 60"),
    ("EOM", "End of Month", 0, 0, 0.00, "Due at end of current month"),
    ("NET30_EOM", "Net 30 End of Month", 30, 0, 0.00, "Net 30 from end of month"),
]

# (account_id, account_name, account_type, account_category, is_active, normal_balance)
GL_ACCOUNTS = [
    # AP / Liabilities 2000-2099
    (2000, "Accounts Payable - Trade", "LIABILITY", "ACCOUNTS_PAYABLE", True, "CREDIT"),
    (2001, "Accounts Payable - Intercompany", "LIABILITY", "ACCOUNTS_PAYABLE", True, "CREDIT"),
    (2002, "Accrued Expenses", "LIABILITY", "ACCRUED_LIABILITY", True, "CREDIT"),
    (2003, "Accrued Payroll", "LIABILITY", "ACCRUED_LIABILITY", True, "CREDIT"),
    (2004, "Sales Tax Payable", "LIABILITY", "ACCRUED_LIABILITY", True, "CREDIT"),
    (2005, "Deferred Revenue", "LIABILITY", "DEFERRED_REVENUE", True, "CREDIT"),
    (2006, "Customer Deposits", "LIABILITY", "DEFERRED_REVENUE", True, "CREDIT"),
    (2007, "Current Portion LT Debt", "LIABILITY", "DEBT", True, "CREDIT"),
    (2008, "Line of Credit Payable", "LIABILITY", "DEBT", True, "CREDIT"),
    (2009, "Accrued Interest Payable", "LIABILITY", "ACCRUED_LIABILITY", True, "CREDIT"),
    (2010, "Warranty Reserve", "LIABILITY", "ACCRUED_LIABILITY", False, "CREDIT"),
    (2011, "Lease Liability - Current", "LIABILITY", "LEASE", True, "CREDIT"),
    # COGS 5000-5999
    (5000, "Cost of Goods Sold - Products", "EXPENSE", "COGS", True, "DEBIT"),
    (5001, "Cost of Goods Sold - Services", "EXPENSE", "COGS", True, "DEBIT"),
    (5002, "Direct Labor", "EXPENSE", "COGS", True, "DEBIT"),
    (5003, "Manufacturing Overhead", "EXPENSE", "COGS", True, "DEBIT"),
    (5004, "Freight In", "EXPENSE", "COGS", True, "DEBIT"),
    (5005, "Inventory Shrinkage", "EXPENSE", "COGS", True, "DEBIT"),
    (5006, "Warranty Expense", "EXPENSE", "COGS", True, "DEBIT"),
    (5007, "Cloud Infrastructure COGS", "EXPENSE", "COGS", True, "DEBIT"),
    (5008, "Third-Party Licenses COGS", "EXPENSE", "COGS", True, "DEBIT"),
    (5009, "Professional Services COGS", "EXPENSE", "COGS", True, "DEBIT"),
    (5010, "Depreciation - COGS Assets", "EXPENSE", "COGS", True, "DEBIT"),
    # OPEX 6000-6999
    (6000, "Salaries & Wages", "EXPENSE", "OPEX", True, "DEBIT"),
    (6001, "Employee Benefits", "EXPENSE", "OPEX", True, "DEBIT"),
    (6002, "Payroll Taxes", "EXPENSE", "OPEX", True, "DEBIT"),
    (6003, "Rent & Lease", "EXPENSE", "OPEX", True, "DEBIT"),
    (6004, "Utilities", "EXPENSE", "OPEX", True, "DEBIT"),
    (6005, "Office Supplies", "EXPENSE", "OPEX", True, "DEBIT"),
    (6006, "Software & Subscriptions", "EXPENSE", "OPEX", True, "DEBIT"),
    (6007, "Marketing & Advertising", "EXPENSE", "OPEX", True, "DEBIT"),
    (6008, "Travel & Entertainment", "EXPENSE", "OPEX", True, "DEBIT"),
    (6009, "Legal & Professional Fees", "EXPENSE", "OPEX", True, "DEBIT"),
    (6010, "Insurance", "EXPENSE", "OPEX", True, "DEBIT"),
    (6011, "Repairs & Maintenance", "EXPENSE", "OPEX", True, "DEBIT"),
    (6012, "Consulting Fees", "EXPENSE", "OPEX", True, "DEBIT"),
    (6013, "Recruiting & Hiring", "EXPENSE", "OPEX", True, "DEBIT"),
    (6014, "Training & Development", "EXPENSE", "OPEX", True, "DEBIT"),
    (6015, "Depreciation - Opex Assets", "EXPENSE", "OPEX", True, "DEBIT"),
    (6016, "Amortization - Intangibles", "EXPENSE", "OPEX", True, "DEBIT"),
    (6017, "Bank Fees", "EXPENSE", "OPEX", True, "DEBIT"),
    (6018, "Dues & Subscriptions", "EXPENSE", "OPEX", True, "DEBIT"),
    (6019, "Printing & Postage", "EXPENSE", "OPEX", False, "DEBIT"),
    (6020, "Telecommunications", "EXPENSE", "OPEX", True, "DEBIT"),
    (6021, "Contract Labor", "EXPENSE", "OPEX", True, "DEBIT"),
    (6022, "Temp Agency Fees", "EXPENSE", "OPEX", True, "DEBIT"),
    (6023, "Security Services", "EXPENSE", "OPEX", True, "DEBIT"),
    (6024, "Janitorial Services", "EXPENSE", "OPEX", True, "DEBIT"),
    (6025, "Conference & Events", "EXPENSE", "OPEX", True, "DEBIT"),
    # CAPEX 7000-7999
    (7000, "Computer Equipment", "ASSET", "CAPEX", True, "DEBIT"),
    (7001, "Office Furniture & Fixtures", "ASSET", "CAPEX", True, "DEBIT"),
    (7002, "Leasehold Improvements", "ASSET", "CAPEX", True, "DEBIT"),
    (7003, "Software - Capitalized", "ASSET", "CAPEX", True, "DEBIT"),
    (7004, "Vehicles", "ASSET", "CAPEX", False, "DEBIT"),
    (7005, "Manufacturing Equipment", "ASSET", "CAPEX", True, "DEBIT"),
    (7006, "Lab Equipment", "ASSET", "CAPEX", True, "DEBIT"),
    (7007, "Networking Equipment", "ASSET", "CAPEX", True, "DEBIT"),
    # Prepaid 1200-1299
    (1200, "Prepaid Insurance", "ASSET", "PREPAID", True, "DEBIT"),
    (1201, "Prepaid Rent", "ASSET", "PREPAID", True, "DEBIT"),
    (1202, "Prepaid Software Licenses", "ASSET", "PREPAID", True, "DEBIT"),
    (1203, "Prepaid Maintenance Contracts", "ASSET", "PREPAID", True, "DEBIT"),
    (1204, "Prepaid Marketing", "ASSET", "PREPAID", True, "DEBIT"),
    (1205, "Deposits - Utilities", "ASSET", "PREPAID", True, "DEBIT"),
]

# (cost_center_id, cost_center_name, department_code, parent_id, is_active, budget_owner)
COST_CENTERS = [
    ("CC100", "Executive", "EXEC", None, True, "CEO"),
    ("CC110", "Engineering", "ENG", "CC100", True, "CTO"),
    ("CC111", "Product", "PROD", "CC110", True, "CPO"),
    ("CC112", "Infrastructure", "INFRA", "CC110", True, "VP Engineering"),
    ("CC200", "Finance", "FIN", "CC100", True, "CFO"),
    ("CC201", "Accounting", "ACCT", "CC200", True, "Controller"),
    ("CC210", "Human Resources", "HR", "CC100", True, "CHRO"),
    ("CC211", "Recruiting", "RECR", "CC210", True, "VP People"),
    ("CC300", "Marketing", "MKT", "CC100", True, "CMO"),
    ("CC400", "Sales", "SALES", "CC100", True, "CRO"),
    ("CC401", "Enterprise Sales", "ENT", "CC400", True, "VP Enterprise"),
    ("CC402", "SMB Sales", "SMB", "CC400", True, "VP SMB"),
    ("CC500", "Customer Success", "CS", "CC100", True, "VP Customer Success"),
    ("CC501", "Support", "SUPP", "CC500", True, "Director Support"),
    ("CC600", "Legal", "LEGAL", "CC100", True, "General Counsel"),
    ("CC700", "Facilities", "FACIL", "CC100", True, "Director Facilities"),
    ("CC800", "IT", "IT", "CC100", True, "CIO"),
    ("CC900", "Data & Analytics", "DATA", "CC110", True, "VP Data"),
    ("CC910", "Security", "SEC", "CC800", True, "CISO"),
    ("CC999", "Corporate", "CORP", None, True, "CFO"),
]

PAYMENT_METHODS = [
    ("ACH", "Automated Clearing House", True, 2),
    ("WIRE", "Wire Transfer", True, 1),
    ("CHECK", "Paper Check", False, 5),
    ("VCARD", "Virtual Card", True, 1),
    ("CREDIT_CARD", "Corporate Credit Card", True, 1),
    ("ZELLE", "Zelle Transfer", True, 1),
    ("PAYPAL", "PayPal", True, 2),
    ("BILLPAY", "Online Bill Pay", True, 3),
]

# ---------------------------------------------------------------------------
# Vendor name building blocks
# ---------------------------------------------------------------------------
V_PREFIXES = [
    "Apex", "Atlas", "Blue", "Bridge", "Capital", "Cedar", "Century", "Clear",
    "Cloud", "Core", "Crest", "Crown", "Delta", "Diamond", "Direct", "Eagle",
    "East", "Elite", "Empire", "Evergreen", "Excel", "First", "Frontier",
    "Global", "Gold", "Grand", "Green", "Harbor", "Highland", "Horizon",
    "Infinite", "Integrated", "Key", "Liberty", "Lincoln", "Maple", "Meridian",
    "Metro", "Mid", "Mountain", "National", "Nexus", "North", "Oak", "Pacific",
    "Patriot", "Peak", "Pinnacle", "Pioneer", "Platform", "Premier", "Prime",
    "Pro", "Pulse", "Quality", "Rapid", "Red", "Ridge", "River", "Rock",
    "Royal", "Sapphire", "Sierra", "Silver", "Sky", "Solid", "South", "Star",
    "State", "Sterling", "Summit", "Sun", "Swift", "Tech", "Titan", "Tri",
    "True", "United", "Universal", "Urban", "Valley", "Vector", "Vertex",
    "Vision", "Westside", "Willow",
]
V_NOUNS = [
    "Analytics", "Associates", "Capital", "Consulting", "Contractors",
    "Dynamics", "Enterprises", "Equipment", "Facilities", "Group",
    "Holdings", "Industries", "Infrastructure", "Innovations", "Labs",
    "Logistics", "Management", "Manufacturing", "Materials", "Networks",
    "Operations", "Partners", "Power", "Products", "Properties",
    "Resources", "Services", "Solutions", "Staffing", "Systems",
    "Technologies", "Telecom", "Utilities", "Ventures",
]
V_SUFFIXES = ["Inc.", "LLC", "Corp.", "Ltd.", "Co.", "Group", "LP", "PLC"]

US_STATES = [
    "AL", "AZ", "CA", "CO", "CT", "FL", "GA", "IL", "IN", "MA",
    "MD", "MI", "MN", "MO", "NC", "NJ", "NV", "NY", "OH", "OR",
    "PA", "TN", "TX", "VA", "WA",
]
CITIES_BY_STATE = {
    "CA": ["San Francisco", "Los Angeles", "San Diego", "San Jose"],
    "TX": ["Austin", "Dallas", "Houston", "San Antonio"],
    "NY": ["New York", "Buffalo", "Albany", "Rochester"],
    "FL": ["Miami", "Tampa", "Orlando", "Jacksonville"],
    "IL": ["Chicago", "Springfield", "Rockford", "Naperville"],
    "WA": ["Seattle", "Tacoma", "Bellevue", "Spokane"],
    "MA": ["Boston", "Cambridge", "Worcester", "Springfield"],
    "GA": ["Atlanta", "Savannah", "Augusta", "Columbus"],
    "CO": ["Denver", "Boulder", "Colorado Springs", "Fort Collins"],
    "NC": ["Charlotte", "Raleigh", "Durham", "Greensboro"],
}

INT_COUNTRIES = ["CA", "GB", "DE", "FR", "IN", "JP", "AU", "SG", "MX", "BR",
                 "NL", "SE", "CH", "IL", "KR", "HK", "NZ", "ZA", "DK", "NO"]
INT_CURRENCY = {
    "CA": "CAD", "GB": "GBP", "DE": "EUR", "FR": "EUR", "IN": "INR",
    "JP": "JPY", "AU": "AUD", "SG": "SGD", "MX": "MXN", "BR": "BRL",
    "NL": "EUR", "SE": "SEK", "CH": "CHF", "IL": "ILS", "KR": "KRW",
    "HK": "HKD", "NZ": "NZD", "ZA": "ZAR", "DK": "DKK", "NO": "NOK",
}

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael",
    "Linda", "William", "Barbara", "David", "Elizabeth", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Christopher",
    "Lisa", "Daniel", "Nancy", "Matthew", "Betty", "Anthony", "Margaret",
    "Mark", "Sandra", "Donald", "Ashley", "Steven", "Dorothy", "Paul",
    "Kimberly", "Andrew", "Emily", "Kenneth", "Donna",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
]

INVOICE_DESCS = [
    "Monthly software license fee",
    "Professional services - Q{q} engagement",
    "Cloud infrastructure services",
    "Hardware equipment purchase",
    "Consulting fees - {month} project",
    "Maintenance and support contract",
    "Marketing services - {month} campaign",
    "Staffing services - contract labor",
    "Office supplies and materials",
    "Telecommunications services",
    "Utilities - {month}",
    "Legal services - {month} retainer",
    "Shipping and logistics",
    "Training and development services",
    "Subscription renewal - annual",
    "Equipment lease payment",
    "Janitorial and facilities services",
    "Security services - {month}",
    "Temporary staffing services",
    "Software development services",
]

APPROVERS = [
    "a.chen@company.com", "b.patel@company.com", "c.smith@company.com",
    "d.johnson@company.com", "e.garcia@company.com", "f.williams@company.com",
    "g.brown@company.com", "h.davis@company.com",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def fmt_date(d) -> str:
    return d.strftime("%Y-%m-%d") if d else ""


def fmt_ts(d: date) -> str:
    hour = random.randint(7, 18)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return f"{d} {hour:02d}:{minute:02d}:{second:02d}"


def round2(x: float) -> str:
    return f"{x:.2f}"


def rand_amount(lo: float, hi: float) -> float:
    # Skew toward lower values
    raw = random.expovariate(1 / ((hi - lo) / 4)) + lo
    return round(min(raw, hi), 2)


def next_weekday(d: date, weekday: int) -> date:
    """Return next occurrence of weekday (0=Mon) on or after d."""
    days_ahead = weekday - d.weekday()
    if days_ahead < 0:
        days_ahead += 7
    return d + timedelta(days=days_ahead)


def write_csv(path: Path, fieldnames: list, rows: list) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Reference table generators
# ---------------------------------------------------------------------------

def gen_ref_currency():
    rows = []
    for code, name, symbol, is_base, decimals in CURRENCIES:
        rows.append({
            "currency_code": code,
            "currency_name": name,
            "symbol": symbol,
            "is_base_currency": is_base,
            "decimal_places": decimals,
        })
    return rows


def gen_ref_payment_terms():
    rows = []
    for code, name, due, disc_days, disc_pct, desc in PAYMENT_TERMS:
        rows.append({
            "payment_terms_code": code,
            "payment_terms_name": name,
            "due_days": due,
            "discount_days": disc_days,
            "discount_pct": round2(disc_pct),
            "description": desc,
        })
    return rows


def gen_ref_gl_account():
    rows = []
    for acct_id, name, acct_type, category, is_active, normal_bal in GL_ACCOUNTS:
        rows.append({
            "gl_account_id": acct_id,
            "account_name": name,
            "account_type": acct_type,
            "account_category": category,
            "is_active": is_active,
            "normal_balance": normal_bal,
        })
    return rows


def gen_ref_cost_center():
    rows = []
    for cc_id, name, dept, parent, is_active, owner in COST_CENTERS:
        rows.append({
            "cost_center_id": cc_id,
            "cost_center_name": name,
            "department_code": dept,
            "parent_cost_center_id": parent if parent else "",
            "is_active": is_active,
            "budget_owner": owner,
        })
    return rows


def gen_ref_payment_method():
    rows = []
    for code, name, is_elec, proc_days in PAYMENT_METHODS:
        rows.append({
            "payment_method_code": code,
            "payment_method_name": name,
            "is_electronic": is_elec,
            "typical_processing_days": proc_days,
        })
    return rows


# ---------------------------------------------------------------------------
# Core AP generators
# ---------------------------------------------------------------------------

def gen_vendors(n: int = 200):
    used_names = set()
    used_tax_ids = set()
    rows = []

    # Status/type distributions
    statuses = (["ACTIVE"] * 160 + ["INACTIVE"] * 25 + ["ON_HOLD"] * 15)
    random.shuffle(statuses)

    types = (["SUPPLIER"] * 120 + ["CONTRACTOR"] * 40 + ["UTILITY"] * 20
             + ["GOVT"] * 15 + ["INTERCOMPANY"] * 5)
    random.shuffle(types)

    currency_codes = [c[0] for c in CURRENCIES]
    terms_codes = [t[0] for t in PAYMENT_TERMS]
    method_codes = [m[0] for m in PAYMENT_METHODS]

    # 140 US, 60 international
    countries = (["US"] * 140 + INT_COUNTRIES[:20] * 3)[:n]
    random.shuffle(countries)

    created_base = date(2018, 1, 1)

    for i in range(n):
        vendor_id = f"V{i+1:04d}"

        # Unique vendor name
        for _ in range(50):
            name = f"{random.choice(V_PREFIXES)} {random.choice(V_NOUNS)} {random.choice(V_SUFFIXES)}"
            if name not in used_names:
                used_names.add(name)
                break

        # Tax ID (EIN format for US, generic for international)
        for _ in range(50):
            if countries[i] == "US":
                tax_id = f"{random.randint(10,99)}-{random.randint(1000000,9999999)}"
            else:
                tax_id = f"TIN{random.randint(100000000, 999999999)}"
            if tax_id not in used_tax_ids:
                used_tax_ids.add(tax_id)
                break

        country = countries[i]
        if country == "US":
            currency_code = "USD"
            state = random.choice(US_STATES)
            city = random.choice(CITIES_BY_STATE.get(state, ["Springfield"]))
            zip_code = f"{random.randint(10000, 99999)}"
            remit_state = state
        else:
            currency_code = INT_CURRENCY.get(country, "USD")
            city = f"City-{country}"
            remit_state = ""
            zip_code = ""

        vendor_type = types[i]
        if vendor_type == "UTILITY":
            method = "ACH"
            terms = "NET30"
            credit_limit = round(random.uniform(5000, 20000), 2)
        elif vendor_type == "CONTRACTOR":
            method = random.choice(["ACH", "WIRE", "CHECK"])
            terms = random.choice(["NET30", "NET45", "NET60"])
            credit_limit = round(random.uniform(50000, 250000), 2)
        elif vendor_type == "INTERCOMPANY":
            method = "WIRE"
            terms = "NET30"
            credit_limit = round(random.uniform(500000, 2000000), 2)
        elif vendor_type == "GOVT":
            method = random.choice(["ACH", "CHECK"])
            terms = random.choice(["NET30", "NET45"])
            credit_limit = round(random.uniform(10000, 100000), 2)
        else:  # SUPPLIER
            method = random.choice(method_codes)
            terms = random.choice(terms_codes)
            credit_limit = round(random.uniform(10000, 500000), 2)

        status = statuses[i]
        created_date = rand_date(created_base, date(2023, 12, 31))
        inactivated_date = ""
        if status == "INACTIVE":
            inactivated_date = fmt_date(rand_date(created_date + timedelta(days=180), TODAY))

        contact_first = random.choice(FIRST_NAMES)
        contact_last = random.choice(LAST_NAMES)
        contact_name = f"{contact_first} {contact_last}"
        contact_email = f"{contact_first[0].lower()}.{contact_last.lower()}@{name.split()[0].lower()}.com"

        rows.append({
            "vendor_id": vendor_id,
            "vendor_name": name,
            "vendor_type": vendor_type,
            "tax_id": tax_id,
            "country_code": country,
            "currency_code": currency_code,
            "payment_terms_code": terms,
            "payment_method_code": method,
            "vendor_status": status,
            "remit_address_line1": f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Elm', 'Park', 'First', 'Second', 'Commerce'])} {random.choice(['St', 'Ave', 'Blvd', 'Dr', 'Way'])}",
            "remit_city": city,
            "remit_state": remit_state,
            "remit_zip": zip_code,
            "credit_limit": round2(credit_limit),
            "is_1099_vendor": vendor_type in ("CONTRACTOR",),
            "created_date": fmt_date(created_date),
            "inactivated_date": inactivated_date,
            "primary_contact_name": contact_name,
            "primary_contact_email": contact_email,
        })

    return rows


def gen_invoices(vendors: list, n: int = 2000):
    vendor_ids = [v["vendor_id"] for v in vendors]
    active_vendors = [v for v in vendors if v["vendor_status"] == "ACTIVE"]
    inactive_vendors = [v for v in vendors if v["vendor_status"] != "ACTIVE"]

    gl_ids = [g[0] for g in GL_ACCOUNTS if g[4]]  # active only
    cc_ids = [c[0] for c in COST_CENTERS if c[4]]  # active only
    terms_codes = [t[0] for t in PAYMENT_TERMS]

    rows = []
    used_inv_nums = set()

    # Status distribution: PAID 60% / OPEN 15% / OVERDUE 10% / PARTIAL 8% / VOID 5% / ON_HOLD 2%
    # We'll assign status after building dates
    # Reserve some invoices for ON_HOLD explicitly
    on_hold_count = int(n * 0.02)
    void_count = int(n * 0.05)
    partial_count = int(n * 0.08)

    for i in range(n):
        invoice_id = f"INV{i+1:06d}"

        # Bias toward active vendors (90% of invoices)
        if random.random() < 0.9 and active_vendors:
            vendor = random.choice(active_vendors)
        else:
            vendor = random.choice(vendors)

        # Invoice number (vendor prefix + sequential)
        v_prefix = vendor["vendor_id"].replace("V", "")
        for _ in range(20):
            inv_num = f"{vendor['vendor_id']}-{random.randint(1000, 99999)}"
            if inv_num not in used_inv_nums:
                used_inv_nums.add(inv_num)
                break

        invoice_date = rand_date(DATE_START, DATE_END)
        received_date = invoice_date + timedelta(days=random.randint(0, 5))
        if received_date > TODAY:
            received_date = TODAY

        # Due date based on vendor payment terms
        terms_code = vendor["payment_terms_code"]
        terms_map = {t[0]: t[2] for t in PAYMENT_TERMS}
        due_days = terms_map.get(terms_code, 30)
        if due_days == 0:
            due_days = 30  # EOM/IMMEDIATE treated as 30 for date calc
        due_date = invoice_date + timedelta(days=due_days)

        # GL account: weighted toward COGS and OPEX
        gl_weights = []
        for g in GL_ACCOUNTS:
            if not g[4]:
                continue
            if g[2] == "EXPENSE":
                gl_weights.append((g[0], 5))
            elif g[3] == "ACCOUNTS_PAYABLE":
                gl_weights.append((g[0], 3))
            else:
                gl_weights.append((g[0], 1))
        total_w = sum(w for _, w in gl_weights)
        r = random.uniform(0, total_w)
        cum = 0
        gl_account_id = gl_weights[0][0]
        for gid, w in gl_weights:
            cum += w
            if r <= cum:
                gl_account_id = gid
                break

        cost_center_id = random.choice(cc_ids)
        currency_code = vendor["currency_code"]
        fx_base = FX_RATES.get(currency_code, 1.0)
        fx_rate = round(fx_base * random.uniform(0.98, 1.02), 6)

        # Amount ranges by vendor type
        vtype = vendor["vendor_type"]
        if vtype == "UTILITY":
            inv_amount = rand_amount(50, 5000)
        elif vtype == "CONTRACTOR":
            inv_amount = rand_amount(500, 25000)
        elif vtype == "GOVT":
            inv_amount = rand_amount(50, 2000)
        elif vtype == "INTERCOMPANY":
            inv_amount = rand_amount(10000, 500000)
        else:  # SUPPLIER
            inv_amount = rand_amount(100, 250000)

        inv_amount = round(inv_amount, 2)
        tax_amount = round(random.choice([0, 0, 0, inv_amount * random.uniform(0.05, 0.10)]), 2)

        # Discount
        terms_disc = {t[0]: t[4] for t in PAYMENT_TERMS}
        disc_pct = terms_disc.get(terms_code, 0)
        discount_amount = round(inv_amount * disc_pct / 100, 2) if disc_pct > 0 else 0.00

        inv_amount_usd = round(inv_amount * fx_rate, 2)

        # Description
        desc_template = random.choice(INVOICE_DESCS)
        month_name = invoice_date.strftime("%B")
        q = (invoice_date.month - 1) // 3 + 1
        description = desc_template.format(q=q, month=month_name)

        # PO number (70% of invoices have one)
        po_number = f"PO{random.randint(10000, 99999)}" if random.random() < 0.7 else ""

        approved_by = random.choice(APPROVERS)
        approved_date = received_date + timedelta(days=random.randint(1, 7))
        if approved_date > TODAY:
            approved_date = TODAY

        created_at = fmt_ts(received_date)
        updated_at = fmt_ts(approved_date)

        rows.append({
            "invoice_id": invoice_id,
            "vendor_id": vendor["vendor_id"],
            "invoice_number": inv_num,
            "invoice_date": fmt_date(invoice_date),
            "received_date": fmt_date(received_date),
            "due_date": fmt_date(due_date),
            "gl_account_id": gl_account_id,
            "cost_center_id": cost_center_id,
            "currency_code": currency_code,
            "invoice_amount": round2(inv_amount),
            "invoice_amount_usd": round2(inv_amount_usd),
            "fx_rate": f"{fx_rate:.6f}",
            "tax_amount": round2(tax_amount),
            "discount_amount": round2(discount_amount),
            "payment_terms_code": terms_code,
            "invoice_status": "PENDING",  # will be assigned below
            "description": description,
            "po_number": po_number,
            "approved_by": approved_by,
            "approved_date": fmt_date(approved_date),
            "created_at": created_at,
            "updated_at": updated_at,
            # Stash for payment gen
            "_due_date_obj": due_date,
            "_inv_amount": inv_amount,
            "_vendor_currency": currency_code,
        })

    return rows


def assign_invoice_statuses(invoices: list, void_count: int, on_hold_count: int, partial_count: int):
    """Assign invoice_status in-place. Returns set of PAID invoice IDs."""
    n = len(invoices)
    indices = list(range(n))
    random.shuffle(indices)

    void_idx = set(indices[:void_count])
    on_hold_idx = set(indices[void_count:void_count + on_hold_count])
    partial_idx = set(indices[void_count + on_hold_count:void_count + on_hold_count + partial_count])

    # Remaining: PAID 60%, OPEN 15%, OVERDUE 10% of total
    remaining = [i for i in indices[void_count + on_hold_count + partial_count:]]
    n_remaining = len(remaining)
    paid_target = int(n * 0.60)
    overdue_target = int(n * 0.10)
    open_target = n_remaining - paid_target - overdue_target
    if open_target < 0:
        open_target = 0

    random.shuffle(remaining)
    paid_idx = set(remaining[:paid_target])
    overdue_idx = set(remaining[paid_target:paid_target + overdue_target])
    open_idx = set(remaining[paid_target + overdue_target:])

    paid_invoice_ids = set()
    for i, inv in enumerate(invoices):
        if i in void_idx:
            inv["invoice_status"] = "VOID"
        elif i in on_hold_idx:
            inv["invoice_status"] = "ON_HOLD"
        elif i in partial_idx:
            inv["invoice_status"] = "PARTIAL"
        elif i in paid_idx:
            inv["invoice_status"] = "PAID"
            paid_invoice_ids.add(inv["invoice_id"])
        elif i in overdue_idx:
            inv["invoice_status"] = "OVERDUE"
        else:
            inv["invoice_status"] = "OPEN"

    return paid_invoice_ids


def gen_payments_and_applications(invoices: list, vendors: list, paid_invoice_ids: set):
    """
    Generate payments by batching PAID invoices per vendor per week.
    Also generate PARTIAL payment applications for PARTIAL invoices.
    Returns (payment_rows, application_rows).
    """
    vendor_map = {v["vendor_id"]: v for v in vendors}

    # Group paid invoices by vendor + ISO week
    from collections import defaultdict
    paid_batches = defaultdict(list)

    for inv in invoices:
        if inv["invoice_id"] in paid_invoice_ids:
            due_date = inv["_due_date_obj"]
            # Payment happens shortly after due date or on payment run day
            pay_date = due_date + timedelta(days=random.randint(-2, 5))
            if pay_date < DATE_START:
                pay_date = DATE_START
            if pay_date > TODAY:
                pay_date = TODAY
            # Snap to Tue (1) or Thu (3)
            pay_date_tue = next_weekday(pay_date, 1)
            pay_date_thu = next_weekday(pay_date, 3)
            pay_date = pay_date_tue if pay_date_tue <= pay_date_thu else pay_date_thu
            if pay_date > TODAY:
                pay_date = today_minus(7)

            year_week = pay_date.isocalendar()[:2]
            batch_key = (inv["vendor_id"], year_week)
            paid_batches[batch_key].append((inv, pay_date))

    payment_rows = []
    application_rows = []
    check_seq = 10001
    pay_seq = 1
    app_seq = 1

    for (vendor_id, _week), batch in sorted(paid_batches.items()):
        vendor = vendor_map[vendor_id]
        method = vendor["payment_method_code"]
        currency_code = vendor["currency_code"]
        fx_base = FX_RATES.get(currency_code, 1.0)
        fx_rate = round(fx_base * random.uniform(0.98, 1.02), 6)

        # One payment per batch
        pay_date = batch[0][1]  # use first invoice's pay date
        total_amount = sum(inv["_inv_amount"] for inv, _ in batch)
        total_amount = round(total_amount, 2)
        total_amount_usd = round(total_amount * fx_rate, 2)

        # 90% CLEARED / 5% OUTSTANDING / 5% VOIDED
        status_roll = random.random()
        if status_roll < 0.90:
            pay_status = "CLEARED"
            cleared_date = fmt_date(pay_date + timedelta(days=random.randint(1, 5)))
            void_date = ""
        elif status_roll < 0.95:
            pay_status = "OUTSTANDING"
            cleared_date = ""
            void_date = ""
        else:
            pay_status = "VOIDED"
            cleared_date = ""
            void_date = fmt_date(pay_date + timedelta(days=random.randint(1, 10)))

        check_number = ""
        if method == "CHECK":
            check_number = str(check_seq)
            check_seq += 1

        bank_account_ref = f"BA-{vendor_id[-4:]}-{random.randint(1000,9999)}"
        memo = f"Payment batch {vendor_id} wk {_week[1]}/{_week[0]}"
        created_by = random.choice(APPROVERS)
        created_at = fmt_ts(pay_date)
        payment_id = f"PAY{pay_seq:06d}"
        pay_seq += 1

        payment_rows.append({
            "payment_id": payment_id,
            "vendor_id": vendor_id,
            "payment_date": fmt_date(pay_date),
            "payment_method_code": method,
            "currency_code": currency_code,
            "payment_amount": round2(total_amount),
            "payment_amount_usd": round2(total_amount_usd),
            "fx_rate": f"{fx_rate:.6f}",
            "check_number": check_number,
            "bank_account_ref": bank_account_ref,
            "payment_status": pay_status,
            "cleared_date": cleared_date,
            "void_date": void_date,
            "memo": memo,
            "created_at": created_at,
            "created_by": created_by,
        })

        # Applications
        for inv, _ in batch:
            app_id = f"APP{app_seq:07d}"
            app_seq += 1

            # Discount taken if early payment
            disc_amount = float(inv["discount_amount"])
            application_rows.append({
                "application_id": app_id,
                "payment_id": payment_id,
                "invoice_id": inv["invoice_id"],
                "vendor_id": vendor_id,
                "applied_amount": round2(inv["_inv_amount"] - disc_amount),
                "discount_taken": round2(disc_amount),
                "applied_date": fmt_date(pay_date),
            })

    # PARTIAL invoice applications (separate partial payments)
    partial_invoices = [inv for inv in invoices if inv["invoice_status"] == "PARTIAL"]
    for inv in partial_invoices:
        pay_date = inv["_due_date_obj"] - timedelta(days=random.randint(0, 10))
        if pay_date < DATE_START:
            pay_date = DATE_START
        if pay_date > TODAY:
            pay_date = TODAY

        vendor = vendor_map[inv["vendor_id"]]
        method = vendor["payment_method_code"]
        currency_code = vendor["currency_code"]
        fx_base = FX_RATES.get(currency_code, 1.0)
        fx_rate = round(fx_base * random.uniform(0.98, 1.02), 6)

        partial_pct = random.uniform(0.25, 0.75)
        partial_amount = round(inv["_inv_amount"] * partial_pct, 2)
        partial_amount_usd = round(partial_amount * fx_rate, 2)

        check_number = ""
        if method == "CHECK":
            check_number = str(check_seq)
            check_seq += 1

        bank_account_ref = f"BA-{inv['vendor_id'][-4:]}-{random.randint(1000,9999)}"
        payment_id = f"PAY{pay_seq:06d}"
        pay_seq += 1

        payment_rows.append({
            "payment_id": payment_id,
            "vendor_id": inv["vendor_id"],
            "payment_date": fmt_date(pay_date),
            "payment_method_code": method,
            "currency_code": currency_code,
            "payment_amount": round2(partial_amount),
            "payment_amount_usd": round2(partial_amount_usd),
            "fx_rate": f"{fx_rate:.6f}",
            "check_number": check_number,
            "bank_account_ref": bank_account_ref,
            "payment_status": "CLEARED",
            "cleared_date": fmt_date(pay_date + timedelta(days=2)),
            "void_date": "",
            "memo": f"Partial payment for {inv['invoice_id']}",
            "created_at": fmt_ts(pay_date),
            "created_by": random.choice(APPROVERS),
        })

        app_id = f"APP{app_seq:07d}"
        app_seq += 1
        application_rows.append({
            "application_id": app_id,
            "payment_id": payment_id,
            "invoice_id": inv["invoice_id"],
            "vendor_id": inv["vendor_id"],
            "applied_amount": round2(partial_amount),
            "discount_taken": "0.00",
            "applied_date": fmt_date(pay_date),
        })

    return payment_rows, application_rows


def today_minus(days: int) -> date:
    return TODAY - timedelta(days=days)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate duck_ap seed CSVs")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    parser.add_argument("--output-dir", default="seeds/", help="Output directory (default: seeds/)")
    args = parser.parse_args()

    random.seed(args.seed)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating seed data (random seed={args.seed}) → {out_dir}/")

    # --- Reference tables ---
    currency_rows = gen_ref_currency()
    write_csv(out_dir / "ref_currency.csv",
              ["currency_code", "currency_name", "symbol", "is_base_currency", "decimal_places"],
              currency_rows)
    print(f"  ref_currency:        {len(currency_rows):>5} rows")

    terms_rows = gen_ref_payment_terms()
    write_csv(out_dir / "ref_payment_terms.csv",
              ["payment_terms_code", "payment_terms_name", "due_days", "discount_days", "discount_pct", "description"],
              terms_rows)
    print(f"  ref_payment_terms:   {len(terms_rows):>5} rows")

    gl_rows = gen_ref_gl_account()
    write_csv(out_dir / "ref_gl_account.csv",
              ["gl_account_id", "account_name", "account_type", "account_category", "is_active", "normal_balance"],
              gl_rows)
    print(f"  ref_gl_account:      {len(gl_rows):>5} rows")

    cc_rows = gen_ref_cost_center()
    write_csv(out_dir / "ref_cost_center.csv",
              ["cost_center_id", "cost_center_name", "department_code", "parent_cost_center_id", "is_active", "budget_owner"],
              cc_rows)
    print(f"  ref_cost_center:     {len(cc_rows):>5} rows")

    method_rows = gen_ref_payment_method()
    write_csv(out_dir / "ref_payment_method.csv",
              ["payment_method_code", "payment_method_name", "is_electronic", "typical_processing_days"],
              method_rows)
    print(f"  ref_payment_method:  {len(method_rows):>5} rows")

    # --- Vendors ---
    vendor_rows = gen_vendors(200)
    write_csv(out_dir / "ap_vendor.csv",
              ["vendor_id", "vendor_name", "vendor_type", "tax_id", "country_code",
               "currency_code", "payment_terms_code", "payment_method_code", "vendor_status",
               "remit_address_line1", "remit_city", "remit_state", "remit_zip",
               "credit_limit", "is_1099_vendor", "created_date", "inactivated_date",
               "primary_contact_name", "primary_contact_email"],
              vendor_rows)
    status_counts = {}
    for v in vendor_rows:
        status_counts[v["vendor_status"]] = status_counts.get(v["vendor_status"], 0) + 1
    print(f"  ap_vendor:           {len(vendor_rows):>5} rows  {status_counts}")

    # --- Invoices ---
    invoice_rows = gen_invoices(vendor_rows, 2000)
    n = len(invoice_rows)
    void_count = int(n * 0.05)
    on_hold_count = int(n * 0.02)
    partial_count = int(n * 0.08)
    paid_invoice_ids = assign_invoice_statuses(invoice_rows, void_count, on_hold_count, partial_count)

    inv_status_counts = {}
    for inv in invoice_rows:
        s = inv["invoice_status"]
        inv_status_counts[s] = inv_status_counts.get(s, 0) + 1

    # Strip internal _fields before writing
    inv_fieldnames = [
        "invoice_id", "vendor_id", "invoice_number", "invoice_date", "received_date",
        "due_date", "gl_account_id", "cost_center_id", "currency_code",
        "invoice_amount", "invoice_amount_usd", "fx_rate", "tax_amount", "discount_amount",
        "payment_terms_code", "invoice_status", "description", "po_number",
        "approved_by", "approved_date", "created_at", "updated_at",
    ]
    inv_clean = [{k: v for k, v in row.items() if not k.startswith("_")} for row in invoice_rows]
    write_csv(out_dir / "ap_invoice.csv", inv_fieldnames, inv_clean)
    print(f"  ap_invoice:          {len(invoice_rows):>5} rows  {inv_status_counts}")

    # --- Payments & Applications ---
    payment_rows, application_rows = gen_payments_and_applications(invoice_rows, vendor_rows, paid_invoice_ids)

    pay_status_counts = {}
    for p in payment_rows:
        s = p["payment_status"]
        pay_status_counts[s] = pay_status_counts.get(s, 0) + 1

    write_csv(out_dir / "ap_payment.csv",
              ["payment_id", "vendor_id", "payment_date", "payment_method_code",
               "currency_code", "payment_amount", "payment_amount_usd", "fx_rate",
               "check_number", "bank_account_ref", "payment_status", "cleared_date",
               "void_date", "memo", "created_at", "created_by"],
              payment_rows)
    print(f"  ap_payment:          {len(payment_rows):>5} rows  {pay_status_counts}")

    write_csv(out_dir / "ap_payment_application.csv",
              ["application_id", "payment_id", "invoice_id", "vendor_id",
               "applied_amount", "discount_taken", "applied_date"],
              application_rows)
    print(f"  ap_payment_application: {len(application_rows):>5} rows")

    total = (len(currency_rows) + len(terms_rows) + len(gl_rows) + len(cc_rows)
             + len(method_rows) + len(vendor_rows) + len(invoice_rows)
             + len(payment_rows) + len(application_rows))
    print(f"\nTotal rows: {total:,}")
    print("Done.")


if __name__ == "__main__":
    main()
