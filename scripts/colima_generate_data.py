"""
generate_data.py — Simulate IDM test data for ail and lnl orgs.

Generates 4 CSV files per org in scratch/data/{org}/:
  {org}basic.csv    — policyholders + policies
  {org}address.csv  — one address per policy
  {org}bankinfo.csv — one bank record per policy
  {org}premiums.csv — 3 months of premium transactions per policy

Validation scenarios baked in (both orgs):
  - One insured has exactly 2 policies → dedup test
  - 5% of basic rows have null SSN → NAME_DOB fallback path
  - 3% of address rows have null city → rejection logic

Usage:
    python scripts/generate_data.py [--org ail|lnl|all]
"""

import argparse
import csv
import os
import random
import uuid
from datetime import date, timedelta

from faker import Faker

POLICY_TYPES  = ["WHOLE_LIFE", "TERM_10", "TERM_20", "UNIVERSAL_LIFE", "ENDOWMENT"]
POLICY_STATUS = ["ACTIVE", "LAPSED", "CANCELLED", "PENDING"]
GENDERS       = ["M", "F", "U"]
ACCT_TYPES    = ["CHECKING", "SAVINGS"]
PAY_METHODS   = ["EFT", "CHECK", "CREDIT_CARD"]
PREM_STATUSES = ["PAID", "PAID", "PAID", "RETURNED", "PENDING"]
ADDR_TYPES    = ["HOME", "MAILING"]

ORG_CONFIG = {
    "ail": {
        "seed":          42,
        "n_insureds":    500,
        "policy_prefix": "AIL",
        "dedup_ssn":     "123-45-6789",
        "batch_date":    date(2026, 3, 31),
    },
    "lnl": {
        "seed":          99,
        "n_insureds":    300,
        "policy_prefix": "LNL",
        "dedup_ssn":     "987-65-4321",
        "batch_date":    date(2026, 3, 31),
    },
}

OUT_BASE = os.path.join(os.path.dirname(__file__), "..", "scratch", "data")


def ssn_str(n):
    s = str(n).zfill(9)
    return f"{s[:3]}-{s[3:5]}-{s[5:]}"


def random_date(start_year=1960, end_year=2000, rng=None):
    rng = rng or random
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)
    return start + timedelta(days=rng.randint(0, (end - start).days))


def random_issue_date(rng=None):
    rng = rng or random
    start = date(2005, 1, 1)
    end   = date(2025, 12, 31)
    return start + timedelta(days=rng.randint(0, (end - start).days))


def write_csv(path, rows, fieldnames):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  [OK] {os.path.basename(path):30s}  {len(rows):>5} rows")


def generate_org(org):
    cfg    = ORG_CONFIG[org]
    rng    = random.Random(cfg["seed"])
    fake   = Faker()
    Faker.seed(cfg["seed"])

    batch_date  = cfg["batch_date"]
    prefix      = cfg["policy_prefix"]
    n_insureds  = cfg["n_insureds"]
    dedup_ssn   = cfg["dedup_ssn"]
    out_dir     = os.path.join(OUT_BASE, org)

    # ── Build insured pool ────────────────────────────────────────────────────
    insureds    = []
    ssn_counter = 100000001

    for i in range(n_insureds):
        ssn  = ssn_str(ssn_counter)
        ssn_counter += rng.randint(1, 5)
        insureds.append({
            "ssn":        ssn,
            "first_name": fake.first_name().upper(),
            "last_name":  fake.last_name().upper(),
            "dob":        random_date(rng=rng),
            "gender":     rng.choice(GENDERS),
        })

    # Force dedup test: insured 0 gets the org-specific dedup SSN with exactly 2 policies
    insureds[0]["ssn"] = dedup_ssn

    # ── Build basic rows ──────────────────────────────────────────────────────
    basic_rows   = []
    policy_count = 0

    for idx, insured in enumerate(insureds):
        n_policies = 2 if idx == 0 else rng.choices([1, 2, 3], weights=[60, 30, 10])[0]

        for _ in range(n_policies):
            policy_count += 1
            policy_number = f"{prefix}{str(policy_count).zfill(7)}"
            issue_date    = random_issue_date(rng=rng)
            face_amount   = round(
                rng.choice([25000, 50000, 100000, 250000, 500000]) * rng.uniform(0.8, 1.2), 2
            )
            # 5% null SSN → NAME_DOB fallback (skip insured 0 — dedup test must keep SSN)
            ssn_val = None if (idx > 0 and rng.random() < 0.05) else insured["ssn"]

            basic_rows.append({
                "policy_number": policy_number,
                "ssn":           ssn_val or "",
                "first_name":    insured["first_name"],
                "last_name":     insured["last_name"],
                "dob":           insured["dob"].isoformat(),
                "gender":        insured["gender"],
                "policy_type":   rng.choice(POLICY_TYPES),
                "policy_status": rng.choices(POLICY_STATUS, weights=[70, 15, 10, 5])[0],
                "issue_date":    issue_date.isoformat(),
                "face_amount":   f"{face_amount:.2f}",
                "org_code":      org,
            })

    # ── Build address rows ────────────────────────────────────────────────────
    address_rows = []
    for row in basic_rows:
        # 3% null city → rejection candidate
        city = "" if rng.random() < 0.03 else fake.city().upper()
        address_rows.append({
            "policy_number":  row["policy_number"],
            "address_type":   rng.choice(ADDR_TYPES),
            "street1":        fake.street_address().upper(),
            "street2":        "",
            "city":           city,
            "state":          fake.state_abbr(),
            "zip":            fake.zipcode(),
            "effective_date": row["issue_date"],
            "org_code":       org,
        })

    # ── Build bankinfo rows ───────────────────────────────────────────────────
    bankinfo_rows = []
    for row in basic_rows:
        bankinfo_rows.append({
            "policy_number":  row["policy_number"],
            "bank_name":      fake.company().upper(),
            "routing_number": str(rng.randint(100000000, 999999999)),
            "account_number": str(rng.randint(1000000000, 9999999999)),
            "account_type":   rng.choice(ACCT_TYPES),
            "effective_date": row["issue_date"],
            "org_code":       org,
        })

    # ── Build premiums rows ───────────────────────────────────────────────────
    premiums_rows = []
    for row in basic_rows:
        base_premium = round(float(row["face_amount"]) * rng.uniform(0.002, 0.008), 2)
        for month_offset in range(3):
            prem_date = batch_date.replace(day=1) - timedelta(days=30 * month_offset)
            premiums_rows.append({
                "policy_number":  row["policy_number"],
                "transaction_id": str(uuid.UUID(int=rng.getrandbits(128))).replace("-", "").upper()[:16],
                "premium_date":   prem_date.isoformat(),
                "amount":         f"{base_premium:.2f}",
                "payment_method": rng.choice(PAY_METHODS),
                "status":         rng.choice(PREM_STATUSES),
                "org_code":       org,
            })

    # ── Write CSVs ────────────────────────────────────────────────────────────
    print(f"\n[{org}]")
    write_csv(os.path.join(out_dir, f"{org}basic.csv"), basic_rows, [
        "policy_number", "ssn", "first_name", "last_name", "dob", "gender",
        "policy_type", "policy_status", "issue_date", "face_amount", "org_code",
    ])
    write_csv(os.path.join(out_dir, f"{org}address.csv"), address_rows, [
        "policy_number", "address_type", "street1", "street2", "city",
        "state", "zip", "effective_date", "org_code",
    ])
    write_csv(os.path.join(out_dir, f"{org}bankinfo.csv"), bankinfo_rows, [
        "policy_number", "bank_name", "routing_number", "account_number",
        "account_type", "effective_date", "org_code",
    ])
    write_csv(os.path.join(out_dir, f"{org}premiums.csv"), premiums_rows, [
        "policy_number", "transaction_id", "premium_date", "amount",
        "payment_method", "status", "org_code",
    ])

    # ── Spot checks ───────────────────────────────────────────────────────────
    dedup_pols  = [r["policy_number"] for r in basic_rows if r["ssn"] == dedup_ssn]
    null_ssn    = sum(1 for r in basic_rows if not r["ssn"])
    null_city   = sum(1 for r in address_rows if not r["city"])
    print(f"  Dedup insured ({dedup_ssn}) policies: {dedup_pols}  (expect 2)")
    print(f"  Null SSN  : {null_ssn}/{len(basic_rows)}  ({null_ssn/len(basic_rows)*100:.1f}%,  target ~5%)")
    print(f"  Null city : {null_city}/{len(address_rows)}  ({null_city/len(address_rows)*100:.1f}%,  target ~3%)")
    print(f"  Policies  : {len(basic_rows)}   Premiums: {len(premiums_rows)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--org", choices=["ail", "lnl", "all"], default="all")
    args = parser.parse_args()

    orgs = ["ail", "lnl"] if args.org == "all" else [args.org]
    for org in orgs:
        generate_org(org)

    print("\nGeneration complete.")
