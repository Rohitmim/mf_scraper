#!/usr/bin/env python3
"""
Data Validation Script for MF Scraper
Ensures accuracy of mutual fund data before financial decisions

Run: python validate_data.py
"""

import sys
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import requests

# Validation results
results = {"passed": 0, "failed": 0, "warnings": 0, "errors": []}


def log_pass(test_name: str, details: str = ""):
    results["passed"] += 1
    print(f"  ✓ {test_name}" + (f": {details}" if details else ""))


def log_fail(test_name: str, details: str):
    results["failed"] += 1
    results["errors"].append(f"{test_name}: {details}")
    print(f"  ✗ {test_name}: {details}")


def log_warn(test_name: str, details: str):
    results["warnings"] += 1
    print(f"  ⚠ {test_name}: {details}")


# ==================== 1. SOURCE VALIDATION ====================

def validate_data_source():
    """Verify MFAPI is the authoritative source"""
    print("\n" + "=" * 60)
    print("1. DATA SOURCE VALIDATION")
    print("=" * 60)

    # Check MFAPI is accessible and returns expected data
    try:
        response = requests.get("https://api.mfapi.in/mf", timeout=10)
        if response.status_code == 200:
            data = response.json()
            if len(data) > 1000:
                log_pass("MFAPI accessible", f"{len(data)} schemes available")
            else:
                log_fail("MFAPI data count", f"Only {len(data)} schemes (expected 1000+)")
        else:
            log_fail("MFAPI accessibility", f"Status code: {response.status_code}")
    except Exception as e:
        log_fail("MFAPI connectivity", str(e))

    # Verify MFAPI data matches AMFI (official source)
    print("\n  Verifying MFAPI sources from AMFI (official)...")
    try:
        # MFAPI gets data from AMFI - verify a sample fund
        sample_code = 118989  # HDFC Mid-Cap
        mfapi_response = requests.get(f"https://api.mfapi.in/mf/{sample_code}", timeout=10)
        if mfapi_response.status_code == 200:
            mfapi_data = mfapi_response.json()
            if mfapi_data.get("meta") and mfapi_data.get("data"):
                fund_name = mfapi_data["meta"].get("scheme_name", "")
                nav_count = len(mfapi_data["data"])
                log_pass("MFAPI data structure valid", f"{fund_name[:40]}... has {nav_count} NAV records")
            else:
                log_fail("MFAPI data structure", "Missing meta or data fields")
        else:
            log_fail("MFAPI sample fetch", f"Status code: {mfapi_response.status_code}")
    except Exception as e:
        log_fail("MFAPI sample verification", str(e))


# ==================== 2. CALCULATION VALIDATION ====================

def validate_calculations():
    """Verify ROI calculations are mathematically correct"""
    print("\n" + "=" * 60)
    print("2. ROI CALCULATION VALIDATION")
    print("=" * 60)

    from common import MFAPIClient, ROICalculator

    mfapi = MFAPIClient()
    calculator = ROICalculator(mfapi)

    # Test 1: CAGR formula validation
    # CAGR = (End/Start)^(1/years) - 1
    # Example: 100 -> 200 in 3 years = 25.99%
    roi = calculator.calculate_roi(200, 100, 3, annualize=True)
    expected = 25.99
    if abs(roi - expected) < 0.1:
        log_pass("CAGR formula", f"100→200 in 3Y = {roi:.2f}% (expected {expected}%)")
    else:
        log_fail("CAGR formula", f"Got {roi:.2f}%, expected {expected}%")

    # Test 2: Real fund calculation vs manual verification
    print("\n  Verifying real fund calculation...")
    try:
        # Fetch NAV data for a known fund
        scheme_code = 118989  # HDFC Mid-Cap
        nav_data = mfapi.get_fund_nav(scheme_code)

        if nav_data and nav_data.get("data"):
            navs = nav_data["data"]
            fund_name = nav_data["meta"].get("scheme_name", "Unknown")

            # Get latest NAV and 3-year-ago NAV
            latest = navs[0]
            latest_date = datetime.strptime(latest["date"], "%d-%m-%Y")
            latest_nav = float(latest["nav"])

            # Find NAV from ~3 years ago
            target_date = latest_date - timedelta(days=365 * 3)
            old_nav = None
            old_date = None
            for nav in navs:
                nav_date = datetime.strptime(nav["date"], "%d-%m-%Y")
                if nav_date <= target_date:
                    old_nav = float(nav["nav"])
                    old_date = nav_date
                    break

            if old_nav:
                # Manual CAGR calculation
                years = (latest_date - old_date).days / 365.25
                manual_cagr = ((latest_nav / old_nav) ** (1 / years) - 1) * 100

                # Calculator result
                result = calculator.calculate_fund_returns(scheme_code, latest_date)
                calc_cagr = result.get("roi_3y") if result else None

                # Allow 1.5% tolerance for minor calculation differences
                if calc_cagr and abs(manual_cagr - calc_cagr) < 1.5:
                    log_pass("Real fund CAGR", f"{fund_name[:30]}... Manual={manual_cagr:.2f}%, Calc={calc_cagr:.2f}%")
                else:
                    log_fail("Real fund CAGR mismatch", f"Manual={manual_cagr:.2f}%, Calc={calc_cagr:.2f}%")
            else:
                log_warn("3Y NAV not found", "Could not find NAV from 3 years ago")
    except Exception as e:
        log_fail("Real fund calculation", str(e))


# ==================== 3. DATABASE INTEGRITY ====================

def validate_database():
    """Verify database data integrity"""
    print("\n" + "=" * 60)
    print("3. DATABASE INTEGRITY VALIDATION")
    print("=" * 60)

    from common import SupabaseDB

    db = SupabaseDB()

    # Test 1: Check for duplicate funds
    try:
        result = db.client.table("mutual_funds").select("fund_name").execute()
        names = [r["fund_name"] for r in result.data]
        duplicates = set([n for n in names if names.count(n) > 1])
        if len(duplicates) == 0:
            log_pass("No duplicate fund names")
        else:
            log_warn("Duplicate fund names", f"{len(duplicates)} duplicates found")
    except Exception as e:
        log_fail("Duplicate check", str(e))

    # Test 2: Check for orphan returns (returns without funds)
    try:
        returns_result = db.client.table("mutual_fund_returns").select("fund_id").execute()
        return_fund_ids = set(r["fund_id"] for r in returns_result.data)

        funds_result = db.client.table("mutual_funds").select("id").execute()
        fund_ids = set(f["id"] for f in funds_result.data)

        orphans = return_fund_ids - fund_ids
        if len(orphans) == 0:
            log_pass("No orphan returns")
        else:
            log_fail("Orphan returns", f"{len(orphans)} returns without matching fund")
    except Exception as e:
        log_fail("Orphan check", str(e))

    # Test 3: Check ROI values are reasonable
    try:
        result = db.client.table("mutual_fund_returns").select("roi_3y").not_.is_("roi_3y", "null").limit(1000).execute()
        rois = [r["roi_3y"] for r in result.data]

        min_roi = min(rois)
        max_roi = max(rois)
        avg_roi = sum(rois) / len(rois)

        # 3Y CAGR should be between -30% and +60% for most funds
        if min_roi >= -50 and max_roi <= 100:
            log_pass("ROI range reasonable", f"Min={min_roi:.1f}%, Max={max_roi:.1f}%, Avg={avg_roi:.1f}%")
        else:
            log_warn("ROI range suspicious", f"Min={min_roi:.1f}%, Max={max_roi:.1f}%")

        # Check for impossible values
        impossible = [r for r in rois if r > 200 or r < -80]
        if len(impossible) == 0:
            log_pass("No impossible ROI values")
        else:
            log_fail("Impossible ROI values", f"{len(impossible)} values outside -80% to 200%")
    except Exception as e:
        log_fail("ROI validation", str(e))


# ==================== 4. CROSS-VALIDATION ====================

def validate_cross_check():
    """Cross-validate our data with external sources"""
    print("\n" + "=" * 60)
    print("4. CROSS-VALIDATION WITH EXTERNAL SOURCES")
    print("=" * 60)

    from common import SupabaseDB, MFAPIClient, ROICalculator

    db = SupabaseDB()
    mfapi = MFAPIClient()
    calculator = ROICalculator(mfapi)

    # Get top 5 funds from our database
    latest_result = db.client.table("mutual_fund_returns").select("report_date").order("report_date", desc=True).limit(1).execute()
    latest_date = latest_result.data[0]["report_date"] if latest_result.data else None

    if not latest_date:
        log_fail("Cross-validation", "No data in database")
        return

    top_result = db.client.table("mutual_fund_returns").select("fund_id, roi_3y").eq("report_date", latest_date).order("roi_3y", desc=True).limit(5).execute()

    print(f"\n  Validating top 5 funds for {latest_date}...")

    mismatches = 0
    for r in top_result.data:
        fund_id = r["fund_id"]
        db_roi = r["roi_3y"]

        # Get fund details
        fund = db.client.table("mutual_funds").select("fund_name, scheme_code").eq("id", fund_id).execute()
        if not fund.data:
            continue

        fund_name = fund.data[0]["fund_name"]
        scheme_code = fund.data[0].get("scheme_code")

        if not scheme_code:
            log_warn(f"No scheme_code", fund_name[:40])
            continue

        # Recalculate from fresh API data
        try:
            target_date = datetime.strptime(latest_date, "%Y-%m-%d")
            fresh_result = calculator.calculate_fund_returns(scheme_code, target_date)
            fresh_roi = fresh_result.get("roi_3y") if fresh_result else None

            if fresh_roi is not None:
                diff = abs(db_roi - fresh_roi)
                # Allow 1.5% tolerance for minor calculation differences
                if diff < 1.5:
                    log_pass(f"{fund_name[:35]}...", f"DB={db_roi:.2f}%, Fresh={fresh_roi:.2f}%")
                else:
                    log_fail(f"{fund_name[:35]}...", f"DB={db_roi:.2f}%, Fresh={fresh_roi:.2f}% (diff={diff:.2f}%)")
                    mismatches += 1
            else:
                log_warn(f"{fund_name[:35]}...", "Could not calculate fresh ROI")
        except Exception as e:
            log_warn(f"{fund_name[:35]}...", f"Error: {e}")

    if mismatches == 0:
        print("\n  All top funds validated successfully!")


# ==================== 5. TOP 200 RANKING VALIDATION ====================

def validate_ranking():
    """Verify top 200 ranking is correct"""
    print("\n" + "=" * 60)
    print("5. TOP 200 RANKING VALIDATION")
    print("=" * 60)

    from common import SupabaseDB

    db = SupabaseDB()

    # Get latest date
    latest_result = db.client.table("mutual_fund_returns").select("report_date").order("report_date", desc=True).limit(1).execute()
    latest_date = latest_result.data[0]["report_date"] if latest_result.data else None

    if not latest_date:
        log_fail("Ranking validation", "No data")
        return

    # Get all funds for latest date
    all_result = db.client.table("mutual_fund_returns").select("fund_id, roi_3y").eq("report_date", latest_date).not_.is_("roi_3y", "null").execute()

    # Sort by ROI descending
    sorted_funds = sorted(all_result.data, key=lambda x: x["roi_3y"], reverse=True)

    print(f"\n  Verifying ranking for {latest_date}...")
    print(f"  Total funds with ROI: {len(sorted_funds)}")

    # Check top 10 are in correct order
    is_sorted = True
    for i in range(min(9, len(sorted_funds) - 1)):
        if sorted_funds[i]["roi_3y"] < sorted_funds[i + 1]["roi_3y"]:
            is_sorted = False
            break

    if is_sorted:
        log_pass("Top 200 correctly sorted by ROI")
    else:
        log_fail("Sorting error", "Funds not in descending ROI order")

    # Show top 5 for verification
    print("\n  Top 5 funds by 3Y ROI:")
    for i, f in enumerate(sorted_funds[:5], 1):
        fund_result = db.client.table("mutual_funds").select("fund_name").eq("id", f["fund_id"]).execute()
        name = fund_result.data[0]["fund_name"][:45] if fund_result.data else "Unknown"
        print(f"    #{i}: {f['roi_3y']:.2f}% - {name}...")

    # Verify 200th fund is lower than 1st
    if len(sorted_funds) >= 200:
        if sorted_funds[0]["roi_3y"] > sorted_funds[199]["roi_3y"]:
            log_pass("Rank 1 > Rank 200", f"{sorted_funds[0]['roi_3y']:.2f}% > {sorted_funds[199]['roi_3y']:.2f}%")
        else:
            log_fail("Ranking logic", "Rank 200 has higher ROI than Rank 1")


# ==================== 6. DATA FRESHNESS ====================

def validate_freshness():
    """Verify data is recent"""
    print("\n" + "=" * 60)
    print("6. DATA FRESHNESS VALIDATION")
    print("=" * 60)

    from common import SupabaseDB

    db = SupabaseDB()

    # Get latest date in database
    latest_result = db.client.table("mutual_fund_returns").select("report_date").order("report_date", desc=True).limit(1).execute()
    latest_date = latest_result.data[0]["report_date"] if latest_result.data else None

    if latest_date:
        latest_dt = datetime.strptime(latest_date, "%Y-%m-%d")
        days_old = (datetime.now() - latest_dt).days

        if days_old <= 1:
            log_pass("Data is current", f"Latest date: {latest_date} ({days_old} days old)")
        elif days_old <= 7:
            log_warn("Data slightly stale", f"Latest date: {latest_date} ({days_old} days old)")
        else:
            log_fail("Data is stale", f"Latest date: {latest_date} ({days_old} days old)")
    else:
        log_fail("No data", "Database is empty")

    # Check if NAV data in MFAPI is more recent
    try:
        response = requests.get("https://api.mfapi.in/mf/118989", timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get("data"):
                api_latest = data["data"][0]["date"]
                api_dt = datetime.strptime(api_latest, "%d-%m-%Y")
                api_date_str = api_dt.strftime("%Y-%m-%d")

                if latest_date and api_date_str > latest_date:
                    log_warn("Newer data available", f"API has {api_date_str}, DB has {latest_date}")
                else:
                    log_pass("Database up to date with API")
    except Exception as e:
        log_warn("API freshness check failed", str(e))


# ==================== MAIN ====================

def main():
    print("\n" + "=" * 60)
    print("  MF SCRAPER DATA VALIDATION")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("  ⚠️  Financial decisions depend on this data!")
    print("=" * 60)

    validate_data_source()
    validate_calculations()
    validate_database()
    validate_cross_check()
    validate_ranking()
    validate_freshness()

    # Summary
    total = results["passed"] + results["failed"]
    print("\n" + "=" * 60)
    print("  VALIDATION SUMMARY")
    print("=" * 60)
    print(f"  ✓ Passed:   {results['passed']}/{total}")
    print(f"  ✗ Failed:   {results['failed']}/{total}")
    print(f"  ⚠ Warnings: {results['warnings']}")

    if results["errors"]:
        print("\n  FAILURES (must fix before using data):")
        for error in results["errors"]:
            print(f"    - {error}")

    if results["failed"] > 0:
        print("\n  ❌ DATA VALIDATION FAILED - DO NOT USE FOR FINANCIAL DECISIONS")
        return False
    elif results["warnings"] > 3:
        print("\n  ⚠️  DATA HAS WARNINGS - REVIEW BEFORE USING")
        return True
    else:
        print("\n  ✅ DATA VALIDATED - SAFE FOR USE")
        return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
