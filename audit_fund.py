#!/usr/bin/env python3
"""
Independent Audit Tool - Verify MF ROI calculations

This script independently validates ROI data by:
1. Fetching NAV directly from MFAPI (bypassing our cache)
2. Manually calculating ROI step-by-step
3. Comparing with database values
4. Providing external URLs for manual verification

Usage:
    python audit_fund.py                           # Audit top fund
    python audit_fund.py --fund "HDFC Mid Cap"     # Audit specific fund
    python audit_fund.py --date 2026-01-09         # Audit specific date
"""

import argparse
import requests
from datetime import datetime, timedelta
from typing import Optional, Tuple
import sys


def fetch_nav_direct(scheme_code: int) -> dict:
    """Fetch NAV data directly from MFAPI (no cache)"""
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def find_nav_exact(nav_data: list, target_date: str) -> Optional[float]:
    """Find exact NAV for a date (format: DD-MM-YYYY)"""
    for item in nav_data:
        if item['date'] == target_date:
            return float(item['nav'])
    return None


def find_nav_nearest(nav_data: list, target_date: datetime, max_days: int = 10) -> Tuple[Optional[float], Optional[str]]:
    """Find nearest NAV within max_days"""
    best_nav = None
    best_date = None
    best_diff = float('inf')

    for item in nav_data:
        item_date = datetime.strptime(item['date'], '%d-%m-%Y')
        diff = abs((item_date - target_date).days)
        if diff <= max_days and diff < best_diff:
            best_nav = float(item['nav'])
            best_date = item['date']
            best_diff = diff

    return best_nav, best_date


def calculate_cagr(current: float, historical: float, years: float) -> float:
    """Calculate CAGR"""
    return ((current / historical) ** (1 / years) - 1) * 100


def calculate_simple_return(current: float, historical: float) -> float:
    """Calculate simple return"""
    return ((current - historical) / historical) * 100


def main():
    parser = argparse.ArgumentParser(description="Independent MF ROI Audit Tool")
    parser.add_argument("--fund", "-f", help="Fund name to search for")
    parser.add_argument("--date", "-d", help="Date to audit (YYYY-MM-DD)")
    parser.add_argument("--scheme", "-s", type=int, help="Direct scheme code")
    args = parser.parse_args()

    print("=" * 70)
    print("  INDEPENDENT MF ROI AUDIT TOOL")
    print("  Verifies calculations against raw MFAPI data")
    print("=" * 70)

    # Get fund to audit
    from common import SupabaseDB
    db = SupabaseDB()

    if args.scheme:
        scheme_code = args.scheme
        fund_result = db.client.table("mutual_funds").select("*").eq("scheme_code", scheme_code).execute()
        if not fund_result.data:
            print(f"Fund with scheme_code {scheme_code} not found in DB")
            return
        fund = fund_result.data[0]
    elif args.fund:
        fund_result = db.client.table("mutual_funds").select("*").ilike("fund_name", f"%{args.fund}%").limit(1).execute()
        if not fund_result.data:
            print(f"Fund matching '{args.fund}' not found")
            return
        fund = fund_result.data[0]
        scheme_code = fund.get("scheme_code")
    else:
        # Get top fund by ROI
        latest = db.client.table("mutual_fund_returns").select("report_date").order("report_date", desc=True).limit(1).execute()
        latest_date = latest.data[0]["report_date"]
        top = db.client.table("mutual_fund_returns").select("fund_id, roi_3y").eq("report_date", latest_date).order("roi_3y", desc=True).limit(1).execute()
        fund_id = top.data[0]["fund_id"]
        fund_result = db.client.table("mutual_funds").select("*").eq("id", fund_id).execute()
        fund = fund_result.data[0]
        scheme_code = fund.get("scheme_code")

    if not scheme_code:
        print("ERROR: Fund has no scheme_code, cannot audit")
        return

    fund_name = fund["fund_name"]
    fund_id = fund["id"]

    # Get date to audit
    if args.date:
        audit_date = args.date
    else:
        latest = db.client.table("mutual_fund_returns").select("report_date").eq("fund_id", fund_id).order("report_date", desc=True).limit(1).execute()
        audit_date = latest.data[0]["report_date"]

    print(f"\n1. FUND IDENTIFICATION")
    print("-" * 70)
    print(f"   Fund Name:   {fund_name}")
    print(f"   Scheme Code: {scheme_code}")
    print(f"   Audit Date:  {audit_date}")
    print(f"   Fund ID:     {fund_id}")

    # Fetch fresh NAV data directly from MFAPI
    print(f"\n2. FETCHING RAW DATA FROM MFAPI")
    print("-" * 70)
    print(f"   URL: https://api.mfapi.in/mf/{scheme_code}")

    try:
        nav_data = fetch_nav_direct(scheme_code)
    except Exception as e:
        print(f"   ERROR: Could not fetch from MFAPI: {e}")
        return

    meta = nav_data.get("meta", {})
    navs = nav_data.get("data", [])

    print(f"   API Fund Name: {meta.get('scheme_name', 'N/A')}")
    print(f"   Total NAV Records: {len(navs)}")
    print(f"   Latest NAV Date: {navs[0]['date'] if navs else 'N/A'}")
    print(f"   Oldest NAV Date: {navs[-1]['date'] if navs else 'N/A'}")

    # Find reference NAV (exact match required)
    print(f"\n3. FINDING REFERENCE NAV FOR {audit_date}")
    print("-" * 70)

    audit_dt = datetime.strptime(audit_date, "%Y-%m-%d")
    ref_date_str = audit_dt.strftime("%d-%m-%Y")
    ref_nav = find_nav_exact(navs, ref_date_str)

    if ref_nav:
        print(f"   ✓ Found exact NAV: {ref_nav} on {ref_date_str}")
    else:
        print(f"   ✗ No NAV found for {ref_date_str}")
        print(f"   Available dates around {audit_date}:")
        for item in navs[:10]:
            print(f"      {item['date']}: {item['nav']}")
        return

    # Find historical NAVs
    print(f"\n4. FINDING HISTORICAL NAVs")
    print("-" * 70)

    # 1 Year ago
    date_1y = audit_dt - timedelta(days=365)
    nav_1y, found_1y = find_nav_nearest(navs, date_1y)
    print(f"   1Y ago target: {date_1y.strftime('%d-%m-%Y')}")
    print(f"   1Y NAV found:  {nav_1y} on {found_1y}")

    # 2 Years ago
    date_2y = audit_dt - timedelta(days=730)
    nav_2y, found_2y = find_nav_nearest(navs, date_2y)
    print(f"   2Y ago target: {date_2y.strftime('%d-%m-%Y')}")
    print(f"   2Y NAV found:  {nav_2y} on {found_2y}")

    # 3 Years ago
    date_3y = audit_dt - timedelta(days=1095)
    nav_3y, found_3y = find_nav_nearest(navs, date_3y)
    print(f"   3Y ago target: {date_3y.strftime('%d-%m-%Y')}")
    print(f"   3Y NAV found:  {nav_3y} on {found_3y}")

    # Calculate ROI manually
    print(f"\n5. MANUAL ROI CALCULATION")
    print("-" * 70)

    if nav_1y and found_1y:
        found_1y_dt = datetime.strptime(found_1y, "%d-%m-%Y")
        years_1y = (audit_dt - found_1y_dt).days / 365.25
        roi_1y = calculate_simple_return(ref_nav, nav_1y)
        print(f"   1Y ROI (simple): ({ref_nav} - {nav_1y}) / {nav_1y} * 100")
        print(f"                  = {roi_1y:.2f}%")
    else:
        roi_1y = None
        print(f"   1Y ROI: N/A (no historical data)")

    if nav_2y and found_2y:
        found_2y_dt = datetime.strptime(found_2y, "%d-%m-%Y")
        years_2y = (audit_dt - found_2y_dt).days / 365.25
        roi_2y = calculate_cagr(ref_nav, nav_2y, years_2y)
        print(f"   2Y ROI (CAGR):  ({ref_nav}/{nav_2y})^(1/{years_2y:.2f}) - 1")
        print(f"                 = {roi_2y:.2f}%")
    else:
        roi_2y = None
        print(f"   2Y ROI: N/A")

    if nav_3y and found_3y:
        found_3y_dt = datetime.strptime(found_3y, "%d-%m-%Y")
        years_3y = (audit_dt - found_3y_dt).days / 365.25
        roi_3y = calculate_cagr(ref_nav, nav_3y, years_3y)
        print(f"   3Y ROI (CAGR):  ({ref_nav}/{nav_3y})^(1/{years_3y:.2f}) - 1")
        print(f"                 = {roi_3y:.2f}%")
    else:
        roi_3y = None
        print(f"   3Y ROI: N/A")

    # Compare with database
    print(f"\n6. DATABASE COMPARISON")
    print("-" * 70)

    db_result = db.client.table("mutual_fund_returns").select("roi_1y, roi_2y, roi_3y, source, created_at").eq("fund_id", fund_id).eq("report_date", audit_date).execute()

    if db_result.data:
        db_row = db_result.data[0]
        db_roi_1y = db_row.get("roi_1y")
        db_roi_2y = db_row.get("roi_2y")
        db_roi_3y = db_row.get("roi_3y")

        print(f"   {'Metric':<12} {'Calculated':<15} {'Database':<15} {'Match'}")
        print(f"   {'-'*12} {'-'*15} {'-'*15} {'-'*10}")

        def check_match(calc, db_val, tolerance=0.1):
            if calc is None and db_val is None:
                return "✓ Both N/A"
            if calc is None or db_val is None:
                return "✗ MISMATCH"
            if abs(calc - db_val) < tolerance:
                return "✓ Match"
            return f"✗ DIFF: {abs(calc - db_val):.2f}"

        print(f"   {'1Y ROI':<12} {roi_1y if roi_1y else 'N/A':<15} {db_roi_1y if db_roi_1y else 'N/A':<15} {check_match(roi_1y, db_roi_1y)}")
        print(f"   {'2Y ROI':<12} {f'{roi_2y:.2f}' if roi_2y else 'N/A':<15} {db_roi_2y if db_roi_2y else 'N/A':<15} {check_match(roi_2y, db_roi_2y)}")
        print(f"   {'3Y ROI':<12} {f'{roi_3y:.2f}' if roi_3y else 'N/A':<15} {db_roi_3y if db_roi_3y else 'N/A':<15} {check_match(roi_3y, db_roi_3y)}")

        print(f"\n   Source:     {db_row.get('source', 'N/A')}")
        print(f"   Created:    {db_row.get('created_at', 'N/A')}")
    else:
        print(f"   No database record found for {audit_date}")

    # External verification links
    print(f"\n7. EXTERNAL VERIFICATION")
    print("-" * 70)
    print(f"   Verify NAV data at:")
    print(f"   - MFAPI:        https://api.mfapi.in/mf/{scheme_code}")
    print(f"   - AMFI India:   https://www.amfiindia.com/nav-history-download")
    print(f"   - Value Research: https://www.valueresearchonline.com/funds/")
    print(f"   - Moneycontrol: https://www.moneycontrol.com/mutual-funds/")

    print("\n" + "=" * 70)

    # Final verdict
    all_match = True
    if db_result.data:
        if roi_1y and db_roi_1y and abs(roi_1y - db_roi_1y) >= 0.1:
            all_match = False
        if roi_2y and db_roi_2y and abs(roi_2y - db_roi_2y) >= 0.1:
            all_match = False
        if roi_3y and db_roi_3y and abs(roi_3y - db_roi_3y) >= 0.1:
            all_match = False

    if all_match:
        print("  ✓ AUDIT PASSED - Database values match independent calculation")
    else:
        print("  ✗ AUDIT FAILED - Discrepancies found")
    print("=" * 70)


if __name__ == "__main__":
    main()
