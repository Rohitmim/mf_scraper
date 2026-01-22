#!/usr/bin/env python3
"""
Backfill missing fund data for display dates.
Fixes None values showing in UI for funds that recently entered top 200.

Usage: python backfill_fund_dates.py
"""

import time
from datetime import datetime
from common import SupabaseDB, MFAPIClient, ROICalculator


def main():
    db = SupabaseDB()
    mfapi = MFAPIClient()
    calculator = ROICalculator(mfapi)

    # Get display dates (last 20)
    dates = db.get_available_dates()
    display_dates = dates[:20]
    latest = display_dates[0]

    print(f"Display dates: {len(display_dates)}")

    # Get funds shown on latest date
    funds_result = db.client.table("mutual_fund_returns").select("fund_id").eq("report_date", latest).not_.is_("roi_3y", "null").execute()
    fund_ids = [r["fund_id"] for r in funds_result.data]
    print(f"Funds in latest view: {len(fund_ids)}")

    # For each fund, find missing dates and backfill
    total_filled = 0
    funds_processed = 0

    for i, fund_id in enumerate(fund_ids):
        # Get fund details
        fund = db.client.table("mutual_funds").select("fund_name, scheme_code").eq("id", fund_id).execute()
        if not fund.data:
            continue

        scheme_code = fund.data[0].get("scheme_code")
        fund_name = fund.data[0]["fund_name"]

        if not scheme_code:
            continue

        # Get existing dates for this fund
        existing = db.client.table("mutual_fund_returns").select("report_date").eq("fund_id", fund_id).execute()
        existing_dates = set(r["report_date"] for r in existing.data)

        # Find missing display dates
        missing = [d for d in display_dates if d not in existing_dates]

        if not missing:
            continue

        funds_processed += 1
        print(f"\n[{funds_processed}] {fund_name[:45]}...")
        print(f"  Missing: {len(missing)} dates")

        # Fetch NAV data once
        nav_data = mfapi.get_fund_nav(scheme_code)
        if not nav_data:
            print(f"  Could not fetch NAV data")
            continue

        # Backfill each missing date
        filled = 0
        for report_date in missing:
            target = datetime.strptime(report_date, "%Y-%m-%d")
            result = calculator.calculate_fund_returns(scheme_code, target)

            if result and result.get("roi_3y") is not None:
                db.client.table("mutual_fund_returns").upsert({
                    "fund_id": fund_id,
                    "report_date": report_date,
                    "roi_1y": result.get("roi_1y"),
                    "roi_2y": result.get("roi_2y"),
                    "roi_3y": result.get("roi_3y"),
                    "source": "backfill_funds"
                }, on_conflict="fund_id,report_date").execute()
                filled += 1

        print(f"  Filled: {filled}/{len(missing)}")
        total_filled += filled
        time.sleep(0.5)  # Rate limit

    print(f"\n\nTotal funds processed: {funds_processed}")
    print(f"Total records filled: {total_filled}")


if __name__ == "__main__":
    main()
