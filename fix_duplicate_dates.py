#!/usr/bin/env python3
"""
Fix duplicate ROI data caused by find_nav_for_date bug.

The bug: find_nav_for_date was returning the FIRST match within tolerance
instead of requiring exact match for significant dates.

Fix: Now uses exact=True for reference date (must be trading day),
     and exact=False for historical 1Y/2Y/3Y lookups.

Run this script when OFF VPN:
    python fix_duplicate_dates.py
"""

import time
from datetime import datetime, timedelta
from common import SupabaseDB, MFAPIClient, ROICalculator


def main():
    db = SupabaseDB()
    mfapi = MFAPIClient()
    calc = ROICalculator(mfapi)

    # Get all dates in the last 30 days that need recalculation
    all_dates = db.get_available_dates()
    cutoff = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    dates_to_fix = [d for d in all_dates if d >= cutoff]

    print(f"Recalculating {len(dates_to_fix)} dates with fixed algorithm...")
    print("=" * 60)

    for date_str in dates_to_fix:
        print(f"\n[{date_str}]")
        target_date = datetime.strptime(date_str, '%Y-%m-%d')

        # Get all funds for this date
        result = db.client.table("mutual_fund_returns").select("fund_id").eq("report_date", date_str).execute()
        fund_ids = [r["fund_id"] for r in result.data]
        print(f"  Funds to update: {len(fund_ids)}")

        updated = 0
        errors = 0

        for fund_id in fund_ids:
            try:
                # Get scheme_code
                fund = db.client.table("mutual_funds").select("scheme_code, fund_name").eq("id", fund_id).execute()
                if not fund.data or not fund.data[0].get("scheme_code"):
                    continue

                scheme_code = fund.data[0]["scheme_code"]

                # Recalculate
                nav_data = mfapi.get_fund_nav(scheme_code)
                if not nav_data:
                    continue

                result = calc.calculate_fund_returns(scheme_code, target_date)
                if not result or result.get("roi_3y") is None:
                    continue

                # Update database
                db.client.table("mutual_fund_returns").update({
                    "roi_1y": result.get("roi_1y"),
                    "roi_2y": result.get("roi_2y"),
                    "roi_3y": result.get("roi_3y"),
                    "source": "recalculated"
                }).eq("fund_id", fund_id).eq("report_date", date_str).execute()

                updated += 1
            except Exception as e:
                errors += 1

        print(f"  Updated: {updated}, Errors: {errors}")
        time.sleep(0.5)

    print("\n" + "=" * 60)
    print("Done!")

    # Verify fix
    print("\nVerifying - checking for duplicates...")
    top_fund = db.client.table("mutual_fund_returns").select("fund_id").order("roi_3y", desc=True).limit(1).execute()
    if top_fund.data:
        fund_id = top_fund.data[0]["fund_id"]
        results = db.client.table("mutual_fund_returns").select("report_date, roi_3y").eq("fund_id", fund_id).in_("report_date", dates_to_fix).order("report_date", desc=True).execute()

        print("\nTop fund ROI after fix:")
        prev_roi = None
        duplicates = 0
        for r in results.data:
            flag = " ← DUPLICATE" if r['roi_3y'] == prev_roi else ""
            if flag:
                duplicates += 1
            print(f"  {r['report_date']}: {r['roi_3y']}{flag}")
            prev_roi = r['roi_3y']

        if duplicates == 0:
            print("\n✓ No duplicates found - fix successful!")
        else:
            print(f"\n⚠ {duplicates} duplicates still present")


if __name__ == "__main__":
    main()
