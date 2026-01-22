#!/usr/bin/env python3
"""
Backfill Historical Data

For funds currently in top 200 or watchlist, calculate and store
ROI for all significant dates where data is missing.

Usage:
    python backfill_history.py              # Backfill all missing dates
    python backfill_history.py --dry-run    # Show what would be filled
"""

import argparse
from datetime import datetime
from common import SupabaseDB, MFAPIClient, ROICalculator


def main():
    parser = argparse.ArgumentParser(description="Backfill historical ROI data")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    args = parser.parse_args()

    db = SupabaseDB()
    mfapi = MFAPIClient()
    calc = ROICalculator(mfapi)

    print("=" * 60)
    print("  BACKFILL HISTORICAL DATA")
    print("=" * 60)

    # Get all significant dates
    sig_dates = db.get_significant_change_dates("SENSEX")
    print(f"\nSignificant dates to check: {len(sig_dates)}")

    # Get latest date
    latest = db.client.table("mutual_fund_returns").select("report_date").order("report_date", desc=True).limit(1).execute()
    latest_date = latest.data[0]["report_date"]

    # Get current top 200 funds
    top_result = db.client.table("mutual_fund_returns").select("fund_id").eq("report_date", latest_date).order("roi_3y", desc=True).limit(200).execute()
    top_fund_ids = set(r["fund_id"] for r in top_result.data)

    # Get watchlist funds
    watchlist = db.get_watchlist()
    watchlist_fund_ids = set(w["fund_id"] for w in watchlist)

    # All funds to backfill
    all_fund_ids = list(top_fund_ids | watchlist_fund_ids)
    print(f"Funds to check: {len(all_fund_ids)} (top 200 + watchlist)")

    # Get fund details
    funds_result = db.client.table("mutual_funds").select("id, fund_name, scheme_code").in_("id", all_fund_ids).execute()
    funds_map = {f["id"]: f for f in funds_result.data}

    # Find missing data
    missing = []
    for fund_id in all_fund_ids:
        fund = funds_map.get(fund_id)
        if not fund or not fund.get("scheme_code"):
            continue

        # Get dates this fund has data for
        result = db.client.table("mutual_fund_returns").select("report_date").eq("fund_id", fund_id).execute()
        existing_dates = set(r["report_date"] for r in result.data)

        # Find missing significant dates
        for date in sig_dates:
            if date not in existing_dates:
                missing.append((fund_id, fund["fund_name"], fund["scheme_code"], date))

    print(f"Missing records to fill: {len(missing)}")

    if not missing:
        print("\nNo missing data - all funds have complete history!")
        return

    if args.dry_run:
        print("\n--dry-run mode, showing first 20 missing records:")
        for fund_id, name, scheme, date in missing[:20]:
            print(f"  {date}: {name[:40]}")
        print(f"\n  ... and {len(missing) - 20} more" if len(missing) > 20 else "")
        print("\nRun without --dry-run to fill missing data.")
        return

    # Fill missing data
    print(f"\nBackfilling {len(missing)} records...")

    filled = 0
    errors = 0
    skipped = 0

    for i, (fund_id, name, scheme_code, date_str) in enumerate(missing):
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d')

            # Fetch NAV data if not cached
            nav_data = mfapi.get_fund_nav(scheme_code)
            if not nav_data:
                skipped += 1
                continue

            # Calculate returns
            result = calc.calculate_fund_returns(scheme_code, target_date)
            if not result or result.get('roi_3y') is None:
                skipped += 1
                continue

            # Insert record
            db.client.table("mutual_fund_returns").insert({
                "fund_id": fund_id,
                "report_date": date_str,
                "roi_1y": result.get("roi_1y"),
                "roi_2y": result.get("roi_2y"),
                "roi_3y": result.get("roi_3y"),
                "source": "backfilled"
            }).execute()

            filled += 1

            if (i + 1) % 100 == 0:
                print(f"  Progress: {i + 1}/{len(missing)} (filled: {filled}, skipped: {skipped})")

        except Exception as e:
            errors += 1
            if errors < 5:
                print(f"  Error for {name[:30]} on {date_str}: {e}")

    print(f"\n{'=' * 60}")
    print(f"  COMPLETE")
    print(f"  Filled: {filled}")
    print(f"  Skipped: {skipped} (no NAV data for that date)")
    print(f"  Errors: {errors}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
