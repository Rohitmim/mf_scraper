#!/usr/bin/env python3
"""
Backfill mutual fund returns for all significant SENSEX change dates.
This fills the data gaps so Min6M, Max6M, Trend etc. work properly.

Usage:
    python backfill_returns.py              # Backfill all missing dates
    python backfill_returns.py --recent     # Only last 6 months
    python backfill_returns.py --dry-run    # Show what would be done
"""

import argparse
import time
from datetime import datetime, timedelta
from common import MFAPIClient, ROICalculator, SupabaseDB


def main():
    parser = argparse.ArgumentParser(description="Backfill MF returns for significant dates")
    parser.add_argument("--recent", action="store_true", help="Only backfill last 6 months")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument("--max-dates", type=int, default=50, help="Max dates to process (default: 50)")
    args = parser.parse_args()

    db = SupabaseDB()

    # Get significant SENSEX change dates
    sig_dates = set(db.get_significant_change_dates("SENSEX"))
    print(f"Significant SENSEX dates: {len(sig_dates)}")

    # Get dates already in returns
    result = db.client.table("mutual_fund_returns").select("report_date").execute()
    existing_dates = set(r["report_date"] for r in result.data)
    print(f"Dates already in DB: {len(existing_dates)}")

    # Find missing dates
    missing_dates = sig_dates - existing_dates

    # Filter to recent if requested
    if args.recent:
        six_months_ago = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
        missing_dates = {d for d in missing_dates if d >= six_months_ago}
        print(f"Missing dates (last 6 months): {len(missing_dates)}")
    else:
        print(f"Missing dates (all): {len(missing_dates)}")

    # Sort and limit
    missing_dates = sorted(missing_dates, reverse=True)[:args.max_dates]

    if not missing_dates:
        print("\nNo missing dates to backfill!")
        return

    print(f"\nWill process {len(missing_dates)} dates:")
    for d in missing_dates[:10]:
        print(f"  {d}")
    if len(missing_dates) > 10:
        print(f"  ... and {len(missing_dates) - 10} more")

    if args.dry_run:
        print("\n[DRY RUN] No data will be fetched.")
        return

    # Initialize API client
    print("\nInitializing MFAPI client...")
    mfapi = MFAPIClient()

    # Load or fetch NAV data
    if not mfapi.load_cache():
        print("Fetching NAV data from API (this may take a few minutes)...")
        schemes = mfapi.get_fund_list()
        print(f"Found {len(schemes)} Direct Growth funds")
        mfapi.fetch_all_nav_data(schemes, max_funds=600, use_cache=True)

    calculator = ROICalculator(mfapi)

    # Process each date
    for i, date in enumerate(missing_dates, 1):
        print(f"\n[{i}/{len(missing_dates)}] Processing {date}...")
        start = time.time()

        # Calculate returns for all cached funds
        funds = calculator.calculate_all_returns(as_of_date=date)
        funds_with_roi = [f for f in funds if f.get("roi_3y") is not None]

        print(f"  Found {len(funds_with_roi)} funds with 3Y data")

        if funds_with_roi:
            # Save top 200 to DB
            saved = db.save_funds_batch(funds_with_roi, date, source="mfapi_backfill", top_n=200)
            print(f"  Saved {saved} funds in {time.time() - start:.1f}s")

    print("\n" + "=" * 50)
    print("Backfill complete!")

    # Show updated stats
    result = db.client.table("mutual_fund_returns").select("report_date").execute()
    final_dates = set(r["report_date"] for r in result.data)
    print(f"Total dates in DB now: {len(final_dates)}")


if __name__ == "__main__":
    main()
