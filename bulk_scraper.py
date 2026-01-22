#!/usr/bin/env python3
"""
Bulk MF Scraper - Refactored to use common modules

Usage:
    python bulk_scraper.py                           # Use cache, save to DB
    python bulk_scraper.py --refresh                 # Force refresh cache
    python bulk_scraper.py --no-db                   # Skip database save
    python bulk_scraper.py -d 2025-11-26 2025-12-16  # Custom dates
"""

import argparse
import sys
import time

# Import common modules
from common import MFAPIClient, ROICalculator, SupabaseDB

# Significance threshold for auto-adding dates (0.5% SENSEX change)
SIGNIFICANCE_THRESHOLD = 0.5


def check_and_add_significant_date(db: SupabaseDB, date_str: str) -> bool:
    """
    Check if date has significant SENSEX change and add to DB if not exists.

    Args:
        db: Database connection
        date_str: Date to check (YYYY-MM-DD)

    Returns:
        True if date was added as significant, False otherwise
    """
    try:
        from common.sensex import SensexClient

        # Check if date already exists in significant changes
        existing = db.client.table("market_significant_changes").select("id").eq("change_date", date_str).eq("index_name", "SENSEX").execute()
        if existing.data:
            return False  # Already exists

        # Check significance
        sensex = SensexClient()
        change = sensex.check_date_significance(date_str, threshold=SIGNIFICANCE_THRESHOLD)

        if change:
            # Add to database
            db.client.table("market_significant_changes").upsert(change, on_conflict="index_name,change_date").execute()
            return True

    except ImportError:
        pass  # yfinance not installed, skip
    except Exception as e:
        print(f"  Warning: Could not check SENSEX significance: {e}")

    return False


def main():
    parser = argparse.ArgumentParser(
        description="Bulk MF Scraper with caching",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python bulk_scraper.py                      # Default dates, use cache
  python bulk_scraper.py --refresh            # Force refresh from API
  python bulk_scraper.py --no-db              # Calculate only, skip DB
  python bulk_scraper.py -d 2025-12-01        # Single date
  python bulk_scraper.py -n 800               # Fetch more funds
        """
    )
    parser.add_argument(
        "--dates", "-d", nargs="+",
        default=["2025-11-26", "2025-12-16", "2026-01-14"],
        help="Dates to calculate ROI for (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--refresh", "-r", action="store_true",
        help="Force refresh cache (ignore existing)"
    )
    parser.add_argument(
        "--no-db", action="store_true",
        help="Skip saving to database"
    )
    parser.add_argument(
        "--max-funds", "-n", type=int, default=600,
        help="Max funds to fetch (default: 600)"
    )
    parser.add_argument(
        "--insecure", action="store_true",
        help="Disable SSL verification"
    )

    args = parser.parse_args()

    # Suppress SSL warnings if insecure mode
    if args.insecure:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    print("=" * 60)
    print("  Bulk MF Scraper - Real Historical ROI Data")
    print("  Using common modules (DRY architecture)")
    print("=" * 60)

    # Initialize MFAPI client (with caching)
    mfapi = MFAPIClient(verify_ssl=not args.insecure)

    # Load or fetch NAV data
    print("\nChecking cache...")
    if args.refresh:
        print("  --refresh specified, forcing API fetch")
        mfapi.clear_cache()

    start = time.time()
    if not mfapi.load_cache():
        print("\nFetching NAV data from API...")
        schemes = mfapi.get_fund_list()
        print(f"  Found {len(schemes)} Direct Growth funds")
        mfapi.fetch_all_nav_data(schemes, max_funds=args.max_funds, use_cache=True)
        print(f"  Completed in {time.time() - start:.1f}s")
    else:
        print("\nUsing cached NAV data (skipped API)")

    # Initialize calculator and DB
    calculator = ROICalculator(mfapi)
    db = None if args.no_db else SupabaseDB()

    # Calculate returns for each date
    print(f"\nCalculating returns for {len(args.dates)} dates...")

    for date in args.dates:
        print(f"\n[{date}]")

        # Calculate returns (uses cached NAV data - instant)
        funds = calculator.calculate_all_returns(as_of_date=date)
        funds_with_roi = [f for f in funds if f.get('roi_3y')]
        print(f"  Found {len(funds_with_roi)} funds with 3Y data")

        # Save to DB
        if db:
            saved = db.save_funds_batch(funds_with_roi, date, source="mfapi_bulk", top_n=200)
            print(f"  Saved {saved} funds to database")

            # Also save watchlist funds (may not be in top 200)
            watchlist = db.get_watchlist()
            if watchlist:
                from datetime import datetime
                target_date = datetime.strptime(date, '%Y-%m-%d')
                watchlist_saved = 0
                for w in watchlist:
                    fund = db.client.table("mutual_funds").select("scheme_code").eq("id", w["fund_id"]).execute()
                    if not fund.data or not fund.data[0].get("scheme_code"):
                        continue
                    scheme_code = fund.data[0]["scheme_code"]
                    result = calculator.calculate_fund_returns(scheme_code, target_date)
                    if result and result.get("roi_3y") is not None:
                        existing = db.client.table("mutual_fund_returns").select("id").eq("fund_id", w["fund_id"]).eq("report_date", date).execute()
                        if not existing.data:
                            db.client.table("mutual_fund_returns").insert({
                                "fund_id": w["fund_id"],
                                "report_date": date,
                                "roi_1y": result.get("roi_1y"),
                                "roi_2y": result.get("roi_2y"),
                                "roi_3y": result.get("roi_3y"),
                                "source": "watchlist"
                            }).execute()
                            watchlist_saved += 1
                if watchlist_saved:
                    print(f"  Saved {watchlist_saved} watchlist funds")

            # Auto-add date to significant changes if SENSEX moved >0.5%
            if check_and_add_significant_date(db, date):
                print(f"  Added to significant SENSEX dates (>{SIGNIFICANCE_THRESHOLD}% change)")

        # Show top 5
        funds_with_roi.sort(key=lambda x: x['roi_3y'], reverse=True)
        print(f"  Top 5:")
        for i, f in enumerate(funds_with_roi[:5], 1):
            print(f"    {i}. {f['fund_name'][:45]} - {f['roi_3y']:.2f}%")

    print("\n" + "=" * 60)
    print("  Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
