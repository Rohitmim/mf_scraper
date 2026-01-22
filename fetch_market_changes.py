#!/usr/bin/env python3
"""
Fetch Market Significant Changes

Identifies dates when SENSEX changed by >3% and stores in database.
Use these dates for mutual fund ROI trend analysis.

Usage:
    python fetch_market_changes.py                    # Fetch and save to DB
    python fetch_market_changes.py --list             # List stored changes
    python fetch_market_changes.py --threshold 5      # Use 5% threshold
    python fetch_market_changes.py --no-db            # Don't save to database
"""

import argparse
import sys

# Import common modules
from common import SupabaseDB
from common.sensex import SensexClient


def print_changes_table(changes):
    """Print changes as a formatted table"""
    if not changes:
        print("No significant changes found.")
        return

    print(f"\n{'Date':<12} {'Index':<10} {'Prev Close':>12} {'Close':>12} {'Change':>10} {'Type':<6}")
    print("-" * 70)

    for c in changes:
        change_date = c.get('change_date', c.get('date', ''))
        index_name = c.get('index_name', 'SENSEX')
        prev_close = c.get('previous_close', 0)
        current_close = c.get('current_close', c.get('close', 0))
        change_pct = c.get('change_percent', 0)
        change_type = c.get('change_type', 'up' if change_pct > 0 else 'down')

        sign = '+' if change_pct > 0 else ''
        print(f"{change_date:<12} {index_name:<10} {prev_close:>12,.2f} {current_close:>12,.2f} {sign}{change_pct:>9.2f}% {change_type:<6}")

    print("-" * 70)
    print(f"Total: {len(changes)} significant changes\n")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch SENSEX significant change dates",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python fetch_market_changes.py                    # Fetch and save SENSEX >3% changes
  python fetch_market_changes.py --list             # List stored changes from DB
  python fetch_market_changes.py --threshold 5     # Use 5% threshold
  python fetch_market_changes.py --index NIFTY50   # Use NIFTY50 instead
  python fetch_market_changes.py --refresh         # Force refresh from Yahoo Finance
        """
    )
    parser.add_argument(
        "--index", "-i", default="SENSEX",
        choices=["SENSEX", "NIFTY50"],
        help="Market index to analyze (default: SENSEX)"
    )
    parser.add_argument(
        "--threshold", "-t", type=float, default=3.0,
        help="Minimum absolute change percentage (default: 3.0)"
    )
    parser.add_argument(
        "--years", "-y", type=int, default=3,
        help="Number of years to analyze (default: 3)"
    )
    parser.add_argument(
        "--list", "-l", action="store_true",
        help="List changes from database (don't fetch new data)"
    )
    parser.add_argument(
        "--no-db", action="store_true",
        help="Don't save to database"
    )
    parser.add_argument(
        "--refresh", "-r", action="store_true",
        help="Force refresh (ignore cache)"
    )

    args = parser.parse_args()

    print("=" * 60)
    print(f"  Market Significant Changes Tracker")
    print(f"  Index: {args.index} | Threshold: {args.threshold}%")
    print("=" * 60)

    try:
        # List mode - just show DB data
        if args.list:
            print("\nFetching from database...")
            db = SupabaseDB()
            changes = db.get_significant_changes(
                index_name=args.index,
                min_threshold=args.threshold
            )
            print_changes_table(changes)

            # Show dates for easy copy
            if changes:
                print("Dates for comparison:")
                dates = [c['change_date'] for c in changes]
                print(f"  {', '.join(dates)}")
            return

        # Fetch mode
        print(f"\nAnalyzing {args.years} years of {args.index} data...")

        client = SensexClient()

        # Clear cache if refresh requested
        if args.refresh:
            cache_file = client._get_cache_file(args.index)
            if cache_file.exists():
                cache_file.unlink()
                print("  Cache cleared")

        # Find significant changes
        changes = client.find_significant_changes(
            index_name=args.index,
            threshold=args.threshold,
            years=args.years,
            use_cache=not args.refresh
        )

        print(f"\nFound {len(changes)} days with >{args.threshold}% change:")
        print_changes_table(changes)

        # Save to database
        if not args.no_db and changes:
            print("Saving to database...")
            db = SupabaseDB()
            saved = db.save_significant_changes_batch(changes)
            print(f"  Saved {saved} records to database")

        # Summary
        if changes:
            up_days = len([c for c in changes if c['change_type'] == 'up'])
            down_days = len(changes) - up_days
            print(f"\nSummary:")
            print(f"  Up days (>{args.threshold}%):   {up_days}")
            print(f"  Down days (<-{args.threshold}%): {down_days}")

            print(f"\nDates for MF comparison:")
            dates = [c['change_date'] for c in changes]
            print(f"  python compare_roi.py -d {' '.join(dates[:5])}")

    except ImportError as e:
        print(f"\nError: {e}")
        print("Install required package: pip install yfinance")
        sys.exit(1)
    except ValueError as e:
        print(f"\nError: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
