#!/usr/bin/env python3
"""
MF ROI Comparison Tool - Refactored to use common modules

Compare top 200 mutual funds' 3-year ROI across multiple dates
"""

import argparse
import sys
from typing import List, Dict

# Import common modules
from common import SupabaseDB


def print_comparison_table(data: List[Dict], dates: List[str]):
    """Print comparison data as a formatted table"""
    if not data:
        print("No data available for comparison.")
        return

    # Format dates for column headers
    date_keys = [d.replace("-", "_") for d in sorted(dates)]

    # Calculate column widths
    name_width = 55
    col_width = 12

    # Print header
    header = f"{'Fund Name':<{name_width}} |"
    for d in sorted(dates):
        header += f" {d:>{col_width-1}} |"
    header += f" {'Change':>8}"

    print("\n" + "=" * len(header))
    print(header)
    print("-" * len(header))

    # Print data rows
    for row in data:
        line = f"{row['fund_name'][:name_width]:<{name_width}} |"

        first_val = None
        last_val = None

        for dk in date_keys:
            val = row.get(f"roi_3y_{dk}")
            if val is not None:
                line += f" {val:>{col_width-3}.2f}% |"
                if first_val is None:
                    first_val = val
                last_val = val
            else:
                line += f" {'-':>{col_width-1}} |"

        # Calculate change
        if first_val is not None and last_val is not None:
            change = last_val - first_val
            sign = "+" if change >= 0 else ""
            line += f" {sign}{change:.2f}%"
        else:
            line += f" {'-':>8}"

        print(line)

    print("=" * len(header))


def main():
    parser = argparse.ArgumentParser(
        description="Compare MF 3-Year ROI across multiple dates",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python compare_roi.py                              # Compare all available dates
  python compare_roi.py --list-dates                 # Show available dates
  python compare_roi.py -d 2025-11-26 2025-12-16    # Compare specific dates
  python compare_roi.py --limit 50                   # Top 50 only
  python compare_roi.py --no-union                   # Rank by latest date only
        """
    )
    parser.add_argument(
        "--dates", "-d", nargs="+",
        help="Dates to compare (YYYY-MM-DD). Default: all available"
    )
    parser.add_argument(
        "--limit", "-n", type=int, default=200,
        help="Number of top funds per date (default: 200)"
    )
    parser.add_argument(
        "--list-dates", action="store_true",
        help="List all available dates in the database"
    )
    parser.add_argument(
        "--no-union", action="store_true",
        help="Rank by latest date only (default: union top N from each date)"
    )

    args = parser.parse_args()

    try:
        # Use common DB module
        db = SupabaseDB()

        # List dates mode
        if args.list_dates:
            dates = db.get_available_dates()
            print("\nAvailable dates in database:")
            for date in dates:
                print(f"  - {date}")
            return

        # Get dates to compare
        if args.dates:
            dates = args.dates
        else:
            dates = db.get_available_dates()
            if len(dates) > 5:
                dates = dates[:5]

        if not dates:
            print("Error: No dates available for comparison.")
            sys.exit(1)

        # Get comparison data using common DB module
        union_mode = not args.no_union
        mode_desc = f"union of top {args.limit} from each date" if union_mode else f"top {args.limit} by latest date"

        print(f"\nComparing ROI data for: {', '.join(sorted(dates))}")
        print(f"Mode: {mode_desc}")

        data = db.get_comparison_data(dates, top_n=args.limit, union_mode=union_mode)

        print(f"  Total unique funds: {len(data)}")

        # Print table
        print_comparison_table(data, dates)

        print(f"\nTotal funds: {len(data)}")

    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
