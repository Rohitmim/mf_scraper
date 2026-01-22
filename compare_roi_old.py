#!/usr/bin/env python3
"""
MF ROI Comparison Tool
Compares top 200 mutual funds' 3-year ROI across multiple dates
"""

import os
import sys
from datetime import datetime
from typing import List, Optional

# Supabase import
try:
    from supabase import create_client, Client
except ImportError:
    print("Error: supabase package not installed. Run: pip install supabase")
    sys.exit(1)


def get_supabase_client() -> Client:
    """Get Supabase client from environment variables"""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY")

    if not url or not key:
        raise ValueError(
            "Supabase credentials not found. Set SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables."
        )

    return create_client(url, key)


def get_available_dates(client: Client) -> List[str]:
    """Get all available report dates from the database"""
    result = client.table("mutual_fund_returns").select("report_date").execute()
    dates = sorted(set(r["report_date"] for r in result.data), reverse=True)
    return dates


def get_comparison_data(client: Client, dates: List[str], limit: int = 200, union_mode: bool = True) -> List[dict]:
    """
    Get side-by-side comparison of ROI data for specified dates.

    If union_mode=True: Gets top N funds from EACH date, then unions them (total > N).
    If union_mode=False: Gets top N funds ranked by latest date only.
    """
    if len(dates) < 1:
        return []

    # Get all funds with their returns for all dates
    funds_result = client.table("mutual_funds").select("id, fund_name, fund_house, category").execute()
    funds_map = {f["id"]: f for f in funds_result.data}

    # Get returns for all specified dates
    returns_result = client.table("mutual_fund_returns").select(
        "fund_id, report_date, roi_1y, roi_2y, roi_3y"
    ).in_("report_date", dates).execute()

    # Build returns lookup: fund_id -> date -> returns
    returns_map = {}
    for r in returns_result.data:
        fund_id = r["fund_id"]
        if fund_id not in returns_map:
            returns_map[fund_id] = {}
        returns_map[fund_id][r["report_date"]] = r

    if union_mode:
        # Get top N funds for EACH date, then union them
        top_fund_ids = set()

        for date in dates:
            # Get funds with data for this date, sorted by ROI
            date_funds = []
            for fund_id, fund_returns in returns_map.items():
                if date in fund_returns and fund_returns[date].get("roi_3y") is not None:
                    date_funds.append((fund_id, fund_returns[date]["roi_3y"]))

            # Sort by ROI descending and take top N
            date_funds.sort(key=lambda x: x[1], reverse=True)
            for fund_id, _ in date_funds[:limit]:
                top_fund_ids.add(fund_id)

        print(f"  Union of top {limit} from each date: {len(top_fund_ids)} unique funds")
    else:
        # Use the latest date for ranking (original behavior)
        latest_date = max(dates)
        date_funds = []
        for fund_id, fund_returns in returns_map.items():
            if latest_date in fund_returns and fund_returns[latest_date].get("roi_3y") is not None:
                date_funds.append((fund_id, fund_returns[latest_date]["roi_3y"]))

        date_funds.sort(key=lambda x: x[1], reverse=True)
        top_fund_ids = set(fund_id for fund_id, _ in date_funds[:limit])

    # Build comparison data for selected funds
    comparison = []
    latest_date = max(dates)

    for fund_id in top_fund_ids:
        if fund_id not in funds_map:
            continue

        fund = funds_map[fund_id]
        fund_returns = returns_map.get(fund_id, {})

        row = {
            "fund_id": fund_id,
            "fund_name": fund["fund_name"],
            "fund_house": fund["fund_house"],
            "category": fund["category"],
        }

        # Add ROI for each date
        for date in sorted(dates):
            date_key = date.replace("-", "_")
            if date in fund_returns:
                row[f"roi_3y_{date_key}"] = fund_returns[date].get("roi_3y")
            else:
                row[f"roi_3y_{date_key}"] = None

        # Use latest available ROI for sorting
        row["sort_key"] = fund_returns.get(latest_date, {}).get("roi_3y") or 0

        comparison.append(row)

    # Sort by latest ROI descending
    comparison.sort(key=lambda x: x["sort_key"], reverse=True)

    # Add rank
    for i, row in enumerate(comparison, 1):
        row["rank"] = i
        del row["sort_key"]
        del row["fund_id"]

    return comparison


def print_comparison_table(data: List[dict], dates: List[str]):
    """Print comparison data as a formatted table"""
    if not data:
        print("No data available for comparison.")
        return

    # Format dates for column headers
    date_keys = [d.replace("-", "_") for d in sorted(dates)]
    date_headers = [d.replace("_", "-") for d in date_keys]

    # Print header
    print("\n" + "=" * 120)
    print(f"{'Rank':>4} | {'Fund Name':<50} | ", end="")
    for header in date_headers:
        print(f"{header:>12} | ", end="")
    print("Change")
    print("-" * 120)

    # Print data rows
    for row in data:
        print(f"{row['rank']:>4} | {row['fund_name'][:50]:<50} | ", end="")

        first_val = None
        last_val = None
        for i, dk in enumerate(date_keys):
            val = row.get(f"roi_3y_{dk}")
            if val is not None:
                print(f"{val:>11.2f}% | ", end="")
                if first_val is None:
                    first_val = val
                last_val = val
            else:
                print(f"{'N/A':>12} | ", end="")

        # Calculate change
        if first_val is not None and last_val is not None:
            change = last_val - first_val
            sign = "+" if change >= 0 else ""
            print(f"{sign}{change:.2f}%")
        else:
            print("N/A")

    print("=" * 120)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Compare MF 3-Year ROI across multiple dates",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--dates", "-d",
        type=str,
        nargs="+",
        help="Dates to compare (YYYY-MM-DD format). If not specified, uses all available dates."
    )

    parser.add_argument(
        "--limit", "-n",
        type=int,
        default=200,
        help="Number of top funds to show (default: 200)"
    )

    parser.add_argument(
        "--list-dates",
        action="store_true",
        help="List all available dates in the database"
    )

    parser.add_argument(
        "--no-union",
        action="store_true",
        help="Rank by latest date only (default: union top N from each date)"
    )

    args = parser.parse_args()

    try:
        client = get_supabase_client()

        if args.list_dates:
            dates = get_available_dates(client)
            print("\nAvailable dates in database:")
            for date in dates:
                print(f"  - {date}")
            return

        # Get dates to compare
        if args.dates:
            dates = args.dates
        else:
            dates = get_available_dates(client)
            if len(dates) > 5:
                dates = dates[:5]  # Limit to 5 most recent

        if len(dates) < 1:
            print("Error: No dates available for comparison.")
            sys.exit(1)

        print(f"\nComparing ROI data for dates: {', '.join(sorted(dates))}")
        union_mode = not args.no_union
        mode_desc = f"union of top {args.limit} from each date" if union_mode else f"top {args.limit} by latest date"
        print(f"Mode: {mode_desc}")

        data = get_comparison_data(client, dates, args.limit, union_mode=union_mode)
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
