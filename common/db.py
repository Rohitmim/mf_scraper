"""
Database operations - Supabase client and CRUD operations
Single source of truth for all database interactions
"""

import os
from typing import List, Dict, Optional, Any

try:
    from supabase import create_client, Client
    HAS_SUPABASE = True
except ImportError:
    HAS_SUPABASE = False
    Client = Any


def get_supabase_client(url: str = None, key: str = None) -> Client:
    """
    Get Supabase client - single factory function for all modules

    Args:
        url: Supabase URL (defaults to SUPABASE_URL env var)
        key: Supabase key (defaults to SUPABASE_SERVICE_KEY env var)

    Returns:
        Supabase client instance

    Raises:
        ImportError: If supabase package not installed
        ValueError: If credentials not provided
    """
    if not HAS_SUPABASE:
        raise ImportError("supabase package not installed. Run: pip install supabase")

    url = url or os.getenv("SUPABASE_URL")
    key = key or os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY")

    if not url or not key:
        raise ValueError(
            "Supabase credentials not found. "
            "Set SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables."
        )

    return create_client(url, key)


class SupabaseDB:
    """
    Database operations wrapper - reusable across all scripts
    Encapsulates all Supabase interactions
    """

    def __init__(self, client: Client = None):
        """Initialize with existing client or create new one"""
        self.client = client or get_supabase_client()

    # ==================== FUNDS ====================

    def get_all_funds(self) -> List[Dict]:
        """Get all mutual funds"""
        result = self.client.table("mutual_funds").select("*").execute()
        return result.data

    def get_fund_by_name(self, name: str) -> Optional[Dict]:
        """Get fund by name"""
        result = self.client.table("mutual_funds").select("*").eq("fund_name", name).execute()
        return result.data[0] if result.data else None

    def upsert_fund(self, fund_name: str, fund_house: str, category: str) -> Dict:
        """Insert or update a fund"""
        result = self.client.table("mutual_funds").upsert({
            "fund_name": fund_name,
            "fund_house": fund_house,
            "category": category,
        }, on_conflict="fund_name").execute()
        return result.data[0] if result.data else {}

    # ==================== RETURNS ====================

    def get_returns_for_dates(self, dates: List[str]) -> List[Dict]:
        """Get returns for specified dates"""
        result = self.client.table("mutual_fund_returns").select(
            "fund_id, report_date, roi_1y, roi_2y, roi_3y"
        ).in_("report_date", dates).execute()
        return result.data

    def get_available_dates(self) -> List[str]:
        """Get all distinct report dates (with pagination to handle large datasets)"""
        all_dates = set()
        offset = 0
        while True:
            result = self.client.table("mutual_fund_returns").select("report_date").range(offset, offset + 999).execute()
            if not result.data:
                break
            all_dates.update(r["report_date"] for r in result.data)
            offset += 1000
            if len(result.data) < 1000:
                break
        return sorted(all_dates, reverse=True)

    def upsert_fund_with_returns(
        self,
        fund_name: str,
        fund_house: str,
        category: str,
        report_date: str,
        roi_1y: float = None,
        roi_2y: float = None,
        roi_3y: float = None,
        source: str = "unknown"
    ) -> bool:
        """
        Upsert fund and its returns in one operation
        Uses the database RPC function for atomicity
        """
        try:
            self.client.rpc("upsert_mutual_fund_with_returns", {
                "p_fund_name": fund_name,
                "p_fund_house": fund_house or "Unknown",
                "p_category": category or "Unknown",
                "p_report_date": report_date,
                "p_roi_1y": roi_1y,
                "p_roi_2y": roi_2y,
                "p_roi_3y": roi_3y,
                "p_source": source,
            }).execute()
            return True
        except Exception:
            return False

    def save_funds_batch(
        self,
        funds: List[Dict],
        report_date: str,
        source: str = "unknown",
        top_n: int = 200
    ) -> int:
        """
        Save multiple funds to database using batch operations

        Args:
            funds: List of fund dicts with fund_name, fund_house, category, roi_1y, roi_2y, roi_3y
            report_date: Date for the returns
            source: Data source identifier
            top_n: Only save top N funds by roi_3y

        Returns:
            Number of funds saved
        """
        # Filter and sort by 3Y ROI
        funds_with_roi = [f for f in funds if f.get('roi_3y') is not None]
        funds_with_roi.sort(key=lambda x: x['roi_3y'], reverse=True)
        top_funds = funds_with_roi[:top_n]

        if not top_funds:
            return 0

        # Step 1: Batch upsert funds to mutual_funds table
        fund_records = [
            {
                "fund_name": f["fund_name"],
                "fund_house": f.get("fund_house", "Unknown"),
                "category": f.get("category", "Unknown"),
            }
            for f in top_funds
        ]

        try:
            self.client.table("mutual_funds").upsert(
                fund_records, on_conflict="fund_name"
            ).execute()
        except Exception as e:
            print(f"  Batch fund upsert error: {e}")
            return 0

        # Step 2: Get fund IDs
        fund_names = [f["fund_name"] for f in top_funds]
        result = self.client.table("mutual_funds").select("id, fund_name").in_("fund_name", fund_names).execute()
        fund_id_map = {r["fund_name"]: r["id"] for r in result.data}

        # Step 3: Batch upsert returns
        returns_records = []
        for f in top_funds:
            fund_id = fund_id_map.get(f["fund_name"])
            if fund_id:
                returns_records.append({
                    "fund_id": fund_id,
                    "report_date": report_date,
                    "roi_1y": f.get("roi_1y"),
                    "roi_2y": f.get("roi_2y"),
                    "roi_3y": f.get("roi_3y"),
                    "source": source,
                })

        if returns_records:
            try:
                self.client.table("mutual_fund_returns").upsert(
                    returns_records, on_conflict="fund_id,report_date"
                ).execute()
                return len(returns_records)
            except Exception as e:
                print(f"  Batch returns upsert error: {e}")
                return 0

        return 0

    # ==================== COMPARISON ====================

    def get_comparison_data(
        self,
        dates: List[str],
        top_n: int = 200,
        union_mode: bool = True
    ) -> List[Dict]:
        """
        Get side-by-side comparison data for multiple dates

        Args:
            dates: List of dates to compare
            top_n: Number of top funds per date
            union_mode: If True, union top N from each date; if False, rank by latest date only

        Returns:
            List of fund dicts with ROI for each date
        """
        # Get all funds and returns
        funds = self.get_all_funds()
        funds_map = {f["id"]: f for f in funds}

        returns = self.get_returns_for_dates(dates)

        # Build returns lookup: fund_id -> {date: returns}
        returns_map = {}
        for r in returns:
            fid = r["fund_id"]
            if fid not in returns_map:
                returns_map[fid] = {}
            returns_map[fid][r["report_date"]] = r

        # Get top funds
        if union_mode:
            top_fund_ids = set()
            for date in dates:
                date_funds = [
                    (fid, returns_map[fid].get(date, {}).get("roi_3y"))
                    for fid in returns_map
                    if returns_map[fid].get(date, {}).get("roi_3y") is not None
                ]
                date_funds.sort(key=lambda x: x[1] or 0, reverse=True)
                for fid, _ in date_funds[:top_n]:
                    top_fund_ids.add(fid)
        else:
            latest_date = max(dates)
            date_funds = [
                (fid, returns_map[fid].get(latest_date, {}).get("roi_3y"))
                for fid in returns_map
                if returns_map[fid].get(latest_date, {}).get("roi_3y") is not None
            ]
            date_funds.sort(key=lambda x: x[1] or 0, reverse=True)
            top_fund_ids = set(fid for fid, _ in date_funds[:top_n])

        # Build comparison data
        comparison = []
        for fid in top_fund_ids:
            if fid not in funds_map:
                continue

            fund = funds_map[fid]
            fund_returns = returns_map.get(fid, {})

            row = {
                "fund_name": fund["fund_name"],
                "fund_house": fund.get("fund_house"),
                "category": fund.get("category"),
            }

            for date in sorted(dates):
                date_key = date.replace("-", "_")
                row[f"roi_3y_{date_key}"] = fund_returns.get(date, {}).get("roi_3y")

            # Sort key
            latest_date = max(dates)
            row["_sort_key"] = fund_returns.get(latest_date, {}).get("roi_3y") or 0

            comparison.append(row)

        # Sort and rank
        comparison.sort(key=lambda x: x["_sort_key"], reverse=True)
        for i, row in enumerate(comparison, 1):
            row["rank"] = i
            del row["_sort_key"]

        return comparison

    # ==================== MARKET CHANGES ====================

    def save_significant_change(
        self,
        index_name: str,
        change_date: str,
        previous_close: float,
        current_close: float,
        change_percent: float,
        change_type: str
    ) -> bool:
        """
        Save a single significant market change

        Args:
            index_name: Index name (e.g., 'SENSEX')
            change_date: Date of the change (YYYY-MM-DD)
            previous_close: Previous day's close
            current_close: Current day's close
            change_percent: Percentage change
            change_type: 'up' or 'down'

        Returns:
            True if saved successfully
        """
        try:
            self.client.rpc("upsert_market_significant_change", {
                "p_index_name": index_name,
                "p_change_date": change_date,
                "p_previous_close": previous_close,
                "p_current_close": current_close,
                "p_change_percent": change_percent,
                "p_change_type": change_type,
            }).execute()
            return True
        except Exception:
            return False

    def save_significant_changes_batch(self, changes: List[Dict]) -> int:
        """
        Save multiple significant market changes

        Args:
            changes: List of change dicts with index_name, change_date, etc.

        Returns:
            Number of changes saved
        """
        saved = 0
        for change in changes:
            if self.save_significant_change(
                index_name=change["index_name"],
                change_date=change["change_date"],
                previous_close=change["previous_close"],
                current_close=change["current_close"],
                change_percent=change["change_percent"],
                change_type=change["change_type"]
            ):
                saved += 1
        return saved

    def get_significant_changes(
        self,
        index_name: str = None,
        min_threshold: float = None
    ) -> List[Dict]:
        """
        Get significant market changes from database

        Args:
            index_name: Filter by index name (optional)
            min_threshold: Minimum absolute change percentage (optional)

        Returns:
            List of change records
        """
        query = self.client.table("market_significant_changes").select("*")

        if index_name:
            query = query.eq("index_name", index_name)

        result = query.order("change_date", desc=True).execute()
        data = result.data

        if min_threshold is not None:
            data = [d for d in data if abs(d["change_percent"]) >= min_threshold]

        return data

    def get_significant_change_dates(self, index_name: str = "SENSEX") -> List[str]:
        """
        Get just the dates of significant changes

        Args:
            index_name: Index name

        Returns:
            List of date strings sorted descending
        """
        changes = self.get_significant_changes(index_name=index_name)
        return [c["change_date"] for c in changes]

    # ==================== USER WATCHLIST ====================

    def get_watchlist(self) -> List[Dict]:
        """Get all funds in user's watchlist"""
        try:
            result = self.client.table("user_watchlist").select("*").order("added_at", desc=True).execute()
            return result.data or []
        except Exception:
            return []

    def add_to_watchlist(self, fund_id: str, fund_name: str) -> bool:
        """Add a fund to watchlist"""
        try:
            self.client.table("user_watchlist").upsert({
                "fund_id": fund_id,
                "fund_name": fund_name
            }).execute()
            return True
        except Exception:
            return False

    def remove_from_watchlist(self, fund_id: str) -> bool:
        """Remove a fund from watchlist"""
        try:
            self.client.table("user_watchlist").delete().eq("fund_id", fund_id).execute()
            return True
        except Exception:
            return False

    def is_in_watchlist(self, fund_id: str) -> bool:
        """Check if a fund is in watchlist"""
        try:
            result = self.client.table("user_watchlist").select("id").eq("fund_id", fund_id).limit(1).execute()
            return len(result.data) > 0
        except Exception:
            return False

    # ==================== APP CONFIG ====================

    def get_last_fetch_date(self) -> Optional[str]:
        """Get the last fetch date from app_config table"""
        try:
            result = self.client.table("app_config").select("value").eq("key", "last_fetch_date").limit(1).execute()
            if result.data:
                return result.data[0]["value"]
            return None
        except Exception:
            return None

    def set_last_fetch_date(self, date: str) -> bool:
        """Set the last fetch date in app_config table"""
        try:
            self.client.table("app_config").upsert({
                "key": "last_fetch_date",
                "value": date
            }).execute()
            return True
        except Exception:
            return False

    # ==================== SINGLE FUND FETCH ====================

    def fetch_fund_returns(self, fund_id: str, scheme_code: int, report_date: str) -> bool:
        """
        Fetch and save returns for a single fund for a single date.
        For watchlist funds, use fetch_fund_returns_all_dates instead.
        """
        from .mfapi import MFAPIClient
        from .calculator import ROICalculator
        from datetime import datetime

        try:
            mfapi = MFAPIClient()
            nav_data = mfapi.get_fund_nav(scheme_code)

            if not nav_data or not nav_data.get('data'):
                return False

            calculator = ROICalculator(mfapi)
            target_date = datetime.strptime(report_date, '%Y-%m-%d')
            result = calculator.calculate_fund_returns(scheme_code, target_date)

            if not result or result.get('roi_3y') is None:
                return False

            # Update fund metadata
            meta = nav_data.get('meta', {})
            if meta:
                self.client.table("mutual_funds").update({
                    "fund_house": meta.get('fund_house', '').replace(' Mutual Fund', '') or 'Unknown',
                    "category": result.get('category', 'Unknown'),
                }).eq("id", fund_id).execute()

            # Save returns
            self.client.table("mutual_fund_returns").upsert({
                "fund_id": fund_id,
                "report_date": report_date,
                "roi_1y": result.get('roi_1y'),
                "roi_2y": result.get('roi_2y'),
                "roi_3y": result.get('roi_3y'),
                "source": "mfapi_watchlist",
            }, on_conflict="fund_id,report_date").execute()

            return True
        except Exception as e:
            print(f"Error fetching returns for fund: {e}")
            return False

    def fetch_fund_returns_all_dates(self, fund_id: str, scheme_code: int) -> int:
        """
        Fetch and save returns for a fund across ALL significant change dates.
        This ensures watchlist funds have complete historical data.

        Args:
            fund_id: Database fund ID
            scheme_code: AMFI scheme code for API lookup

        Returns:
            Number of dates with returns saved
        """
        from .mfapi import MFAPIClient
        from .calculator import ROICalculator
        from datetime import datetime

        try:
            # Get all significant change dates
            sig_dates = self.get_significant_change_dates("SENSEX")
            if not sig_dates:
                return 0

            # Also get today's date
            today = datetime.now().strftime('%Y-%m-%d')
            all_dates = list(set(sig_dates + [today]))

            # Fetch NAV data from API (once)
            mfapi = MFAPIClient()
            nav_data = mfapi.get_fund_nav(scheme_code)

            if not nav_data or not nav_data.get('data'):
                return 0

            calculator = ROICalculator(mfapi)

            # Update fund metadata
            meta = nav_data.get('meta', {})
            if meta:
                # Get category from first successful calculation
                sample_result = calculator.calculate_fund_returns(scheme_code, datetime.now())
                category = sample_result.get('category', 'Unknown') if sample_result else 'Unknown'

                self.client.table("mutual_funds").update({
                    "fund_house": meta.get('fund_house', '').replace(' Mutual Fund', '') or 'Unknown',
                    "category": category,
                }).eq("id", fund_id).execute()

            # Calculate returns for each date
            returns_records = []
            for date_str in all_dates:
                try:
                    target_date = datetime.strptime(date_str, '%Y-%m-%d')
                    result = calculator.calculate_fund_returns(scheme_code, target_date)

                    if result and result.get('roi_3y') is not None:
                        returns_records.append({
                            "fund_id": fund_id,
                            "report_date": date_str,
                            "roi_1y": result.get('roi_1y'),
                            "roi_2y": result.get('roi_2y'),
                            "roi_3y": result.get('roi_3y'),
                            "source": "mfapi_watchlist",
                        })
                except Exception:
                    continue

            # Batch upsert all returns
            if returns_records:
                self.client.table("mutual_fund_returns").upsert(
                    returns_records, on_conflict="fund_id,report_date"
                ).execute()

            return len(returns_records)

        except Exception as e:
            print(f"Error fetching returns for fund: {e}")
            return 0

    def get_fund_by_id(self, fund_id: str) -> Optional[Dict]:
        """Get fund by ID including scheme_code"""
        try:
            result = self.client.table("mutual_funds").select("*").eq("id", fund_id).limit(1).execute()
            return result.data[0] if result.data else None
        except Exception:
            return None

    # ==================== CLEANUP ====================

    def clear_all_data(self) -> None:
        """Clear all data from database (use with caution!)"""
        self.client.table("mutual_fund_returns").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
        self.client.table("mutual_funds").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
