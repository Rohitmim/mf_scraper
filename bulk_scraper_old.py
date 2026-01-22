#!/usr/bin/env python3
"""
Bulk MF Scraper - Fetches NAV data once, calculates ROI for multiple dates
Much more efficient than fetching per-date

Features:
- Persistent file cache (avoids re-fetching same data)
- Concurrent API requests (15 parallel)
- Calculate ROI for any historical date
"""

import os
import sys
import time
import json
import concurrent.futures
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path
import requests

try:
    from supabase import create_client, Client
except ImportError:
    print("Error: supabase package not installed. Run: pip install supabase")
    sys.exit(1)

# Cache settings
CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_FILE = CACHE_DIR / "nav_data.json"
CACHE_MAX_AGE_HOURS = 24  # Re-fetch if cache older than this


class BulkMFScraper:
    def __init__(self, verify_ssl: bool = True):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Encoding": "gzip, deflate",
        })
        self.session.verify = verify_ssl
        self.nav_cache: Dict[int, dict] = {}  # scheme_code -> {meta, data}
        CACHE_DIR.mkdir(exist_ok=True)

    def load_cache(self) -> bool:
        """Load NAV data from file cache if valid"""
        if not CACHE_FILE.exists():
            return False

        try:
            # Check cache age
            cache_age = time.time() - CACHE_FILE.stat().st_mtime
            cache_age_hours = cache_age / 3600

            if cache_age_hours > CACHE_MAX_AGE_HOURS:
                print(f"  Cache expired ({cache_age_hours:.1f}h old, max {CACHE_MAX_AGE_HOURS}h)")
                return False

            # Load cache
            with open(CACHE_FILE, 'r') as f:
                data = json.load(f)

            self.nav_cache = {int(k): v for k, v in data.get('nav_cache', {}).items()}
            cached_time = datetime.fromisoformat(data.get('cached_at', ''))

            print(f"  Loaded {len(self.nav_cache)} funds from cache")
            print(f"  Cache age: {cache_age_hours:.1f} hours (cached at {cached_time.strftime('%Y-%m-%d %H:%M')})")
            return True

        except Exception as e:
            print(f"  Cache load error: {e}")
            return False

    def save_cache(self):
        """Save NAV data to file cache"""
        try:
            data = {
                'cached_at': datetime.now().isoformat(),
                'nav_cache': {str(k): v for k, v in self.nav_cache.items()}
            }
            with open(CACHE_FILE, 'w') as f:
                json.dump(data, f)
            print(f"  Saved {len(self.nav_cache)} funds to cache")
        except Exception as e:
            print(f"  Cache save error: {e}")

    def fetch_fund_list(self) -> List[dict]:
        """Get list of all Direct Growth funds"""
        print("Fetching fund list...")
        resp = self.session.get("https://api.mfapi.in/mf", timeout=30)
        all_schemes = resp.json()

        # Filter for Direct Growth plans
        direct_growth = [s for s in all_schemes
                        if 'direct' in s.get('schemeName', '').lower()
                        and 'growth' in s.get('schemeName', '').lower()
                        and 'idcw' not in s.get('schemeName', '').lower()
                        and 'dividend' not in s.get('schemeName', '').lower()]

        print(f"Found {len(direct_growth)} Direct Growth funds")
        return direct_growth

    def fetch_single_fund_nav(self, scheme_code: int) -> Optional[dict]:
        """Fetch NAV data for a single fund"""
        try:
            resp = self.session.get(f"https://api.mfapi.in/mf/{scheme_code}", timeout=15)
            if resp.status_code == 200:
                return resp.json()
        except:
            pass
        return None

    def fetch_all_nav_data(self, schemes: List[dict], max_funds: int = 500, workers: int = 10):
        """Fetch NAV data for all funds using concurrent requests"""
        print(f"Fetching NAV data for {min(len(schemes), max_funds)} funds ({workers} concurrent)...")

        schemes_to_fetch = schemes[:max_funds]
        completed = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_code = {
                executor.submit(self.fetch_single_fund_nav, s['schemeCode']): s['schemeCode']
                for s in schemes_to_fetch
            }

            for future in concurrent.futures.as_completed(future_to_code):
                scheme_code = future_to_code[future]
                try:
                    data = future.result()
                    if data and data.get('data') and len(data['data']) >= 100:
                        self.nav_cache[scheme_code] = data
                except:
                    pass

                completed += 1
                if completed % 100 == 0:
                    print(f"  Fetched {completed}/{len(schemes_to_fetch)} funds...")

        print(f"  Cached NAV data for {len(self.nav_cache)} funds")

    def find_nav_for_date(self, nav_data: List[dict], target_date: datetime, tolerance_days: int = 10):
        """Find NAV closest to target date"""
        for item in nav_data:
            try:
                item_date = datetime.strptime(item['date'], '%d-%m-%Y')
                if abs((item_date - target_date).days) <= tolerance_days:
                    return float(item['nav']), item_date
            except:
                continue
        return None, None

    def calculate_returns_for_date(self, as_of_date: str) -> List[dict]:
        """Calculate 3Y ROI for all cached funds as of a specific date"""
        target_date = datetime.strptime(as_of_date, '%Y-%m-%d')
        funds = []

        for scheme_code, data in self.nav_cache.items():
            nav_data = data.get('data', [])
            meta = data.get('meta', {})

            # Get reference NAV for target date
            ref_nav, ref_date = self.find_nav_for_date(nav_data, target_date)
            if not ref_nav:
                continue

            # Get 3Y ago NAV
            nav_3y, _ = self.find_nav_for_date(nav_data, ref_date - timedelta(days=1095))
            if not nav_3y:
                continue

            # Get 1Y and 2Y NAVs
            nav_1y, _ = self.find_nav_for_date(nav_data, ref_date - timedelta(days=365))
            nav_2y, _ = self.find_nav_for_date(nav_data, ref_date - timedelta(days=730))

            # Calculate annualized returns
            roi_1y = ((ref_nav - nav_1y) / nav_1y * 100) if nav_1y else None
            roi_2y = (((ref_nav / nav_2y) ** 0.5 - 1) * 100) if nav_2y else None
            roi_3y = (((ref_nav / nav_3y) ** (1/3) - 1) * 100) if nav_3y else None

            # Categorize
            category = meta.get('scheme_category', 'Unknown')
            for cat, name in [('Large Cap', 'Large Cap'), ('Mid Cap', 'Mid Cap'),
                             ('Small Cap', 'Small Cap'), ('Multi Cap', 'Multi Cap'),
                             ('Flexi Cap', 'Flexi Cap'), ('ELSS', 'ELSS'),
                             ('Hybrid', 'Hybrid'), ('Balanced', 'Hybrid'),
                             ('Sectoral', 'Sectoral'), ('Thematic', 'Sectoral')]:
                if cat in category:
                    category = name
                    break

            funds.append({
                'fund_name': meta.get('scheme_name', ''),
                'fund_house': meta.get('fund_house', '').replace(' Mutual Fund', ''),
                'category': category,
                'roi_1y': round(roi_1y, 2) if roi_1y else None,
                'roi_2y': round(roi_2y, 2) if roi_2y else None,
                'roi_3y': round(roi_3y, 2) if roi_3y else None,
            })

        return funds


def save_to_supabase(funds: List[dict], report_date: str):
    """Save funds to Supabase"""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")

    if not url or not key:
        print("Error: SUPABASE_URL and SUPABASE_SERVICE_KEY required")
        return 0

    client = create_client(url, key)

    # Sort by 3Y ROI and take top 200
    funds_with_roi = [f for f in funds if f.get('roi_3y')]
    funds_with_roi.sort(key=lambda x: x['roi_3y'], reverse=True)
    top_200 = funds_with_roi[:200]

    print(f"  Saving {len(top_200)} funds for {report_date}...")

    saved = 0
    for fund in top_200:
        try:
            client.rpc("upsert_mutual_fund_with_returns", {
                "p_fund_name": fund["fund_name"],
                "p_fund_house": fund.get("fund_house", "Unknown"),
                "p_category": fund.get("category", "Unknown"),
                "p_report_date": report_date,
                "p_roi_1y": fund.get("roi_1y"),
                "p_roi_2y": fund.get("roi_2y"),
                "p_roi_3y": fund.get("roi_3y"),
                "p_source": "mfapi_bulk",
            }).execute()
            saved += 1
        except Exception as e:
            pass

    print(f"  Saved {saved} funds")
    return saved


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Bulk MF Scraper with caching")
    parser.add_argument("--dates", "-d", nargs="+", default=["2025-11-26", "2025-12-16", "2026-01-14"],
                       help="Dates to calculate ROI for (YYYY-MM-DD)")
    parser.add_argument("--refresh", "-r", action="store_true",
                       help="Force refresh cache (ignore existing)")
    parser.add_argument("--no-db", action="store_true",
                       help="Skip saving to database")
    parser.add_argument("--max-funds", "-n", type=int, default=600,
                       help="Max funds to fetch (default: 600)")
    parser.add_argument("--insecure", action="store_true",
                       help="Disable SSL verification (use if certificate errors)")
    args = parser.parse_args()

    dates = args.dates

    print("=" * 60)
    print("  Bulk MF Scraper - Real Historical ROI Data")
    print("=" * 60)

    # Initialize scraper
    scraper = BulkMFScraper(verify_ssl=not args.insecure)

    if args.insecure:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Try to load from cache first
    print("\nChecking cache...")
    cache_loaded = False if args.refresh else scraper.load_cache()

    if not cache_loaded:
        # Fetch fund list
        schemes = scraper.fetch_fund_list()

        # Fetch all NAV data ONCE (this is the slow part)
        print("\nFetching NAV data from API...")
        start = time.time()
        scraper.fetch_all_nav_data(schemes, max_funds=args.max_funds, workers=15)
        print(f"NAV fetch completed in {time.time() - start:.1f}s")

        # Save to cache for next time
        print("\nSaving to cache...")
        scraper.save_cache()
    else:
        print("\nUsing cached NAV data (skip API calls)")

    # Calculate returns for each date (fast - uses cached data)
    print("\nCalculating returns for each date...")
    for date in dates:
        print(f"\n[{date}]")
        funds = scraper.calculate_returns_for_date(date)
        print(f"  Found {len(funds)} funds with 3Y data")

        # Save to database (unless --no-db)
        if not args.no_db:
            save_to_supabase(funds, date)

        # Show top 5
        funds_sorted = sorted([f for f in funds if f.get('roi_3y')],
                             key=lambda x: x['roi_3y'], reverse=True)
        print(f"  Top 5 for {date}:")
        for i, f in enumerate(funds_sorted[:5], 1):
            print(f"    {i}. {f['fund_name'][:45]} - {f['roi_3y']:.2f}%")

    print("\n" + "=" * 60)
    print("  Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
