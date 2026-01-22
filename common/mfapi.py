"""
MFAPI Client - Fetches NAV data from api.mfapi.in
Features:
- Persistent file caching (avoids re-fetching)
- Concurrent requests for bulk fetching
- Configurable cache TTL
"""

import os
import json
import time
import concurrent.futures
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import requests


# Default cache settings
DEFAULT_CACHE_DIR = Path(__file__).parent.parent / ".cache"
DEFAULT_CACHE_FILE = "nav_data.json"
DEFAULT_CACHE_MAX_AGE_HOURS = 24


class MFAPIClient:
    """
    Client for MFAPI.in - official AMFI NAV data
    Reusable across all scripts with built-in caching
    """

    BASE_URL = "https://api.mfapi.in"

    def __init__(
        self,
        cache_dir: Path = None,
        cache_file: str = None,
        cache_max_age_hours: int = None,
        verify_ssl: bool = True
    ):
        """
        Initialize MFAPI client

        Args:
            cache_dir: Directory for cache files
            cache_file: Cache filename
            cache_max_age_hours: Max cache age before refresh (0 = no caching)
            verify_ssl: Whether to verify SSL certificates
        """
        self.cache_dir = cache_dir or DEFAULT_CACHE_DIR
        self.cache_file = self.cache_dir / (cache_file or DEFAULT_CACHE_FILE)
        self.cache_max_age_hours = cache_max_age_hours if cache_max_age_hours is not None else DEFAULT_CACHE_MAX_AGE_HOURS

        # Setup session
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Encoding": "gzip, deflate",
        })
        self.session.verify = verify_ssl

        # In-memory cache
        self.nav_cache: Dict[int, dict] = {}  # scheme_code -> {meta, data}
        self._cache_loaded = False

        # Ensure cache dir exists
        self.cache_dir.mkdir(exist_ok=True)

    # ==================== CACHE OPERATIONS ====================

    def load_cache(self, force: bool = False) -> bool:
        """
        Load NAV data from file cache

        Args:
            force: If True, load even if already loaded

        Returns:
            True if cache was loaded successfully
        """
        if self._cache_loaded and not force:
            return True

        if not self.cache_file.exists():
            return False

        try:
            # Check cache age
            cache_age_hours = (time.time() - self.cache_file.stat().st_mtime) / 3600

            if self.cache_max_age_hours > 0 and cache_age_hours > self.cache_max_age_hours:
                print(f"  Cache expired ({cache_age_hours:.1f}h old, max {self.cache_max_age_hours}h)")
                return False

            # Load cache
            with open(self.cache_file, 'r') as f:
                data = json.load(f)

            self.nav_cache = {int(k): v for k, v in data.get('nav_cache', {}).items()}
            cached_time = datetime.fromisoformat(data.get('cached_at', ''))

            print(f"  Loaded {len(self.nav_cache)} funds from cache")
            print(f"  Cache age: {cache_age_hours:.1f}h (cached at {cached_time.strftime('%Y-%m-%d %H:%M')})")

            self._cache_loaded = True
            return True

        except Exception as e:
            print(f"  Cache load error: {e}")
            return False

    def save_cache(self) -> bool:
        """Save NAV data to file cache"""
        if self.cache_max_age_hours == 0:
            return False  # Caching disabled

        try:
            data = {
                'cached_at': datetime.now().isoformat(),
                'nav_cache': {str(k): v for k, v in self.nav_cache.items()}
            }
            with open(self.cache_file, 'w') as f:
                json.dump(data, f)
            print(f"  Saved {len(self.nav_cache)} funds to cache")
            return True
        except Exception as e:
            print(f"  Cache save error: {e}")
            return False

    def clear_cache(self) -> None:
        """Clear both memory and file cache"""
        self.nav_cache = {}
        self._cache_loaded = False
        if self.cache_file.exists():
            self.cache_file.unlink()

    # ==================== API OPERATIONS ====================

    def get_fund_list(self, filter_direct_growth: bool = True) -> List[dict]:
        """
        Get list of all mutual fund schemes

        Args:
            filter_direct_growth: If True, only return Direct Growth plans

        Returns:
            List of scheme dicts with schemeCode, schemeName
        """
        resp = self.session.get(f"{self.BASE_URL}/mf", timeout=30)
        resp.raise_for_status()
        all_schemes = resp.json()

        if filter_direct_growth:
            return [
                s for s in all_schemes
                if 'direct' in s.get('schemeName', '').lower()
                and 'growth' in s.get('schemeName', '').lower()
                and 'idcw' not in s.get('schemeName', '').lower()
                and 'dividend' not in s.get('schemeName', '').lower()
            ]

        return all_schemes

    def get_fund_nav(self, scheme_code: int) -> Optional[dict]:
        """
        Get NAV data for a single fund

        Args:
            scheme_code: AMFI scheme code

        Returns:
            Dict with 'meta' and 'data' (NAV history), or None if failed
        """
        # Check cache first
        if scheme_code in self.nav_cache:
            return self.nav_cache[scheme_code]

        try:
            resp = self.session.get(f"{self.BASE_URL}/mf/{scheme_code}", timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('data') and len(data['data']) >= 100:
                    self.nav_cache[scheme_code] = data
                    return data
        except Exception:
            pass

        return None

    def fetch_all_nav_data(
        self,
        schemes: List[dict] = None,
        max_funds: int = 600,
        workers: int = 15,
        use_cache: bool = True
    ) -> int:
        """
        Fetch NAV data for multiple funds concurrently

        Args:
            schemes: List of schemes to fetch (if None, fetches fund list first)
            max_funds: Maximum number of funds to fetch
            workers: Number of concurrent workers
            use_cache: If True, try loading from cache first

        Returns:
            Number of funds in cache after operation
        """
        # Try cache first
        if use_cache and self.load_cache():
            return len(self.nav_cache)

        # Get fund list if not provided
        if schemes is None:
            print("  Fetching fund list...")
            schemes = self.get_fund_list()
            print(f"  Found {len(schemes)} Direct Growth funds")

        schemes_to_fetch = schemes[:max_funds]
        print(f"  Fetching NAV data for {len(schemes_to_fetch)} funds ({workers} concurrent)...")

        completed = 0

        def fetch_single(scheme):
            return scheme['schemeCode'], self.get_fund_nav(scheme['schemeCode'])

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(fetch_single, s): s for s in schemes_to_fetch}

            for future in concurrent.futures.as_completed(futures):
                completed += 1
                if completed % 100 == 0:
                    print(f"    Fetched {completed}/{len(schemes_to_fetch)} funds...")

        print(f"  Cached NAV data for {len(self.nav_cache)} funds")

        # Save to cache
        if use_cache:
            self.save_cache()

        return len(self.nav_cache)

    # ==================== NAV LOOKUP ====================

    def find_nav_for_date(
        self,
        scheme_code: int,
        target_date: datetime,
        exact: bool = True
    ) -> Tuple[Optional[float], Optional[datetime]]:
        """
        Find NAV for a specific date

        Args:
            scheme_code: AMFI scheme code
            target_date: Date to find NAV for
            exact: If True, require exact date match (for significant dates)
                   If False, find nearest date within 10 days (for historical 1Y/2Y/3Y lookups)

        Returns:
            Tuple of (NAV value, actual date) or (None, None) if not found
        """
        data = self.nav_cache.get(scheme_code)
        if not data:
            data = self.get_fund_nav(scheme_code)

        if not data or not data.get('data'):
            return None, None

        target_str = target_date.strftime('%d-%m-%Y')

        # First try exact match
        for item in data['data']:
            if item['date'] == target_str:
                return float(item['nav']), target_date

        # If exact match required but not found, return None
        if exact:
            return None, None

        # For historical lookups, find nearest date within 10 days
        best_match = None
        best_diff = float('inf')

        for item in data['data']:
            try:
                item_date = datetime.strptime(item['date'], '%d-%m-%Y')
                diff = abs((item_date - target_date).days)

                if diff <= 10 and diff < best_diff:
                    best_match = (float(item['nav']), item_date)
                    best_diff = diff
            except (ValueError, KeyError):
                continue

        return best_match if best_match else (None, None)

    def get_fund_meta(self, scheme_code: int) -> Optional[dict]:
        """Get fund metadata (name, house, category)"""
        data = self.nav_cache.get(scheme_code)
        if not data:
            data = self.get_fund_nav(scheme_code)

        return data.get('meta') if data else None

    # ==================== ITERATION ====================

    def iter_cached_funds(self):
        """Iterate over all cached funds"""
        for scheme_code, data in self.nav_cache.items():
            yield scheme_code, data.get('meta', {}), data.get('data', [])
