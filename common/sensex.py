"""
SENSEX Client - Fetches market index data from Yahoo Finance
Identifies significant daily changes (>threshold%)
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
from pathlib import Path

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False


# Default settings
DEFAULT_CACHE_DIR = Path(__file__).parent.parent / ".cache"
DEFAULT_THRESHOLD = 3.0  # Percentage


class SensexClient:
    """
    Client for fetching SENSEX/market index data
    Uses Yahoo Finance as data source
    """

    # Yahoo Finance tickers for Indian indices
    TICKERS = {
        "SENSEX": "^BSESN",
        "NIFTY50": "^NSEI",
    }

    def __init__(
        self,
        cache_dir: Path = None,
        cache_max_age_hours: int = 24
    ):
        """
        Initialize SENSEX client

        Args:
            cache_dir: Directory for cache files
            cache_max_age_hours: Max cache age before refresh
        """
        if not HAS_YFINANCE:
            raise ImportError("yfinance package not installed. Run: pip install yfinance")

        self.cache_dir = cache_dir or DEFAULT_CACHE_DIR
        self.cache_max_age_hours = cache_max_age_hours
        self.cache_dir.mkdir(exist_ok=True)

    def _get_cache_file(self, index_name: str) -> Path:
        """Get cache file path for an index"""
        return self.cache_dir / f"{index_name.lower()}_data.json"

    def _load_cache(self, index_name: str) -> Optional[Dict]:
        """Load cached data if valid"""
        cache_file = self._get_cache_file(index_name)

        if not cache_file.exists():
            return None

        try:
            import time
            cache_age_hours = (time.time() - cache_file.stat().st_mtime) / 3600

            if self.cache_max_age_hours > 0 and cache_age_hours > self.cache_max_age_hours:
                return None

            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception:
            return None

    def _save_cache(self, index_name: str, data: Dict) -> None:
        """Save data to cache"""
        cache_file = self._get_cache_file(index_name)
        try:
            with open(cache_file, 'w') as f:
                json.dump(data, f)
        except Exception:
            pass

    def fetch_historical_data(
        self,
        index_name: str = "SENSEX",
        years: int = 3,
        use_cache: bool = True
    ) -> List[Dict]:
        """
        Fetch historical data for an index

        Args:
            index_name: Index name ('SENSEX' or 'NIFTY50')
            years: Number of years of history
            use_cache: Whether to use cached data

        Returns:
            List of dicts with date, close, previous_close, change_percent
        """
        # Check cache
        if use_cache:
            cached = self._load_cache(index_name)
            if cached:
                print(f"  Using cached {index_name} data")
                return cached.get('data', [])

        # Get ticker
        ticker = self.TICKERS.get(index_name.upper())
        if not ticker:
            raise ValueError(f"Unknown index: {index_name}. Supported: {list(self.TICKERS.keys())}")

        print(f"  Fetching {index_name} data from Yahoo Finance...")

        # Fetch data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years * 365 + 30)  # Extra buffer

        df = yf.download(
            ticker,
            start=start_date.strftime('%Y-%m-%d'),
            end=end_date.strftime('%Y-%m-%d'),
            progress=False
        )

        if df.empty:
            raise ValueError(f"No data returned for {index_name}")

        # Process data
        data = []
        prev_close = None

        for date, row in df.iterrows():
            close = float(row['Close'].iloc[0]) if hasattr(row['Close'], 'iloc') else float(row['Close'])

            if prev_close is not None:
                change_percent = ((close - prev_close) / prev_close) * 100
                data.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'close': round(close, 2),
                    'previous_close': round(prev_close, 2),
                    'change_percent': round(change_percent, 2)
                })

            prev_close = close

        print(f"  Fetched {len(data)} trading days")

        # Save to cache
        if use_cache:
            self._save_cache(index_name, {
                'cached_at': datetime.now().isoformat(),
                'index_name': index_name,
                'data': data
            })

        return data

    def find_significant_changes(
        self,
        index_name: str = "SENSEX",
        threshold: float = DEFAULT_THRESHOLD,
        years: int = 3,
        use_cache: bool = True
    ) -> List[Dict]:
        """
        Find dates where index changed by more than threshold

        Args:
            index_name: Index name
            threshold: Minimum absolute change percentage (default 3%)
            years: Number of years to look back
            use_cache: Whether to use cached data

        Returns:
            List of significant change events sorted by date
        """
        data = self.fetch_historical_data(index_name, years, use_cache)

        significant = []
        for day in data:
            if abs(day['change_percent']) >= threshold:
                significant.append({
                    'index_name': index_name,
                    'change_date': day['date'],
                    'previous_close': day['previous_close'],
                    'current_close': day['close'],
                    'change_percent': day['change_percent'],
                    'change_type': 'up' if day['change_percent'] > 0 else 'down'
                })

        # Sort by date
        significant.sort(key=lambda x: x['change_date'])

        return significant

    def get_change_dates(
        self,
        index_name: str = "SENSEX",
        threshold: float = DEFAULT_THRESHOLD,
        years: int = 3
    ) -> List[str]:
        """
        Get just the dates of significant changes

        Args:
            index_name: Index name
            threshold: Minimum absolute change percentage
            years: Number of years

        Returns:
            List of date strings (YYYY-MM-DD)
        """
        changes = self.find_significant_changes(index_name, threshold, years)
        return [c['change_date'] for c in changes]

    def check_date_significance(
        self,
        date_str: str,
        index_name: str = "SENSEX",
        threshold: float = 0.5
    ) -> Optional[Dict]:
        """
        Check if a specific date had a significant market change.
        Uses lower threshold (0.5%) for daily tracking.

        Args:
            date_str: Date to check (YYYY-MM-DD)
            index_name: Index name
            threshold: Minimum absolute change percentage (default 0.5% for daily)

        Returns:
            Dict with change info if significant, None otherwise
        """
        # Fetch recent data (use cache)
        data = self.fetch_historical_data(index_name, years=1, use_cache=True)

        # Find the date in data
        for day in data:
            if day['date'] == date_str:
                if abs(day['change_percent']) >= threshold:
                    return {
                        'index_name': index_name,
                        'change_date': day['date'],
                        'previous_close': day['previous_close'],
                        'current_close': day['close'],
                        'change_percent': day['change_percent'],
                        'change_type': 'up' if day['change_percent'] > 0 else 'down'
                    }
                return None

        # Date not found in cache, try fetching fresh
        data = self.fetch_historical_data(index_name, years=1, use_cache=False)
        for day in data:
            if day['date'] == date_str:
                if abs(day['change_percent']) >= threshold:
                    return {
                        'index_name': index_name,
                        'change_date': day['date'],
                        'previous_close': day['previous_close'],
                        'current_close': day['close'],
                        'change_percent': day['change_percent'],
                        'change_type': 'up' if day['change_percent'] > 0 else 'down'
                    }
                return None

        return None
