"""
Holdings Scraper - Fetches mutual fund holdings and estimates current NAV
Sources: Value Research, stock prices from Yahoo Finance
Database caching: Holdings stored in DB, reused if same day
"""

import requests
from bs4 import BeautifulSoup
import yfinance as yf
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta, date
import re


class HoldingsScraper:
    """Scrape fund holdings and calculate estimated NAV"""

    def __init__(self, db_client=None):
        """
        Initialize scraper with optional database client for persistent caching.

        Args:
            db_client: Supabase client for database operations. If None, uses memory cache only.
        """
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.db = db_client
        # Cache for stock prices (symbol -> {price, timestamp})
        self._price_cache: Dict[str, dict] = {}
        # In-memory cache for holdings (scheme_code -> {holdings, timestamp})
        self._holdings_cache: Dict[int, dict] = {}

    def _get_holdings_from_db(self, scheme_code: int) -> List[dict]:
        """Get holdings from database if fetched today"""
        if not self.db:
            return []

        try:
            today = date.today().isoformat()
            result = self.db.table("fund_holdings").select("*").eq(
                "scheme_code", scheme_code
            ).eq("fetch_date", today).execute()

            if result.data:
                return [
                    {
                        'stock_name': r['stock_name'],
                        'nse_symbol': r['nse_symbol'],
                        'percentage': float(r['percentage']) if r['percentage'] else 0,
                        'sector': r['sector'] or ''
                    }
                    for r in result.data
                ]
        except Exception as e:
            print(f"DB read error: {e}")

        return []

    def _save_holdings_to_db(self, scheme_code: int, fund_id: Optional[int], holdings: List[dict]) -> bool:
        """Save holdings to database"""
        if not self.db or not holdings:
            return False

        try:
            today = date.today().isoformat()

            # Delete old entries for this fund/date (in case of re-fetch)
            self.db.table("fund_holdings").delete().eq(
                "scheme_code", scheme_code
            ).eq("fetch_date", today).execute()

            # Insert new holdings
            rows = [
                {
                    'fund_id': fund_id,
                    'scheme_code': scheme_code,
                    'fetch_date': today,
                    'stock_name': h['stock_name'],
                    'nse_symbol': h.get('nse_symbol', ''),
                    'percentage': h.get('percentage', 0),
                    'sector': h.get('sector', '')
                }
                for h in holdings
            ]

            self.db.table("fund_holdings").insert(rows).execute()
            return True
        except Exception as e:
            print(f"DB write error: {e}")

        return False

    def get_holdings_from_mfapi(self, scheme_code: int) -> List[dict]:
        """
        Try to get holdings from MFAPI (if available)
        Returns list of {stock_name, percentage, sector}
        """
        try:
            url = f"https://api.mfapi.in/mf/{scheme_code}"
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                # MFAPI doesn't provide holdings, but check anyway
                if 'holdings' in data:
                    return data['holdings']
        except Exception:
            pass
        return []

    def search_moneycontrol_fund(self, fund_name: str) -> Optional[str]:
        """Search for fund on Moneycontrol and return fund URL"""
        try:
            # Clean fund name for search
            search_term = fund_name.replace('-', ' ').replace('Direct Plan', '').replace('Growth', '')
            search_term = ' '.join(search_term.split()[:5])  # First 5 words

            search_url = f"https://www.moneycontrol.com/mc/searchresult.php?search_str={search_term}"
            resp = self.session.get(search_url, timeout=10)

            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                # Look for mutual fund links
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if '/mutual-funds/nav/' in href:
                        return href
        except Exception:
            pass
        return None

    def get_holdings_from_moneycontrol(self, fund_url: str) -> List[dict]:
        """Scrape holdings from Moneycontrol fund page"""
        holdings = []
        try:
            # Convert nav URL to portfolio URL
            portfolio_url = fund_url.replace('/nav/', '/portfolio-holdings/')

            resp = self.session.get(portfolio_url, timeout=15)
            if resp.status_code != 200:
                return []

            soup = BeautifulSoup(resp.text, 'html.parser')

            # Find holdings table
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows[1:]:  # Skip header
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        stock_name = cols[0].get_text(strip=True)
                        try:
                            percentage = float(cols[2].get_text(strip=True).replace('%', ''))
                            if stock_name and percentage > 0:
                                holdings.append({
                                    'stock_name': stock_name,
                                    'percentage': percentage,
                                    'sector': cols[1].get_text(strip=True) if len(cols) > 1 else ''
                                })
                        except (ValueError, IndexError):
                            continue
        except Exception:
            pass

        return holdings[:15]  # Top 15 holdings

    def get_holdings(self, scheme_code: int, fund_name: str, fund_id: Optional[int] = None) -> List[dict]:
        """
        Get holdings for a fund (with database + memory caching)
        Returns list of {stock_name, percentage, nse_symbol}

        Cache strategy:
        1. Check in-memory cache (valid for current session)
        2. Check database (valid if fetched today)
        3. Scrape fresh and save to database
        """
        # 1. Check in-memory cache
        if scheme_code in self._holdings_cache:
            cached = self._holdings_cache[scheme_code]
            if datetime.now() - cached['timestamp'] < timedelta(hours=1):
                return cached['holdings']

        # 2. Check database cache (same day)
        holdings = self._get_holdings_from_db(scheme_code)
        if holdings:
            # Update in-memory cache
            self._holdings_cache[scheme_code] = {
                'holdings': holdings,
                'timestamp': datetime.now()
            }
            return holdings

        # 3. Scrape fresh
        holdings = []
        mc_url = self.search_moneycontrol_fund(fund_name)
        if mc_url:
            holdings = self.get_holdings_from_moneycontrol(mc_url)

        # Map stock names to NSE symbols
        for holding in holdings:
            holding['nse_symbol'] = self._guess_nse_symbol(holding['stock_name'])

        # Save to database and memory cache
        if holdings:
            self._save_holdings_to_db(scheme_code, fund_id, holdings)
            self._holdings_cache[scheme_code] = {
                'holdings': holdings,
                'timestamp': datetime.now()
            }

        return holdings

    def _guess_nse_symbol(self, stock_name: str) -> str:
        """
        Try to guess NSE symbol from stock name
        Common mappings for major stocks
        """
        # Clean the name
        name = stock_name.upper().strip()

        # Common stock mappings
        mappings = {
            'RELIANCE': 'RELIANCE.NS',
            'HDFC BANK': 'HDFCBANK.NS',
            'ICICI BANK': 'ICICIBANK.NS',
            'INFOSYS': 'INFY.NS',
            'TCS': 'TCS.NS',
            'TATA CONSULTANCY': 'TCS.NS',
            'BHARTI AIRTEL': 'BHARTIARTL.NS',
            'ITC': 'ITC.NS',
            'KOTAK': 'KOTAKBANK.NS',
            'LARSEN': 'LT.NS',
            'L&T': 'LT.NS',
            'AXIS BANK': 'AXISBANK.NS',
            'STATE BANK': 'SBIN.NS',
            'SBI': 'SBIN.NS',
            'BAJAJ FINANCE': 'BAJFINANCE.NS',
            'MARUTI': 'MARUTI.NS',
            'HCL TECH': 'HCLTECH.NS',
            'WIPRO': 'WIPRO.NS',
            'ASIAN PAINTS': 'ASIANPAINT.NS',
            'SUN PHARMA': 'SUNPHARMA.NS',
            'TITAN': 'TITAN.NS',
            'ULTRATECH': 'ULTRACEMCO.NS',
            'NESTLE': 'NESTLEIND.NS',
            'POWER GRID': 'POWERGRID.NS',
            'NTPC': 'NTPC.NS',
            'TATA MOTORS': 'TATAMOTORS.NS',
            'TATA STEEL': 'TATASTEEL.NS',
            'MAHINDRA': 'M&M.NS',
            'M&M': 'M&M.NS',
            'ADANI': 'ADANIENT.NS',
            'HINDALCO': 'HINDALCO.NS',
            'GRASIM': 'GRASIM.NS',
            'DIVIS LAB': 'DIVISLAB.NS',
            'TECH MAHINDRA': 'TECHM.NS',
            'BRITANNIA': 'BRITANNIA.NS',
            'CIPLA': 'CIPLA.NS',
            'EICHER': 'EICHERMOT.NS',
            'HERO MOTOCORP': 'HEROMOTOCO.NS',
            'HDFC LIFE': 'HDFCLIFE.NS',
            'SBI LIFE': 'SBILIFE.NS',
            'BAJAJ FINSERV': 'BAJAJFINSV.NS',
            'INDUSIND': 'INDUSINDBK.NS',
            'JSW STEEL': 'JSWSTEEL.NS',
            'TATA CONSUMER': 'TATACONSUM.NS',
            'APOLLO': 'APOLLOHOSP.NS',
            'DR REDDY': 'DRREDDY.NS',
            'ONGC': 'ONGC.NS',
            'COAL INDIA': 'COALINDIA.NS',
            'ZOMATO': 'ZOMATO.NS',
        }

        # Try exact and partial matches
        for key, symbol in mappings.items():
            if key in name:
                return symbol

        # Default: try common format
        # Remove common suffixes and create symbol
        clean = re.sub(r'\s*(LTD|LIMITED|INDIA|INDUSTRIES|CORPORATION)\.?$', '', name)
        clean = clean.replace(' ', '').replace('.', '')[:12]
        return f"{clean}.NS" if clean else ""

    def get_stock_price(self, symbol: str) -> Optional[Tuple[float, float]]:
        """
        Get current stock price and previous close from Yahoo Finance
        Returns (current_price, prev_close) or None if failed
        """
        if not symbol:
            return None

        # Check cache (valid for 5 minutes)
        if symbol in self._price_cache:
            cached = self._price_cache[symbol]
            if datetime.now() - cached['timestamp'] < timedelta(minutes=5):
                return cached['price'], cached['prev_close']

        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            current = info.get('currentPrice') or info.get('regularMarketPrice')
            prev_close = info.get('previousClose') or info.get('regularMarketPreviousClose')

            if current and prev_close:
                self._price_cache[symbol] = {
                    'price': current,
                    'prev_close': prev_close,
                    'timestamp': datetime.now()
                }
                return current, prev_close
        except Exception:
            pass

        return None

    def estimate_nav_change(self, scheme_code: int, fund_name: str, last_nav: float, fund_id: Optional[int] = None) -> Optional[dict]:
        """
        Estimate current NAV based on holdings and stock price changes

        Returns dict with:
        - estimated_nav: float
        - change_percent: float
        - holdings_used: int (number of holdings with price data)
        - last_nav: float
        - calculation_time: datetime
        """
        holdings = self.get_holdings(scheme_code, fund_name, fund_id=fund_id)
        if not holdings:
            return None

        total_weight = 0
        weighted_change = 0
        holdings_with_price = 0

        for holding in holdings:
            symbol = holding.get('nse_symbol')
            pct = holding.get('percentage', 0)

            if not symbol or pct <= 0:
                continue

            price_data = self.get_stock_price(symbol)
            if price_data:
                current, prev_close = price_data
                stock_change = ((current - prev_close) / prev_close) * 100

                weighted_change += stock_change * (pct / 100)
                total_weight += pct
                holdings_with_price += 1

                holding['current_price'] = current
                holding['prev_close'] = prev_close
                holding['change_pct'] = stock_change

        if holdings_with_price == 0 or total_weight == 0:
            return None

        # Scale weighted change to account for holdings we have data for
        # (if we only have 60% of holdings, scale up the change proportionally)
        if total_weight < 100:
            weighted_change = weighted_change * (100 / total_weight)

        estimated_nav = last_nav * (1 + weighted_change / 100)

        return {
            'estimated_nav': round(estimated_nav, 2),
            'change_percent': round(weighted_change, 2),
            'last_nav': last_nav,
            'holdings_used': holdings_with_price,
            'total_holdings': len(holdings),
            'coverage_pct': round(total_weight, 1),
            'holdings': holdings,
            'calculation_time': datetime.now()
        }
