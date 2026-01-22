#!/usr/bin/env python3
"""
Comprehensive Test Suite for MF Scraper Application
Run: python test_suite.py
"""

import sys
import time
import os
import subprocess
import signal
from datetime import datetime


def load_env_file(env_path=".env"):
    """Load environment variables from .env file"""
    env = os.environ.copy()
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Strip quotes if present
                    value = value.strip().strip('"').strip("'")
                    env[key.strip()] = value
    return env

# Test results tracking
results = {"passed": 0, "failed": 0, "errors": []}


def test_passed(name):
    results["passed"] += 1
    print(f"  ✓ {name}")


def test_failed(name, reason):
    results["failed"] += 1
    results["errors"].append(f"{name}: {reason}")
    print(f"  ✗ {name}: {reason}")


def run_test(name, test_func):
    """Run a test function and catch exceptions"""
    try:
        test_func()
    except Exception as e:
        test_failed(name, str(e))


# ==================== DATABASE TESTS ====================

def test_database():
    """Test database connectivity and data integrity"""
    print("\n" + "=" * 60)
    print("DATABASE TESTS")
    print("=" * 60)

    from common import SupabaseDB

    try:
        db = SupabaseDB()
        test_passed("Database connection")
    except Exception as e:
        test_failed("Database connection", str(e))
        return

    # Test 1: Check mutual_funds table
    try:
        funds = db.get_all_funds()
        if len(funds) > 0:
            test_passed(f"mutual_funds table has {len(funds)} funds")
        else:
            test_failed("mutual_funds table", "No funds found")
    except Exception as e:
        test_failed("mutual_funds table", str(e))

    # Test 2: Check funds have scheme_code
    try:
        result = db.client.table("mutual_funds").select("id,scheme_code").not_.is_("scheme_code", "null").execute()
        funds_with_code = len(result.data)
        total_funds = len(funds)
        pct = (funds_with_code / total_funds * 100) if total_funds > 0 else 0
        if pct > 90:
            test_passed(f"scheme_code coverage: {funds_with_code}/{total_funds} ({pct:.1f}%)")
        else:
            test_failed("scheme_code coverage", f"Only {pct:.1f}% funds have scheme_code")
    except Exception as e:
        test_failed("scheme_code coverage", str(e))

    # Test 3: Check mutual_fund_returns table (with proper pagination)
    try:
        all_dates = set()
        offset = 0
        while True:
            result = db.client.table("mutual_fund_returns").select("report_date").range(offset, offset + 999).execute()
            if not result.data:
                break
            all_dates.update(r["report_date"] for r in result.data)
            offset += 1000
            if len(result.data) < 1000:
                break
        if len(all_dates) > 0:
            test_passed(f"mutual_fund_returns has {len(all_dates)} unique dates")
        else:
            test_failed("mutual_fund_returns", "No dates found")
    except Exception as e:
        test_failed("mutual_fund_returns", str(e))

    # Test 4: Check for NULL roi_3y values
    try:
        result = db.client.table("mutual_fund_returns").select("id", count="exact").is_("roi_3y", "null").execute()
        null_count = result.count
        total_result = db.client.table("mutual_fund_returns").select("id", count="exact").execute()
        total_count = total_result.count
        pct_null = (null_count / total_count * 100) if total_count > 0 else 0
        if pct_null < 5:
            test_passed(f"roi_3y NULL rate: {null_count}/{total_count} ({pct_null:.1f}%)")
        else:
            test_failed("roi_3y NULL rate", f"{pct_null:.1f}% of returns have NULL roi_3y")
    except Exception as e:
        test_failed("roi_3y NULL check", str(e))

    # Test 5: Check significant changes table
    try:
        changes = db.get_significant_changes(index_name="SENSEX")
        if len(changes) > 0:
            test_passed(f"market_significant_changes has {len(changes)} records")
        else:
            test_failed("market_significant_changes", "No records found")
    except Exception as e:
        test_failed("market_significant_changes", str(e))

    # Test 6: Check watchlist table
    try:
        watchlist = db.get_watchlist()
        test_passed(f"user_watchlist has {len(watchlist)} funds")
    except Exception as e:
        test_failed("user_watchlist", str(e))

    # Test 7: Check data consistency - funds in returns should exist in mutual_funds
    try:
        # Get fund_ids from returns
        returns_result = db.client.table("mutual_fund_returns").select("fund_id").execute()
        return_fund_ids = set(r["fund_id"] for r in returns_result.data)

        # Get fund_ids from mutual_funds
        funds_result = db.client.table("mutual_funds").select("id").execute()
        fund_ids = set(f["id"] for f in funds_result.data)

        orphan_returns = return_fund_ids - fund_ids
        if len(orphan_returns) == 0:
            test_passed("Data consistency: All returns have valid fund_id")
        else:
            test_failed("Data consistency", f"{len(orphan_returns)} orphan returns found")
    except Exception as e:
        test_failed("Data consistency", str(e))


# ==================== DATA QUALITY TESTS ====================

def test_data_quality():
    """Test data quality and completeness"""
    print("\n" + "=" * 60)
    print("DATA QUALITY TESTS")
    print("=" * 60)

    from common import SupabaseDB

    db = SupabaseDB()

    # Test 1: Check latest date has enough funds
    try:
        dates = db.get_available_dates()
        if dates:
            latest_date = dates[0]
            result = db.client.table("mutual_fund_returns").select("id", count="exact").eq("report_date", latest_date).not_.is_("roi_3y", "null").execute()
            funds_count = result.count
            if funds_count >= 200:
                test_passed(f"Latest date ({latest_date}) has {funds_count} funds with roi_3y")
            else:
                test_failed("Latest date fund count", f"Only {funds_count} funds (expected >= 200)")
    except Exception as e:
        test_failed("Latest date check", str(e))

    # Test 2: Check date coverage - how many dates have good data
    try:
        dates = db.get_available_dates()
        good_dates = 0
        for date in dates[:30]:  # Check last 30 dates
            result = db.client.table("mutual_fund_returns").select("id", count="exact").eq("report_date", date).not_.is_("roi_3y", "null").execute()
            if result.count >= 100:
                good_dates += 1

        pct = (good_dates / min(30, len(dates)) * 100) if dates else 0
        if pct >= 80:
            test_passed(f"Date coverage: {good_dates}/30 dates have 100+ funds ({pct:.1f}%)")
        else:
            test_failed("Date coverage", f"Only {pct:.1f}% dates have adequate data")
    except Exception as e:
        test_failed("Date coverage", str(e))

    # Test 3: Check for funds with all NULL returns
    try:
        # Find funds that have returns but all are NULL
        result = db.client.table("mutual_fund_returns").select("fund_id, roi_3y").execute()

        fund_returns = {}
        for r in result.data:
            fid = r["fund_id"]
            if fid not in fund_returns:
                fund_returns[fid] = {"total": 0, "nulls": 0}
            fund_returns[fid]["total"] += 1
            if r["roi_3y"] is None:
                fund_returns[fid]["nulls"] += 1

        all_null_funds = [fid for fid, data in fund_returns.items() if data["total"] == data["nulls"]]

        if len(all_null_funds) == 0:
            test_passed("No funds with all-NULL returns")
        else:
            test_failed("Funds with all-NULL returns", f"{len(all_null_funds)} funds have only NULL roi_3y")
    except Exception as e:
        test_failed("All-NULL funds check", str(e))

    # Test 4: Check ROI values are reasonable
    try:
        result = db.client.table("mutual_fund_returns").select("roi_3y").not_.is_("roi_3y", "null").limit(1000).execute()
        roi_values = [r["roi_3y"] for r in result.data]

        if roi_values:
            min_roi = min(roi_values)
            max_roi = max(roi_values)
            avg_roi = sum(roi_values) / len(roi_values)

            # Reasonable range: -50% to 100% for 3Y ROI
            if -50 <= min_roi and max_roi <= 150:
                test_passed(f"ROI range reasonable: {min_roi:.1f}% to {max_roi:.1f}% (avg: {avg_roi:.1f}%)")
            else:
                test_failed("ROI range", f"Suspicious values: min={min_roi:.1f}%, max={max_roi:.1f}%")
    except Exception as e:
        test_failed("ROI range check", str(e))

    # Test 5: Watchlist funds have data
    try:
        watchlist = db.get_watchlist()
        if watchlist:
            missing_data = []
            for w in watchlist:
                result = db.client.table("mutual_fund_returns").select("id", count="exact").eq("fund_id", w["fund_id"]).not_.is_("roi_3y", "null").execute()
                if result.count < 10:
                    missing_data.append(w["fund_name"][:30])

            if len(missing_data) == 0:
                test_passed(f"All {len(watchlist)} watchlist funds have sufficient data")
            else:
                test_failed("Watchlist data", f"{len(missing_data)} funds missing data: {missing_data}")
        else:
            test_passed("No watchlist funds to check")
    except Exception as e:
        test_failed("Watchlist data check", str(e))


# ==================== API TESTS ====================

def test_api():
    """Test MFAPI connectivity"""
    print("\n" + "=" * 60)
    print("API TESTS")
    print("=" * 60)

    from common import MFAPIClient

    try:
        mfapi = MFAPIClient()
        test_passed("MFAPIClient initialized")
    except Exception as e:
        test_failed("MFAPIClient init", str(e))
        return

    # Test 1: Fetch fund list
    try:
        schemes = mfapi.get_fund_list(filter_direct_growth=True)
        if len(schemes) > 1000:
            test_passed(f"Fund list fetched: {len(schemes)} Direct Growth funds")
        else:
            test_failed("Fund list", f"Only {len(schemes)} funds found")
    except Exception as e:
        test_failed("Fund list fetch", str(e))

    # Test 2: Fetch single fund NAV
    try:
        # Use a known scheme code (HDFC Mid-Cap Opportunities)
        nav_data = mfapi.get_fund_nav(118989)
        if nav_data and nav_data.get("data") and len(nav_data["data"]) > 100:
            test_passed(f"NAV data fetched: {len(nav_data['data'])} data points")
        else:
            test_failed("NAV data fetch", "Insufficient NAV data")
    except Exception as e:
        test_failed("NAV data fetch", str(e))


# ==================== UI TESTS ====================

def test_ui():
    """Test Streamlit UI with Playwright"""
    print("\n" + "=" * 60)
    print("UI TESTS (Playwright)")
    print("=" * 60)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        test_failed("Playwright import", "playwright not installed")
        return

    # Start Streamlit with env vars loaded
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(script_dir, ".env")

    # Kill any existing Streamlit processes on port 8501
    subprocess.run("pkill -f 'streamlit run ui_app.py' 2>/dev/null; lsof -ti:8501 | xargs kill -9 2>/dev/null",
                   shell=True, capture_output=True)
    time.sleep(2)

    # Start Streamlit using shell to source env file directly
    streamlit_cmd = f"set -a && source {env_path} && set +a && python3 -m streamlit run ui_app.py --server.headless=true --server.port=8501"
    streamlit_proc = subprocess.Popen(
        streamlit_cmd,
        shell=True,
        cwd=script_dir,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    # Wait for Streamlit to start
    time.sleep(8)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Test 1: Page loads
            try:
                page.goto("http://localhost:8501", timeout=30000)
                page.wait_for_selector("div[data-testid='stAppViewContainer']", timeout=20000)
                test_passed("Page loads successfully")
            except Exception as e:
                test_failed("Page load", str(e))
                browser.close()
                streamlit_proc.terminate()
                return

            # Wait for data to load - check for sidebar or data table with retries
            max_wait = 60  # max seconds to wait for data
            for i in range(max_wait // 5):
                time.sleep(5)
                # Check if loading is complete (sidebar or table visible)
                sidebar = page.query_selector("section[data-testid='stSidebar']")
                tables = page.query_selector_all("div[data-testid='stDataFrame']")
                if sidebar or tables:
                    break

            # Test 2: Check for errors
            error_elements = page.query_selector_all("div[data-testid='stException']")
            if len(error_elements) == 0:
                test_passed("No errors on page")
            else:
                test_failed("Page errors", f"{len(error_elements)} errors found")

            # Test 3: Data table exists
            tables = page.query_selector_all("div[data-testid='stDataFrame']")
            if len(tables) > 0:
                test_passed(f"Data tables found: {len(tables)}")
            else:
                test_failed("Data tables", "No data tables found")

            # Test 4: Check for None/NaN in visible content
            page_content = page.content()
            none_count = page_content.lower().count(">none<") + page_content.lower().count(">nan<")
            if none_count < 10:
                test_passed(f"None/NaN count in HTML: {none_count}")
            else:
                test_failed("None/NaN values", f"Found {none_count} None/NaN in page")

            # Test 5: Sidebar elements
            sidebar = page.query_selector("section[data-testid='stSidebar']")
            if sidebar:
                test_passed("Sidebar present")

                # Check watchlist section
                sidebar_text = sidebar.inner_text()
                if "Watchlist" in sidebar_text:
                    test_passed("Watchlist section present")
                else:
                    test_failed("Watchlist section", "Not found in sidebar")
            else:
                test_failed("Sidebar", "Not found")

            # Test 6: Take screenshot for manual review
            page.screenshot(path="/Users/zop7782/mf_scraper/test_suite_screenshot.png", full_page=True)
            test_passed("Screenshot saved: test_suite_screenshot.png")

            browser.close()

    except Exception as e:
        test_failed("UI test", str(e))
    finally:
        # Stop Streamlit
        streamlit_proc.terminate()
        try:
            streamlit_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            streamlit_proc.kill()


# ==================== CALCULATION TESTS ====================

def test_calculations():
    """Test ROI calculation logic"""
    print("\n" + "=" * 60)
    print("CALCULATION TESTS")
    print("=" * 60)

    from common import MFAPIClient, ROICalculator

    try:
        mfapi = MFAPIClient()
        calculator = ROICalculator(mfapi)
        test_passed("Calculator initialized")
    except Exception as e:
        test_failed("Calculator init", str(e))
        return

    # Test 1: ROI calculation formula
    try:
        # Test CAGR: 100 -> 200 in 3 years = 26%
        roi = calculator.calculate_roi(200, 100, 3, annualize=True)
        expected = 25.99  # (200/100)^(1/3) - 1 = 0.2599
        if abs(roi - expected) < 1:
            test_passed(f"CAGR calculation: {roi:.2f}% (expected ~{expected}%)")
        else:
            test_failed("CAGR calculation", f"Got {roi:.2f}%, expected ~{expected}%")
    except Exception as e:
        test_failed("CAGR calculation", str(e))

    # Test 2: Fetch and calculate for a known fund
    try:
        nav_data = mfapi.get_fund_nav(118989)  # HDFC Mid-Cap
        if nav_data:
            result = calculator.calculate_fund_returns(118989)
            if result and result.get("roi_3y") is not None:
                test_passed(f"Fund returns calculated: 3Y ROI = {result['roi_3y']:.2f}%")
            else:
                test_failed("Fund returns", "Could not calculate returns")
        else:
            test_failed("Fund returns", "Could not fetch NAV data")
    except Exception as e:
        test_failed("Fund returns calculation", str(e))

    # Test 3: find_nav_for_date with exact=True requires exact match
    try:
        from datetime import datetime, timedelta

        # Fetch a fund with data
        nav_data = mfapi.get_fund_nav(118989)
        if nav_data and nav_data.get('data'):
            # Find an exact date that exists in the data
            exact_date_str = nav_data['data'][5]['date']  # Pick a date that exists
            exact_date = datetime.strptime(exact_date_str, '%d-%m-%Y')

            # Test exact=True - should return exact match only
            nav, found_date = mfapi.find_nav_for_date(118989, exact_date, exact=True)

            if found_date and found_date == exact_date:
                test_passed(f"find_nav_for_date exact=True returns exact match: {exact_date.date()}")
            elif found_date:
                test_failed("find_nav_for_date exact=True", f"Expected {exact_date.date()}, got {found_date.date()}")
            else:
                test_failed("find_nav_for_date exact=True", "No date found")

            # Test exact=True with non-existent date (weekend) - should return None
            # Find a Saturday
            test_date = exact_date
            while test_date.weekday() != 5:  # 5 = Saturday
                test_date += timedelta(days=1)
            nav_weekend, date_weekend = mfapi.find_nav_for_date(118989, test_date, exact=True)
            if nav_weekend is None:
                test_passed(f"find_nav_for_date exact=True returns None for weekend: {test_date.date()}")
            else:
                test_failed("find_nav_for_date exact=True", f"Should return None for weekend, got {date_weekend}")

    except Exception as e:
        test_failed("find_nav_for_date regression test", str(e))


# ==================== DATA VALIDATION TESTS ====================

def test_data_validation():
    """Validate data accuracy for financial decisions"""
    print("\n" + "=" * 60)
    print("DATA VALIDATION TESTS")
    print("=" * 60)

    from datetime import timedelta
    import requests
    from common import SupabaseDB, MFAPIClient, ROICalculator

    db = SupabaseDB()
    mfapi = MFAPIClient()
    calculator = ROICalculator(mfapi)

    # Test 1: MFAPI data source accessible
    try:
        response = requests.get("https://api.mfapi.in/mf", timeout=10)
        if response.status_code == 200:
            data = response.json()
            if len(data) > 1000:
                test_passed(f"MFAPI accessible: {len(data)} schemes")
            else:
                test_failed("MFAPI data count", f"Only {len(data)} schemes")
        else:
            test_failed("MFAPI accessibility", f"Status {response.status_code}")
    except Exception as e:
        test_failed("MFAPI connectivity", str(e))

    # Test 2: CAGR formula mathematically correct
    try:
        # 100 -> 200 in 3 years = 25.99% CAGR
        roi = calculator.calculate_roi(200, 100, 3, annualize=True)
        expected = 25.99
        if abs(roi - expected) < 0.1:
            test_passed(f"CAGR formula: 100→200 in 3Y = {roi:.2f}%")
        else:
            test_failed("CAGR formula", f"Got {roi:.2f}%, expected {expected}%")
    except Exception as e:
        test_failed("CAGR formula", str(e))

    # Test 3: Real fund CAGR matches manual calculation
    try:
        scheme_code = 118989  # HDFC Mid-Cap
        nav_data = mfapi.get_fund_nav(scheme_code)

        if nav_data and nav_data.get("data"):
            navs = nav_data["data"]
            latest = navs[0]
            latest_date = datetime.strptime(latest["date"], "%d-%m-%Y")
            latest_nav = float(latest["nav"])

            # Find NAV from ~3 years ago
            target_date = latest_date - timedelta(days=365 * 3)
            old_nav = None
            old_date = None
            for nav in navs:
                nav_date = datetime.strptime(nav["date"], "%d-%m-%Y")
                if nav_date <= target_date:
                    old_nav = float(nav["nav"])
                    old_date = nav_date
                    break

            if old_nav:
                years = (latest_date - old_date).days / 365.25
                manual_cagr = ((latest_nav / old_nav) ** (1 / years) - 1) * 100
                result = calculator.calculate_fund_returns(scheme_code, latest_date)
                calc_cagr = result.get("roi_3y") if result else None

                # Allow 1.5% tolerance for date matching
                if calc_cagr and abs(manual_cagr - calc_cagr) < 1.5:
                    test_passed(f"Real fund CAGR: Manual={manual_cagr:.2f}%, Calc={calc_cagr:.2f}%")
                else:
                    test_failed("Real fund CAGR", f"Manual={manual_cagr:.2f}%, Calc={calc_cagr}")
            else:
                test_failed("Real fund CAGR", "Could not find 3Y old NAV")
    except Exception as e:
        test_failed("Real fund CAGR", str(e))

    # Test 4: Cross-validate top funds with fresh API data
    try:
        latest_result = db.client.table("mutual_fund_returns").select("report_date").order("report_date", desc=True).limit(1).execute()
        latest_date = latest_result.data[0]["report_date"] if latest_result.data else None

        if latest_date:
            top_result = db.client.table("mutual_fund_returns").select("fund_id, roi_3y").eq("report_date", latest_date).order("roi_3y", desc=True).limit(5).execute()

            validated = 0
            for r in top_result.data:
                fund = db.client.table("mutual_funds").select("scheme_code").eq("id", r["fund_id"]).execute()
                if not fund.data or not fund.data[0].get("scheme_code"):
                    continue

                scheme_code = fund.data[0]["scheme_code"]
                target_dt = datetime.strptime(latest_date, "%Y-%m-%d")
                fresh_result = calculator.calculate_fund_returns(scheme_code, target_dt)
                fresh_roi = fresh_result.get("roi_3y") if fresh_result else None

                if fresh_roi and abs(r["roi_3y"] - fresh_roi) < 1.5:
                    validated += 1

            if validated >= 3:
                test_passed(f"Cross-validation: {validated}/5 top funds match fresh API data")
            else:
                test_failed("Cross-validation", f"Only {validated}/5 funds validated")
        else:
            test_failed("Cross-validation", "No data in database")
    except Exception as e:
        test_failed("Cross-validation", str(e))

    # Test 5: Top 200 ranking is correctly sorted
    try:
        latest_result = db.client.table("mutual_fund_returns").select("report_date").order("report_date", desc=True).limit(1).execute()
        latest_date = latest_result.data[0]["report_date"] if latest_result.data else None

        if latest_date:
            all_result = db.client.table("mutual_fund_returns").select("roi_3y").eq("report_date", latest_date).not_.is_("roi_3y", "null").order("roi_3y", desc=True).execute()

            if len(all_result.data) >= 10:
                # Verify first 10 are in descending order
                is_sorted = all(all_result.data[i]["roi_3y"] >= all_result.data[i + 1]["roi_3y"] for i in range(9))
                if is_sorted:
                    test_passed(f"Ranking sorted correctly ({len(all_result.data)} funds)")
                else:
                    test_failed("Ranking", "Funds not in descending ROI order")
            else:
                test_failed("Ranking", "Insufficient funds to verify")
    except Exception as e:
        test_failed("Ranking validation", str(e))

    # Test 6: Data freshness
    try:
        latest_result = db.client.table("mutual_fund_returns").select("report_date").order("report_date", desc=True).limit(1).execute()
        latest_date = latest_result.data[0]["report_date"] if latest_result.data else None

        if latest_date:
            latest_dt = datetime.strptime(latest_date, "%Y-%m-%d")
            days_old = (datetime.now() - latest_dt).days

            if days_old <= 7:
                test_passed(f"Data freshness: {latest_date} ({days_old} days old)")
            else:
                test_failed("Data freshness", f"Data is {days_old} days old")
        else:
            test_failed("Data freshness", "No data in database")
    except Exception as e:
        test_failed("Data freshness", str(e))

    # Test 7: No impossible ROI values
    try:
        result = db.client.table("mutual_fund_returns").select("roi_3y").not_.is_("roi_3y", "null").execute()
        rois = [r["roi_3y"] for r in result.data]

        impossible = [r for r in rois if r > 200 or r < -80]
        if len(impossible) == 0:
            test_passed(f"No impossible ROI values (checked {len(rois)} records)")
        else:
            test_failed("Impossible ROI", f"{len(impossible)} values outside -80% to 200%")
    except Exception as e:
        test_failed("Impossible ROI check", str(e))


# ==================== INFRASTRUCTURE TESTS ====================

def test_infrastructure():
    """Test cron job and cache configuration"""
    print("\n" + "=" * 60)
    print("INFRASTRUCTURE TESTS")
    print("=" * 60)

    import os
    import subprocess
    import requests
    from common import SupabaseDB

    # Test 1: Daily fetch script uses --refresh flag
    try:
        script_path = "/Users/zop7782/mf_scraper/daily_fetch.sh"
        if os.path.exists(script_path):
            with open(script_path, 'r') as f:
                content = f.read()
            if '--refresh' in content:
                test_passed("daily_fetch.sh uses --refresh flag (avoids stale cache)")
            else:
                test_failed("daily_fetch.sh", "Missing --refresh flag - will use stale cache!")
        else:
            test_failed("daily_fetch.sh", "Script not found")
    except Exception as e:
        test_failed("daily_fetch.sh check", str(e))

    # Test 2: Daily fetch script has correct PATH for launchd
    try:
        script_path = "/Users/zop7782/mf_scraper/daily_fetch.sh"
        if os.path.exists(script_path):
            with open(script_path, 'r') as f:
                content = f.read()
            if 'export PATH=' in content:
                test_passed("daily_fetch.sh sets PATH for launchd")
            else:
                test_failed("daily_fetch.sh", "Missing PATH export - will fail in launchd!")
        else:
            test_failed("daily_fetch.sh", "Script not found")
    except Exception as e:
        test_failed("daily_fetch.sh PATH check", str(e))

    # Test 3: MFAPI has recent data (not stale)
    try:
        response = requests.get("https://api.mfapi.in/mf/120503", timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get('data'):
                latest_nav_date = data['data'][0]['date']
                from datetime import datetime
                nav_date = datetime.strptime(latest_nav_date, '%d-%m-%Y')
                days_old = (datetime.now() - nav_date).days
                if days_old <= 3:
                    test_passed(f"MFAPI has recent data: {latest_nav_date} ({days_old} days old)")
                else:
                    test_failed("MFAPI freshness", f"Latest NAV is {days_old} days old: {latest_nav_date}")
            else:
                test_failed("MFAPI data", "No NAV data in response")
        else:
            test_failed("MFAPI request", f"Status {response.status_code}")
    except Exception as e:
        test_failed("MFAPI freshness check", str(e))

    # Test 4: Database latest date matches MFAPI (within 2 days)
    try:
        db = SupabaseDB()
        latest_result = db.client.table("mutual_fund_returns").select("report_date").order("report_date", desc=True).limit(1).execute()
        db_date = latest_result.data[0]["report_date"] if latest_result.data else None

        response = requests.get("https://api.mfapi.in/mf/120503", timeout=30)
        if response.status_code == 200 and db_date:
            data = response.json()
            if data.get('data'):
                from datetime import datetime
                api_date_str = data['data'][0]['date']
                api_date = datetime.strptime(api_date_str, '%d-%m-%Y')
                db_date_dt = datetime.strptime(db_date, '%Y-%m-%d')
                diff_days = abs((api_date - db_date_dt).days)

                if diff_days <= 2:
                    test_passed(f"DB date ({db_date}) matches MFAPI ({api_date_str}) within 2 days")
                else:
                    test_failed("DB vs MFAPI sync", f"DB has {db_date}, MFAPI has {api_date_str} ({diff_days} days diff)")
    except Exception as e:
        test_failed("DB vs MFAPI sync check", str(e))

    # Test 5: Launchd job is loaded
    try:
        result = subprocess.run(['launchctl', 'list'], capture_output=True, text=True)
        if 'com.mfscraper.dailyfetch' in result.stdout:
            test_passed("Launchd job com.mfscraper.dailyfetch is loaded")
        else:
            test_failed("Launchd job", "com.mfscraper.dailyfetch not loaded")
    except Exception as e:
        test_failed("Launchd check", str(e))

    # Test 6: Top 200 fund coverage for display dates
    try:
        db = SupabaseDB()

        # Get latest date and top 200 fund IDs
        latest = db.client.table("mutual_fund_returns").select("report_date").order("report_date", desc=True).limit(1).execute()
        latest_date = latest.data[0]["report_date"]

        top = db.client.table("mutual_fund_returns").select("fund_id").eq("report_date", latest_date).order("roi_3y", desc=True).limit(200).execute()
        top_fund_ids = [r["fund_id"] for r in top.data]

        # Get last 10 significant dates
        sig_dates = sorted(db.get_significant_change_dates("SENSEX"), reverse=True)[:10]

        incomplete = []
        for date in sig_dates:
            result = db.client.table("mutual_fund_returns").select("fund_id").in_("fund_id", top_fund_ids).eq("report_date", date).execute()
            if len(result.data) < 180:  # Less than 90%
                incomplete.append((date, len(result.data)))

        if not incomplete:
            test_passed(f"Top 200 funds have data for all {len(sig_dates)} display dates")
        else:
            test_failed("Top 200 coverage", f"Incomplete dates: {incomplete[:3]}")
    except Exception as e:
        test_failed("Top 200 coverage check", str(e))

    # Test 7: Watchlist funds have data for latest date
    try:
        db = SupabaseDB()

        # Get latest date
        latest = db.client.table("mutual_fund_returns").select("report_date").order("report_date", desc=True).limit(1).execute()
        latest_date = latest.data[0]["report_date"]

        # Get watchlist funds
        watchlist = db.get_watchlist()

        if watchlist:
            missing = []
            for w in watchlist:
                result = db.client.table("mutual_fund_returns").select("roi_3y").eq("fund_id", w["fund_id"]).eq("report_date", latest_date).execute()
                if not result.data or result.data[0].get("roi_3y") is None:
                    missing.append(w["fund_name"][:30])

            if not missing:
                test_passed(f"All {len(watchlist)} watchlist funds have data for {latest_date}")
            else:
                test_failed("Watchlist latest date", f"Missing: {missing}")
        else:
            test_passed("No watchlist funds to check")
    except Exception as e:
        test_failed("Watchlist latest date check", str(e))

    # Test 8: bulk_scraper.py includes watchlist funds
    try:
        script_path = "/Users/zop7782/mf_scraper/bulk_scraper.py"
        if os.path.exists(script_path):
            with open(script_path, 'r') as f:
                content = f.read()
            if 'get_watchlist' in content and 'watchlist' in content.lower():
                test_passed("bulk_scraper.py includes watchlist fund handling")
            else:
                test_failed("bulk_scraper.py", "Missing watchlist fund handling!")
        else:
            test_failed("bulk_scraper.py", "Script not found")
    except Exception as e:
        test_failed("bulk_scraper.py watchlist check", str(e))


# ==================== MAIN ====================

def main():
    print("\n" + "=" * 60)
    print("  MF SCRAPER TEST SUITE")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    start_time = time.time()

    # Run all tests
    test_database()
    test_data_quality()
    test_api()
    test_calculations()
    test_data_validation()
    test_infrastructure()
    test_ui()

    # Summary
    elapsed = time.time() - start_time
    total = results["passed"] + results["failed"]

    print("\n" + "=" * 60)
    print("  TEST SUMMARY")
    print("=" * 60)
    print(f"  Passed: {results['passed']}/{total}")
    print(f"  Failed: {results['failed']}/{total}")
    print(f"  Time: {elapsed:.1f}s")

    if results["errors"]:
        print("\n  FAILURES:")
        for error in results["errors"]:
            print(f"    - {error}")

    print("=" * 60)

    return results["failed"] == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
