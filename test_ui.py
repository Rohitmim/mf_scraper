#!/usr/bin/env python3
"""Playwright test for Streamlit UI"""

from playwright.sync_api import sync_playwright
import time

def test_streamlit_ui():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print("1. Loading page...")
        page.goto("http://localhost:8501", timeout=60000)

        # Wait for Streamlit to load
        print("2. Waiting for Streamlit to initialize...")
        page.wait_for_selector("div[data-testid='stAppViewContainer']", timeout=30000)

        # Wait for content to load (data tables take time)
        print("3. Waiting for data to load...")
        time.sleep(20)  # Give time for data fetch from Supabase

        # Check page title
        title = page.title()
        print(f"4. Page title: {title}")

        # Take screenshot
        screenshot_path = "/Users/zop7782/mf_scraper/test_screenshot.png"
        page.screenshot(path=screenshot_path, full_page=True)
        print(f"5. Screenshot saved: {screenshot_path}")

        # Scroll table to show date columns
        try:
            table = page.query_selector("div[data-testid='stDataFrame']")
            if table:
                table.evaluate("el => el.scrollLeft = 500")
                time.sleep(1)
                page.screenshot(path="/Users/zop7782/mf_scraper/test_screenshot_scrolled.png", full_page=True)
                print("5b. Scrolled screenshot saved")
        except:
            pass

        # Click on Market Changes tab and take screenshot
        try:
            tabs = page.query_selector_all("button[data-baseweb='tab']")
            if len(tabs) >= 2:
                tabs[1].click()  # Click second tab (Market Changes)
                time.sleep(3)
                page.screenshot(path="/Users/zop7782/mf_scraper/test_screenshot_market.png", full_page=True)
                print("5c. Market Changes tab screenshot saved")

                # Go back to first tab
                tabs[0].click()
                time.sleep(2)
        except:
            pass

        # Click on a row in the dataframe to test popup
        try:
            # Streamlit dataframe uses glide-data-grid, need special interaction
            dataframe = page.query_selector("div[data-testid='stDataFrame']")
            if dataframe:
                # Click to focus the grid first
                dataframe.click()
                time.sleep(0.5)

                # Find the canvas and click on a row
                canvas = page.query_selector("div[data-testid='stDataFrame'] canvas")
                if canvas:
                    box = canvas.bounding_box()
                    if box:
                        # Click directly on checkbox area of first row
                        x = box['x'] + 15
                        y = box['y'] + 55
                        page.mouse.click(x, y)
                        print(f"   Clicked at ({x}, {y})")
                        time.sleep(5)

                        # Take screenshot
                        page.screenshot(path="/Users/zop7782/mf_scraper/test_screenshot_popup.png", full_page=True)
                        print("5d. Popup screenshot saved")

                        # Find and scroll to expander
                        expander = page.query_selector("div[data-testid='stExpander']")
                        if expander:
                            expander.scroll_into_view_if_needed()
                            time.sleep(1)
                            page.screenshot(path="/Users/zop7782/mf_scraper/test_screenshot_popup2.png", full_page=True)
                            print("5e. Expander found and screenshot saved")
                        else:
                            # Try scrolling main content
                            page.evaluate("document.querySelector('section.main').scrollTo(0, 10000)")
                            time.sleep(1)
                            page.screenshot(path="/Users/zop7782/mf_scraper/test_screenshot_popup2.png", full_page=True)
                            print("5e. Scrolled main content, expander not found directly")
        except Exception as e:
            print(f"5d. Could not test popup: {e}")

        # Check for errors in the page
        error_elements = page.query_selector_all("div[data-testid='stException']")
        if error_elements:
            print("6. ERRORS FOUND:")
            for el in error_elements:
                print(f"   - {el.inner_text()[:200]}")
        else:
            print("6. No errors found on page")

        # Check for data table (Streamlit uses different selectors)
        tables = page.query_selector_all("div[data-testid='stDataFrame']")
        tables2 = page.query_selector_all("table")
        tables3 = page.query_selector_all("div[class*='dataframe']")
        print(f"7. Data tables found: stDataFrame={len(tables)}, table={len(tables2)}, dataframe={len(tables3)}")

        # Check for specific content
        page_content = page.content()

        # Check for key columns
        columns_to_check = ["fund_name", "Trend", "Count>50%", "Min6M", "Max6M", "Slope", "Count%", "Today%", "DipMax", "Multiplier"]
        found_columns = []
        for col in columns_to_check:
            if col in page_content:
                found_columns.append(col)

        print(f"8. Columns found: {found_columns}")

        # Check if we have data rows (look for fund names pattern)
        if "Quant" in page_content or "ICICI" in page_content or "HDFC" in page_content:
            print("9. Fund data is loading - PASS")
        else:
            print("9. No fund data visible - might still be loading")

        browser.close()
        print("\n✓ Test completed!")
        return True

def test_watchlist_add():
    """Test adding Motilal Oswal Midcap to watchlist"""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print("=" * 50)
        print("WATCHLIST TEST: Motilal Oswal Midcap Fund")
        print("=" * 50)

        print("\n1. Loading page...")
        page.goto("http://localhost:8501", timeout=60000)

        # Wait for Streamlit to load
        print("2. Waiting for Streamlit to initialize...")
        page.wait_for_selector("div[data-testid='stAppViewContainer']", timeout=30000)
        time.sleep(10)  # Wait for data to load

        # Take initial screenshot
        page.screenshot(path="/Users/zop7782/mf_scraper/test_watchlist_1_initial.png", full_page=True)
        print("3. Initial screenshot saved")

        # Find the watchlist search in sidebar
        print("4. Looking for watchlist search input...")
        try:
            # Find sidebar
            sidebar = page.query_selector("section[data-testid='stSidebar']")
            if sidebar:
                print("   Sidebar found")

                # Find text inputs - the search input for watchlist
                text_inputs = sidebar.query_selector_all("input[type='text']")
                print(f"   Found {len(text_inputs)} text inputs in sidebar")

                # Find the search input (should be the second one after "Search Fund Name")
                search_input = None
                for inp in text_inputs:
                    placeholder = inp.get_attribute("placeholder") or ""
                    if "fund name" in placeholder.lower() or "search" in placeholder.lower():
                        search_input = inp
                        break

                if not search_input and len(text_inputs) >= 2:
                    search_input = text_inputs[1]  # Second text input

                if search_input:
                    print("5. Found search input, typing 'motilal midcap'...")
                    search_input.click()
                    time.sleep(0.5)
                    search_input.fill("motilal midcap")
                    page.keyboard.press("Enter")  # Press Enter to apply search
                    time.sleep(5)  # Wait for search results to load
                    page.screenshot(path="/Users/zop7782/mf_scraper/test_watchlist_2_search.png", full_page=True)
                    print("   Search results screenshot saved")

                    # Now find the selectbox that appeared with results
                    print("6. Looking for fund selectbox...")
                    time.sleep(1)
                    selectboxes = sidebar.query_selector_all("div[data-testid='stSelectbox']")
                    print(f"   Found {len(selectboxes)} selectboxes after search")

                    if len(selectboxes) >= 2:
                        # The second selectbox should be the fund selection
                        fund_select = selectboxes[1]
                        fund_select.click()
                        time.sleep(1)
                        page.screenshot(path="/Users/zop7782/mf_scraper/test_watchlist_3_dropdown.png", full_page=True)
                        print("   Dropdown screenshot saved")

                        # Select first option
                        page.keyboard.press("ArrowDown")
                        time.sleep(0.3)
                        page.keyboard.press("Enter")
                        time.sleep(1)
                        page.screenshot(path="/Users/zop7782/mf_scraper/test_watchlist_4_selected.png", full_page=True)
                        print("7. Selected fund screenshot saved")

                    # Find and click "Add to Watchlist" button
                    print("8. Looking for Add to Watchlist button...")
                    # Try multiple selectors for the button
                    add_button = sidebar.query_selector("button:has-text('Add to Watchlist')")
                    if not add_button:
                        # Try finding by partial text
                        buttons = sidebar.query_selector_all("button")
                        for btn in buttons:
                            text = btn.inner_text()
                            if "Add" in text and "Watchlist" in text:
                                add_button = btn
                                break
                            if "➕" in text:
                                add_button = btn
                                break

                    if add_button:
                        print("   Found Add to Watchlist button, clicking...")
                        add_button.click()
                        print("9. Waiting for performance data fetch (this may take 10-20 seconds)...")
                        time.sleep(25)  # Wait for API fetch
                        page.screenshot(path="/Users/zop7782/mf_scraper/test_watchlist_5_added.png", full_page=True)
                        print("   Added fund screenshot saved")

                        # Check if fund appears in watchlist
                        page_content = page.content()
                        if "Motilal" in page_content:
                            print("10. SUCCESS: Motilal Oswal fund found on page!")
                        else:
                            print("10. Fund name not visible on page yet")

                        # Check for success/warning message
                        if "Added with data" in page_content:
                            print("11. SUCCESS MESSAGE: 'Added with data!' found")
                        elif "no 3Y data" in page_content:
                            print("11. WARNING: Fund added but no 3Y data available")
                        elif "Added!" in page_content:
                            print("11. SUCCESS: 'Added!' message found")

                        # Take final screenshot of the table
                        time.sleep(3)
                        page.screenshot(path="/Users/zop7782/mf_scraper/test_watchlist_6_final.png", full_page=True)
                        print("12. Final screenshot saved")

                    else:
                        print("   ERROR: Add to Watchlist button not found")
                else:
                    print("   ERROR: Could not find search input")
            else:
                print("   ERROR: Sidebar not found")

        except Exception as e:
            print(f"ERROR: {e}")
            page.screenshot(path="/Users/zop7782/mf_scraper/test_watchlist_error.png", full_page=True)

        browser.close()
        print("\n" + "=" * 50)
        print("Watchlist test completed!")
        print("=" * 50)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "watchlist":
        test_watchlist_add()
    else:
        test_streamlit_ui()
