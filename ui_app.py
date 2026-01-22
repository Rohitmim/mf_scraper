#!/usr/bin/env python3
"""
MF Scaled ROI Viewer - Streamlit UI
Displays mutual fund performance scaled relative to best performer on each date
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone
import subprocess
import os

from common import SupabaseDB, MFAPIClient

# Page config
st.set_page_config(
    page_title="MF ROI Comparison",
    page_icon="📈",
    layout="wide"
)

def get_ist_today():
    """Get today's date in IST (UTC+5:30)"""
    ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(ist).strftime("%Y-%m-%d")

def check_and_fetch_data():
    """
    Check if fresh data is needed based on latest date in mutual_fund_returns.
    - If today (IST) == latest data date → no fetch needed
    - If today (IST) != latest data date → fetch top 200 MFs
    """
    try:
        db = SupabaseDB()

        # Get latest date from mutual_fund_returns table (this is the "header date")
        latest_result = db.client.table("mutual_fund_returns").select("report_date").order("report_date", desc=True).limit(1).execute()
        data_date = latest_result.data[0]["report_date"] if latest_result.data else None

        # Get today's date in IST
        ist_today = get_ist_today()

        if data_date and data_date == ist_today:
            return data_date, False  # Already have today's data, no fetch needed

        # Need to fetch fresh data (different day or no data)
        return data_date, True
    except Exception as e:
        # On error (e.g., SSL issues), skip fresh data check
        return None, False

def fetch_fresh_data():
    """Fetch fresh data using bulk_scraper (top 200 by 3Y ROI)"""
    ist_today = get_ist_today()
    script_path = os.path.join(os.path.dirname(__file__), "bulk_scraper.py")

    # Run bulk_scraper for today's date (fetches top 200 by 3Y ROI)
    env = os.environ.copy()
    result = subprocess.run(
        ["python3", script_path, "-d", ist_today],
        capture_output=True,
        text=True,
        env=env,
        cwd=os.path.dirname(__file__),
        timeout=300  # 5 minute timeout
    )

    return result.returncode == 0

# Focus extraction keywords (priority ordered)
FOCUS_KEYWORDS = [
    ("Infra", ["infrastructure", "infra"]),
    ("Banking", ["banking", "bank"]),
    ("Financial", ["financial", "finance"]),
    ("Pharma", ["pharma", "healthcare"]),
    ("IT", ["technology", "tech", "digital"]),
    ("FMCG", ["fmcg", "consumption", "consumer"]),
    ("Auto", ["auto", "automobile"]),
    ("Energy", ["energy", "power"]),
    ("Realty", ["realty", "real estate", "housing"]),
    ("Metal", ["metal", "steel"]),
    ("Manufacturing", ["manufacturing"]),
    ("PSU", ["psu", "public sector"]),
    ("MNC", ["mnc"]),
    ("Gold", ["gold"]),
    ("Silver", ["silver"]),
    ("Large Cap", ["large cap", "largecap", "bluechip"]),
    ("Mid Cap", ["mid cap", "midcap"]),
    ("Small Cap", ["small cap", "smallcap"]),
    ("Micro Cap", ["micro cap", "microcap"]),
    ("Multi Cap", ["multi cap", "multicap"]),
    ("Flexi Cap", ["flexi cap", "flexicap"]),
    ("Contra", ["contra", "contrarian"]),
    ("Value", ["value"]),
    ("Focused", ["focused"]),
    ("Momentum", ["momentum"]),
    ("Quant", ["quant"]),
    ("ESG", ["esg", "sustainable"]),
    ("Balanced", ["balanced", "hybrid"]),
    ("Arbitrage", ["arbitrage"]),
    ("Tax Saver", ["tax", "elss"]),
    ("Nifty 50", ["nifty 50", "nifty50"]),
    ("Index", ["index"]),
    ("US", ["us ", "usa", "nasdaq"]),
    ("Global", ["global", "international", "world"]),
]

def extract_focus(fund_name):
    """Extract the primary focus/theme from a fund name"""
    name_lower = fund_name.lower()
    for focus, keywords in FOCUS_KEYWORDS:
        for keyword in keywords:
            if keyword in name_lower:
                return focus
    return "Equity"

@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_data():
    """Load scaled ROI data from database - optimized"""
    db = SupabaseDB()

    # Get ALL significant change dates (>0.5% SENSEX movement) for calculations
    all_sig_dates = db.get_significant_change_dates("SENSEX")
    significant_dates_set = set(all_sig_dates)

    # Get the actual latest date in the database (today's data)
    latest_result = db.client.table("mutual_fund_returns").select("report_date").order("report_date", desc=True).limit(1).execute()
    today_date = latest_result.data[0]["report_date"] if latest_result.data else None

    # Get today's top 200 funds based on latest available date
    top_result = db.client.table("mutual_fund_returns").select("fund_id, roi_3y").eq("report_date", today_date).order("roi_3y", desc=True).limit(200).execute()
    top_fund_ids = set(r["fund_id"] for r in top_result.data)
    today_roi_map = {r["fund_id"]: r["roi_3y"] for r in top_result.data}

    # Get watchlist funds (always include these)
    watchlist = db.get_watchlist()
    watchlist_fund_ids = set(w["fund_id"] for w in watchlist)

    # Combine top 200 + watchlist fund IDs
    all_fund_ids = list(top_fund_ids | watchlist_fund_ids)

    # Get funds for all (top 200 + watchlist)
    funds_result = db.client.table("mutual_funds").select("*").in_("id", all_fund_ids).execute()
    funds_map = {f["id"]: f for f in funds_result.data}

    # Get dates that exist in returns (optimized - just get unique dates first)
    dates_result = db.client.table("mutual_fund_returns").select("report_date").in_("fund_id", all_fund_ids).execute()
    dates_in_returns = set(r["report_date"] for r in dates_result.data)

    # Filter to only significant dates that exist in returns
    # Today is ALWAYS included (whether or not it's a significant SENSEX date)
    # Using set ensures no duplicates - if today is also a significant date, it appears only once
    valid_dates = (significant_dates_set & dates_in_returns)
    if today_date:
        valid_dates.add(today_date)  # No-op if already in set (today is significant)

    # Sort and get dates we need
    sorted_valid_dates = sorted(valid_dates, reverse=True)
    display_dates = set(sorted_valid_dates[:20])  # Last 20 for display

    # For calculations, limit to last 6 months + display dates (not ALL dates)
    from datetime import datetime, timedelta
    six_months_ago = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
    calc_dates = set(d for d in sorted_valid_dates if d >= six_months_ago) | display_dates
    all_dates = calc_dates

    # Fetch returns ONLY for the dates we need
    # Note: Fetch per date to avoid Supabase pagination issues with in_() filters
    calc_data = []
    for date in all_dates:
        batch = db.client.table("mutual_fund_returns").select("fund_id,report_date,roi_3y").in_("fund_id", all_fund_ids).eq("report_date", date).execute().data
        calc_data.extend(batch)

    # Filter display data from calc_data
    returns = [r for r in calc_data if r["report_date"] in display_dates]

    # Build max ROI per date from ALL dates (exclude "Other Scheme - FoF Overseas" from being 100%)
    max_roi_per_date = {}
    for r in calc_data:
        fund = funds_map.get(r["fund_id"])
        if not fund:
            continue
        # Skip FoF Overseas when calculating max
        if fund.get("category") == "Other Scheme - FoF Overseas":
            continue
        date = r["report_date"]
        roi = r.get("roi_3y")
        if roi is not None:
            max_roi_per_date[date] = max(max_roi_per_date.get(date, 0), roi)

    # Calculate scaled ROI for ALL dates (for stats calculation)
    all_scaled = {}  # {fund_id: {date: scaled_roi}}
    for r in calc_data:
        fund_id = r["fund_id"]
        date = r["report_date"]
        roi = r.get("roi_3y")
        max_roi = max_roi_per_date.get(date, 1)
        scaled_roi = round((roi / max_roi) * 100, 2) if roi and max_roi else None
        if fund_id not in all_scaled:
            all_scaled[fund_id] = {}
        all_scaled[fund_id][date] = scaled_roi

    # Build display dataframe (only last 20 dates)
    rows = []
    for r in returns:
        fund = funds_map.get(r["fund_id"])
        if not fund:
            continue
        date = r["report_date"]
        roi = r.get("roi_3y")
        max_roi = max_roi_per_date.get(date, 1)
        scaled_roi = round((roi / max_roi) * 100, 2) if roi and max_roi else None

        rows.append({
            "fund_name": fund["fund_name"],
            "fund_id": r["fund_id"],
            "category": fund.get("category", ""),
            "report_date": date,
            "roi_3y": roi,
            "scaled_roi": scaled_roi
        })

    df = pd.DataFrame(rows)

    # Get latest ROI for each fund
    nav_df = None
    if not df.empty:
        latest_date = df["report_date"].max()
        nav_df = df[df["report_date"] == latest_date][["fund_name", "roi_3y"]].copy()
        nav_df.rename(columns={"roi_3y": "today_roi"}, inplace=True)

    # Build fund_id to fund_name mapping for stats
    fund_id_to_name = {f["id"]: f["fund_name"] for f in funds_result.data}

    # Pass all_scaled data for calculations
    calc_info = {
        "all_scaled": all_scaled,
        "all_dates": sorted(all_dates, reverse=True),  # Dates we have data for (6mo + display)
        "fund_id_to_name": fund_id_to_name,
        "data_date": today_date,  # The date of the latest data
        "watchlist_fund_ids": watchlist_fund_ids  # For highlighting
    }

    return df, nav_df, calc_info

@st.cache_data(ttl=300)
def load_market_changes():
    """Load significant market change dates"""
    db = SupabaseDB()
    changes = db.get_significant_changes(index_name="SENSEX")
    return pd.DataFrame(changes)

@st.cache_data(ttl=600)
def fetch_nav_data(scheme_code: int) -> pd.DataFrame:
    """Fetch NAV data for a fund from MFAPI"""
    mfapi = MFAPIClient(cache_max_age_hours=24)
    data = mfapi.get_fund_nav(scheme_code)
    if data and data.get('data'):
        nav_list = data['data']
        df = pd.DataFrame(nav_list)
        df['nav'] = pd.to_numeric(df['nav'], errors='coerce')
        df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
        df = df.sort_values('date')
        return df
    return pd.DataFrame()

def get_nav_comparison_data(fund_infos: list, days: int = 365) -> pd.DataFrame:
    """
    Fetch and normalize NAV data for multiple funds for comparison.

    Args:
        fund_infos: List of dicts with 'scheme_code' and 'fund_name'
        days: Number of days of history to include

    Returns:
        DataFrame with date index and one column per fund (normalized to 100 at start)
    """
    from datetime import datetime, timedelta
    cutoff_date = datetime.now() - timedelta(days=days)

    all_data = {}
    for info in fund_infos:
        scheme_code = info.get('scheme_code')
        fund_name = info.get('fund_name', f'Fund {scheme_code}')

        if not scheme_code:
            continue

        nav_df = fetch_nav_data(scheme_code)
        if nav_df.empty:
            continue

        # Filter to last N days
        nav_df = nav_df[nav_df['date'] >= cutoff_date]
        if nav_df.empty:
            continue

        # Normalize to 100 at the start
        first_nav = nav_df['nav'].iloc[0]
        if first_nav and first_nav > 0:
            nav_df['normalized'] = (nav_df['nav'] / first_nav) * 100
            # Use short name for legend
            short_name = fund_name[:40] + '...' if len(fund_name) > 40 else fund_name
            all_data[short_name] = nav_df.set_index('date')['normalized']

    if not all_data:
        return pd.DataFrame()

    # Combine all series into one DataFrame
    result = pd.DataFrame(all_data)
    return result

def pivot_data(df, nav_data=None, calc_info=None):
    """Pivot data to have dates as columns with summary stats

    Args:
        calc_info: Dict with all_scaled, all_dates, fund_id_to_name for calculations from ALL dates
    """
    if df.empty:
        return df, []

    calc_info = calc_info or {}

    # Extract calc data
    all_scaled = calc_info.get("all_scaled", {})
    all_dates = calc_info.get("all_dates", [])  # Sorted desc
    fund_id_to_name = calc_info.get("fund_id_to_name", {})
    watchlist_fund_ids = calc_info.get("watchlist_fund_ids", set())

    # Build name to fund_id mapping from df
    name_to_fund_id = {}
    if "fund_id" in df.columns:
        for _, row in df[["fund_name", "fund_id"]].drop_duplicates().iterrows():
            name_to_fund_id[row["fund_name"]] = row["fund_id"]

    # All dates sorted chronologically for calculations
    all_dates_chrono = sorted(all_dates)

    # Get last 6 months dates from ALL dates
    from datetime import datetime, timedelta
    six_months_ago = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
    all_recent_dates = [d for d in all_dates_chrono if d >= six_months_ago]

    # Pivot for DISPLAY (only last 20 dates)
    pivot = df.pivot_table(
        index=["fund_name", "category"],
        columns="report_date",
        values="scaled_roi",
        aggfunc="first"
    ).reset_index()

    # Display date columns (last 20)
    date_cols = sorted([c for c in pivot.columns if c not in ["fund_name", "category"]])

    # Round to integers
    for col in date_cols:
        pivot[col] = pivot[col].round(0).astype('Int64')

    # === CALCULATIONS USING ALL DATES ===

    # Sparkline from last 6 months + today
    def get_sparkline(row):
        fund_id = name_to_fund_id.get(row["fund_name"])
        if fund_id and fund_id in all_scaled:
            fund_data = all_scaled[fund_id]
            # Use last 6 months dates (already includes today if within 6 months)
            values = [fund_data.get(d) for d in all_recent_dates]
            return [v if v is not None else 0 for v in values]
        return [0] * len(all_recent_dates)

    pivot["Trend"] = pivot.apply(get_sparkline, axis=1)

    # Count>50% from ALL dates
    def count_above_50(row):
        fund_id = name_to_fund_id.get(row["fund_name"])
        if fund_id and fund_id in all_scaled:
            fund_data = all_scaled[fund_id]
            values = [fund_data.get(d) for d in all_dates_chrono if fund_data.get(d) is not None]
            return sum(1 for v in values if v >= 50)
        return 0

    pivot["Count>50%"] = pivot.apply(count_above_50, axis=1)

    # Today's percentile (latest date from display)
    latest_date = date_cols[-1] if date_cols else None
    if latest_date:
        pivot["Today%"] = pivot[latest_date]

    # Min/Max in last 6 months from ALL dates
    def get_min_6m(row):
        fund_id = name_to_fund_id.get(row["fund_name"])
        if fund_id and fund_id in all_scaled:
            fund_data = all_scaled[fund_id]
            values = [fund_data.get(d) for d in all_recent_dates if fund_data.get(d) is not None]
            return min(values) if values else None
        return None

    def get_max_6m(row):
        fund_id = name_to_fund_id.get(row["fund_name"])
        if fund_id and fund_id in all_scaled:
            fund_data = all_scaled[fund_id]
            values = [fund_data.get(d) for d in all_recent_dates if fund_data.get(d) is not None]
            return max(values) if values else None
        return None

    pivot["Min6M"] = pivot.apply(get_min_6m, axis=1)
    pivot["Max6M"] = pivot.apply(get_max_6m, axis=1)

    # Get today's ROI if available
    if nav_data is not None and "today_roi" in nav_data.columns:
        pivot = pivot.merge(nav_data[["fund_name", "today_roi"]], on="fund_name", how="left")
        pivot.rename(columns={"today_roi": "TodayROI"}, inplace=True)
    else:
        pivot["TodayROI"] = None

    # Slope from ALL dates (last 3)
    def calc_slope(row):
        fund_id = name_to_fund_id.get(row["fund_name"])
        if fund_id and fund_id in all_scaled and len(all_dates_chrono) >= 3:
            fund_data = all_scaled[fund_id]
            # Last 3 dates chronologically
            last3 = all_dates_chrono[-3:]
            vals = [fund_data.get(d) for d in last3]
            if any(v is None for v in vals):
                return 0
            # vals[2] is latest, vals[1] is 2nd, vals[0] is 3rd
            if vals[2] > vals[1] > vals[0]:
                return 1  # Uptrend
            elif vals[2] < vals[1] < vals[0]:
                return -1  # Downtrend
        return 0

    pivot["Slope"] = pivot.apply(calc_slope, axis=1)

    # Col K - Sell Advice
    pivot["Sell"] = pivot["Slope"].apply(lambda x: "Sell" if x == -1 else "")

    # Col L - Dip from Max (Today% - Max6M, negative = below max, positive = above max)
    pivot["DipMax"] = pivot["Today%"] - pivot["Max6M"]

    # Col N - Count % (Count>50% / total ALL dates as %)
    total_all_dates = len(all_dates_chrono)
    pivot["Count%"] = (pivot["Count>50%"] / total_all_dates * 100).round(0).astype('Int64') if total_all_dates > 0 else 0

    # Col L - Buy Advice: slope=1, count%>75, dip<0 (meaning today > max, very strong)
    def calc_buy(row):
        if row["Slope"] == 1 and row["Count%"] is not None and row["Count%"] > 75 and row["DipMax"] is not None and row["DipMax"] < 0:
            return "Buy"
        return ""

    pivot["Buy"] = pivot.apply(calc_buy, axis=1)

    # Col N - Multiplier: DipMax × Count% (both are %, so divide by 100)
    pivot["Multiplier"] = ((pivot["DipMax"] * pivot["Count%"]) / 100).round(0).astype('Int64')

    # Add Focus column - extract key theme from fund name
    pivot["Focus"] = pivot["fund_name"].apply(extract_focus)

    # Mark watchlist funds with ⭐ prefix
    def mark_watchlist(row):
        fund_id = name_to_fund_id.get(row["fund_name"])
        if fund_id and fund_id in watchlist_fund_ids:
            return "⭐ " + row["fund_name"]
        return row["fund_name"]

    pivot["fund_name"] = pivot.apply(mark_watchlist, axis=1)

    # Sort by Multiplier ascending (default sort)
    pivot = pivot.sort_values("Multiplier", ascending=True, na_position="last")

    # Reorder columns - Focus first, then other summary cols, then date cols
    summary_cols = ["Focus", "fund_name", "category", "Trend", "Count>50%", "TodayROI", "Today%", "Min6M", "Max6M", "Slope", "Sell", "Buy", "DipMax", "Count%", "Multiplier"]
    ordered_cols = [c for c in summary_cols if c in pivot.columns] + date_cols
    pivot = pivot[ordered_cols]

    # Rename date columns to short format (MM-DD)
    short_names = {}
    for col in ordered_cols:
        if col in date_cols:
            short_names[col] = col[5:]  # MM-DD only
        else:
            short_names[col] = col  # Keep as is

    pivot = pivot.rename(columns=short_names)
    new_date_cols = [short_names[c] for c in date_cols]

    return pivot, new_date_cols

def main():
    # Custom CSS for dark grey headers
    st.markdown("""
    <style>
    /* Page title */
    h1 {
        color: #9a9a9a !important;
    }
    /* Subheaders */
    h2, h3 {
        color: #5a5a5a !important;
    }
    /* Tab headers */
    .stTabs [data-baseweb="tab-list"] button {
        color: #4a4a4a !important;
    }
    /* Sidebar header */
    .css-1d391kg, .st-emotion-cache-1d391kg {
        color: #4a4a4a !important;
    }
    /* Metric labels */
    [data-testid="stMetricLabel"] {
        color: #5a5a5a !important;
    }
    /* Table headers */
    thead th {
        background-color: #f0f0f0 !important;
        color: #4a4a4a !important;
        white-space: pre-wrap !important;
        height: 60px !important;
        vertical-align: bottom !important;
        line-height: 1.2 !important;
    }
    /* Dataframe header cells */
    [data-testid="stDataFrame"] th {
        white-space: pre-wrap !important;
        min-height: 60px !important;
    }
    </style>
    """, unsafe_allow_html=True)

    # Check data freshness (no auto-fetch - rely on 6am cron job)
    db_date, needs_fetch = check_and_fetch_data()
    ist_today = get_ist_today()

    # Load data
    with st.spinner("Loading..."):
        df, nav_df, calc_info = load_data()
        market_changes = load_market_changes()

    if df.empty:
        st.error("No data found. Run bulk_scraper.py first.")
        return

    # Get header date (last fetch date from DB, or fallback to data date)
    header_date = db_date or calc_info.get("data_date", "Unknown")
    # Format date as "14th Jan'26"
    if header_date and header_date != "Unknown":
        dt = datetime.strptime(header_date, "%Y-%m-%d")
        day = dt.day
        suffix = "th" if 11 <= day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
        formatted_date = f"{day}{suffix} {dt.strftime('%b')}\'{dt.strftime('%y')}"
    else:
        formatted_date = "Unknown"

    # Compact header with data date
    st.markdown(f"### MF ROI Comparison &nbsp;<small style='color:#888'>Today: {formatted_date}</small>", unsafe_allow_html=True)

    # Data freshness indicator and manual refresh
    if needs_fetch:
        st.sidebar.warning(f"Data from {db_date or 'N/A'}")
        if st.sidebar.button("Refresh Data", type="primary"):
            with st.spinner(f"Fetching fresh data for {ist_today}..."):
                success = fetch_fresh_data()
                if success:
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.sidebar.error("Fetch failed")
    else:
        st.sidebar.success(f"Data: {db_date}")

    st.sidebar.markdown("---")

    # Sidebar filters
    st.sidebar.header("Filters")
    categories = ["All"] + sorted(df["category"].dropna().unique().tolist())
    selected_category = st.sidebar.selectbox("Category", categories)
    search_term = st.sidebar.text_input("Search Fund Name", "")

    # Watchlist section
    st.sidebar.markdown("---")
    st.sidebar.header("⭐ Watchlist")

    # Get current watchlist
    db_sidebar = SupabaseDB()
    watchlist = db_sidebar.get_watchlist()
    watchlist_fund_ids = set(w["fund_id"] for w in watchlist)

    # Search to add MF to watchlist - use text input for search
    search_query = st.sidebar.text_input("Search fund to add", placeholder="Type fund name...")

    if search_query and len(search_query) >= 3:
        # Search in database - handle multiple words by searching for each
        # Build pattern: "%word1%word2%..." to match words in order
        words = search_query.strip().split()
        pattern = "%" + "%".join(words) + "%"
        search_result = db_sidebar.client.table("mutual_funds").select("id,fund_name,scheme_code").ilike("fund_name", pattern).limit(20).execute()
        matching_funds = {f["fund_name"]: {"id": f["id"], "scheme_code": f.get("scheme_code")} for f in search_result.data} if search_result.data else {}

        if matching_funds:
            add_fund = st.sidebar.selectbox(
                "Select fund",
                options=list(matching_funds.keys()),
                format_func=lambda x: x[:60] + "..." if len(x) > 60 else x
            )

            if add_fund and st.sidebar.button("➕ Add to Watchlist"):
                fund_info = matching_funds[add_fund]
                fund_id = fund_info["id"]
                scheme_code = fund_info.get("scheme_code")

                if db_sidebar.add_to_watchlist(fund_id, add_fund):
                    # Fetch returns for ALL dates if fund has scheme_code
                    if scheme_code:
                        with st.sidebar:
                            with st.spinner("Fetching historical data (this may take a minute)..."):
                                num_dates = db_sidebar.fetch_fund_returns_all_dates(fund_id, scheme_code)
                                if num_dates > 0:
                                    st.sidebar.success(f"Added with {num_dates} dates of data!")
                                    st.cache_data.clear()
                                else:
                                    st.sidebar.warning("Added (no 3Y data available)")
                    else:
                        st.sidebar.success("Added!")
                    st.rerun()
        else:
            st.sidebar.caption("No funds found matching your search")
    elif search_query:
        st.sidebar.caption("Type at least 3 characters to search")

    # Show current watchlist with remove option
    if watchlist:
        st.sidebar.markdown("**Current watchlist:**")
        for w in watchlist:
            col1, col2 = st.sidebar.columns([4, 1])
            with col1:
                st.sidebar.text(w["fund_name"][:35] + "..." if len(w["fund_name"]) > 35 else w["fund_name"])
            with col2:
                if st.sidebar.button("❌", key=f"remove_{w['fund_id']}"):
                    db_sidebar.remove_from_watchlist(w["fund_id"])
                    st.rerun()
    else:
        st.sidebar.caption("No funds in watchlist")

    # Apply filters
    filtered_df = df.copy()
    if selected_category != "All":
        filtered_df = filtered_df[filtered_df["category"] == selected_category]
    if search_term:
        filtered_df = filtered_df[filtered_df["fund_name"].str.contains(search_term, case=False, na=False)]

    # Pivot for display
    pivot_df, date_cols = pivot_data(filtered_df, nav_df, calc_info)

    # Compact metrics inline
    st.markdown(f"**Funds:** {len(pivot_df)} &nbsp;|&nbsp; **Dates:** {len(date_cols)} &nbsp;|&nbsp; **Categories:** {df['category'].nunique()}")

    # Tabs
    tab1, tab2, tab3 = st.tabs(["📊 ROI Table", "📉 Market Changes", "🔍 Fund Detail"])

    with tab1:

        if not pivot_df.empty:
            # Column configuration for sparklines and formatting
            column_config = {
                "Focus": st.column_config.TextColumn("Focus", width="small", help="Fund focus/theme extracted from name"),
                "fund_name": st.column_config.TextColumn("Fund Name", width="large", help="Mutual fund name - click row for details"),
                "category": st.column_config.TextColumn("Category", width="small", help="Fund category (Large Cap, Mid Cap, etc.)"),
                "Trend": st.column_config.LineChartColumn(
                    "Trend",
                    width="medium",
                    y_min=0,
                    y_max=100,
                    help="Sparkline of scaled ROI (last 6 months)"
                ),
                "Count>50%": st.column_config.NumberColumn("Cnt>50%", width="small", help="Count of dates where percentile >= 50%"),
                "TodayROI": st.column_config.NumberColumn("3Y ROI%", format="%.1f%%", width="small", help="Today's actual 3-year annualized ROI"),
                "Today%": st.column_config.NumberColumn("Today%", format="%d%%", width="small", help=f"Percentile as of {header_date} (scaled ROI where max=100)"),
                "Min6M": st.column_config.NumberColumn("Min6M", format="%d%%", width="small", help="Minimum percentile in last 6 months"),
                "Max6M": st.column_config.NumberColumn("Max6M", format="%d%%", width="small", help="Maximum percentile in last 6 months"),
                "Slope": st.column_config.NumberColumn("Slope", width="small", help="1=uptrend (today>prev>prev2), -1=downtrend, 0=neutral"),
                "Sell": st.column_config.TextColumn("Sell", width="small", help="'Sell' if Slope=-1 (consistent downtrend)"),
                "Buy": st.column_config.TextColumn("Buy", width="small", help="'Buy' if Slope=1 AND Count%>75% AND DipMax<0"),
                "DipMax": st.column_config.NumberColumn("DipMax", format="%d%%", width="small", help="Dip from Max = Today% - Max6M (negative = below max, positive = above max)"),
                "Count%": st.column_config.NumberColumn("Cnt%", format="%d%%", width="small", help="Count>50% / Total dates * 100"),
                "Multiplier": st.column_config.NumberColumn("Mult", format="%d%%", width="small", help="Multiplier = DipMax × Count% / 100"),
            }

            # Dataframe with multi-row selection
            event = st.dataframe(
                pivot_df,
                column_config=column_config,
                use_container_width=True,
                height=600,
                hide_index=True,
                on_select="rerun",
                selection_mode="multi-row",
            )

            # Show NAV chart when funds are selected
            if event and event.selection and event.selection.rows:
                selected_rows = event.selection.rows

                # Gather fund info for selected funds
                selected_fund_infos = []
                db_nav = SupabaseDB()

                for row_idx in selected_rows:
                    fund_name = pivot_df.iloc[row_idx]["fund_name"]
                    clean_name = fund_name.replace("⭐ ", "")

                    fund_row = df[df["fund_name"] == clean_name]
                    if not fund_row.empty:
                        fund_id = fund_row.iloc[0].get("fund_id")
                        if fund_id:
                            fund_info = db_nav.client.table("mutual_funds").select("scheme_code").eq("id", fund_id).execute()
                            if fund_info.data and fund_info.data[0].get("scheme_code"):
                                selected_fund_infos.append({
                                    "scheme_code": fund_info.data[0]["scheme_code"],
                                    "fund_name": clean_name
                                })

                # Show NAV chart
                if selected_fund_infos:
                    st.markdown("---")

                    col1, col2 = st.columns([1, 5])
                    with col1:
                        period = st.selectbox("Period", ["1Y", "6M", "2Y", "3Y"], index=0, key="nav_period")
                        days_map = {"6M": 180, "1Y": 365, "2Y": 730, "3Y": 1095}
                        days = days_map.get(period, 365)

                    with st.spinner("Loading NAV..."):
                        nav_data = get_nav_comparison_data(selected_fund_infos, days=days)

                    if not nav_data.empty:
                        st.line_chart(nav_data, height=350)
                    else:
                        st.warning("Could not fetch NAV data")

            # Download button
            csv = pivot_df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name="mf_scaled_roi.csv",
                mime="text/csv"
            )
        else:
            st.warning("No data matches your filters.")

    with tab2:
        st.subheader("SENSEX Significant Changes (>0.5%)")

        if not market_changes.empty:
            # Format the dataframe
            display_cols = ["change_date", "change_percent", "change_type", "previous_close", "current_close"]
            mc_display = market_changes[display_cols].copy()
            mc_display = mc_display.sort_values("change_date", ascending=False)

            # Format date as "9th Jan'26"
            def format_date(d):
                from datetime import datetime
                dt = datetime.strptime(str(d), "%Y-%m-%d")
                day = dt.day
                suffix = "th" if 11 <= day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
                return f"{day}{suffix} {dt.strftime('%b')}\'{dt.strftime('%y')}"

            mc_display["change_date"] = mc_display["change_date"].apply(format_date)

            # Format change_percent as 2 decimals with %
            mc_display["change_percent"] = mc_display["change_percent"].apply(lambda x: f"{x:.2f}%")

            # Round previous_close and current_close without decimals
            mc_display["previous_close"] = mc_display["previous_close"].round(0).astype(int)
            mc_display["current_close"] = mc_display["current_close"].round(0).astype(int)

            # Color based on change type
            def color_change(row):
                if row["change_type"] == "up":
                    return ["background-color: #90EE90"] * len(row)
                else:
                    return ["background-color: #FFB6C1"] * len(row)

            styled_mc = mc_display.style.apply(color_change, axis=1)
            st.dataframe(styled_mc, use_container_width=True, height=400)

            # Summary
            up_days = len(market_changes[market_changes["change_type"] == "up"])
            down_days = len(market_changes[market_changes["change_type"] == "down"])
            st.markdown(f"**Up days:** {up_days} | **Down days:** {down_days}")
        else:
            st.info("No market change data. Run fetch_market_changes.py first.")

    with tab3:
        st.subheader("Fund Detail View")

        # Fund selector
        fund_names = sorted(df["fund_name"].unique().tolist())
        selected_fund = st.selectbox("Select Fund", fund_names)

        if selected_fund:
            fund_data = df[df["fund_name"] == selected_fund].sort_values("report_date")

            if not fund_data.empty:
                # Fund info
                st.markdown(f"**Category:** {fund_data.iloc[0]['category']}")

                # Chart
                chart_data = fund_data[["report_date", "scaled_roi", "roi_3y"]].set_index("report_date")

                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**Scaled ROI Over Time**")
                    st.line_chart(chart_data["scaled_roi"])
                with col2:
                    st.markdown("**Actual 3Y ROI Over Time**")
                    st.line_chart(chart_data["roi_3y"])

                # Data table
                st.markdown("**Historical Data**")
                st.dataframe(fund_data[["report_date", "roi_3y", "scaled_roi"]], use_container_width=True)


if __name__ == "__main__":
    main()
