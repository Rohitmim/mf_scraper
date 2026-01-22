# Page Refresh Flow (`ui_app.py`)

This document explains what happens when the MF ROI Comparison UI is refreshed.

## Overview

```
Page Refresh
    │
    ├─► Check: Is today's data in DB?
    │       │
    │       ├─► YES → Use cached load_data()
    │       │
    │       └─► NO → Run bulk_scraper.py
    │                   │
    │                   └─► Fetch top 200 from MFAPI
    │                       Store in DB
    │
    └─► load_data() [cached 10 min]
            │
            ├─► Query DB: funds, returns, dates
            ├─► Calculate scaled ROI
            └─► Return DataFrame + calc_info
                    │
                    └─► pivot_data()
                            │
                            └─► Display in Streamlit
```

---

## Step 1: Fresh Data Check

**Function:** `check_and_fetch_data()` (Lines 27-50)

- Gets latest date from `mutual_fund_returns` table
- Compares with today's date (IST timezone, UTC+5:30)
- **If dates match**: No fetch needed, uses cached data
- **If dates differ**: Triggers fresh data fetch

```python
# Pseudocode
latest_data_date = get_latest_date_from_db()
ist_today = get_today_in_ist()

if latest_data_date == ist_today:
    return (data_date, False)  # No fetch needed
else:
    return (data_date, True)   # Fetch needed
```

---

## Step 2: Fresh Data Fetch (Only if needed)

**Function:** `fetch_fresh_data()` (Lines 52-68)

- Runs `bulk_scraper.py -d <today>` as subprocess
- Fetches top 200 funds by 3Y ROI from MFAPI
- Stores results in `mutual_fund_returns` table
- **Auto-adds significant SENSEX dates**: If SENSEX changed >0.5% on this date, it's automatically added to `market_significant_changes`
- **Timeout**: 5 minutes

```python
# Runs this command
python3 bulk_scraper.py -d 2026-01-20

# bulk_scraper.py automatically:
# 1. Saves top 200 funds to mutual_fund_returns
# 2. Checks if SENSEX moved >0.5% on this date
# 3. If yes, adds to market_significant_changes table
```

---

## Step 3: Load Data (Cached 10 minutes)

**Function:** `load_data()` (Lines 117-249)

**Decorator:** `@st.cache_data(ttl=600)`

| Step | Action |
|------|--------|
| 3a | Get all significant SENSEX change dates (>0.5% moves) |
| 3b | Get latest date from database (header date) |
| 3c | Get top 200 funds by 3Y ROI for latest date |
| 3d | Get watchlist funds (always included) |
| 3e | Combine: top 200 + watchlist fund IDs |
| 3f | Get fund details from `mutual_funds` table |
| 3g | Calculate dates needed: last 6 months + last 20 display dates |
| 3h | Fetch returns with pagination (1000 rows at a time) |
| 3i | Calculate max ROI per date (excluding FoF Overseas) |
| 3j | Calculate scaled ROI: `(fund_roi / max_roi) * 100` |
| 3k | Build DataFrame for display |

### Scaled ROI Calculation

```python
scaled_roi = (fund_roi_3y / max_roi_for_date) * 100
```

- The best performing fund on each date gets 100%
- Other funds are scaled relative to the best
- FoF Overseas funds are excluded from being the "max" (benchmark)

---

## Step 4: Load Market Changes (Cached 5 minutes)

**Function:** `load_market_changes()` (Lines 251-256)

**Decorator:** `@st.cache_data(ttl=300)`

- Gets significant SENSEX change dates from `market_significant_changes` table
- Used for the "Market Changes" tab

---

## Step 5: Pivot & Calculate Stats

**Function:** `pivot_data(df, nav_data, calc_info)` (Lines 258-420)

Transforms the data and calculates summary statistics:

| Column | Description | Calculation |
|--------|-------------|-------------|
| **Trend** | Sparkline | Last 6 months scaled ROI values |
| **Count>50%** | Consistency | Days where scaled ROI >= 50 |
| **TodayROI** | Performance | Actual 3-year CAGR % |
| **Today%** | Percentile | Today's scaled ROI (0-100) |
| **Min6M** | 6M Low | Minimum scaled % in last 6 months |
| **Max6M** | 6M High | Maximum scaled % in last 6 months |
| **Slope** | Trend | 1=uptrend, -1=downtrend, 0=neutral |
| **Sell** | Signal | "Sell" if Slope = -1 |
| **DipMax** | Dip from Max | Today% - Max6M |
| **Count%** | Consistency % | Count>50% / total dates × 100 |
| **Buy** | Signal | "Buy" if Slope=1 AND Count%>75 AND DipMax<0 |
| **Multiplier** | Score | DipMax × Count% / 100 |

### Slope Calculation

```python
# Uses last 3 dates chronologically
vals = [scaled_roi_3_days_ago, scaled_roi_2_days_ago, scaled_roi_today]

if vals[2] > vals[1] > vals[0]:
    slope = 1   # Strict uptrend
elif vals[2] < vals[1] < vals[0]:
    slope = -1  # Strict downtrend
else:
    slope = 0   # Neutral/mixed
```

---

## Step 6: Render UI

**Location:** Lines 500+

### Sidebar
- Category filter dropdown
- Fund name search
- Watchlist management (search, add, remove)

### Main Content
- **Tab 1: ROI Table** - Main data table with all columns
- **Tab 2: Market Changes** - SENSEX significant change dates
- **Tab 3: Fund Detail** - Detailed view of selected fund

---

## Cache Behavior

| Cache | TTL | Description |
|-------|-----|-------------|
| `load_data()` | 10 min | Main data (funds, returns, calculations) |
| `load_market_changes()` | 5 min | SENSEX change dates |

### Cache Cleared When:
- Watchlist fund added → `st.cache_data.clear()`
- Watchlist fund removed → `st.rerun()` triggers reload
- TTL expires
- Manual browser hard refresh (Ctrl+Shift+R)

---

## Database Tables Used

| Table | Purpose |
|-------|---------|
| `mutual_funds` | Fund master data (name, category, scheme_code) |
| `mutual_fund_returns` | Daily ROI data (roi_1y, roi_2y, roi_3y) |
| `market_significant_changes` | SENSEX dates with >3% change |
| `user_watchlist` | User's saved funds |

---

## Scheduled Jobs

| Job | Schedule | Script |
|-----|----------|--------|
| Sync Fund List | Monday 00:01 AM | `sync_fund_list.py` via launchd |

This keeps the full fund list updated for watchlist search.

---

## Automatic Significant Date Detection

When `bulk_scraper.py` runs for a new date, it automatically:

1. **Checks SENSEX change** for that date using Yahoo Finance (via `SensexClient`)
2. **If change > 0.5%**: Adds to `market_significant_changes` table
3. **If already exists**: Skips (no duplicate)

This ensures significant market dates are tracked automatically without manual intervention.

```
bulk_scraper.py -d 2026-01-21
    │
    ├─► Save top 200 funds to DB
    │
    └─► check_and_add_significant_date()
            │
            ├─► Date exists in DB? → Skip
            │
            └─► Fetch SENSEX data for date
                    │
                    ├─► Change < 0.5%? → Skip
                    │
                    └─► Change >= 0.5%? → Add to market_significant_changes
```

---

## Date Deduplication

**Important:** Today's date is always included in the display, whether or not it's a significant SENSEX date.

The system uses Python `set` operations to ensure no date appears twice:

```python
# In load_data():
valid_dates = (significant_dates_set & dates_in_returns)  # Significant dates
valid_dates.add(today_date)  # Add today (no-op if already in set)
```

**Scenarios:**

| Today's SENSEX Change | In significant_dates_set? | Result |
|-----------------------|---------------------------|--------|
| < 0.5% | No | Today added separately, appears once |
| >= 0.5% | Yes (auto-added by bulk_scraper) | Already in set, appears once |

This ensures today is **never counted twice** - it appears exactly once in the date columns regardless of whether it's a significant market day.
