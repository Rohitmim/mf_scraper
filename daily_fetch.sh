#!/bin/bash
# Daily MF data fetch script
# Runs at 6am to fetch latest NAV data

cd /Users/zop7782/mf_scraper

# Set PATH for launchd (doesn't inherit user's PATH)
export PATH="/usr/local/bin:/Library/Frameworks/Python.framework/Versions/3.12/bin:$PATH"

# Load environment variables
export $(grep -v '^#' .env | xargs)

# Log file
LOG_FILE="logs/daily_fetch_$(date +%Y-%m-%d).log"
mkdir -p logs

echo "=== Daily Fetch Started: $(date) ===" >> "$LOG_FILE"

# Run bulk scraper for today's date (--refresh to avoid stale cache)
TODAY=$(date +%Y-%m-%d)
python3 bulk_scraper.py --dates "$TODAY" --refresh >> "$LOG_FILE" 2>&1

echo "=== Fetch Completed: $(date) ===" >> "$LOG_FILE"

# Backfill historical data for any new funds in top 200 or watchlist
echo "=== Backfilling History: $(date) ===" >> "$LOG_FILE"
python3 backfill_history.py >> "$LOG_FILE" 2>&1

echo "=== Backfill Completed: $(date) ===" >> "$LOG_FILE"

# Run data audit and auto-fix any issues
echo "=== Running Data Audit: $(date) ===" >> "$LOG_FILE"
python3 data_audit.py --fix --days 30 >> "$LOG_FILE" 2>&1

echo "=== Audit Completed: $(date) ===" >> "$LOG_FILE"

# Run test suite to verify everything is working
echo "=== Running Test Suite: $(date) ===" >> "$LOG_FILE"
python3 test_suite.py >> "$LOG_FILE" 2>&1
TEST_RESULT=$?

if [ $TEST_RESULT -eq 0 ]; then
    echo "=== Tests PASSED ===" >> "$LOG_FILE"
else
    echo "=== Tests FAILED - Check log for details ===" >> "$LOG_FILE"
fi

echo "=== Daily Job Completed: $(date) ===" >> "$LOG_FILE"
