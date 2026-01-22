#!/bin/bash
# Cron job script to sync mutual fund list every Monday at 00:01 AM
# This ensures all funds are in the database for watchlist search

cd /Users/zop7782/mf_scraper

# Load environment variables
export $(grep -v '^#' .env | xargs)

# Run sync with logging
echo "$(date): Starting fund list sync..." >> /Users/zop7782/mf_scraper/logs/sync_funds.log
/Library/Frameworks/Python.framework/Versions/3.12/bin/python3 sync_fund_list.py >> /Users/zop7782/mf_scraper/logs/sync_funds.log 2>&1
echo "$(date): Sync completed" >> /Users/zop7782/mf_scraper/logs/sync_funds.log
echo "---" >> /Users/zop7782/mf_scraper/logs/sync_funds.log
