#!/bin/bash
# Restart Streamlit server

echo "Stopping existing Streamlit server..."
pkill -f "streamlit run ui_app.py" 2>/dev/null

sleep 2

echo "Loading environment variables..."
cd /Users/zop7782/mf_scraper
set -a
source .env
set +a

echo "Starting Streamlit server..."
nohup streamlit run ui_app.py --server.port 8501 --server.headless true > /tmp/streamlit.log 2>&1 &

sleep 3

echo "Server started!"
echo "URL: http://localhost:8501"
