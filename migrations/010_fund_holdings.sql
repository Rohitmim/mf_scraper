-- Fund Holdings table
-- Stores scraped holdings data with daily refresh

CREATE TABLE IF NOT EXISTS fund_holdings (
    id SERIAL PRIMARY KEY,
    fund_id INTEGER REFERENCES mutual_funds(id),
    scheme_code INTEGER NOT NULL,
    fetch_date DATE NOT NULL,
    stock_name TEXT NOT NULL,
    nse_symbol TEXT,
    percentage DECIMAL(5,2),
    sector TEXT,
    created_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(scheme_code, fetch_date, stock_name)
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_fund_holdings_scheme_date
ON fund_holdings(scheme_code, fetch_date);

-- Clean up old holdings (keep only last 7 days)
-- Run this periodically: DELETE FROM fund_holdings WHERE fetch_date < CURRENT_DATE - INTERVAL '7 days';
