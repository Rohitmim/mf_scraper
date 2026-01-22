-- User watchlist table to store pinned/favorite mutual funds
-- These MFs will always be shown regardless of top 200 ranking

CREATE TABLE IF NOT EXISTS user_watchlist (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fund_id UUID NOT NULL REFERENCES mutual_funds(id) ON DELETE CASCADE,
    fund_name TEXT NOT NULL,
    added_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(fund_id)
);

CREATE INDEX IF NOT EXISTS idx_user_watchlist_fund_id ON user_watchlist(fund_id);
