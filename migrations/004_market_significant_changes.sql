-- Migration: Market Significant Changes Table
-- Tracks dates when market indices changed by significant percentages

-- Table to store significant market change dates
CREATE TABLE IF NOT EXISTS market_significant_changes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    index_name TEXT NOT NULL,               -- 'SENSEX', 'NIFTY50', etc.
    change_date DATE NOT NULL,
    previous_close DECIMAL(12,2),
    current_close DECIMAL(12,2),
    change_percent DECIMAL(6,2) NOT NULL,   -- Can be positive or negative
    change_type TEXT NOT NULL,              -- 'up' or 'down'
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(index_name, change_date)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_market_changes_date ON market_significant_changes(change_date);
CREATE INDEX IF NOT EXISTS idx_market_changes_index ON market_significant_changes(index_name);
CREATE INDEX IF NOT EXISTS idx_market_changes_percent ON market_significant_changes(change_percent);

-- Enable RLS
ALTER TABLE market_significant_changes ENABLE ROW LEVEL SECURITY;

-- RLS policies (allow all for now - adjust as needed)
CREATE POLICY "Allow all operations on market_significant_changes"
    ON market_significant_changes FOR ALL
    USING (true)
    WITH CHECK (true);

-- RPC function to upsert market changes
CREATE OR REPLACE FUNCTION upsert_market_significant_change(
    p_index_name TEXT,
    p_change_date DATE,
    p_previous_close DECIMAL,
    p_current_close DECIMAL,
    p_change_percent DECIMAL,
    p_change_type TEXT
)
RETURNS void AS $$
BEGIN
    INSERT INTO market_significant_changes (
        index_name, change_date, previous_close, current_close, change_percent, change_type
    ) VALUES (
        p_index_name, p_change_date, p_previous_close, p_current_close, p_change_percent, p_change_type
    )
    ON CONFLICT (index_name, change_date) DO UPDATE SET
        previous_close = EXCLUDED.previous_close,
        current_close = EXCLUDED.current_close,
        change_percent = EXCLUDED.change_percent,
        change_type = EXCLUDED.change_type;
END;
$$ LANGUAGE plpgsql;
