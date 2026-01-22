-- Add focus column to mutual_funds table
-- This stores the extracted key theme/focus of the fund (e.g., Infrastructure, Gold, Midcap)

ALTER TABLE mutual_funds ADD COLUMN IF NOT EXISTS focus TEXT;

-- Create index for filtering by focus
CREATE INDEX IF NOT EXISTS idx_mutual_funds_focus ON mutual_funds(focus);
