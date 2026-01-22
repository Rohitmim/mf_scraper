-- Add scheme_code column to mutual_funds table for AMFI scheme code
-- This allows fetching NAV data for specific funds later

ALTER TABLE mutual_funds ADD COLUMN IF NOT EXISTS scheme_code INTEGER;

-- Create index for faster lookup by scheme_code
CREATE INDEX IF NOT EXISTS idx_mutual_funds_scheme_code ON mutual_funds(scheme_code);
