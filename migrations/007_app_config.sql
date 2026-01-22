-- App config table to store settings like last fetch date
CREATE TABLE IF NOT EXISTS app_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Insert initial last_fetch_date
INSERT INTO app_config (key, value) VALUES ('last_fetch_date', '2000-01-01')
ON CONFLICT (key) DO NOTHING;
