-- MF ROI Comparison View - Side by Side (Union Mode)
-- Run this in Supabase SQL Editor: https://supabase.com/dashboard/project/rbbthbufcxpomkzxrkms/sql/new

-- Drop existing view if it exists
DROP VIEW IF EXISTS public.mf_roi_comparison;

-- Create a view that shows UNION of top 200 funds from EACH date
-- Total funds will be > 200 if different funds are in top 200 on different dates
CREATE OR REPLACE VIEW public.mf_roi_comparison AS
WITH
-- Get top 200 funds for each date
top_funds_date1 AS (
    SELECT fund_id FROM public.mutual_fund_returns
    WHERE report_date = '2025-11-26' AND roi_3y IS NOT NULL
    ORDER BY roi_3y DESC LIMIT 200
),
top_funds_date2 AS (
    SELECT fund_id FROM public.mutual_fund_returns
    WHERE report_date = '2025-12-16' AND roi_3y IS NOT NULL
    ORDER BY roi_3y DESC LIMIT 200
),
top_funds_date3 AS (
    SELECT fund_id FROM public.mutual_fund_returns
    WHERE report_date = '2026-01-14' AND roi_3y IS NOT NULL
    ORDER BY roi_3y DESC LIMIT 200
),
-- Union all unique fund IDs
all_top_funds AS (
    SELECT fund_id FROM top_funds_date1
    UNION
    SELECT fund_id FROM top_funds_date2
    UNION
    SELECT fund_id FROM top_funds_date3
)
SELECT
    ROW_NUMBER() OVER (ORDER BY COALESCE(r3.roi_3y, r2.roi_3y, r1.roi_3y) DESC NULLS LAST) as rank,
    mf.fund_name,
    mf.fund_house,
    mf.category,
    r1.roi_3y as "roi_3y_2025_11_26",
    r2.roi_3y as "roi_3y_2025_12_16",
    r3.roi_3y as "roi_3y_2026_01_14",
    COALESCE(r3.roi_3y, 0) - COALESCE(r1.roi_3y, 0) as roi_change
FROM all_top_funds atf
JOIN public.mutual_funds mf ON mf.id = atf.fund_id
LEFT JOIN public.mutual_fund_returns r1
    ON atf.fund_id = r1.fund_id AND r1.report_date = '2025-11-26'
LEFT JOIN public.mutual_fund_returns r2
    ON atf.fund_id = r2.fund_id AND r2.report_date = '2025-12-16'
LEFT JOIN public.mutual_fund_returns r3
    ON atf.fund_id = r3.fund_id AND r3.report_date = '2026-01-14'
ORDER BY rank;

-- RPC function for dynamic date comparison (more flexible)
CREATE OR REPLACE FUNCTION compare_mf_roi_by_dates(
    p_dates DATE[] DEFAULT ARRAY['2025-11-26'::DATE, '2025-12-16'::DATE, '2026-01-14'::DATE],
    p_limit INTEGER DEFAULT 200
)
RETURNS TABLE (
    rank BIGINT,
    fund_name VARCHAR,
    fund_house VARCHAR,
    category VARCHAR,
    date_1 DATE,
    roi_3y_date_1 DECIMAL,
    date_2 DATE,
    roi_3y_date_2 DECIMAL,
    date_3 DATE,
    roi_3y_date_3 DECIMAL,
    roi_change DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    WITH ranked_funds AS (
        SELECT
            mf.id as fund_id,
            mf.fund_name,
            mf.fund_house,
            mf.category,
            ROW_NUMBER() OVER (ORDER BY r.roi_3y DESC NULLS LAST) as rank
        FROM public.mutual_funds mf
        JOIN public.mutual_fund_returns r ON mf.id = r.fund_id
        WHERE r.report_date = p_dates[array_length(p_dates, 1)]  -- Use latest date for ranking
          AND r.roi_3y IS NOT NULL
        LIMIT p_limit
    )
    SELECT
        rf.rank,
        rf.fund_name,
        rf.fund_house,
        rf.category,
        p_dates[1] as date_1,
        r1.roi_3y as roi_3y_date_1,
        p_dates[2] as date_2,
        r2.roi_3y as roi_3y_date_2,
        p_dates[3] as date_3,
        r3.roi_3y as roi_3y_date_3,
        COALESCE(r3.roi_3y, 0) - COALESCE(r1.roi_3y, 0) as roi_change
    FROM ranked_funds rf
    LEFT JOIN public.mutual_fund_returns r1
        ON rf.fund_id = r1.fund_id AND r1.report_date = p_dates[1]
    LEFT JOIN public.mutual_fund_returns r2
        ON rf.fund_id = r2.fund_id AND r2.report_date = p_dates[2]
    LEFT JOIN public.mutual_fund_returns r3
        ON rf.fund_id = r3.fund_id AND r3.report_date = p_dates[3]
    ORDER BY rf.rank;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Grant access
GRANT SELECT ON public.mf_roi_comparison TO authenticated, anon;
