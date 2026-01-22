-- MF ROI Comparison View - Date-wise columns
-- Run this in Supabase SQL Editor: https://supabase.com/dashboard/project/rbbthbufcxpomkzxrkms/sql/new

DROP VIEW IF EXISTS public.mf_roi_by_date;

CREATE VIEW public.mf_roi_by_date AS
WITH
top_funds_d1 AS (
    SELECT fund_id FROM public.mutual_fund_returns
    WHERE report_date = '2025-11-26' AND roi_3y IS NOT NULL
    ORDER BY roi_3y DESC LIMIT 200
),
top_funds_d2 AS (
    SELECT fund_id FROM public.mutual_fund_returns
    WHERE report_date = '2025-12-16' AND roi_3y IS NOT NULL
    ORDER BY roi_3y DESC LIMIT 200
),
top_funds_d3 AS (
    SELECT fund_id FROM public.mutual_fund_returns
    WHERE report_date = '2026-01-14' AND roi_3y IS NOT NULL
    ORDER BY roi_3y DESC LIMIT 200
),
all_funds AS (
    SELECT fund_id FROM top_funds_d1
    UNION SELECT fund_id FROM top_funds_d2
    UNION SELECT fund_id FROM top_funds_d3
)
SELECT
    mf.fund_name,
    r1.roi_3y AS "2025-11-26",
    r2.roi_3y AS "2025-12-16",
    r3.roi_3y AS "2026-01-14"
FROM all_funds af
JOIN public.mutual_funds mf ON mf.id = af.fund_id
LEFT JOIN public.mutual_fund_returns r1 ON af.fund_id = r1.fund_id AND r1.report_date = '2025-11-26'
LEFT JOIN public.mutual_fund_returns r2 ON af.fund_id = r2.fund_id AND r2.report_date = '2025-12-16'
LEFT JOIN public.mutual_fund_returns r3 ON af.fund_id = r3.fund_id AND r3.report_date = '2026-01-14'
ORDER BY COALESCE(r3.roi_3y, r2.roi_3y, r1.roi_3y) DESC NULLS LAST;
