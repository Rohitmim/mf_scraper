-- View: Scaled ROI comparison across all significant market change dates
-- Each fund's ROI is scaled relative to max ROI of that date (max = 100)
-- Only shows data from last 3 years (relative to current date)

CREATE OR REPLACE VIEW mf_scaled_roi_comparison AS
WITH max_roi_per_date AS (
  SELECT report_date, MAX(roi_3y) as max_roi
  FROM mutual_fund_returns
  WHERE roi_3y IS NOT NULL
    AND report_date >= CURRENT_DATE - INTERVAL '3 years'
  GROUP BY report_date
),
scaled_returns AS (
  SELECT
    f.fund_name,
    f.fund_house,
    f.category,
    r.report_date,
    r.roi_3y,
    m.max_roi,
    ROUND((r.roi_3y / NULLIF(m.max_roi, 0)) * 100, 2) as scaled_roi
  FROM mutual_fund_returns r
  JOIN mutual_funds f ON r.fund_id = f.id
  JOIN max_roi_per_date m ON r.report_date = m.report_date
  WHERE r.report_date >= CURRENT_DATE - INTERVAL '3 years'
)
SELECT
  fund_name,
  fund_house,
  category,
  MAX(CASE WHEN report_date = '2022-12-23' THEN scaled_roi END) AS "2022_12_23",
  MAX(CASE WHEN report_date = '2023-02-03' THEN scaled_roi END) AS "2023_02_03",
  MAX(CASE WHEN report_date = '2023-02-22' THEN scaled_roi END) AS "2023_02_22",
  MAX(CASE WHEN report_date = '2023-03-03' THEN scaled_roi END) AS "2023_03_03",
  MAX(CASE WHEN report_date = '2023-03-13' THEN scaled_roi END) AS "2023_03_13",
  MAX(CASE WHEN report_date = '2023-03-31' THEN scaled_roi END) AS "2023_03_31",
  MAX(CASE WHEN report_date = '2023-12-04' THEN scaled_roi END) AS "2023_12_04",
  MAX(CASE WHEN report_date = '2024-01-17' THEN scaled_roi END) AS "2024_01_17",
  MAX(CASE WHEN report_date = '2024-01-23' THEN scaled_roi END) AS "2024_01_23",
  MAX(CASE WHEN report_date = '2024-01-29' THEN scaled_roi END) AS "2024_01_29",
  MAX(CASE WHEN report_date = '2024-03-01' THEN scaled_roi END) AS "2024_03_01",
  MAX(CASE WHEN report_date = '2024-05-23' THEN scaled_roi END) AS "2024_05_23",
  MAX(CASE WHEN report_date = '2024-06-03' THEN scaled_roi END) AS "2024_06_03",
  MAX(CASE WHEN report_date = '2024-06-04' THEN scaled_roi END) AS "2024_06_04",
  MAX(CASE WHEN report_date = '2024-06-05' THEN scaled_roi END) AS "2024_06_05",
  MAX(CASE WHEN report_date = '2024-06-07' THEN scaled_roi END) AS "2024_06_07",
  MAX(CASE WHEN report_date = '2024-07-26' THEN scaled_roi END) AS "2024_07_26",
  MAX(CASE WHEN report_date = '2024-08-05' THEN scaled_roi END) AS "2024_08_05",
  MAX(CASE WHEN report_date = '2024-08-16' THEN scaled_roi END) AS "2024_08_16",
  MAX(CASE WHEN report_date = '2024-09-12' THEN scaled_roi END) AS "2024_09_12",
  MAX(CASE WHEN report_date = '2024-09-20' THEN scaled_roi END) AS "2024_09_20",
  MAX(CASE WHEN report_date = '2024-10-03' THEN scaled_roi END) AS "2024_10_03",
  MAX(CASE WHEN report_date = '2024-11-22' THEN scaled_roi END) AS "2024_11_22",
  MAX(CASE WHEN report_date = '2025-01-02' THEN scaled_roi END) AS "2025_01_02",
  MAX(CASE WHEN report_date = '2025-01-06' THEN scaled_roi END) AS "2025_01_06",
  MAX(CASE WHEN report_date = '2025-01-21' THEN scaled_roi END) AS "2025_01_21",
  MAX(CASE WHEN report_date = '2025-02-04' THEN scaled_roi END) AS "2025_02_04",
  MAX(CASE WHEN report_date = '2025-02-28' THEN scaled_roi END) AS "2025_02_28",
  MAX(CASE WHEN report_date = '2025-03-18' THEN scaled_roi END) AS "2025_03_18",
  MAX(CASE WHEN report_date = '2025-04-01' THEN scaled_roi END) AS "2025_04_01",
  MAX(CASE WHEN report_date = '2025-04-07' THEN scaled_roi END) AS "2025_04_07",
  MAX(CASE WHEN report_date = '2025-04-11' THEN scaled_roi END) AS "2025_04_11",
  MAX(CASE WHEN report_date = '2025-04-15' THEN scaled_roi END) AS "2025_04_15",
  MAX(CASE WHEN report_date = '2025-04-17' THEN scaled_roi END) AS "2025_04_17",
  MAX(CASE WHEN report_date = '2025-05-12' THEN scaled_roi END) AS "2025_05_12",
  MAX(CASE WHEN report_date = '
  2025-05-13' THEN scaled_roi END) AS "2025_05_13"
FROM scaled_returns
GROUP BY fund_name, fund_house, category
ORDER BY fund_name;
