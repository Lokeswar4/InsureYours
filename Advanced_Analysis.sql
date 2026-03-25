-- InsureYours Advanced Analytics — MySQL 8.0+
-- Demonstrates: window functions, CTEs, percentiles, segmentation.
-- Run after StoredProcedure.sql (depends on vw_PatientAgeGroups).

USE Healthcare_Group_Project;

-- ============================================================
-- 1. BILLING PERCENTILES BY CONDITION
--    Shows the distribution shape, not just the average.
-- ============================================================
SELECT '=== 1. BILLING PERCENTILES BY CONDITION ===' AS section;

WITH Ranked AS (
    SELECT
        MedicalCondition,
        BillingAmount,
        ROW_NUMBER() OVER (PARTITION BY MedicalCondition ORDER BY BillingAmount) AS rn,
        COUNT(*) OVER (PARTITION BY MedicalCondition) AS total
    FROM vw_PatientAgeGroups
)
SELECT
    MedicalCondition,
    MAX(CASE WHEN rn = GREATEST(1, FLOOR(total * 0.25)) THEN BillingAmount END) AS p25,
    MAX(CASE WHEN rn = GREATEST(1, FLOOR(total * 0.50)) THEN BillingAmount END) AS median,
    MAX(CASE WHEN rn = GREATEST(1, FLOOR(total * 0.75)) THEN BillingAmount END) AS p75,
    MAX(CASE WHEN rn = GREATEST(1, FLOOR(total * 0.90)) THEN BillingAmount END) AS p90,
    MAX(CASE WHEN rn = GREATEST(1, FLOOR(total * 0.99)) THEN BillingAmount END) AS p99,
    ROUND(MAX(CASE WHEN rn = GREATEST(1, FLOOR(total * 0.75)) THEN BillingAmount END)
        - MAX(CASE WHEN rn = GREATEST(1, FLOOR(total * 0.25)) THEN BillingAmount END), 2) AS iqr
FROM Ranked
GROUP BY MedicalCondition
ORDER BY MedicalCondition;

-- ============================================================
-- 2. PROVIDER MARKET SHARE
--    Which insurer covers the most patients per condition?
-- ============================================================
SELECT '=== 2. PROVIDER MARKET SHARE BY CONDITION ===' AS section;

SELECT
    MedicalCondition,
    InsuranceProvider,
    COUNT(*) AS claims,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY MedicalCondition), 1) AS market_share_pct,
    RANK() OVER (PARTITION BY MedicalCondition ORDER BY COUNT(*) DESC) AS share_rank
FROM vw_PatientAgeGroups
GROUP BY MedicalCondition, InsuranceProvider
ORDER BY MedicalCondition, share_rank;

-- ============================================================
-- 3. HIGH-COST OUTLIER DETECTION
--    Flag claims > 2 standard deviations above condition mean.
-- ============================================================
SELECT '=== 3. HIGH-COST OUTLIERS (>2 StdDev) ===' AS section;

WITH ConditionStats AS (
    SELECT
        MedicalCondition,
        AVG(BillingAmount) AS mean_bill,
        STDDEV(BillingAmount) AS std_bill
    FROM vw_PatientAgeGroups
    GROUP BY MedicalCondition
)
SELECT
    v.MedicalCondition,
    COUNT(*) AS outlier_count,
    ROUND(AVG(v.BillingAmount), 2) AS avg_outlier_bill,
    ROUND(cs.mean_bill, 2) AS condition_mean,
    ROUND(cs.mean_bill + 2 * cs.std_bill, 2) AS threshold
FROM vw_PatientAgeGroups v
JOIN ConditionStats cs ON v.MedicalCondition = cs.MedicalCondition
WHERE v.BillingAmount > cs.mean_bill + 2 * cs.std_bill
GROUP BY v.MedicalCondition, cs.mean_bill, cs.std_bill
ORDER BY outlier_count DESC;

-- ============================================================
-- 4. COST EFFICIENCY SCORE
--    Combines avg billing AND length of stay into one metric.
--    Lower score = more cost-efficient insurer.
-- ============================================================
SELECT '=== 4. COST EFFICIENCY SCORE (Billing / Stay Days) ===' AS section;

SELECT
    InsuranceProvider,
    MedicalCondition,
    COUNT(*) AS claims,
    ROUND(AVG(BillingAmount), 2) AS avg_billing,
    ROUND(AVG(LengthOfStay), 1) AS avg_stay_days,
    ROUND(AVG(CASE WHEN LengthOfStay > 0
              THEN BillingAmount / LengthOfStay
              ELSE NULL END), 2) AS cost_per_day,
    DENSE_RANK() OVER (
        PARTITION BY MedicalCondition
        ORDER BY AVG(CASE WHEN LengthOfStay > 0
                      THEN BillingAmount / LengthOfStay
                      ELSE NULL END) ASC
    ) AS efficiency_rank
FROM vw_PatientAgeGroups
WHERE LengthOfStay IS NOT NULL AND LengthOfStay > 0
GROUP BY InsuranceProvider, MedicalCondition
HAVING claims >= 10
ORDER BY MedicalCondition, efficiency_rank;

-- ============================================================
-- 5. READMISSION PROXY
--    Patients with multiple admissions for the same condition
--    may indicate treatment quality differences between insurers.
-- ============================================================
SELECT '=== 5. REPEAT ADMISSION ANALYSIS (Readmission Proxy) ===' AS section;

WITH PatientAdmissions AS (
    SELECT
        Name,
        MedicalCondition,
        InsuranceProvider,
        COUNT(*) AS admission_count
    FROM HealthCare_Dataset
    GROUP BY Name, MedicalCondition, InsuranceProvider
    HAVING COUNT(*) > 1
)
SELECT
    InsuranceProvider,
    MedicalCondition,
    COUNT(*) AS patients_with_readmissions,
    ROUND(AVG(admission_count), 1) AS avg_admissions_per_patient,
    MAX(admission_count) AS max_admissions
FROM PatientAdmissions
GROUP BY InsuranceProvider, MedicalCondition
ORDER BY patients_with_readmissions DESC
LIMIT 20;

-- ============================================================
-- 6. BILLING TREND BY ADMISSION MONTH
--    Shows seasonal patterns in healthcare costs.
-- ============================================================
SELECT '=== 6. MONTHLY BILLING TREND ===' AS section;

WITH MonthlyStats AS (
    SELECT
        DATE_FORMAT(DateOfAdmission, '%Y-%m') AS admission_month,
        COUNT(*) AS admissions,
        ROUND(AVG(BillingAmount), 2) AS avg_billing,
        ROUND(SUM(BillingAmount), 2) AS total_billing
    FROM vw_PatientAgeGroups
    GROUP BY admission_month
)
SELECT
    admission_month,
    admissions,
    avg_billing,
    total_billing,
    ROUND(avg_billing - LAG(avg_billing) OVER (ORDER BY admission_month), 2) AS month_over_month_change
FROM MonthlyStats
ORDER BY admission_month;

-- ============================================================
-- 7. CROSS-TABULATION: CONDITION x ADMISSION TYPE x INSURER
--    Pivot-style view for dashboard consumption.
-- ============================================================
SELECT '=== 7. COST MATRIX: CONDITION x ADMISSION TYPE ===' AS section;

SELECT
    MedicalCondition,
    ROUND(AVG(CASE WHEN AdmissionType = 'Elective' THEN BillingAmount END), 2) AS elective_avg,
    ROUND(AVG(CASE WHEN AdmissionType = 'Emergency' THEN BillingAmount END), 2) AS emergency_avg,
    ROUND(AVG(CASE WHEN AdmissionType = 'Urgent' THEN BillingAmount END), 2) AS urgent_avg,
    ROUND(AVG(CASE WHEN AdmissionType = 'Emergency' THEN BillingAmount END)
        - AVG(CASE WHEN AdmissionType = 'Elective' THEN BillingAmount END), 2) AS emergency_premium
FROM vw_PatientAgeGroups
GROUP BY MedicalCondition
ORDER BY emergency_premium DESC;
