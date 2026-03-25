-- InsureYours Stored Procedures — MySQL 8.0+
-- Depends on: Tables_Creation.sql (schema), ETL (data loaded)
-- Idempotent: safe to re-run.

USE Healthcare_Group_Project;

-- ============================================================
-- View: Patient age groups (replaces repeated CASE logic)
--   - Explicit column list (no SELECT *)
--   - Excludes PII (Patient.Name, Doctor.Name)
--   - Queries the staging table directly since the analytical
--     pipeline was designed around the flat structure
-- ============================================================
DROP VIEW IF EXISTS vw_PatientAgeGroups;

CREATE VIEW vw_PatientAgeGroups AS
SELECT
    Age,
    Gender,
    BloodType,
    MedicalCondition,
    DateOfAdmission,
    Hospital,
    InsuranceProvider,
    BillingAmount,
    RoomNumber,
    AdmissionType,
    DischargeDate,
    Medication,
    TestResults,
    DATEDIFF(DischargeDate, DateOfAdmission) AS LengthOfStay,
    CASE
        WHEN Age BETWEEN  0 AND  1  THEN '0-1'
        WHEN Age BETWEEN  2 AND  5  THEN '2-5'
        WHEN Age BETWEEN  6 AND 12  THEN '6-12'
        WHEN Age BETWEEN 13 AND 18  THEN '13-18'
        WHEN Age BETWEEN 19 AND 30  THEN '19-30'
        WHEN Age BETWEEN 31 AND 45  THEN '31-45'
        WHEN Age BETWEEN 46 AND 60  THEN '46-60'
        WHEN Age BETWEEN 61 AND 80  THEN '61-80'
        WHEN Age >= 81              THEN '81+'
        ELSE 'Unknown'
    END AS AgeGroup
FROM HealthCare_Dataset;

-- ============================================================
-- Procedure 1: Average billing per age group and medical condition
-- ============================================================
DROP PROCEDURE IF EXISTS Avg_BillingAmount_Per_AgeGroup_MedicalCondition;

DELIMITER //
CREATE PROCEDURE Avg_BillingAmount_Per_AgeGroup_MedicalCondition()
BEGIN
    SELECT
        AgeGroup,
        MedicalCondition,
        ROUND(AVG(BillingAmount), 2) AS AvgBilling,
        COUNT(*)                     AS ClaimCount,
        ROUND(MIN(BillingAmount), 2) AS MinBilling,
        ROUND(MAX(BillingAmount), 2) AS MaxBilling
    FROM vw_PatientAgeGroups
    GROUP BY AgeGroup, MedicalCondition
    ORDER BY AgeGroup, MedicalCondition;
END //
DELIMITER ;

-- ============================================================
-- Procedure 2: Average cost per condition, admission type,
--              blood type, and insurance provider
--   - Now includes BloodType for consistency with Analysis.sql
-- ============================================================
DROP PROCEDURE IF EXISTS Avg_Cost_Per_Condition_For_Insurance;

DELIMITER //
CREATE PROCEDURE Avg_Cost_Per_Condition_For_Insurance()
BEGIN
    SELECT
        AgeGroup,
        BloodType,
        MedicalCondition,
        AdmissionType,
        InsuranceProvider,
        ROUND(AVG(BillingAmount), 2) AS AvgBilling,
        COUNT(*)                     AS ClaimCount,
        ROUND(MIN(BillingAmount), 2) AS MinBilling,
        ROUND(MAX(BillingAmount), 2) AS MaxBilling
    FROM vw_PatientAgeGroups
    GROUP BY AgeGroup, BloodType, MedicalCondition, AdmissionType, InsuranceProvider
    ORDER BY AgeGroup, BloodType, MedicalCondition, AdmissionType, InsuranceProvider;
END //
DELIMITER ;

-- ============================================================
-- Procedure 3 (NEW): Average length of stay by condition and
--                     insurance provider — controls for care
--                     intensity when comparing costs
-- ============================================================
DROP PROCEDURE IF EXISTS Avg_LengthOfStay_Per_Condition_For_Insurance;

DELIMITER //
CREATE PROCEDURE Avg_LengthOfStay_Per_Condition_For_Insurance()
BEGIN
    SELECT
        AgeGroup,
        MedicalCondition,
        InsuranceProvider,
        ROUND(AVG(LengthOfStay), 1)   AS AvgStayDays,
        ROUND(AVG(BillingAmount), 2)   AS AvgBilling,
        ROUND(AVG(
            CASE WHEN LengthOfStay > 0
                 THEN BillingAmount / LengthOfStay
                 ELSE NULL
            END
        ), 2)                          AS AvgCostPerDay,
        COUNT(*)                       AS ClaimCount
    FROM vw_PatientAgeGroups
    WHERE LengthOfStay IS NOT NULL
    GROUP BY AgeGroup, MedicalCondition, InsuranceProvider
    ORDER BY AgeGroup, MedicalCondition, AvgCostPerDay;
END //
DELIMITER ;
