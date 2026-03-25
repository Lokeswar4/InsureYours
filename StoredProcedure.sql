USE Healthcare_Group_Project;
GO

-- Reusable view for age grouping (eliminates repeated CASE logic)
CREATE OR ALTER VIEW vw_PatientAgeGroups AS
SELECT
    *,
    CASE
        WHEN [Age] BETWEEN 0 AND 1 THEN '0-1'
        WHEN [Age] BETWEEN 2 AND 5 THEN '2-5'
        WHEN [Age] BETWEEN 6 AND 12 THEN '6-12'
        WHEN [Age] BETWEEN 13 AND 18 THEN '13-18'
        WHEN [Age] BETWEEN 19 AND 30 THEN '19-30'
        WHEN [Age] BETWEEN 31 AND 45 THEN '31-45'
        WHEN [Age] BETWEEN 46 AND 60 THEN '46-60'
        WHEN [Age] BETWEEN 61 AND 80 THEN '61-80'
        WHEN [Age] >= 81 THEN '81+'
        ELSE 'Unknown'
    END AS AgeGroup
FROM HealthCare_Dataset;
GO

-- Average billing amount per age group and medical condition
CREATE OR ALTER PROCEDURE Avg_BillingAmount_Per_AgeGroup_MedicalCondition
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        SELECT
            AVG([Billing Amount]) AS BillingAmount_AVG,
            AgeGroup,
            [Medical Condition]
        FROM vw_PatientAgeGroups
        GROUP BY AgeGroup, [Medical Condition]
        ORDER BY AgeGroup, [Medical Condition];
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END;
GO

-- Average cost per condition, admission type, and insurance provider
CREATE OR ALTER PROCEDURE Avg_Cost_Per_Condition_For_Insurance
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        SELECT
            AVG([Billing Amount]) AS BillingAmount_AVG,
            AgeGroup,
            [Medical Condition],
            [Admission Type],
            [Insurance Provider]
        FROM vw_PatientAgeGroups
        GROUP BY AgeGroup, [Medical Condition], [Admission Type], [Insurance Provider]
        ORDER BY AgeGroup, [Medical Condition], [Admission Type], [Insurance Provider];
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END;
GO
