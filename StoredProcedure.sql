USE [Healthcare_Group_Project];
GO

-- Creating Stored Procedure for Avg_BillingAmount_Per_AgeGroup_MedicalCondition

CREATE PROCEDURE Avg_BillingAmount_Per_AgeGroup_MedicalCondition
AS
BEGIN
    SELECT
        AVG([Billing Amount]) AS BillingAmount_AVG,
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
        END AS AgeGroup,
        [Medical Condition]
    FROM
        HealthCare_Dataset
    GROUP BY
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
        END,
        [Medical Condition]
    ORDER BY
        AgeGroup, [Medical Condition];
END;
GO

-- Deleting the Stored Procedure Avg_BillingAmount_Per_AgeGroup_MedicalCondition

DROP PROCEDURE Avg_BillingAmount_Per_AgeGroup_MedicalCondition


-- Creating Stored Procedure for Avg_Cost_Per_Condition_For_Insurance

CREATE PROCEDURE Avg_Cost_Per_Condition_For_Insurance
AS
BEGIN
    SELECT
        AVG([Billing Amount]) AS BillingAmount_AVG,
        CASE
            WHEN [Age] BETWEEN 13 AND 18 THEN '13-18'
            WHEN [Age] BETWEEN 19 AND 30 THEN '19-30'
            WHEN [Age] BETWEEN 31 AND 45 THEN '31-45'
            WHEN [Age] BETWEEN 46 AND 60 THEN '46-60'
            WHEN [Age] BETWEEN 61 AND 80 THEN '61-80'
            WHEN [Age] >= 81 THEN '81+'
            ELSE 'Unknown'
        END AS AgeGroup,
        [Medical Condition],[Admission Type],
        [Insurance Provider]
    FROM
        HealthCare_Dataset
    GROUP BY
        CASE
            WHEN [Age] BETWEEN 13 AND 18 THEN '13-18'
            WHEN [Age] BETWEEN 19 AND 30 THEN '19-30'
            WHEN [Age] BETWEEN 31 AND 45 THEN '31-45'
            WHEN [Age] BETWEEN 46 AND 60 THEN '46-60'
            WHEN [Age] BETWEEN 61 AND 80 THEN '61-80'
            WHEN [Age] >= 81 THEN '81+'
            ELSE 'Unknown'
        END,
        [Medical Condition],[Admission Type],
        [Insurance Provider]
    ORDER BY
        AgeGroup,
        [Medical Condition], [Admission Type],
        [Insurance Provider];
END;
GO

-- Deleting the Avg_Cost_Per_Condition_For_Insurance

DROP PROCEDURE Avg_Cost_Per_Condition_For_Insurance;
