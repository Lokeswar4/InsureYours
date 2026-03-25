-- Find the lowest-cost insurance provider for each combination of
-- age group, blood type, medical condition, and medication.
USE Healthcare_Group_Project;
GO

WITH RankedInsurance AS (
    SELECT
        AgeGroup,
        [Blood Type],
        [Medical Condition],
        [Insurance Provider],
        [Billing Amount],
        [Medication],
        ROW_NUMBER() OVER (
            PARTITION BY AgeGroup, [Blood Type], [Medical Condition], [Medication]
            ORDER BY [Billing Amount] ASC
        ) AS InsuranceRank
    FROM vw_PatientAgeGroups
)
SELECT
    AgeGroup,
    [Blood Type],
    [Medical Condition],
    [Insurance Provider],
    [Billing Amount],
    [Medication]
INTO Health_Data_analysis
FROM RankedInsurance
WHERE InsuranceRank = 1;
