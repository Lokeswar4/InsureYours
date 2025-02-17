-- The query aims to identify, for each unique combination of Age, Blood Type, and Medical Condition, the insurance provider with the lowest total billing amount.
USE Healthcare_Group_Project;

WITH RankedInsurance AS (
    SELECT
        CASE
            WHEN [Age] BETWEEN 0 AND 1 THEN '0-1'
            WHEN [Age] BETWEEN 2 AND 5 THEN '2-5'
            WHEN [Age] BETWEEN 6 AND 12 THEN '6-12'
            WHEN [Age] BETWEEN 13 AND 18 THEN '13-18'
            WHEN [Age] BETWEEN 19 AND 30 THEN '19-30'
            WHEN [Age] BETWEEN 31 AND 45 THEN '31-45'
            WHEN [Age] BETWEEN 46 AND 60 THEN '46-60'
            WHEN [Age] BETWEEN 61 AND 80 THEN '61-80'
            ELSE 'Unknown'
        END AS AgeGroup,
        [Blood Type],
        [Medical Condition],
        [Insurance Provider],
        [Billing Amount],
		[Medication],
        ROW_NUMBER() OVER (PARTITION BY 
            CASE
                WHEN [Age] BETWEEN 0 AND 1 THEN '0-1'
                WHEN [Age] BETWEEN 2 AND 5 THEN '2-5'
                WHEN [Age] BETWEEN 6 AND 12 THEN '6-12'
                WHEN [Age] BETWEEN 13 AND 18 THEN '13-18'
                WHEN [Age] BETWEEN 19 AND 30 THEN '19-30'
                WHEN [Age] BETWEEN 31 AND 45 THEN '31-45'
                WHEN [Age] BETWEEN 46 AND 60 THEN '46-60'
                WHEN [Age] BETWEEN 61 AND 80 THEN '61-80'
                ELSE 'Unknown'
            END,
            [Blood Type], [Medical Condition],[Medication] ORDER BY MIN([Billing Amount]) ASC) AS InsuranceRank
    FROM
        [dbo].[HealthCare_Dataset]
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
            ELSE 'Unknown'
        END,
        [Blood Type], [Medical Condition], [Insurance Provider], [Billing Amount], [Medication]
)
SELECT
    AgeGroup,
    [Blood Type],
    [Medical Condition],
    [Insurance Provider],
    [Billing Amount],
	[Medication]
INTO Health_Data_analysis
FROM
    RankedInsurance
WHERE
    InsuranceRank = 1;
