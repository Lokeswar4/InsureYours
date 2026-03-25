-- InsureYours Insurance Ranking — MySQL 8.0+
-- Finds the lowest-cost insurance provider for each combination of
-- age group, blood type, medical condition, and medication.
--
-- CRITICAL FIX: Aggregates by provider FIRST (AVG billing), then ranks.
-- The old version ranked individual claims, so one freak cheap claim
-- would make a provider appear cheapest even if its average was highest.
--
-- Depends on: StoredProcedure.sql (vw_PatientAgeGroups view)
-- Idempotent: safe to re-run (truncates output table first).

USE Healthcare_Group_Project;

-- Clear previous results
TRUNCATE TABLE Health_Data_Analysis;

-- Step 1: Aggregate billing by provider per demographic group
-- Step 2: Rank providers using DENSE_RANK (preserves ties)
-- Step 3: Insert all top-ranked providers (rank = 1) into output table
INSERT INTO Health_Data_Analysis
    (AgeGroup, BloodType, MedicalCondition, Medication,
     InsuranceProvider, AvgBillingAmount, MinBillingAmount,
     MaxBillingAmount, ClaimCount, ProviderRank)
WITH ProviderAggregates AS (
    SELECT
        AgeGroup,
        BloodType,
        MedicalCondition,
        Medication,
        InsuranceProvider,
        ROUND(AVG(BillingAmount), 2) AS AvgBillingAmount,
        ROUND(MIN(BillingAmount), 2) AS MinBillingAmount,
        ROUND(MAX(BillingAmount), 2) AS MaxBillingAmount,
        COUNT(*)                     AS ClaimCount
    FROM vw_PatientAgeGroups
    GROUP BY AgeGroup, BloodType, MedicalCondition, Medication, InsuranceProvider
),
RankedProviders AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY AgeGroup, BloodType, MedicalCondition, Medication
            ORDER BY AvgBillingAmount ASC
        ) AS ProviderRank
    FROM ProviderAggregates
)
SELECT
    AgeGroup,
    BloodType,
    MedicalCondition,
    Medication,
    InsuranceProvider,
    AvgBillingAmount,
    MinBillingAmount,
    MaxBillingAmount,
    ClaimCount,
    ProviderRank
FROM RankedProviders
WHERE ProviderRank = 1;

-- Verification: show summary of results
SELECT
    COUNT(*) AS TotalRecommendations,
    COUNT(DISTINCT CONCAT(AgeGroup, BloodType, MedicalCondition, Medication)) AS UniqueDemographicGroups,
    ROUND(AVG(ClaimCount), 1) AS AvgClaimsPerRecommendation
FROM Health_Data_Analysis;
