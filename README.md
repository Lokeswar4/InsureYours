# InsureYours

A healthcare data analytics project that uses SQL Server and SSIS to build an ETL pipeline for analyzing insurance billing patterns across patient demographics, medical conditions, and insurance providers.

## Objective

Help patients find the most cost-effective insurance provider based on their age group, blood type, medical condition, and medication needs.

## Dataset

[Healthcare Dataset](https://www.kaggle.com/datasets/prasad22/healthcare-dataset) from Kaggle — 10,000 synthetic patient records with 15 attributes including demographics, medical conditions, billing amounts, and insurance providers.

## Architecture

```
CSV (Kaggle) → SSIS ETL Pipeline → SQL Server → Stored Procedures → Power BI Dashboard
```

### Components

| Component | File | Purpose |
|-----------|------|---------|
| Schema | `Tables_Creation.sql` | Normalized tables with constraints and indexes |
| ETL | `ETL/HealthCare.dtsx` | SSIS package to load CSV into SQL Server |
| Analysis | `StoredProcedure.sql` | Avg billing by age group, condition, and insurer |
| Ranking | `Analysis.sql` | Lowest-cost insurance provider per demographic |
| Dashboard | `HealthCare_Final_Project.pbix` | Power BI visualizations |

## Key Analyses

1. **Average billing per age group and medical condition** — identifies which demographics face highest costs
2. **Cost comparison across insurance providers** — broken down by condition, admission type, and age
3. **Optimal insurance ranking** — finds the cheapest provider for each combination of age/blood type/condition/medication

## Prerequisites

- SQL Server 2019+ (or Azure SQL)
- SQL Server Integration Services (SSIS)
- Power BI Desktop (for dashboard)
- Visual Studio with SSIS extension (for ETL development)

## Setup

1. Create the database:
   ```sql
   CREATE DATABASE Healthcare_Group_Project;
   ```
2. Run `Tables_Creation.sql` to create the schema
3. Configure the SSIS package connection to point to your SQL Server instance and CSV file path
4. Execute the SSIS package to load data
5. Run `StoredProcedure.sql` to create the stored procedures
6. Run `Analysis.sql` to generate the insurance ranking table
7. Open `HealthCare_Final_Project.pbix` in Power BI and update the data source connection

## License

GNU General Public License v3.0
