# Online Retail Data Engineering Pipeline

An end-to-end data engineering project on Azure that processes the Online Retail transaction dataset using a **Medallion Architecture** (Bronze → Silver → Gold) with an automated data-quality check, orchestrated by **Databricks Jobs**.

**Stack:** Azure Data Lake Storage Gen2 · Azure Data Factory · Azure Databricks · Unity Catalog · Databricks SQL · Databricks Jobs

**Author:** Prajjwal Mohan

---

## 1. Project Overview

Retail transaction data arrives as a raw CSV with mixed data types, missing values, cancelled invoices and non-positive quantities or prices. This project builds a repeatable cloud pipeline that:

- ingests the raw file into a Bronze Delta table,
- cleans, types and standardizes it in a Silver table,
- produces business-ready Gold aggregates (sales, product and country),
- validates the outputs in a final data-quality task,
- runs the whole chain as a dependency-driven Databricks Job on a schedule.

## 2. Source Data

| Property | Value |
|---|---|
| Dataset | Online Retail (UCI), UK-based online retailer |
| Raw records | 541,909 |
| Columns | `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country` |
| Period | 2010-12-01 to 2011-12-09 |
| Countries | 38 |
| Distinct customers | 4,372 |

Data-quality issues present in the raw file, which the Silver layer is designed to handle:

| Issue | Rows in source |
|---|---|
| Missing `CustomerID` | 135,080 |
| Missing `Description` | 1,454 |
| Cancelled invoices (`InvoiceNo` starts with `C`) | 9,288 |
| `Quantity` ≤ 0 | 10,624 |
| `UnitPrice` ≤ 0 | 2,517 |
| Exact duplicate rows | 5,268 |

## 3. Architecture

```text
                 Online Retail CSV
                        │
                        ▼
              Azure Data Lake Storage Gen2
                        │
                        ▼
              ┌───────────────────┐
              │      BRONZE       │  raw ingestion
              └─────────┬─────────┘
                        ▼
              ┌───────────────────┐
              │      SILVER       │  clean, typed, standardized
              └─────────┬─────────┘
                        ▼
              ┌───────────────────┐
              │       GOLD        │  business aggregates
              └─────────┬─────────┘
                        ▼
              ┌───────────────────┐
              │   DATA QUALITY    │  validation checks
              └───────────────────┘

        Orchestrated by one Databricks Job (task dependencies)
```

## 4. Azure Resources

| Resource | Name / Purpose |
|---|---|
| Resource group | `rg-online-retail-de` |
| Storage account (ADLS Gen2, hierarchical namespace) | `stonlineretailde2026` with containers `bronze`, `silver`, `gold` |
| Azure Data Factory | `adf-online-retail-de` (provisioned as the integration environment) |
| Azure Databricks workspace | `dbw-online-retail-de` |
| SQL warehouse | `wh_online_retail` (2X-Small, serverless) |
| Unity Catalog | Catalog `online_retail` with schemas `bronze`, `silver`, `gold` |

## 5. Pipeline Layers

### Bronze: `01_bronze_ingestion`

Loads the source CSV unchanged into `online_retail.bronze.online_retail_raw`.

Validation: 541,909 records, matching the source file.

### Silver: `02_silver_transformation`

Produces `online_retail.silver.online_retail_clean`.

- Trims text fields (`InvoiceNo`, `StockCode`, `Description`)
- Casts `Quantity` to `INT`, `UnitPrice` to `DECIMAL(10,2)`, `CustomerID` to `INT`
- Parses `InvoiceDate` using the format `dd-MM-yyyy HH:mm`
- Removes records with missing required transaction fields
- Keeps only `Quantity > 0` and `UnitPrice > 0`
- Excludes cancelled invoices (`InvoiceNo` beginning with `C`)

### Gold: `03_gold_transformation`

| Table | Purpose |
|---|---|
| `online_retail.gold.sales_summary` | Daily sales KPIs |
| `online_retail.gold.product_sales` | Product-level sales analysis |
| `online_retail.gold.country_sales` | Country-level sales analysis |

Validation results:

- Sales period: 2010-12-01 to 2011-12-09
- Total orders: 19,960
- Product-level aggregate records: 4,158

### Data Quality: `04_data_quality`

Runs row-count and consistency queries across the Bronze, Silver and Gold tables and fails the job if an expected table is missing or empty.

## 6. Job Orchestration

```text
01_bronze_ingestion → 02_silver_transformation → 03_gold_transformation → 04_data_quality
```

Each task runs only after the previous task succeeds, so a failure upstream stops bad data from reaching Gold. The job is configured with a recurring schedule trigger.

Final run:

| Task | Status | Approx. duration |
|---|---|---|
| `01_bronze_ingestion` | Success | 1m 4s |
| `02_silver_transformation` | Success | 13s |
| `03_gold_transformation` | Success | 19s |
| `04_data_quality` | Success | 13s |

## 7. Issue Encountered and Resolution

**Problem:** The first Silver run failed with `[CANNOT_PARSE_TIMESTAMP]` on values such as `01-12-2010 08:26`.

**Root cause:** The parsing format did not match the source format `dd-MM-yyyy HH:mm`.

**Fix:**

```sql
TO_TIMESTAMP(InvoiceDate, 'dd-MM-yyyy HH:mm')
```

After the fix, Silver, Gold and Data Quality all succeeded and the full job was rerun successfully.

## 8. Verification Queries

Run these in Databricks SQL to check the pipeline. Adjust column names if your schema differs.

```sql
-- Layer row counts
SELECT 'bronze' AS layer, COUNT(*) AS rows FROM online_retail.bronze.online_retail_raw
UNION ALL
SELECT 'silver', COUNT(*) FROM online_retail.silver.online_retail_clean;

-- Orders, revenue and period in Silver
SELECT COUNT(DISTINCT InvoiceNo)        AS total_orders,
       SUM(Quantity * UnitPrice)        AS total_revenue,
       MIN(InvoiceDate)                 AS first_sale,
       MAX(InvoiceDate)                 AS last_sale
FROM online_retail.silver.online_retail_clean;
```

## 9. Repository Structure

```text
online-retail-medallion-pipeline/
├── notebooks/
│   ├── 01_bronze_ingestion
│   ├── 02_silver_transformation
│   ├── 03_gold_transformation
│   └── 04_data_quality
├── sql/
│   ├── bronze.sql
│   ├── silver.sql
│   ├── gold.sql
│   └── data_quality.sql
├── docs/
│   ├── architecture.png
│   └── Online_Retail_Medallion_Project_Report.docx
├── data/
│   └── README.md          # where to obtain the dataset (do not commit the raw file)
├── .gitignore
└── README.md
```

## 10. How to Reproduce

1. **Create Azure resources:** resource group, ADLS Gen2 storage account with `bronze`, `silver`, `gold` containers, Azure Data Factory and an Azure Databricks workspace.
2. **Upload the source data:** convert the Online Retail file to CSV and place it in the configured storage location.
3. **Create the catalog and schemas:**

   ```sql
   CREATE CATALOG IF NOT EXISTS online_retail;
   CREATE SCHEMA IF NOT EXISTS online_retail.bronze;
   CREATE SCHEMA IF NOT EXISTS online_retail.silver;
   CREATE SCHEMA IF NOT EXISTS online_retail.gold;
   ```

4. **Run the notebooks in order:** `01_bronze_ingestion`, `02_silver_transformation`, `03_gold_transformation`, `04_data_quality`.
5. **Create the Databricks Job** with the dependency chain above, then add a schedule trigger.

## 11. Security and Cost

- Never commit secrets: client secrets, passwords, access keys, SAS tokens, connection strings or `.env` files.
- For production use managed identities or service principals, secret scopes and least-privilege RBAC.
- After testing, stop or delete unused compute, review ADLS storage and Databricks usage, and remove resources that are no longer needed.

## 12. Known Limitations and Future Work

- The pipeline runs a full reload on each execution; incremental loading (for example Auto Loader) would scale better.
- The source contains 5,268 exact duplicate rows and 135,080 rows without a `CustomerID`. Explicit de-duplication and a documented rule for handling missing customers would strengthen Silver.
- Cancelled invoices are excluded rather than stored separately; a dedicated returns table would support return-rate analysis.
- Data-quality checks are count-based; stricter rules (null, range and referential checks) and failure notifications would improve monitoring.
- Azure Data Factory is provisioned but orchestration is handled by Databricks Jobs; ADF pipelines could be added for ingestion from external sources.
- Further options: Delta table optimization, CI/CD with GitHub Actions or Azure DevOps, infrastructure as code with Terraform, and a Power BI dashboard on the Gold tables.
