# Banking Data Engineering Platform



An end-to-end banking data engineering portfolio project demonstrating Azure Data Factory, Azure Data Lake Storage Gen2, Databricks, PySpark, Delta Lake and Power BI.



## Project Overview



This project demonstrates how banking transaction data can be ingested, transformed, validated and prepared for business reporting using a modern data engineering architecture.



The platform follows the Medallion Architecture:



\- \*\*Bronze:\*\* Raw banking transaction data.

\- \*\*Silver:\*\* Cleaned, standardized and validated transactions.

\- \*\*Gold:\*\* Aggregated datasets for business analytics and reporting.



## Technology Stack



| Technology | Purpose |

|---|---|

| Azure Data Factory | Data ingestion and orchestration |

| Azure Data Lake Storage Gen2 | Cloud data storage |

| Azure Key Vault | Secrets management setup |

| Databricks Free Edition | Spark development environment |

| PySpark | Data transformation |

| Delta Lake | Transactional data storage and MERGE |

| Power BI Desktop | Business intelligence and reporting |

| Python | Pipeline logic and validation |

| Git and GitHub | Version control |



## Architecture



```text

Banking Transaction CSV

&#x20;         |

&#x20;         v

Azure Data Factory

&#x20;         |

&#x20;         v

ADLS Gen2 - Landing

&#x20;         |

&#x20;         v

ADLS Gen2 - Bronze

&#x20;         |

&#x20;         | Sample data exported and uploaded

&#x20;         v

Databricks Free Edition

&#x20;         |

&#x20;         v

PySpark Transformations

&#x20;         |

&#x20;         v

Silver Delta Layer

&#x20;         |

&#x20;         v

Gold Aggregation Layer

&#x20;         |

&#x20;         v

Power BI Dashboard

```



\*\*Implementation note:\*\* Azure Data Factory successfully copies data from Landing to Bronze in Azure Storage. For the Databricks transformation exercises, the Bronze CSV was downloaded and uploaded separately to a Databricks Unity Catalog volume. These are demonstrated stages, not a fully automated Azure-to-Databricks integration.



## Key Features Implemented



### 1. Data Ingestion

\- Banking transaction CSV ingestion.

\- Azure Data Factory Copy Activity.

\- ADLS Gen2 Landing and Bronze containers.

\- Managed identity authentication for storage access.



### 2. Bronze-to-Silver Transformation

\- PySpark data processing.

\- Column standardization and trimming.

\- Data type conversion.

\- Duplicate transaction ID removal.

\- Required-field validation.

\- Invalid record filtering.



### 3. Gold Analytics

Created business-ready datasets:



\- Daily transaction summary.

\- Customer transaction summary.

\- Transaction counts by status.

\- Transaction amount aggregations.



### 4. Incremental Data Processing

\- Identify new transactions.

\- Detect previously processed transaction IDs.

\- Delta Lake MERGE operations.

\- Duplicate prevention.

\- Idempotency testing.



### 5. Data Quality and Reconciliation

\- Missing-value validation.

\- Duplicate transaction checks.

\- Invalid amount checks.

\- Transaction status validation.

\- Silver-to-Gold count and amount reconciliation.



### 6. Pipeline Monitoring and Auditing

\- Batch execution tracking.

\- Success and failure status recording.

\- Error message capture.

\- Processing audit logs.

\- Pipeline monitoring summaries.



### 7. Power BI Reporting

Gold datasets are exported as CSV files and imported into Power BI Desktop for banking analytics.



Planned dashboard metrics include:

\- Total transactions.

\- Total transaction amount.

\- Successful transactions.

\- Failed transactions.

\- Pending transactions.



## Sample Results



The completed end-to-end test processed:



| Metric | Result |

|---|---:|

| Silver transactions | 9 |

| Gold daily transactions | 9 |

| Gold customer transactions | 9 |

| Total transaction amount | ZAR 13,250.50 |

| Duplicate transaction IDs | 0 |

| Reconciliation | Passed |



These are results from the synthetic sample dataset and development tests.



## Repository Structure



```text

azure-databricks-banking-data-engineering-platform/

â”‚

â”œâ”€â”€ data/

â”‚   â””â”€â”€ source/

â”‚       â””â”€â”€ transactions.csv

â”‚

â”œâ”€â”€ notebooks/

â”‚   â””â”€â”€ 01\_bronze\_to\_silver\_transactions.py

â”‚

â”œâ”€â”€ config/

â”œâ”€â”€ deployment/

â”œâ”€â”€ pipelines/

â”œâ”€â”€ scripts/

â”œâ”€â”€ src/

â”œâ”€â”€ tests/

â”‚

â”œâ”€â”€ .gitignore

â””â”€â”€ README.md

```



Some folders are reserved for supporting implementation files as the project evolves.



## Future Enhancements



\- Automate Azure-to-Databricks data integration.

\- Implement CDC and SCD Type 2.

\- Add automated unit and integration tests.

\- Implement quarantine handling for invalid records.

\- Add CI/CD deployment workflows.

\- Complete and publish the Power BI dashboard.

\- Improve monitoring and operational alerting.



## Important



This is a learning and portfolio project using synthetic banking data. No real customer or banking information is included.



Azure resources and Databricks Free Edition are used in separate development workflows where noted above.



## Author



\*\*Data Engineering Portfolio Project\*\*



Technologies: Azure | Databricks | PySpark | Delta Lake | Power BI | Python




