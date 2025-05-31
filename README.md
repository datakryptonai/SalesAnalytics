# Medallion Architecture for Real-Time Retail Sales Analytics

This project implements a scalable **Medallion Architecture** data pipeline on **Azure Databricks** using **Delta Lake** and **PySpark**. The architecture enables real-time ingestion, cleansing, enrichment, and aggregation of retail sales data from multiple stores to support near real-time analytics and business intelligence.

---

## Project Overview

The retail client operates 50+ stores and collects point-of-sale (POS) transactions continuously. The existing system had issues with delayed, inconsistent, and low-quality sales data, affecting timely business decisions.

This project solves these problems by implementing a Medallion Architecture pipeline with three key layers:

- **Bronze Layer:** Raw streaming data ingestion from Kafka into Delta Lake, capturing immutable, schema-evolving sales transactions.
- **Silver Layer:** Cleansing, deduplication, validation, and enrichment of data by joining with master Product and Store dimension tables.
- **Gold Layer:** Aggregated business-ready datasets with daily sales summaries optimized for fast reporting and analytics consumption.

The solution achieves data latency of under 3 minutes and enables accurate, timely insights for sales performance and inventory management.

---

## Repository Contents

| File                      | Description                                                   |
| ------------------------- | -------------------------------------------------------------|
| `bronze_ingestion.py`     | Streaming ingestion from Kafka into Bronze Delta tables.      |
| `silver_clean_enrich.py`  | Streaming batch pipeline that cleans, deduplicates, and enriches data into Silver Delta tables. |
| `gold_aggregates.py`      | Batch job to aggregate daily sales metrics into Gold Delta tables. |
| `dimension_tables.py`     | Scripts to create and load Product and Store master dimension tables. |
| `synthetic_data_generator.py` | Python script to generate sample JSON sales data for testing. |
| `README.md`               | Project overview, setup instructions, and usage guide.        |

---

## Setup Instructions

1. **Deploy Dimension Tables**  
   Run `dimension_tables.py` in your Azure Databricks environment to create Product and Store master dimension Delta tables.

2. **Start Bronze Layer Ingestion**  
   Launch `bronze_ingestion.py` as a streaming job to ingest raw sales data from Kafka topics into the Bronze Delta table.

3. **Run Silver Layer Pipeline**  
   Run `silver_clean_enrich.py` as a streaming job to process Bronze data, cleanse, deduplicate, enrich with dimensions, and write to Silver Delta tables.

4. **Schedule Gold Layer Aggregations**  
   Schedule `gold_aggregates.py` as a batch job (e.g., via Azure Data Factory or Databricks Jobs) to produce daily aggregated sales metrics for reporting.

5. **Test with Synthetic Data**  
   Use `synthetic_data_generator.py` to create sample sales data if you do not have a live Kafka data source for testing.

---

## Key Features

- **Delta Lake with ACID Transactions:** Ensures reliable and consistent incremental updates.
- **Schema Evolution Support:** Handles changes in incoming sales data schema without pipeline failure.
- **Exactly-Once Processing:** Streaming checkpoints guarantee no duplicates beyond business logic deduplication.
- **Data Quality Checks:** Filters invalid data and removes duplicate transactions.
- **Dimension Table Enrichment:** Joins with Product and Store data for richer analytics context.
- **Partitioned Aggregates:** Gold tables partitioned by date and store for query performance.

---

## Future Enhancements

- Add real-time anomaly detection on sales data for fraud prevention.
- Integrate customer dimension and loyalty data for personalized analytics.
- Enable multi-tenant support for additional retail chains.
- Automate data quality monitoring and alerting dashboards.

---

## Contact

For questions or consulting inquiries, please contact:  
**Debajyoti Kar**  
Email: debjyoti@datakrypton.ai


---

Thank you for exploring this Medallion Architecture example!  
Feel free to fork, contribute, or raise issues.

