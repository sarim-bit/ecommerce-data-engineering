# Olist E-commerce Medallion Pipeline
[![GitHub Pages](https://img.shields.io/badge/Live_Report-GitHub_Pages-blue?style=for-the-badge&logo=github)](https://sarim-bit.github.io/ecommerce-data-engineering/)

**Modern Data Stack Portfolio: Snowflake | dbt-core | Airflow | Looker Studio**

**[View Interactive Dashboard](https://lookerstudio.google.com/s/jrxRFB0w0cI)**

### Executive Summary
This project transforms 100k+ raw Brazilian e-commerce records into a high-performance analytics suite. Using a **Medallion Architecture**, I resolved significant data integrity issues (duplicates/nulls) to deliver an **RFM Customer Segmentation** and a **Regional Logistics Performance Tracker**.

### The Tech Stack
* **Data Warehouse:** Snowflake (Storage & Compute)
* **Transformation:** dbt-core (Modular SQL, Testing, Documentation)
* **Orchestration:** Apache Airflow (Automated DAGs via dbt Cloud API)
* **BI & Visualisation:** Looker Studio (Executive Dashboards)

### Data Architecture
1. **Staging (Bronze):** Raw ingestion, type casting, and deduplication of Products/Sellers/Customers.
2. **Intermediate (Silver):** Complex joins and business logic (Delivery SLA calculations).
3. **Marts (Gold):** Final reporting tables (RFM Segments, MoM Revenue Growth).

### Key Business Discovery
> **The Delivery Estimation:** Analysis revealed that Olist consistently beats its delivery estimates by **8-20 days** across all Brazilian states. I identified this as a strategic opportunity to tighten "Promised Delivery Dates" to increase checkout conversion rates.

---
**[View Detailed Technical Case Study](https://sarim-bit.github.io/ecommerce-data-engineering/)** | **[View Interactive Dashboard](https://lookerstudio.google.com/s/jrxRFB0w0cI)**