# 🛠️ Data Engineering for E-commerce Decision Support

![headers](img/header.png)

## 📌 Overview

This project implements a complete **data engineering pipeline** that supports business intelligence and decision-making for **Olist**, a Brazilian e-commerce marketplace. The pipeline extracts **raw transactional data**, cleans and structures it in **PostgreSQL**, stages it in **Google Cloud Storage**, and transforms it into analytics-ready **BigQuery marts**—all orchestrated via **Apache Airflow (Docker)** with integrated **Slack alerting**.

> 💡 **Shoutout to [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data)** for making their e-commerce dataset freely available on Kaggle. This rich dataset forms the foundation of this project and enables practical learning for the data community.

---

## 1. 📈 Business Case

**Olist**, operating in a competitive online marketplace environment, requires data-driven strategies to optimize operations and gain a competitive edge. Reliable and timely insights are essential to understand customer behavior, order activity, product performance, and operational bottlenecks.

As a **data engineer**, I designed and implemented this pipeline to ensure:
- Clean, consistent data across systems.
- Automated, reproducible ETL processes.
- High availability of cloud-based analytics resources.

### 🎯 Key Business Questions

- How is our e-commerce activity performing from buyer and seller perspectives?
- What is the sales and operational performance of each product, which products category drive the most revenue, and how does pricing influence performance??
- Who are our customers, what are their key characteristics, and how do customer behavior and retention vary across regions and time periods??
- What are the bottlenecks in our order fulfillment process, and how can we improve delivery speed and reliability?

---

## 2. 📂 About the Data

![E-Commerce Data Overview](img/bg2.png)

All raw datasets are stored in the `datas/` directory and derived from the Olist e-commerce dataset. The total data volume used in this project contains over **500,000 rows**, covering multiple years of e-commerce transactions in Brazil.

| Filename | Description |
|----------|-------------|
| `raw_customer.csv` | Customer ID, location, and first purchase date. |
| `raw_item.csv` | Order items with product, seller, pricing, and freight data. |
| `raw_order.csv` | Order metadata, statuses, and timestamps. |
| `raw_order_failed_or_pending.csv` | Orders that failed or are pending. |
| `raw_product.csv` | Product details including name, category, and dimensions. |
| `raw_seller.csv` | Seller profiles and geographical data. |

> Transformed outputs are saved in `datas/exports/`, partitioned by date and entity, in **CSV** or **Parquet** formats.

---

## 3. ⚙️ Project Structure

```
project-root/
├── dags/                     # Airflow DAGs
│   ├── raw_to_postgres.py    # Load raw CSVs into PostgreSQL
│   └── pg_to_gcp.py          # Extract from PostgreSQL and load into GCS/BigQuery
├── datas/                    # Contains all raw and exported data
│   └── exports/              # Partitioned/transformed datasets
├── plugins/                  # (Optional) Airflow custom plugins
├── logs/                     # Airflow task and pipeline logs
├── docker-compose.yaml       # Docker environment for Airflow and PostgreSQL
└── README.md                 # Project documentation
```

---

## 4. 🌟 Key Features

- **End-to-end orchestration** with Airflow
- **Cloud-native integration** with GCS and BigQuery
- **Explicit schema definitions** and SQL-based marts
- **Partitioned and clustered tables** for optimal performance
- **Slack notifications** for pipeline observability
- **Dockerized setup** for reproducibility and isolation

---

## 5. DAGs Breakdown

### 5.1 `raw_to_postgres.py`

#### 🎯 Purpose

Automates the **initial ingestion** of raw CSVs into a structured **PostgreSQL** database.

#### 🔧 Features

- Creates necessary schemas and tables if they do not exist.
- Transforms data types and formats for analytics-readiness.
- Loads raw CSVs into PostgreSQL using entity-specific logic.
- Modular design: each dataset (orders, customers, etc.) has its own loader.

#### ✅ Benefit

Ensures that scattered raw files are reliably ingested, cleaned, and normalized for downstream processing.

---

### 5.2 `pg_to_gcp.py`

#### 🎯 Purpose

Automates the **extraction** of data from PostgreSQL and the **loading** into **Google Cloud Storage** and **BigQuery**, powering cloud-based analytics and dashboards.

#### 🧩 Key Task Flow

```
[Extract from PostgreSQL] 
     ↓
[Save CSV locally]
     ↓
[Upload to GCS bucket]
     ↓
[Load to BigQuery staging]
     ↓
[Transform to marts with SQL]
     ↓
[Slack alert (success/failure)]
```

#### 🔍 Task Roles & Advantages

| Task | Description | Advantages |
|------|-------------|------------|
| **Extract** | Queries each entity and exports to CSV | Modular, reusable logic per table |
| **Upload to GCS** | Stores CSVs in structured paths by entity and date | Centralized backup and easy access |
| **Load to BigQuery (Staging)** | Uses explicit schemas, with partitioning/clustering | Optimized for cost, performance, and type safety |
| **Transform to Data Marts** | SQL logic converts staging tables to fact/dimensional marts | Easy to iterate and scale for new insights |
| **Slack Notification** | Alerts on failure or success | Enables proactive monitoring |

#### ✅ Technical Benefits

- **Modular & Testable**: Each table runs independently with reusable functions.
- **Explicit Schema Handling**: Improves data quality and traceability.
- **Optimized BigQuery Usage**: Leverages partitioning on timestamps and clustering on high-cardinality fields.
- **Cloud-Ready**: Easily deployable to Cloud Composer or GCP-native orchestration.
- **Incremental-Load Ready**: Structure supports future delta-based ingestion for scalability.

---

## 6. 🚀 How to Run This Project

### 🔧 6.1 Set Up Docker Environment

1. Place `docker-compose.yaml` in the project root.
2. Run:
   ```bash
   docker-compose up
   ```
3. Access Airflow UI at [http://localhost:8080](http://localhost:8080).

---

### 🔗 6.2 Configure Airflow Connections

- **PostgreSQL**
  - Add connection in UI: `Admin → Connections → Add`
  - Conn ID: `postgres`

- **Google Cloud**
  - Service Account with BigQuery & GCS access
  - Connection Type: *Google Cloud*
  - Upload JSON key in Airflow

- **Slack (Optional)**
  - Add webhook connection for alerts

---

### 📥 6.3 Prepare & Run

1. Place all raw CSVs in `datas/` directory. Download data in [here](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data)
2. In Airflow UI:
   - Run `raw_to_postgres` DAG
   - Run `pg_to_gcp` DAG
3. Monitor task success/failure
4. View Slack notifications (if configured)

---

## 7. 📌 Future Improvements

| Area               | Suggestions                                                                  |
| ------------------ | ---------------------------------------------------------------------------- |
| 📦 Data Expansion  | Add more datasets to uncover broader and deeper business insights.           |
| ☁️ Cloud Readiness | Migrate the pipeline to a **Cloud VM or Composer** for production stability. |

---

## 8. 📚 Reference

- Dataset: [Brazilian E-Commerce Public Dataset by Olist on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data)
- Company: [Olist](https://olist.com/)

---

## 📌 Summary

This project demonstrates how a structured, cloud-integrated data engineering workflow can turn raw e-commerce files into meaningful, actionable insights through PostgreSQL, GCS, BigQuery, and Apache Airflow. With modular design, clear separation of concerns, and automation, it lays the foundation for scalable analytics in a modern data stack.