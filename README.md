# 🛒 E-Commerce Data Pipeline Project

![Build Status](https://img.shields.io/badge/build-passing-brightgreen)


## 📌 Project Overview
This project implements an **end-to-end E-Commerce Data Analytics Pipeline** that transforms raw transactional data into **analytics-ready datasets** and **interactive BI dashboards**.  
It follows modern data engineering practices including ETL pipelines, data warehousing, automation, monitoring, and testing.

---

## 👩‍🎓 Student Information
- **Name:** Durga Lalitha Sri Varshitha  
- **Roll Number:** 23A91A0516  
- **Submission Date:** 26 Dec 2025  
- **Email:** 23A91A0516@aec.edu.in  

---

## 🏗️ Project Architecture

### Data Flow
Raw CSV Data
↓
Staging Schema (PostgreSQL)
↓
Production Schema (Cleaned & Normalized)
↓
Warehouse Schema (Star Schema)
↓
Analytics Queries & Aggregates
↓
BI Dashboard (Power BI)

yaml
Copy code

---

## 🛠️ Technology Stack

| Layer | Technology |
|------|-----------|
| Data Generation | Python, Faker |
| Database | PostgreSQL |
| ETL / Transformation | Python (Pandas, psycopg2) |
| Orchestration | Python Scheduler |
| Monitoring | Python (Custom Monitoring Scripts) |
| BI Tool | Power BI Desktop |
| Containerization | Docker |
| Testing | Pytest, pytest-cov |

---

## 📁 Project Structure
ecommerce-data-pipeline/
├── data/
│ ├── raw/
│ ├── staging/
│ └── processed/
├── scripts/
│ ├── data_generation/
│ ├── ingestion/
│ ├── transformation/
│ ├── monitoring/
│ └── scheduler.py
├── sql/
│ ├── ddl/
│ └── queries/
├── dashboards/
│ ├── powerbi/
│ └── screenshots/
├── tests/
├── docs/
├── logs/
└── README.md

yaml
Copy code

---

## ⚙️ Setup Instructions

### Prerequisites
- Python **3.10+**
- PostgreSQL
- Docker (optional but recommended)

### Installation
```bash
git clone <repo-url>
cd ecommerce-data-pipeline
pip install -r requirements.txt
```

Configuration
Update database credentials in config.yaml

Ensure PostgreSQL service is running

▶️ Running the Pipeline
Full Pipeline Execution
bash
Copy code
python scripts/pipeline_orchestrator.py
Individual Steps
```bash
Copy code
python scripts/data_generation/generate_data.py
python scripts/ingestion/ingest_to_staging.py
python scripts/transformation/staging_to_production.py
python scripts/transformation/load_warehouse.py
python scripts/transformation/generate_analytics.py
```

🧪 Running Tests
```bash
Copy code
python scripts/run_tests.py
or

bash
Copy code
pytest tests/ -v
```

📊 Dashboard Access
Power BI File: dashboards/powerbi/ecommerce_analytics.pbix

Dashboard Screenshots: dashboards/screenshots/

🗄️ Database Schemas
Staging Schema
staging.customers

staging.products

staging.transactions

staging.transaction_items

Production Schema
production.customers

production.products

production.transactions

production.transaction_items

Warehouse Schema
warehouse.dim_customers

warehouse.dim_products

warehouse.dim_date

warehouse.dim_payment_method

warehouse.fact_sales

warehouse.agg_daily_sales

warehouse.agg_product_performance

warehouse.agg_customer_metrics

📈 Key Insights from Analytics
Electronics category generates the highest revenue

Revenue shows steady growth throughout 2024

VIP customers contribute a major share of total revenue

Top 5 states account for the majority of sales

Online payment methods dominate transactions

⚠️ Challenges & Solutions
Challenge	Solution
PostgreSQL connection issues	Dockerized setup & proper configuration
Data duplication	Idempotent transformation logic
Slow queries	Indexing and aggregate tables
Scheduling reliability	Lock-file based scheduler
Monitoring complexity	Centralized monitoring reports

🚀 Future Enhancements
Real-time streaming using Apache Kafka

Cloud deployment (AWS / Azure / GCP)

Machine Learning based demand forecasting

Real-time alerting and anomaly detection

📞 Contact
Durga Lalitha Sri Varshitha
📧 23A91A0516@aec.edu.in
🎓 Roll Number: 23A91A0516


