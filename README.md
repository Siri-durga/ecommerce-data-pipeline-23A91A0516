# \# E-Commerce Data Pipeline Project

# 

# \*\*Student Name:\*\* Durga Lalitha Sri Varshitha  

# \*\*Roll Number:\*\* 23A91A0516

# \*\*Submission Date:\*\* 29 Dec 2025  

# 

📌 Project Architecture
This project implements an end-to-end E-Commerce Data Analytics Pipeline that processes raw transactional data into analytics-ready datasets and interactive BI dashboards.

Data Flow:

graphql
Copy code
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
🛠 Technology Stack
Layer	Technology
Data Generation	Python, Faker
Database	PostgreSQL
ETL / Transformation	Python (Pandas, psycopg2)
Orchestration	Python Scheduler
Monitoring	Python (Custom Monitoring Scripts)
BI Tool	Power BI Desktop
Containerization	Docker
Testing	Pytest, pytest-cov

📁 Project Structure
kotlin
Copy code
ecommerce-data-pipeline/
├── data/
│   ├── raw/
│   ├── staging/
│   └── processed/
├── scripts/
│   ├── data_generation/
│   ├── ingestion/
│   ├── transformation/
│   ├── monitoring/
│   └── scheduler.py
├── sql/
│   ├── ddl/
│   └── queries/
├── dashboards/
│   ├── powerbi/
│   └── screenshots/
├── tests/
├── docs/
├── logs/
└── README.md
⚙️ Setup Instructions
Install Python 3.10+

Install PostgreSQL

Clone repository

bash
Copy code
git clone <repo-url>
cd ecommerce-data-pipeline
Install dependencies

bash
Copy code
pip install -r requirements.txt
Configure database in config.yaml

Ensure PostgreSQL is running

▶️ Running the Pipeline
Full Pipeline Execution

bash
Copy code
python scripts/pipeline_orchestrator.py
Individual Steps

bash
Copy code
python scripts/data_generation/generate_data.py
python scripts/ingestion/ingest_to_staging.py
python scripts/transformation/staging_to_production.py
python scripts/transformation/load_warehouse.py
python scripts/transformation/generate_analytics.py
🧪 Running Tests
bash
Copy code
python scripts/run_tests.py
or

bash
Copy code
pytest tests/ -v
📊 Dashboard Access
Power BI File: dashboards/powerbi/ecommerce_analytics.pbix

Screenshots: dashboards/screenshots/

🗄 Database Schemas
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

Revenue shows steady growth across 2024

VIP customers contribute a major portion of total revenue

Top 5 states account for majority of sales

Online payment methods dominate transactions

⚠️ Challenges & Solutions
Challenge	Solution
PostgreSQL connection issues	Used Docker & proper config
Data duplication	Implemented idempotent transformations
Slow queries	Added indexes & aggregate tables
Scheduling reliability	Lock-file based scheduler
Monitoring complexity	Centralized monitoring report

🚀 Future Enhancements
Real-time streaming with Apache Kafka

Cloud deployment (AWS / Azure / GCP)

ML-based demand forecasting

Real-time alerting system

📞 Contact
Name: Siri Durga
Roll Number: 23A91A0516
Email: your-email@example.com