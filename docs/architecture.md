\# E-Commerce Data Pipeline Architecture



\## Overview

This document describes the architecture of the E-Commerce Data Analytics Platform.



\## System Components



\### 1. Data Generation Layer

\- Generates synthetic data using Python Faker

\- Outputs CSV files for customers, products, transactions



\### 2. Data Ingestion Layer

\- Loads raw CSVs into PostgreSQL staging schema

\- Batch ingestion using psycopg2



\### 3. Data Storage Layer

\- \*\*Staging Schema\*\*: Raw data as-is

\- \*\*Production Schema\*\*: Cleaned and normalized (3NF)

\- \*\*Warehouse Schema\*\*: Star schema for analytics



\### 4. Data Processing Layer

\- Data validation and cleansing

\- SCD Type 2 handling

\- Aggregate table generation



\### 5. Data Serving Layer

\- Analytical SQL queries

\- Optimized aggregates



\### 6. Visualization Layer

\- Power BI dashboards

\- Interactive analytics



\### 7. Orchestration Layer

\- Pipeline orchestrator

\- Scheduler

\- Monitoring \& alerting



\## Data Models



\### Staging Model

\- Exact CSV structure

\- Minimal validation



\### Production Model

\- Normalized tables

\- Enforced constraints



\### Warehouse Model

\- Dimensions + Fact

\- Optimized for BI queries



\## Technologies Used

\- Python 3.11

\- PostgreSQL 14

\- Power BI Desktop

\- Docker

\- Pytest



\## Deployment Architecture

\- Dockerized PostgreSQL

\- Python-based pipeline execution



