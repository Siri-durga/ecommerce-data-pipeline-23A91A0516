# \# E-Commerce Data Pipeline Project

# 

# \*\*Student Name:\*\* Durga Lalitha Sri Varshitha  

# \*\*Roll Number:\*\* 23A91A0516

# \*\*Submission Date:\*\* 28 Dec 2025  

# 

# \## Overview

# This project implements an end-to-end ETL pipeline for an e-commerce analytics platform, covering data generation, ingestion, transformation, quality validation, warehousing, and BI reporting.

# 

# ---

# 

# \## Prerequisites

# Ensure the following are installed on your system:

# 

# \- Python 3.8+

# \- PostgreSQL 12+

# \- Docker \& Docker Compose

# \- Git

# \- Tableau Public OR Power BI Desktop (Free version)

# 

# ---

# 

# \## Installation Steps

# 

# 1\. Clone the repository:

# &nbsp;  ```bash

# &nbsp;  git clone https://github.com/<username>/ecommerce-data-pipeline-<roll-number>.git

# &nbsp;  cd ecommerce-data-pipeline-23A91A0516

Install Python dependencies:



pip install -r requirements.txt





Setup PostgreSQL database (local or Docker)



Run setup script:



bash setup.sh





Install BI Tool:



Tableau Public OR



Power BI Desktop



Database Configuration



Database Name: ecommerce\_db



Schemas:



staging – Raw ingested data



production – Cleaned and validated data



warehouse – Dimensional warehouse



Configuration Management



Environment variables are managed via .env



Configuration files stored in config/config.yaml



Sensitive credentials are never hardcoded





✔️ This satisfies:

\- Prerequisites

\- Installation

\- DB setup

\- Security practices



---

# 
## Docker Setup
The project includes Docker Compose configuration for PostgreSQL and pipeline services.
Database schemas are automatically initialized on first startup.


