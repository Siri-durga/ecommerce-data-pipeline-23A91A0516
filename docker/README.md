# Docker Deployment Guide – E-Commerce Data Pipeline

This document explains how to run the E-Commerce Data Pipeline using Docker and Docker Compose.

---

## 1. Prerequisites

Before running the project, ensure the following are installed:

### Software Requirements
- Docker Engine: **v20.10+**
- Docker Compose: **v2.0+**

### System Requirements
- Minimum RAM: **4 GB**
- Disk Space: **10 GB free**
- OS: Windows / Linux / macOS (Docker-supported)

Verify installation:
```bash
docker --version
docker compose version
2. Quick Start Guide
Step 1: Build Docker Images
bash
Copy code
docker compose build
Step 2: Start Services
bash
Copy code
docker compose up -d
This starts:

PostgreSQL database container

Data pipeline container

Step 3: Verify Running Containers
bash
Copy code
docker ps
Expected services:

postgres

pipeline

Step 4: Verify Database Health
bash
Copy code
docker logs postgres
PostgreSQL should show:

pgsql
Copy code
database system is ready to accept connections
Step 5: Run Pipeline Inside Container
bash
Copy code
docker compose exec pipeline python scripts/pipeline_orchestrator.py
Step 6: Access PostgreSQL Database
bash
Copy code
docker compose exec postgres psql -U postgres -d ecommerce_db
Step 7: View Logs
bash
Copy code
docker compose logs -f
Step 8: Stop Services
bash
Copy code
docker compose down
Step 9: Cleanup (Remove Volumes & Containers)
bash
Copy code
docker compose down -v
3. Configuration
Environment Variables
Configured in docker-compose.yml:

POSTGRES_DB

POSTGRES_USER

POSTGRES_PASSWORD

DB_HOST

DB_PORT

Volume Mounts
PostgreSQL data stored in Docker volume

Ensures data persistence across restarts

Network Configuration
Default Docker Compose network

Services communicate using service names (e.g., postgres)

Resource Limits
CPU & memory managed by Docker Engine

Can be extended in docker-compose if required

4. Data Persistence
PostgreSQL uses a named volume

Data remains intact after container restart

Logs and generated data persist if volumes are mounted

Example:

bash
Copy code
docker compose down
docker compose up -d
Data remains available.

5. Troubleshooting
Issue: Port Already in Use
Solution:

bash
Copy code
docker compose down
or change exposed port in docker-compose.yml

Issue: Database Not Ready
Cause: Pipeline started before DB
Solution: Health check ensures pipeline waits for DB readiness

Issue: Container Fails to Start
Solution:

bash
Copy code
docker logs <container_name>
Issue: Volume Permission Error
Solution:

bash
Copy code
docker compose down -v
docker compose up -d
Issue: Network Connectivity Problems
Solution:

Ensure services are on same Docker network

Use service names, not localhost

6. Architecture Summary
Services are isolated using Docker containers

PostgreSQL health check ensures dependency readiness

Pipeline connects using service name (postgres)

Volumes ensure persistent data storage

Docker Compose orchestrates services cleanly