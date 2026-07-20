# DHAP-34 – Local CSV → PostgreSQL via Dockerized Airflow

## Overview

This project implements a Dockerized Apache Airflow pipeline that ingests a local CSV dataset, validates it against a predefined schema, performs basic data transformation, and loads the cleaned data into a PostgreSQL database.

The project was completed as part of **DHAP-34** and demonstrates an end-to-end ETL workflow using Apache Airflow, Docker, PostgreSQL, and Python.

---

# Objective

Build a containerized Apache Airflow pipeline that:

- Reads a local CSV file
- Validates the dataset against a schema definition
- Performs basic data cleaning and transformation
- Creates the destination PostgreSQL table if needed
- Loads the transformed data into PostgreSQL
- Supports safe re-execution without creating duplicate records

---

# Architecture

```
Local CSV
    │
    ▼
Apache Airflow (Docker)
    │
    ├── Validate Dataset
    ├── Transform Data
    ├── Prepare PostgreSQL Table
    └── Load Data
           │
           ▼
      PostgreSQL
```

All services run locally using Docker Compose.

---

# Technology Stack

- Apache Airflow 2.9.3
- Docker & Docker Compose
- PostgreSQL
- Python 3.11
- Pandas
- PyYAML
- psycopg2-binary
- Apache Airflow Postgres Provider

---

# Project Structure

```
customer_care_emails/
│
├── dags/
│   └── load_customer_care_emails_to_postgres.py
│
├── dataset.csv
├── schema_expected.yaml
├── create_table.sql
├── MANIFEST.md
│
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
├── .env.example
└── README.md
```

---

# Prerequisites

Install the following software before starting:

- Docker Desktop
- Docker Compose
- Git

Verify installation:

```bash
docker --version
docker compose version
```

---

# Setup

Clone the repository.

```bash
git clone https://github.com/Akshara-sarode/DHAP-34-SPRINGER-CAPITAL.git
```

Navigate to the project directory.

```bash
cd DHAP-34-SPRINGER-CAPITAL/intern-project/Akshara-sarode/project-DHAP-34/customer_care_emails
```

---

# Build the Environment

Stop any existing containers.

```bash
docker compose down --remove-orphans
```

Build the Docker images.

```bash
docker compose build --no-cache
```

Initialize Apache Airflow.

```bash
docker compose up airflow-init
```

Start all services.

```bash
docker compose up -d
```

Verify running containers.

```bash
docker compose ps
```

---

# Access Airflow

Open your browser and navigate to:

```
http://localhost:8080
```

Default credentials:

Username

```
airflow
```

Password

```
airflow
```

---

# Airflow DAG

The project contains one DAG:

```
load_customer_care_emails_to_postgres
```

Workflow:

```
validate_source
        │
        ▼
transform_csv
        │
        ▼
prepare_table
        │
        ▼
load_to_postgres
```

The DAG is configured with:

- `catchup=False`
- Schema validation before loading
- Safe reload using truncate-before-load
- PostgreSQL connection through Airflow's PostgresHook

---

# Data Validation

Before loading data, the pipeline validates:

- Dataset manifest exists
- Dataset status is marked as **done**
- CSV file exists
- Schema file exists
- CSV columns match the schema definition
- Required fields are present

If validation fails, the DAG stops before any data is loaded.

---

# Verify Data in PostgreSQL

After a successful DAG run, verify that data has been loaded.

```bash
docker compose exec target-postgres \
psql -U postgres \
-d customer_care_emails \
-c "SELECT COUNT(*) FROM public.customer_care_emails;"
```

Example output:

```
 count
-------
5
```

---

# Re-running the Pipeline

The pipeline is idempotent.

Before inserting new records, the target table is cleared to prevent duplicate data.

Running the DAG multiple times results in the same number of rows being loaded.

---

# Validation Test

To verify schema validation:

1. Modify the CSV header (for example, rename or remove a required column).
2. Trigger the DAG again.

Expected result:

- The `validate_source` task fails.
- No records are loaded into PostgreSQL.

---

# Useful Docker Commands

Build containers:

```bash
docker compose build --no-cache
```

Initialize Airflow:

```bash
docker compose up airflow-init
```

Start services:

```bash
docker compose up -d
```

Stop services:

```bash
docker compose down
```

View running containers:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs
```

---

# Troubleshooting

## Airflow UI is not accessible

Restart the Docker services.

```bash
docker compose down
docker compose up -d
```

---

## DAG is not visible

- Verify the DAG file is located inside the `dags/` directory.
- Wait for the Airflow scheduler to refresh.
- Restart the containers if necessary.

---

## Validation fails

Verify that:

- `MANIFEST.md` exists.
- `dataset.csv` exists.
- `schema_expected.yaml` exists.
- `create_table.sql` exists.
- The CSV header matches the schema.

---

## PostgreSQL table is empty

Confirm that all Airflow tasks completed successfully before checking the database.

---

# Project Outcomes

This implementation successfully delivers:

- Dockerized Apache Airflow environment
- Dockerized PostgreSQL database
- CSV ingestion pipeline
- Schema-based validation
- Data transformation
- PostgreSQL data loading
- Idempotent execution
- Reproducible setup through Docker Compose
- Complete project documentation

---

# Akshara Avinash Sarode 
# LinkedIn: https://www.linkedin.com/in/akshara-avinash-sarode/
