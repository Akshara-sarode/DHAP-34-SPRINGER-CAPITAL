# DHAP-34 – Customer Care Emails
## Local CSV → PostgreSQL using Dockerized Apache Airflow

## Project Overview

This project is part of **DHAP-34**.

The pipeline reads a local CSV dataset, validates it against a predefined schema, performs basic data transformation, and loads the cleaned data into a PostgreSQL database using Apache Airflow running inside Docker.

---

# Tech Stack

- Apache Airflow 2.9.3
- Docker & Docker Compose
- PostgreSQL 15
- Python 3.11
- Pandas
- PyYAML
- psycopg2
- PostgresHook

---

# Project Structure

```
customer_care_emails/
│
├── dags/
│   └── load_customer_care_emails_to_postgres.py
│
├── MANIFEST.md
├── schema_expected.yaml
├── create_table.sql
├── dataset.csv
│
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── .env.example
```

---

# Prerequisites

Before running the project install:

- Docker Desktop
- Docker Compose

Verify Docker is installed:

```bash
docker --version
docker compose version
```

---

# Starting the Project

Navigate to the project directory.

```bash
cd "/Users/<your-username>/Desktop/Springer Capital/DHAP-34-SPRINGER-CAPITAL-main/intern-project/Akshara-sarode/project-DHAP-34/customer_care_emails"
```

---

## Stop Existing Containers

```bash
docker compose down --remove-orphans
```

---

## Build the Docker Image

```bash
docker compose build --no-cache
```

---

## Initialize Airflow

```bash
docker compose up airflow-init
```

Wait until the container exits successfully.

---

## Start Airflow

```bash
docker compose up -d
```

---

# Open Airflow

Open:

```
http://localhost:8080
```

Login credentials:

Username

```
airflow
```

Password

```
airflow
```

---

# Running the DAG

Locate the DAG

```
load_customer_care_emails_to_postgres
```

Turn the DAG ON.

Click **Trigger DAG**.

The tasks execute in the following order:

```
validate_source
        ↓
transform_csv
        ↓
prepare_table
        ↓
load_to_postgres
```

All tasks should complete successfully.

---

# Data Validation

The pipeline validates:

- MANIFEST.md exists
- Dataset status is **done**
- CSV file exists
- Schema file exists
- CSV columns match schema
- Required fields are not null

If validation fails, the DAG stops before loading data into PostgreSQL.

---

# PostgreSQL Verification

Verify rows loaded successfully.

```bash
docker compose exec target-postgres psql \
-U postgres \
-d customer_care_emails \
-c "SELECT COUNT(*) FROM public.customer_care_emails;"
```

Example output

```
 count
-------
5
```

---

# Re-running the DAG

The pipeline is idempotent.

Before loading data, the target table is truncated.

Running the DAG multiple times does **not** create duplicate rows.

---

# Validation Failure Test

To test validation:

1. Modify the CSV header.
2. Remove a required column.
3. Trigger the DAG again.

Expected result:

The DAG fails during the **validate_source** task.

No data is loaded into PostgreSQL.

---

# Docker Commands

Build

```bash
docker compose build --no-cache
```

Initialize

```bash
docker compose up airflow-init
```

Start

```bash
docker compose up -d
```

Stop

```bash
docker compose down
```

Check containers

```bash
docker compose ps
```

---

# Troubleshooting

### Airflow UI does not open

Restart the containers.

```bash
docker compose down
docker compose up -d
```

---

### DAG does not appear

Verify the DAG file exists inside:

```
dags/
```

Restart Airflow.

---

### Validation fails

Check:

- MANIFEST.md
- schema_expected.yaml
- create_table.sql
- dataset.csv

Ensure all required files are present.

---

### PostgreSQL contains no data

Verify the DAG completed successfully.

Run:

```bash
docker compose exec target-postgres psql \
-U postgres \
-d customer_care_emails \
-c "SELECT COUNT(*) FROM public.customer_care_emails;"
```

---

# Project Outcome

The completed pipeline performs the following workflow:

```
Local CSV
      │
      ▼
Validate Schema
      │
      ▼
Transform Data
      │
      ▼
Create PostgreSQL Table
      │
      ▼
Load Data
      │
      ▼
PostgreSQL
```

The project satisfies the DHAP-34 requirements for:

- Dockerized Airflow environment
- Schema validation
- Data transformation
- PostgreSQL loading
- Idempotent execution
- Documentation and reproducibility
