# DHAP 34 — Story 1 Manifest

## Dataset
customer_care_emails

## Source
Completed CSV dataset selected from the approved dataset list.

## Local CSV folder path
sample_data/

## Target PostgreSQL table
public.customer_care_emails

## Notes
- The source CSV contains one email/message record per row.
- `email_types` and `product_types` are stored as stringified list values in the raw CSV; the pipeline can keep them as `text` in PostgreSQL for the initial load.
- The external PostgreSQL target is expected to be reachable from Dockerized Airflow using environment variables from `.env`.
