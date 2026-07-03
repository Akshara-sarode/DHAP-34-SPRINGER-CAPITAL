# Dataset Manifest

## Dataset Name
customer_care_emails

## Local CSV Folder Path
sample_data/

## Target PostgreSQL Table
public.customer_care_emails

## Description
This dataset contains customer care email records that will be ingested from a local CSV file into PostgreSQL using a Dockerized Apache Airflow pipeline.

## Source
- Dataset selected from the approved dataset list (Analysis Status: Complete)
- CSV downloaded from the provided SharePoint location

## Notes
- The CSV file is stored in the `sample_data/` folder.
- The schema for this dataset is defined in `config/schema_expected.yaml`.
- The PostgreSQL table definition is provided in `config/create_table.sql`.
