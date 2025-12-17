# Cloud Task Manager – Data Engineering Project

## Architecture
Bronze → Silver → Gold Medallion Architecture

## Stack
- Python
- Azure Blob Storage
- Azure SQL Database
- Azure Data Factory
- GitHub

## Features
- Immutable raw ingestion
- Schema-normalized silver layer (Parquet)
- Incremental gold loading using watermark
- SQL analytics-ready tables
- ADF orchestration-ready

## How to Run Locally
1. Run bronze ingestion
2. Run silver transform
3. Run gold load
