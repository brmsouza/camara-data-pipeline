# Camara Data Pipeline

End-to-end data engineering pipeline designed to ingest, process, and analyze Brazilian legislative data from the Chamber of Deputies API.

This project implements a scalable data platform using Databricks, PySpark, and Delta Lake, following the Medallion architecture (Bronze, Silver, Gold) to ensure data quality, traceability, and analytical readiness.

## Key Features

- Incremental data ingestion from REST API
- Structured data processing using PySpark
- Medallion architecture (Bronze, Silver, Gold)
- Data quality validation and anomaly detection
- Analytical datasets for parliamentary insights
- Pipeline monitoring and audit tracking

## Data Source

- Brazilian Chamber of Deputies Open Data API  
  https://dadosabertos.camara.leg.br/api/v2

## Project Structure

- **Bronze**: Raw data ingestion from API  
- **Silver**: Cleaned and standardized datasets  
- **Gold**: Analytical models and business-ready tables  

## Technologies

- Databricks
- PySpark
- Delta Lake
- REST API Integration
