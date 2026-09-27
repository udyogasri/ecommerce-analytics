# E-Commerce Data Lakehouse Architecture

## Overview
This project builds an end-to-end Data Lakehouse on AWS S3 using Apache Spark, Delta Lake, and Apache Kafka. 

## Layers
1. **Raw / Ingestion**: Unstructured/Semi-structured CSVs and streaming Kafka JSON events.
2. **Bronze**: Raw data ingested into Delta Lake format for schema enforcement and tracking.
3. **Silver**: Cleaned, deduplicated, and typed data.
4. **Gold**: Business-level aggregations (e.g., daily sales, active users).

## Technologies
- **Storage**: AWS S3 (via PySpark Hadoop integration)
- **Format**: Delta Lake (ACID transactions, time travel)
- **Compute**: PySpark (Batch & Structured Streaming)
- **Message Broker**: Apache Kafka (KRaft mode)
- **Orchestration**: Apache Airflow
- **Catalog**: AWS Glue
- **Query / BI**: AWS Athena & PowerBI

## Folder Structure
- `/data`: Local scratchpad for raw and test data.
- `/src/ingestion`: Scripts to move data from raw to Bronze.
- `/src/etl`: Silver and Gold transformation logic.
- `/src/producers` & `/src/consumers`: Kafka streaming components.
- `/airflow/dags`: Orchestration schedules.
