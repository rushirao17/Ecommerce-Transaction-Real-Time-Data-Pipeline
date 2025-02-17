# Real-Time E-Commerce Data Pipeline

This repository contains the code and resources for a real-time data pipeline that processes e-commerce transaction data using **Kafka**, **AWS Lambda**, **AWS RDS**, **Databricks (PySpark)**, **AWS S3**, and **Power BI**. The goal is to provide scalable, near real-time insights into sales trends, customer behavior, and other key metrics.

---

## Overview

1. **Data Ingestion (Kafka → AWS Lambda → RDS)**  
   - Kafka producers send transaction events (Orders, Order Details, Payments, Customers).  
   - AWS Lambda processes these events in near real-time.  
   - Cleaned and categorized data is stored in **AWS RDS** for transactional queries.

2. **Data Processing & Transformation (Databricks / PySpark → AWS S3)**  
   - **Databricks** loads data from RDS (or directly from Kafka if needed).  
   - Data is transformed using **PySpark** (joins, aggregations, data enrichment).  
   - The processed data is stored in **AWS S3** in a columnar format (e.g., Parquet or Delta Lake).

3. **Analytics & Visualization (Power BI)**  
   - **Power BI** connects to the transformed data in S3 (via Athena or Redshift) for scalable querying.  
   - Dashboards and reports provide real-time insights into **sales trends**, **customer analytics**, etc.

4. **Monitoring & Observability (AWS CloudWatch)**  
   - **AWS CloudWatch** monitors Lambda execution, RDS performance, and Databricks job status.  
   - Alarms and logs are configured for proactive troubleshooting.

---

## Architecture Diagram

Below is a high-level diagram of the improved architecture:

![Real-Time E-Commerce Pipeline Architecture](https://user-images.githubusercontent.com/images/architecture_diagram.png)

---

## Key Components

1. **Kafka**  
   - Acts as the entry point for real-time data ingestion.  
   - Producers publish transaction events (e.g., orders, payments).

2. **AWS Lambda**  
   - Subscribes to Kafka topics (directly or via Amazon MSK / EventBridge).  
   - Cleans and validates data in near real-time.  
   - Writes valid records to **AWS RDS**.

3. **AWS RDS**  
   - Relational database hosting core e-commerce tables: **Orders**, **Order Details**, **Payments**, and **Customers**.  
   - Ideal for transactional queries and data integrity.

4. **Databricks (PySpark)**  
   - Loads data from RDS or directly from Kafka (if required).  
   - Performs batch/streaming transformations at scale using PySpark.  
   - Writes transformed data to **AWS S3** in a columnar format (e.g., Parquet or Delta Lake).

5. **AWS S3**  
   - Serves as a data lake for storing transformed datasets.  
   - Scalable, cost-effective, and supports querying via Athena or Redshift Spectrum.

6. **Power BI**  
   - Connects to S3 through Athena or Redshift.  
   - Creates dashboards and reports for sales trends, customer analytics, and operational insights.

7. **AWS CloudWatch**  
   - Monitors Lambda performance (invocations, errors), RDS metrics (CPU, connections), and Databricks job logs.  
   - Alarms can be configured to send notifications on threshold breaches.

---
