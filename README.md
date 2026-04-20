# 🚀 Real-Time Payments Risk Monitoring Platform

## 🎯 Goal

To build an end-to-end **real-time data pipeline** for monitoring payment transactions and identifying risk signals using modern data engineering tools.

---

## 📌 Overview

This project ingests payment events from Stripe, processes them in real time using Kafka and Flink, stores them in BigQuery, transforms them using dbt, and orchestrates workflows using Airflow.

It demonstrates a **production-style data pipeline** combining streaming, batch transformation, and orchestration.

---

## 🏗️ Architecture

![Architecture](https://github.com/vandithavb/Real-Time-Payments-Risk-Monitoring-Platform/blob/main/images/architecture_diagram.png)

> End-to-end pipeline showing ingestion, processing, storage, transformation, and orchestration layers.

---

## 🧱 Tech Stack

- **API Layer**: FastAPI  
- **Streaming Platform**: Kafka (Redpanda)  
- **Stream Processing**: Apache Flink (PyFlink)  
- **Data Warehouse**: Google BigQuery  
- **Transformation Layer**: dbt (BigQuery)  
- **Orchestration**: Apache Airflow  
- **Language**: Python  

---

## 🔄 Data Flow

1. Stripe sends webhook events → FastAPI  
2. FastAPI publishes events to Kafka (`stripe.raw`)  
3. PyFlink:
   - normalizes events  
   - routes valid events → `stripe.processed`  
   - routes invalid/unsupported → `stripe.dlq`  
4. BigQuery Loader:
   - consumes Kafka topics  
   - appends data to:
     - `stripe_dw.stripe_processed`  
     - `stripe_dw.stripe_dlq`  
5. Airflow:
   - triggers dbt transformations  
6. dbt:
   - builds staging + fact tables in `stripe_analytics`  

---

## 📊 Data Models (dbt)

### 🔹 Staging Layer

- `stg_stripe_processed`
- `stg_stripe_dlq`

---

### 🔹 Fact Tables

#### 1. `fct_successful_charges`

- Grain: **one successful charge event**
- Source: `charge.succeeded`
- Purpose:
  - revenue tracking  
  - transaction analytics  

---

#### 2. `fct_risk_events`

- Includes:
  - failed payments  
  - disputes  
  - refunds  

- Purpose:
  - risk monitoring  
  - anomaly detection  

---

## ⚙️ Orchestration (Airflow)

- Airflow orchestrates dbt workflows
- DAG: `dbt_run → dbt_test`
- Ensures transformations run in correct order
- Performs data quality validation

---

## 🧪 Data Quality Strategy

- Invalid or malformed events routed to **DLQ**
- dbt tests ensure:
  - non-null critical fields  
- Clear separation of:
  - valid vs invalid data  
  - success vs risk events  

---

## ▶️ How to Run

### 1. Start infrastructure

```bash
docker-compose up
```

### 2. Start FastAPI (webhook listener)
```bash
uvicorn app.main:app --reload
```
### 3. Start Stripe listener (trigger events)
```bash
stripe listen --forward-to localhost:8000/webhook
```
Trigger a test event:
```bash
stripe trigger payment_intent.succeeded
```
### 4. Start Flink job
```bash
python stripe_processor_job.py
```
### 5. Run BigQuery loader
```bash
python bigquery_loader.py
```
### 6. Run Airflow (dbt orchestration)
```bash
export AIRFLOW_HOME=./airflow
airflow standalone
```
Open:
http://localhost:8080

Trigger DAG:
dbt_stripe_pipeline

(Optional) Run dbt manually
```bash
cd stripe_dbt
dbt run
dbt test
```
## 🧠 Key Design Decisions

- Used **Kafka + Flink** for real-time processing  
- Introduced **DLQ** for handling invalid events  
- Chose `charge.succeeded` as fact table grain to avoid duplication  
- Used **dbt for transformation** instead of embedding logic in Flink  
- Used **Airflow strictly for orchestration** (not streaming)  

---

## 🚀 Future Improvements

- Implement **dbt incremental models** using `processed_at`  
- Dockerize entire pipeline (Airflow + dbt + Flink)  
- Deploy to cloud infrastructure  
- Add alerting for high-risk events  
- Implement CI/CD pipelines  

---

## 📌 Key Takeaway

This project demonstrates a modern data engineering pipeline combining:

- real-time ingestion  
- stream processing  
- data warehousing  
- transformation  
- orchestration  

---

## 👤 Author

Vanditha

