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

### Kafka Topics

The following topics are used in the pipeline:

- stripe.raw  
- stripe.processed  
- stripe.dlq  

Topics may be auto-created by Redpanda when first written to.  
For better control and reproducibility, they can be explicitly created:

```bash
rpk topic create stripe.raw
rpk topic create stripe.processed
rpk topic create stripe.dlq
```

## 🔄 Data Flow

1. Stripe sends webhook events → FastAPI  
2. FastAPI publishes events to Kafka (`stripe.raw`)  
3. PyFlink (stream processing):
   - normalizes events  
   - routes valid events → `stripe.processed`  
   - routes invalid/unsupported → `stripe.dlq`  
4. BigQuery Loader (consumer service):
   - continuously reads from Kafka topics  
   - appends data to:
     - `stripe_dw.stripe_processed`  
     - `stripe_dw.stripe_dlq`  
5. Airflow triggers dbt workflows  
6. dbt transforms raw data into analytics tables (`stripe_analytics`)

---

## 📊 Data Models (dbt)
### 🔹 BigQuery Output

![BigQuery Tables](https://github.com/vandithavb/Real-Time-Payments-Risk-Monitoring-Platform/blob/main/images/BigQuery.png)

> BigQuery datasets showing raw (`stripe_dw`) and transformed (`stripe_analytics`) tables.

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

### 🔹 Airflow DAG Execution

![Airflow DAG](https://github.com/vandithavb/Real-Time-Payments-Risk-Monitoring-Platform/blob/main/images/airflow_dag.png)

> Airflow DAG showing successful execution of dbt transformations (dbt_run → dbt_test).   

- Airflow orchestrates the transformation layer using dbt  
- DAG: `dbt_run → dbt_test`  
- Ensures:
  - transformations run in the correct order  
  - data quality checks are executed after transformations  
- Airflow does not process or move data — it only schedules and triggers workflows  

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

### Prerequisites
Ensure the following BigQuery datasets are created:
- stripe_dw (raw data)
- stripe_analytics (dbt models)
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

# 📌 Key Takeaway

This project demonstrates a complete modern data pipeline that combines:

- real-time ingestion using Kafka  
- stream processing using Flink  
- scalable storage using BigQuery  
- transformation using dbt  
- orchestration using Airflow  

It reflects how real-time and batch systems work together in production-grade data architectures.

## 👤 Author

Vanditha

