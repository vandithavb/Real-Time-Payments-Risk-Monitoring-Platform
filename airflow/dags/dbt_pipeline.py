from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/workspaces/Real-Time-Payments-Risk-Monitoring-Platform/stripe_dbt"
DBT_PROFILES_DIR = "/home/codespace/.dbt"

with DAG(
    dag_id="dbt_stripe_pipeline",
    start_date=datetime(2026, 4, 20),
    schedule=None,
    catchup=False,
    tags=["dbt", "bigquery"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt run --profiles-dir {DBT_PROFILES_DIR}
        """
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt test --profiles-dir {DBT_PROFILES_DIR}
        """
    )

    dbt_run >> dbt_test