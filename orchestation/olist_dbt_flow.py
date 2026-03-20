from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor
from datetime import datetime, timedelta

# Default arguments for the pipeline
default_args = {
    'owner': 'analytics_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='olist_ecommerce_transformation',
    default_args=default_args,
    description='Orchestrates the Olist Medallion Pipeline in dbt Cloud',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['production', 'olist'],
) as dag:

    # 1. Trigger the dbt Cloud Job we created earlier
    trigger_dbt_job = DbtCloudRunJobOperator(
        task_id='trigger_dbt_production_job',
        dbt_cloud_conn_id='dbt_cloud_default',
        job_id=70471823569979,
        check_interval=10,
        timeout=300,
    )

    # 2. Wait and verify the job finished successfully
    wait_for_dbt_job = DbtCloudJobRunSensor(
        task_id='wait_for_dbt_production_job',
        dbt_cloud_conn_id='dbt_cloud_default',
        run_id=trigger_dbt_job.output,
        timeout=600,
    )

    trigger_dbt_job >> wait_for_dbt_job