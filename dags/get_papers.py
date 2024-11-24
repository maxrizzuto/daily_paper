import requests
from airflow.decorators import dag, task
from airflow.models import Variable

with DAG(
    dag_id="get_papers",
    catchup=False,
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    tags=["papers"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2)
    }
) as dag:
    @task
    def scholar_to_s3():
        pass

    @task
    def s3_to_rds():
        pass

    scholar_to_s3 >> s3_to_rds
