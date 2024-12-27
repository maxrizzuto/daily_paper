import json
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.decorators import task
from get_papers_utils import API_Throttler, doi_by_title, scrape_pdf_to_s3
from lxml import html

with DAG(
    dag_id="mass_papers",
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
    def data_source_a():
        pass

    @task
    def data_source_b():
        pass

    @task
    def data_source_c():
        pass

    @task
    def s3_to_rds():
        pass

    [data_source_a(), data_source_b(), data_source_c()] >> s3_to_rds()