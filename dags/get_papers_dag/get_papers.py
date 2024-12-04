import json
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from lxml import html

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
    def database_readings():
        """
        Make sure to reference source name and url when storing to S3 for attribution purposes
        """

        # construct tree for scraping from website
        src = 'https://github.com/rxin/db-readings'
        page = requests.get(src)
        tree = html.fromstring(page.content)

        # get papers and add to dictionary
        papers = dict()
        subcategories = tree.xpath('//ul/preceding-sibling::div[@class="markdown-heading"]')[2:-2]
        for subcategory in subcategories:
            subcat_name = subcategory.xpath('./h2/a')[0].text.strip()
            subcat_papers = subcategory.xpath('./following-sibling::ul[1]//a[contains(@href, ".pdf")]')
            for paper in subcat_papers:
                paper_link = src + paper.xpath('./@href')[0]
                paper_name = paper.text
                papers[paper_link] = {'name': paper_name, 'category': 'databases', 'subcategory': subcat_name}

        print(json.dumps(papers, sort_keys=True, indent=4))
        return papers

    @task
    def data_source_b():
        pass

    @task
    def data_source_c():
        pass

    @task
    def s3_to_rds():
        pass

    [database_readings(), data_source_b(), data_source_c()] >> s3_to_rds()
