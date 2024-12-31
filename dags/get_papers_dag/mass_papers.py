import json
import os
from datetime import datetime, timedelta

import requests
import scholarly
from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv
from get_papers_utils import API_Throttler, doi_by_title, scrape_pdf_to_s3
from lxml import html

# load environmental variables
load_dotenv()

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
    def google_scholar_papers():

        # make list of categories to search
        categories = ['Data Science', 'Data Engineering', 'Physics', 'Astronomy', 'Literature']
        for category in categories:

            # search for papers on google scholar
            search = scholarly.search_papers(category)
            # papers[doi] = {'title': paper_name, 'category': 'databases', 'subcategory': subcat_name, 'url': paper_link, 'src': src}

            # check if paper is already in s3, or if url already in cache


            # if not, add to s3

            pass
        pass

    @task
    def core_api_papers():

        # initialize API caller
        CORE_API_KEY = os.environ.get("CORE_API_KEY")
        core_api = API_Throttler()
        headers = {"Authorization": f"Bearer {CORE_API_KEY}"}
        base_url = 'https://api.core.ac.uk/v3/search/works?limit=20'

        # make list of categories to search
        categories = ['Data Science', 'Data Engineering', 'Physics', 'Astronomy', 'Literature']

        for category in categories:

            # search for papers
            response = core_api.request(base_url + f'&q={category}', headers=headers)

            for result in response['results']:
                id = result['id']    
                doi = result['doi']
                try:
                    link = [url['url'] for url in result['links'] if url['type']=='display'][0]
                except IndexError:
                    link = result['links'][0]['url']

                data = {'title': result['title'], 'abstract': result['abstract'], 'url': link, 'src': 'CORE API'}

                # add pdf to s3
                pdf_url = f'https://api.core.ac.uk/v3/works/{id}/download'
                scrape_pdf_to_s3(pdf_url, doi, metadata=data)


            # papers[doi] = {'title': paper_name, 'category': 'databases', 'subcategory': subcat_name, 'url': paper_link, 'src': src}

            # check if paper is already in s3, or if url already in cache


            # if not, add to s3

    @task
    def data_source_c():
        pass

    @task
    def s3_to_rds():
        pass

    [core_api_papers(), data_source_c()] >> s3_to_rds()