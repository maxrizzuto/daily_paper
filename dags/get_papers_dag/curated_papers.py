import json
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.decorators import task
from get_papers_utils import API_Throttler, doi_by_title, scrape_pdf_to_s3
from lxml import html

with DAG(
    dag_id="curated_papers",
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

        # initialize api caller
        crossref_api = API_Throttler()

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
                paper_link = src + paper.xpath('./@href')[0].replace('blob', 'raw')
                paper_name = paper.text


                """
                add try except that checks if this is in the cache
                """

                # get article's DOI, store to dictionary, add doi to cache
                doi_url = doi_by_title(paper_name)
                response = crossref_api.request(doi_url)
                doi = response['message']['items'][0]['DOI']
                papers[doi] = {'title': paper_name, 'category': 'databases', 'subcategory': subcat_name, 'url': paper_link, 'src': src}
                crossref_api.cache['dois'].append(doi)

                # update API rate limiting
                max_calls = response.headers['X-Rate-Limit-Limit']
                limit_interval = response.headers['X-Rate-Limit-Interval']
                crossref_api.max_calls = max_calls
                crossref_api.limit_interval = limit_interval

                # get pdf and add to s3
                scrape_pdf_to_s3(paper_link, doi, metadata=papers[doi])

        # print(json.dumps(papers, sort_keys=True, indent=4))
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