"""
Utils for get_papers DAG
"""
import logging
import time
from datetime import datetime, timedelta

import boto3
import requests
from botocore.exceptions import ClientError


def scrape_pdf_to_s3(pdf_url, key, metadata=None):
    """
    Get a pdf from a given url and upload it to S3.
    """
    conn = boto3.client('s3')
    pdf_file = requests.get(pdf_url).content

    try:
        if metadata:
            print(metadata)
            print(type(metadata))
            response = conn.put_object(Bucket='paper-pdfs',
                                       Key=key,
                                       Body=pdf_file,
                                       Metadata=metadata
                                       )
        else:
            response = conn.put_object(Bucket='paper-pdfs',
                                       Key=key,
                                       Body=pdf_file
                                       )

    except ClientError as e:
        logging.error(e)
        return e

    return response


def doi_by_title(title, author=None):
    """
    Generate URL for calling Crossref's rest API, to be passed into API_Throttler class
    """
    request_url = f'https://api.crossref.org/works?query.title={title.strip().replace(" ", "+")}'
    if author:
        request_url += f'&query.author={author.strip().replace(" ", "+")}'

    # add mailto to get directed to crossref's "Polite Pool", sort scores by relevance
    request_url += f'&sort=relevance&order=desc&mailto=mrizzuto@proton.me'
    return request_url


class API_Throttler():
    # inspired by https://stackoverflow.com/questions/40748687/python-api-rate-limiting-how-to-limit-api-calls-globally
    def __init__(self, max_calls=50, _limit_interval=5):
        self.num_calls = 0
        self.next_reset_at = 0

        self.max_calls = max_calls
        self._limit_interval = _limit_interval

        self.cache = {'urls': [], 'dois': []}

    @property
    def limit_interval(self):
        return self._limit_interval

    # make sure next reset is updated when limit_interval is updated, use setter
    @limit_interval.setter
    def limit_interval(self, value):
        self._limit_interval = value
        self._update_next_reset()

    def _update_next_reset(self):
        self.next_reset_at = datetime.now() + timedelta(seconds=self.limit_interval)


    def request(self, url, headers={}):
        # reset limits if interval reset
        now = datetime.now()
        if now > self.next_reset_at():
            self.num_calls = 0
            self._update_next_reset
        
        # if exceed max rate, wait
        if self.num_calls >= self.max_calls:
            wait_time = (self.next_reset_at - now).seconds
            time.sleep(wait_time)
        
        # make request
        response = requests.get(url, headers=headers)

        # add url to cache and update num calls
        self.cache['urls'].append(url)
        self.num_calls += 1

        return response