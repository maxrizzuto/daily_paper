"""
Utils for get_papers DAG
"""
import os

from dotenv import load_dotenv


def s3_conn():
    load_dotenv()
    API_KEY = os.getenv('API_KEY')
    pass

def scrape_pdf_to_s3(conn, url):
    """
    Get a pdf from a given url, and upload it to S3 with the url as its key.
    """
    pass
