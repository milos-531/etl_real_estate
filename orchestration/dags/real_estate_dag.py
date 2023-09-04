import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import sys
import os

path = __file__
path1 = os.path.split(path)[0]
path2 = os.path.split(path1)[0]
path3 = os.path.split(path2)[0]
# sys.path.append('/home/milos/etl_real_estate')
sys.path.append(path1)
sys.path.append(path2)
sys.path.append(path3)
from extract.site_crawler import HaloSpyder
from scrapy.crawler import CrawlerProcess
from load.loader import Loader

from transform.transformer import Transformer


def extract():
    crawler = CrawlerProcess({"LOG_FILE": "/tmp/scrapy2.log"})
    crawler.crawl(HaloSpyder)
    crawler.start()


def transform():
    input_file = "/tmp/etl/raw_output.csv"
    output_file = "/tmp/etl/processed_output.csv"

    Transformer.run(input_file, output_file)


def load():
    input_file = "/tmp/etl/processed_output.csv"
    table_name = "real_estate"
    Loader.run(input_file, table_name)


default_args = {
    "owner": "admin",
    "start_date": dt.datetime(2023, 6, 7),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}

with DAG(
    "RealEstateETLdag",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    # extractData = PythonOperator(task_id= 'Extract', python_callable=extract)
    extractData = BashOperator(
        task_id="Extract", bash_command=f"python {path3}/extract/run_extract.py"
    )
    transformData = PythonOperator(task_id="Transform", python_callable=transform)
    loadData = PythonOperator(task_id="Load", python_callable=load)

    extractData >> transformData >> loadData
