from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator

import logging

ENV_ID = "DEV"

PROJECT_ID = "ee-india-se-data"

DAG_ID = "hello_world"

BUCKET_NAME = "se-data-landing-ankur"

FILE_NAME = "hello_world.txt"

UPLOAD_FILE_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)

DOWNLOAD_FILE_PATH = str(Path(__file__).parent / "output" / FILE_NAME)

DEFAULT_TIMEOUT = 60

gcs_hook = GCSHook(
            google_cloud_storage_conn_id="google_cloud_default",
            impersonation_chain=None,
        )

def upload_file():
    gcs_hook.upload(
    bucket_name=BUCKET_NAME,
    object_name=FILE_NAME,
    filename=UPLOAD_FILE_PATH,
    data=None,
    mime_type=None,
    gzip=False,
    encoding='utf-8',
    chunk_size=None,
    timeout=DEFAULT_TIMEOUT,
    num_max_attempts=1,
    metadata=None,
    cache_control=None,
    user_project=None)

def read_file():
    # file = gcs_hook.provide_file(
    # bucket_name=BUCKET_NAME,
    # object_name=FILE_NAME)

    gcs_hook.download(
        bucket_name = BUCKET_NAME,
        object_name = FILE_NAME,
        filename = DOWNLOAD_FILE_PATH,
        chunk_size = None,
        timeout = DEFAULT_TIMEOUT,
        num_max_attempts = 1,
        user_project = None,
    )
    file_to_read = open("dags/output/hello_world.txt", "r")
    for line in file_to_read:
        logging.info(line)

with DAG(
    DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["gcs"],
) as dag:

    task1 = PythonOperator(
        task_id="upload_file",
        python_callable=upload_file)

    task2 = PythonOperator(
        task_id="read_file",
        python_callable=read_file
    )

task1 >> task2