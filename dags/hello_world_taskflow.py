from __future__ import annotations

from datetime import datetime
from pathlib import Path


from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.decorators import dag, task

import logging

ENV_ID = "DEV"

PROJECT_ID = "ee-india-se-data"

DAG_ID = "hello_world_taskflow"

BUCKET_NAME = "se-data-landing-ankur"

FILE_NAME = "hello_world.txt"

UPLOAD_FILE_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)

DOWNLOAD_FILE_PATH = str(Path(__file__).parent / "output" / FILE_NAME)

DEFAULT_TIMEOUT = 60



@dag(
    DAG_ID,
    schedule=None,
    start_date=datetime(2024, 11, 4),
    catchup=False,
    tags=["gcs"],
)
def hello_world_dag():

    gcs_hook = GCSHook(
        google_cloud_storage_conn_id="google_cloud_default",
        impersonation_chain=None,
    )

    @task()
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
        return FILE_NAME

    @task()
    def read_file(file_name):
        with gcs_hook.provide_file(
                bucket_name=BUCKET_NAME,
                object_name=file_name) as gcs_file_handle:
            logging.info(gcs_file_handle.name)
            temp_file_path = gcs_file_handle.name
            with open(temp_file_path, 'r') as temp_file:
                content = temp_file.read()
                logging.info(content)

    file_name = upload_file()
    read_file(file_name)


hello_world_dag()