from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.decorators import dag, task

import logging

ENV_ID = "DEV"

PROJECT_ID = "ee-india-se-data"

DAG_ID = "taxi_trip"

BUCKET_NAME = "se-data-landing-ankur"

FILE_NAME = "hello_world.txt"

UPLOAD_FILE_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)

DOWNLOAD_FILE_PATH = str(Path(__file__).parent / "output" / FILE_NAME)

DEFAULT_TIMEOUT = 60

@dag(
DAG_ID,
    schedule=None,
    start_date=datetime(2024, 11, 19),
    catchup=False,
    tags=["gcs"],
)
def taxi_trip_dag():

    gcs_hook = GCSHook(
        google_cloud_storage_conn_id="google_cloud_default",
        impersonation_chain=None,
    )

    @task()
    def check_for_new_files():
        file_list = gcs_hook.list(
            bucket_name=BUCKET_NAME,
            prefix="taxi_trips/taxi_trips_",
        )
        for file in file_list:
            logging.info(file)

    check_for_new_files()

taxi_trip_dag()