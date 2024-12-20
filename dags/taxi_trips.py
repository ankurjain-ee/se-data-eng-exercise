from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime, duration
from airflow.decorators import dag, task

_SNOWFLAKE_CONN_ID = "snowflake_conn_id"
_SNOWFLAKE_DB = "EE_SE_DE_DB"
_SNOWFLAKE_SCHEMA = "ANKURJ"
_SNOWFLAKE_TABLE = "aj_taxi_trips_raw"
DAG_ID = "taxi_trips"

query = """
COPY INTO EE_SE_DE_DB.ANKURJ.taxi_trips_raw
FROM (SELECT
$1,
$2,
$3,
$4,
$5,
$6,
$7,
$8,
$9,
$10,
$11,
$12,
$13,
$14,
$15,
$16,
$17,
$18,
$19,
$20,
$21,
CURRENT_TIMESTAMP as created_timestamp
FROM '@"EE_SE_DE_DB"."ANKURJ"."taxi_trips"/')
FILE_FORMAT = (FORMAT_NAME = 'EE_SE_DE_DB.ANKURJ."taxi_trips_csv"');
"""

@dag(
DAG_ID,
    schedule=None,
    start_date=datetime(2024, 11, 19),
    catchup=False,
    tags=["gcs"],
)
def taxi_trip_dag():

    shook = SnowflakeHook(
        snowflake_conn_id=_SNOWFLAKE_CONN_ID,
    )

    @task()
    def ingest_data_into_raw_layer():
        snowflake_op_template_file = shook.run(
            sql = query,
        )
        return True

    @task.bash
    def unit_test_consistent_layer(flag):
        return """
                dbt test --select "tag:consistent,test_type:unit" --profiles-dir /opt/airflow/dags/dbt/profile --project-dir /opt/airflow/dags/dbt/taxi_trips
                """

    @task.bash
    def transform_data_into_consistent_layer(flag):
        return """
                dbt run --select tag:consistent --profiles-dir /opt/airflow/dags/dbt/profile --project-dir /opt/airflow/dags/dbt/taxi_trips
                """

    @task.bash
    def data_quality_check_consistent_layer(flag):
        return """
                dbt test --select "tag:consistent,test_type:data" --profiles-dir /opt/airflow/dags/dbt/profile --project-dir /opt/airflow/dags/dbt/taxi_trips
                """

    @task.bash
    def unit_test_presentation_layer(flag):
        return """
                dbt test --select "tag:presentation,test_type:unit" --profiles-dir /opt/airflow/dags/dbt/profile --project-dir /opt/airflow/dags/dbt/taxi_trips
                """

    @task.bash
    def transform_data_into_presentation_layer(flag):
        return """
                dbt run --select tag:presentation --profiles-dir /opt/airflow/dags/dbt/profile --project-dir /opt/airflow/dags/dbt/taxi_trips
                """

    flag = ingest_data_into_raw_layer()
    flag = unit_test_consistent_layer(flag)
    flag = transform_data_into_consistent_layer(flag)
    flag = data_quality_check_consistent_layer(flag)
    flag = unit_test_presentation_layer(flag)
    flag = transform_data_into_presentation_layer(flag)




taxi_trip_dag()