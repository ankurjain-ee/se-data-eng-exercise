resource "snowflake_stage" "taxi_trips" {
  name        = "taxi_trips"
  url         = "gcs://se-data-landing-ankur/taxi_trips"
  database    = snowflake_schema.schema.database
  schema      = snowflake_schema.schema.name
  storage_integration = "EE_SE_DATA_ENGG_GCS"
  file_format = "TYPE = CSV"
}