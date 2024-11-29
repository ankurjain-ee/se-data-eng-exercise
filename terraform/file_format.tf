resource "snowflake_file_format" "taxi_trips_csv" {
  name        = "taxi_trips_csv"
  database    = snowflake_schema.schema.database
  schema      = snowflake_schema.schema.name
  format_type = "CSV"
  compression = "AUTO"
  field_delimiter = ","
  record_delimiter = "\n"
  date_format = "string"
  timestamp_format = "string"
  error_on_column_count_mismatch = false
  trim_space = true
  empty_field_as_null = true
  skip_header = 1
}