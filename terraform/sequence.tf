resource "snowflake_sequence" "sequence" {
  database = snowflake_schema.schema.database
  schema   = snowflake_schema.schema.name
  name     = "sequence"
}
