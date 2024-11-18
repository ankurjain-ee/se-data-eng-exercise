resource "snowflake_table" "table" {
  database                    = snowflake_schema.schema.database
  schema                      = snowflake_schema.schema.name
  name                        = "aj_taxi_trips_raw"
  comment                     = "RAW Taxi Trips Data"
  cluster_by                  = ["to_date(CREATED_TIMESTAMP)"]
  change_tracking             = true

  column {
    name     = "id"
    type     = "int"
    nullable = true

    default {
      sequence = snowflake_sequence.sequence.fully_qualified_name
    }
  }

  column {
    name     = "vendor_name"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "tpep_pickup_datetime"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "tpep_dropoff_datetime"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "passenger_count"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "trip_distance"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "pickup_longitude"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "pickup_latitude"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "RatecodeID"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "store_and_fwd_flag"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "dropoff_longitude"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "dropoff_latitude"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "payment_type"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "payment_type_name"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "fare_amount"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "extra"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "mta_tax"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "tip_amount"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "tolls_amount"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "improvement_surcharge"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "total_amount"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "trip_duration_minutes"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "trip_speed_mph"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name = "CREATED_TIMESTAMP"
    type = "TIMESTAMP_NTZ(9)"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }

}