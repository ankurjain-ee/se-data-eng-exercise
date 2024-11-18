terraform {
  backend "gcs" {
    bucket = "ee-se-data-engg"
    prefix = "ankur/terraform/state/"
  }
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "0.92.0"
    }
  }
}

provider "snowflake" {
  user = "ankurjain"
  authenticator = "JWT"
  private_key = var.snowflake_private_key
  private_key_passphrase = var.snowflake_passphrase
  account  = "gusdatd-dab70621"
  role = "SE_DE_PARTICIPANT"
}