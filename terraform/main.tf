resource "google_storage_bucket" "static" {
  name          = "se-data-landing-ankur"
  project       = "ee-india-se-data"
  location      = "US"
  storage_class = "STANDARD"

  public_access_prevention    = "enforced"
  uniform_bucket_level_access = true
}

terraform {
  backend "gcs" {
    bucket  = "ee-se-data-engg"
    prefix  = "ankur/terraform/state/"
  }
}