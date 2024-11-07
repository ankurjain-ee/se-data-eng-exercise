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
    bucket  = "f7f3190898ce24b8-terraform-remote-backend"
    prefix  = "terraform/state" // Please keep a prefix unique to your env/project
  }
}