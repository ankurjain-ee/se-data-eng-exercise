resource "google_storage_bucket" "static" {
 name          = "se-data-landing-ankur"
 location      = "US"
 storage_class = "STANDARD"

 public_access_prevention = "enforced"
 uniform_bucket_level_access = true
}