# ------------------------------------------------------------------------------
# PROVIDER CONFIGURATION
# ------------------------------------------------------------------------------
provider "google" {
  # Replace these with your actual project ID and desired region
  project = "your-gcp-project-id"
  region  = "us-central1"
}

# ------------------------------------------------------------------------------
# GOOGLE CLOUD STORAGE BUCKET
# ------------------------------------------------------------------------------
resource "google_storage_bucket" "auto-expire" {
  name          = var.bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    action {
      type = "AbortIncompleteMultipartUpload"
    }
    condition {
      age = 1 // days
    }
  }
}

# ------------------------------------------------------------------------------
# GCS BUCKET FOLDERS (2019, 2020, 2021)
# ------------------------------------------------------------------------------
# Using a for_each loop to cleanly create the simulated folders
resource "google_storage_bucket_object" "folders" {
  for_each = toset(["2019/", "2020/", "2021/"])
  
  name    = each.value
  content = " " # Empty content simulates the folder
  bucket  = google_storage_bucket.project_bucket.name
}

# ------------------------------------------------------------------------------
# BIGQUERY DATASET
# ------------------------------------------------------------------------------
resource "google_bigquery_dataset" "bicycle_dataset" {
  dataset_id                  = "bicycle"
  friendly_name               = "Bicycle Dataset"
  description                 = "Dataset containing bicycle data"
  location                    = "US"

  # Optional: Default table expiration in milliseconds (e.g., 30 days)
  default_table_expiration_ms = 2592000000 
}