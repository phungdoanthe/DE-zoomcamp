terraform {
  required_providers {
    google = {

      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

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

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
}
