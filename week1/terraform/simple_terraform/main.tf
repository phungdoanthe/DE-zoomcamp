terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  project = "de-zoom-camp-2026"
  region  = "asia-southeast1"
}

resource "google_storage_bucket" "auto-expire" {
  name          = "de-zoom-camp-2026-terra-bucket"
  location      = "asia-southeast1"
  force_destroy = true

  lifecycle_rule {
    action {
      type = "AbortIncompleteMultipartUpload"
    }
    condition {
      age = 1  // days
    }
  }


}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = "de_zoom_camp_2026_terra_dataset"
}
