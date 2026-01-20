variable "credentials" {
  description = "path to gcloud credentials file"
  default     = "D:/Study_DE/DE-zoomcamp/terraform/keys/my-creds.json"
}
variable "project" {
  description = "project"
  default     = "de-zoom-camp-2026"
}

variable "region" {
  description = "project location"
  default     = "asia-southeast1"
}

variable "location" {
  description = "project location"
  default     = "Tokyo"
}

variable "bq_dataset_name" {
  description = "bigquery dataset name"
  default     = "de_zoom_camp_2026_terra_dataset"
}


variable "bucket_name" {
  description = "gcloud storage bucket name"
  default     = "de_zoom_camp_2026_terra_bucket"
}
