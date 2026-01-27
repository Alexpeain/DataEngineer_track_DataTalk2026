variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "gcs_bucket_name" {
  description = "Name of the GCS bucket"
  type        = string
}

variable "bq_dataset_name" {
  description = "Name of the BigQuery dataset"
  type        = string
}
