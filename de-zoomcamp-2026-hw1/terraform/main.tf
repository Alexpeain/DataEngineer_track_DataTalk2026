resource "google_storage_bucket" "data_lake_bucket" {
  name     = var.gcs_bucket_name
  location = var.region

  uniform_bucket_level_access = true
}


resource "google_bigquery_dataset" "ny_taxi_dataset" {
  dataset_id  = var.bq_dataset_name
  location    = var.region
  description = "Zoomcamp Module 1 dataset"
}

