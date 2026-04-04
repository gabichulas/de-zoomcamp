variable "location" {
  description = "Project location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "demo-dataset"
}

variable "gcs_bucket_name" {
  description = "My GCS Bucket dataset name"
  default     = "project-ad6a13b6-36d6-4a7b-b67-demo-bucket"
}

variable "gcs_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}