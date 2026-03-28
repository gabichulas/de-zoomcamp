terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.25.0"
    }
  }
}

provider "google" {
  project                     = "project-ad6a13b6-36d6-4a7b-b67"
  region                      = "us-central1"
  impersonate_service_account = "terraform-test@project-ad6a13b6-36d6-4a7b-b67.iam.gserviceaccount.com"
}


resource "google_storage_bucket" "demo-bucket" {
  name                        = var.gcs_bucket_name
  location                    = var.location
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}


resource "google_bigquery_dataset" "demo-dataset" {
  dataset_id = "my_dataset"
}