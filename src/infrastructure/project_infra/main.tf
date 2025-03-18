terraform {
  backend "gcs" {
    bucket  = "terraform_state_bucket"
    prefix  = "terraform/state"
  }

  required_providers {
    gcp = {
      source  = "hashicorp/gcp"
      version = "~> 5.35.0"
    }
  }
}

resource "google_storage_bucket" "dummy_csv_data" {
    name = var.dummy_data_bucket
    location = var.region
    versioning {
      enabled = true
    }
}