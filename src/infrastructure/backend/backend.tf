terraform {
  required_providers {
    gcp = {
      source  = "hashicorp/gcp"
      version = "5.35.0"
    }

    random = {
      source  = "hashicorp/random"
      version = "3.6.2"
    }
  }
}

provider "gcp" {
  project = var.project_id
  region = var.region
}

resource "google_storage_bucket" "terraform_state_bucket" {
  name = var.bucket_name
  location = var.region
  versioning {
    enabled = true
  }
}

