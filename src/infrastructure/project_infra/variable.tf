variable "dummy_data_bucket" {
  description = "The name of the GCS bucket for Terraform state"
  type        = string
  default     = "dummy_data"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}