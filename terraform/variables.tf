variable "bg_dataset_name" {
  description = "My BigQuery Dataset Name"
  type        = string
  default     = "demo_dataset"
}

variable "credentials" {
  description = "GCP credentials JSON file"
  type        = string
  default     = "keys/gcp-terraform-demo.json"
}