variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "southamerica-east1"
}

variable "bucket_name" {
  type = string
}

variable "repository_name" {
  type = string
}

variable "service_name" {
  type = string
}

variable "bq_source_table" {
  type = string
}