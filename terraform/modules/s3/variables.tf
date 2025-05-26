variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
}

variable "random_suffix" {
  description = "Random suffix for bucket name"
  type        = string
}
