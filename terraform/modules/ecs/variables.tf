variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "ecr_repository_urls" {
  description = "ECR repository URLs for each pipeline"
  type = object({
    bronze = string
    silver = string
    gold   = string
  })
}

variable "vpc_id" {
  description = "VPC ID where ECS will run"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for ECS tasks"
  type        = list(string)
}
