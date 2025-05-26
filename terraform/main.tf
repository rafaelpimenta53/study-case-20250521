terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# S3 bucket for medallion architecture
module "s3" {
  source = "./modules/s3"
  
  project_name   = var.project_name
  environment    = var.environment
  random_suffix  = random_id.bucket_suffix.dec
}

# # ECR repositories for Docker images
# module "ecr" {
#   source = "./modules/ecr"
#   project_name = var.project_name
#   environment  = var.environment
# }

# # ECS cluster for running Docker containers
# module "ecs" {
#   source = "./modules/ecs"
  
#   project_name = var.project_name
#   environment  = var.environment
#   aws_region   = var.aws_region
  
#   ecr_repository_urls = {
#     bronze = module.ecr.bronze_repository_url
#     silver = module.ecr.silver_repository_url
#     gold   = module.ecr.gold_repository_url
#   }
  
#   vpc_id     = data.aws_vpc.default.id
#   subnet_ids = data.aws_subnets.default.ids
  
#   depends_on = [module.ecr]
# }

# # Data sources for VPC and subnets
# data "aws_vpc" "default" {
#   default = true
# }

# data "aws_subnets" "default" {
#   filter {
#     name   = "vpc-id"
#     values = [data.aws_vpc.default.id]
#   }
# }

# Generate cloud resources configuration file
resource "local_file" "cloud_resources" {
  content = jsonencode({
    s3_bucket_name   = module.s3.bucket_name
    s3_bucket_arn    = module.s3.bucket_arn
    # ecs_cluster_name = module.ecs.cluster_name
    # ecs_cluster_arn  = module.ecs.cluster_arn
    # ecr_repositories = {
    #   bronze = module.ecr.bronze_repository_url
    #   silver = module.ecr.silver_repository_url
    #   gold   = module.ecr.gold_repository_url
    # }
    aws_region = var.aws_region
  })
  filename = "../src/config/cloud-resources.json"
  
  depends_on = [
    module.s3
    # module.ecs,
    # module.ecr
  ]
}
