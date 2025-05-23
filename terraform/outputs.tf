output "s3_bucket_name" {
  description = "Name of the S3 bucket for data lake"
  value       = module.s3.bucket_name
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = module.s3.bucket_arn
}

# output "ecs_cluster_name" {
#   description = "Name of the ECS cluster"
#   value       = module.ecs.cluster_name
# }

# output "ecs_cluster_arn" {
#   description = "ARN of the ECS cluster"
#   value       = module.ecs.cluster_arn
# }
