output "bronze_repository_url" {
  description = "URL of the bronze ECR repository"
  value       = aws_ecr_repository.bronze.repository_url
}

output "silver_repository_url" {
  description = "URL of the silver ECR repository"
  value       = aws_ecr_repository.silver.repository_url
}

output "gold_repository_url" {
  description = "URL of the gold ECR repository"
  value       = aws_ecr_repository.gold.repository_url
}

output "repository_urls" {
  description = "All ECR repository URLs"
  value = {
    bronze = aws_ecr_repository.bronze.repository_url
    silver = aws_ecr_repository.silver.repository_url
    gold   = aws_ecr_repository.gold.repository_url
  }
}
