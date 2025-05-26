output "cluster_name" {
  description = "Name of the ECS cluster"
  value       = aws_ecs_cluster.main.name
}

output "cluster_arn" {
  description = "ARN of the ECS cluster"
  value       = aws_ecs_cluster.main.arn
}

output "task_definitions" {
  description = "Task definition ARNs"
  value = {
    bronze = aws_ecs_task_definition.bronze.arn
    # silver = aws_ecs_task_definition.silver.arn
    # gold   = aws_ecs_task_definition.gold.arn
  }
}

output "task_role_arn" {
  description = "ARN of the ECS task role"
  value       = aws_iam_role.ecs_task_role.arn
}
