resource "aws_ecs_cluster" "main" {
  name = "${var.project_name}-${var.environment}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecs_cluster_capacity_providers" "main" {
  cluster_name = aws_ecs_cluster.main.name

  capacity_providers = ["FARGATE"]

  default_capacity_provider_strategy {
    base              = 1
    weight            = 100
    capacity_provider = "FARGATE"
  }
}

# IAM role for ECS task execution
resource "aws_iam_role" "ecs_task_execution_role" {
  name = "${var.project_name}-${var.environment}-ecs-task-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_role_policy" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# IAM role for ECS tasks (with S3 access)
resource "aws_iam_role" "ecs_task_role" {
  name = "${var.project_name}-${var.environment}-ecs-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}

# Policy for S3 access
resource "aws_iam_policy" "s3_access_policy" {
  name = "${var.project_name}-${var.environment}-s3-access-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.project_name}-${var.environment}-data-lake",
          "arn:aws:s3:::${var.project_name}-${var.environment}-data-lake/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_task_s3_policy" {
  role       = aws_iam_role.ecs_task_role.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "bronze" {
  name              = "/ecs/${var.project_name}-${var.environment}-bronze"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "silver" {
  name              = "/ecs/${var.project_name}-${var.environment}-silver"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "gold" {
  name              = "/ecs/${var.project_name}-${var.environment}-gold"
  retention_in_days = 7
}

# ECS Task Definitions
resource "aws_ecs_task_definition" "bronze" {
  family                   = "${var.project_name}-${var.environment}-bronze"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 1024
  memory                   = 2048
  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
  task_role_arn           = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name  = "bronze-pipeline"
      image = "${var.ecr_repository_urls.bronze}:latest"
      essential = true
      
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.bronze.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ecs"
        }
      }

      environment = [
        {
          name  = "S3_BUCKET"
          value = "${var.project_name}-${var.environment}-data-lake"
        },
        {
          name  = "ENVIRONMENT"
          value = var.environment
        },
        {
          name  = "AWS_DEFAULT_REGION"
          value = var.aws_region
        }
      ]
    }
  ])
}

# resource "aws_ecs_task_definition" "silver" {
#   family                   = "${var.project_name}-${var.environment}-silver"
#   requires_compatibilities = ["FARGATE"]
#   network_mode             = "awsvpc"
#   cpu                      = 512
#   memory                   = 1024
#   execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
#   task_role_arn           = aws_iam_role.ecs_task_role.arn

#   container_definitions = jsonencode([
#     {
#       name  = "silver-pipeline"
#       image = "${var.ecr_repository_urls.silver}:latest"
#       essential = true
      
#       logConfiguration = {
#         logDriver = "awslogs"
#         options = {
#           awslogs-group         = aws_cloudwatch_log_group.silver.name
#           awslogs-region        = var.aws_region
#           awslogs-stream-prefix = "ecs"
#         }
#       }

#       environment = [
#         {
#           name  = "S3_BUCKET"
#           value = "${var.project_name}-${var.environment}-data-lake"
#         },
#         {
#           name  = "ENVIRONMENT"
#           value = var.environment
#         },
#         {
#           name  = "AWS_DEFAULT_REGION"
#           value = var.aws_region
#         }
#       ]
#     }
#   ])
# }

# resource "aws_ecs_task_definition" "gold" {
#   family                   = "${var.project_name}-${var.environment}-gold"
#   requires_compatibilities = ["FARGATE"]
#   network_mode             = "awsvpc"
#   cpu                      = 512
#   memory                   = 1024
#   execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
#   task_role_arn           = aws_iam_role.ecs_task_role.arn

#   container_definitions = jsonencode([
#     {
#       name  = "gold-pipeline"
#       image = "${var.ecr_repository_urls.gold}:latest"
#       essential = true
      
#       logConfiguration = {
#         logDriver = "awslogs"
#         options = {
#           awslogs-group         = aws_cloudwatch_log_group.gold.name
#           awslogs-region        = var.aws_region
#           awslogs-stream-prefix = "ecs"
#         }
#       }

#       environment = [
#         {
#           name  = "S3_BUCKET"
#           value = "${var.project_name}-${var.environment}-data-lake"
#         },
#         {
#           name  = "ENVIRONMENT"
#           value = var.environment
#         },
#         {
#           name  = "AWS_DEFAULT_REGION"
#           value = var.aws_region
#         }
#       ]
#     }
#   ])
# }
