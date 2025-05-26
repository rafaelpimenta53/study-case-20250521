resource "aws_ecr_repository" "bronze" {
  name = "bronze-pipeline"
}

resource "aws_ecr_repository" "silver" {
  name = "silver-pipeline"
}

resource "aws_ecr_repository" "gold" {
  name = "gold-pipeline"
}