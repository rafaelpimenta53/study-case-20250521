resource "random_id" "bucket_suffix" {
  byte_length = 3
  
  keepers = {
    project_name = var.project_name
    environment  = var.environment
  }
}
