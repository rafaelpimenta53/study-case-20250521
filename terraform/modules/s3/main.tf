resource "aws_s3_bucket" "s3_bucket" {
  bucket = "${var.project_name}-${var.environment}-${var.random_suffix}"
}


resource "aws_s3_bucket_public_access_block" "s3_bucket_pab" {
  bucket = aws_s3_bucket.s3_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Create folder structure for medallion architecture
resource "aws_s3_object" "bronze_folder" {
  bucket = aws_s3_bucket.s3_bucket.id
  key    = "bronze/"
  source = "/dev/null"
}

resource "aws_s3_object" "silver_folder" {
  bucket = aws_s3_bucket.s3_bucket.id
  key    = "silver/"
  source = "/dev/null"
}

resource "aws_s3_object" "gold_folder" {
  bucket = aws_s3_bucket.s3_bucket.id
  key    = "gold/"
  source = "/dev/null"
}
