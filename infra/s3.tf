resource "aws_s3_bucket" "finance_pipeline_dublin" {
  bucket = "finance-pipeline-ingest-2026"
}

resource "aws_s3_bucket_public_access_block" "finance_pipeline_dublin" {
  bucket = aws_s3_bucket.finance_pipeline_dublin.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "finance_pipeline_dublin" {
  bucket = aws_s3_bucket.finance_pipeline_dublin.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "finance_pipeline_dublin" {
  bucket = aws_s3_bucket.finance_pipeline_dublin.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}