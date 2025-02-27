terraform {
  required_version = ">= 1.0"
}

provider "aws" {
  region = "us-east-1"
}

# S3 bucket to store CSV outputs for the pipeline.
resource "aws_s3_bucket" "openlibrary_data" {
  bucket = "drivewealth-openlibrary-data"
  acl    = "private"

  # Enable versioning to safeguard against accidental deletions or overwrites.
  versioning {
    enabled = true
  }

  # Enable server-side encryption to protect data at rest.
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }

  # Lifecycle rules for cost management and data retention.
  lifecycle_rule {
    id      = "transition_and_expiration"
    enabled = true

    # Transition objects to STANDARD_IA (Infrequent Access) after 30 days.
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    # Delete objects after 365 days.
    expiration {
      days = 365
    }
  }

  tags = {
    Name        = "drivewealth-openlibrary-data"
    Environment = "dev"  # Change to "prod" for production environments.
  }
}

# Public Access Block to ensure the bucket is not publicly accessible.
resource "aws_s3_bucket_public_access_block" "openlibrary_data_block" {
  bucket = aws_s3_bucket.openlibrary_data.bucket

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}