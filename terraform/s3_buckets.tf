resource "aws_kms_key" "storage_key" {
  description         = "Encrypts ingestkit buckets and the state database"
  enable_key_rotation = true
}

resource "aws_s3_bucket" "landing_zone" {
  bucket = "${local.name_prefix}-landing-zone"
}

# Versioning keeps every earlier version of an object, so nothing in raw/ can be silently
# overwritten. For production, raw/ moves to its own bucket with Object Lock (see docs).
resource "aws_s3_bucket" "destination_store" {
  bucket = "${local.name_prefix}-destination"
}

resource "aws_s3_bucket_versioning" "landing_versioning" {
  bucket = aws_s3_bucket.landing_zone.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "destination_versioning" {
  bucket = aws_s3_bucket.destination_store.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "landing_encryption" {
  bucket = aws_s3_bucket.landing_zone.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.storage_key.arn
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "destination_encryption" {
  bucket = aws_s3_bucket.destination_store.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.storage_key.arn
    }
  }
}

resource "aws_s3_bucket_public_access_block" "landing_public_block" {
  bucket                  = aws_s3_bucket.landing_zone.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "destination_public_block" {
  bucket                  = aws_s3_bucket.destination_store.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "destination_lifecycle" {
  bucket = aws_s3_bucket.destination_store.id

  rule {
    id     = "clean-staging"
    status = "Enabled"
    filter {
      prefix = "staging/"
    }
    expiration {
      days = 2
    }
    noncurrent_version_expiration {
      noncurrent_days = 1
    }
  }

  rule {
    id     = "raw-to-cheaper-storage"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }
}
