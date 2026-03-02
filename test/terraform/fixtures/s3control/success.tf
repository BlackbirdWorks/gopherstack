resource "aws_s3_account_public_access_block" "this" {
  account_id              = "000000000000"
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
