resource "aws_s3_directory_bucket" "example" {
  bucket        = "{{.BucketName}}"
  force_destroy = true

  location {
    name = "{{.AZID}}"
    type = "AvailabilityZone"
  }

  data_redundancy = "SingleAvailabilityZone"
  type            = "Directory"
}

resource "aws_s3_access_point" "example" {
  account_id = "000000000000"
  bucket     = aws_s3_directory_bucket.example.bucket
  name       = "{{.AccessPointName}}"
}

resource "aws_s3control_directory_bucket_access_point_scope" "example" {
  account_id = "000000000000"
  name       = aws_s3_access_point.example.name

  scope {
    permissions = ["GetObject", "PutObject"]
    prefixes    = ["logs/"]
  }
}
