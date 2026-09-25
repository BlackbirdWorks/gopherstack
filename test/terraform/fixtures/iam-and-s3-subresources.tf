resource "aws_iam_role" "example" {
  name = "iams3-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_instance_profile" "example" {
  name = "iams3-instance-profile"
  role = aws_iam_role.example.name
}

resource "aws_iam_group" "example" {
  name = "iams3-group"
}

resource "aws_iam_user" "example" {
  name = "iams3-user"
}

resource "aws_iam_group_membership" "example" {
  name  = "iams3-group-membership"
  users = [aws_iam_user.example.name]
  group = aws_iam_group.example.name
}

resource "aws_iam_policy" "example" {
  name = "iams3-policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:ListAllMyBuckets"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_group_policy" "example" {
  name  = "iams3-group-policy"
  group = aws_iam_group.example.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_group_policy_attachment" "example" {
  group      = aws_iam_group.example.name
  policy_arn = aws_iam_policy.example.arn
}

resource "aws_iam_user_policy" "example" {
  name = "iams3-user-policy"
  user = aws_iam_user.example.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_user_policy_attachment" "example" {
  user       = aws_iam_user.example.name
  policy_arn = aws_iam_policy.example.arn
}

resource "aws_iam_access_key" "example" {
  user = aws_iam_user.example.name
}

resource "aws_iam_user_login_profile" "example" {
  user                    = aws_iam_user.example.name
  password_reset_required = false
  password_length         = 20
}

resource "aws_iam_user_ssh_key" "example" {
  username   = aws_iam_user.example.name
  encoding   = "SSH"
  public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDWabniWqjht+WYiPUNX1Wcw124S9Aue2prEAR3MZeENTxiZNLOAxPrwhcRDH+pGdlnZXZ8n9wZ+xOHCbVbqk+qkle3eem2iYqv+5xjAs7Ihs+ag2v94ZG+wEyRFXo+dFL53ApxC1Rc40AtoVQuG2D5Ix2sTRBrcAg8Qsy1EigDQlyDBT+NCO5qosuwmZsi364nghAhUsAa5thohbHua54npM0Y4ICyYLbQw3S6MsFBoxLmlxrd2q9dglUplLEs52w+cxRUoBkOJ1xaZyTmcE7/SrrerOTcpJr1zDs8tSJqAlGZL2+IzOR7PyFlDl6Ixn6X8vFK6FXS1yDuHg0PLAop iams3"
}

resource "aws_iam_service_specific_credential" "example" {
  service_name = "cassandra.amazonaws.com"
  user_name    = aws_iam_user.example.name
}

resource "aws_iam_virtual_mfa_device" "example" {
  virtual_mfa_device_name = "iams3-mfa"
}

resource "aws_iam_account_alias" "example" {
  account_alias = "iams3-alias"
}

resource "aws_iam_account_password_policy" "example" {
  minimum_password_length = 12
  require_numbers         = true
}

resource "aws_iam_openid_connect_provider" "example" {
  url             = "https://iams3.oidc.example.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["9e99a48a9960b14926bb7f3b02e22da2b0ab7280"]
}

resource "aws_iam_saml_provider" "example" {
  name = "iams3-saml"
  # The AWS provider requires saml_metadata_document to be at least 1000
  # bytes long, so this padding comment brings a minimal metadata document
  # over that threshold without affecting the parsed SAML content.
  saml_metadata_document = <<EOF
<?xml version="1.0"?>
<!--
padding padding padding padding padding padding padding padding padding
padding padding padding padding padding padding padding padding padding
padding padding padding padding padding padding padding padding padding
padding padding padding padding padding padding padding padding padding
padding padding padding padding padding padding padding padding padding
padding padding padding padding padding padding padding padding padding
padding padding padding padding padding padding padding padding padding
padding padding padding padding padding padding padding padding padding
padding padding padding padding padding padding padding padding padding
padding padding padding padding padding padding padding padding padding
padding padding padding padding padding padding padding padding padding
-->
<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata" entityID="https://iams3.example.com/saml">
  <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol"/>
</EntityDescriptor>
EOF
}

resource "aws_iam_service_linked_role" "example" {
  aws_service_name = "elasticbeanstalk.amazonaws.com"
}

resource "aws_s3_bucket" "example" {
  bucket        = "iams3-bucket"
  force_destroy = true
}

resource "aws_s3_bucket_accelerate_configuration" "example" {
  bucket = aws_s3_bucket.example.id
  status = "Enabled"
}

resource "aws_s3_bucket_ownership_controls" "example" {
  bucket = aws_s3_bucket.example.id
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_acl" "example" {
  bucket = aws_s3_bucket.example.id
  acl    = "private"

  depends_on = [aws_s3_bucket_ownership_controls.example]
}

resource "aws_s3_bucket_analytics_configuration" "example" {
  bucket = aws_s3_bucket.example.id
  name   = "iams3-analytics"

  storage_class_analysis {
    data_export {
      destination {
        s3_bucket_destination {
          bucket_arn = aws_s3_bucket.example.arn
        }
      }
    }
  }
}

resource "aws_s3_bucket_cors_configuration" "example" {
  bucket = aws_s3_bucket.example.id

  cors_rule {
    allowed_methods = ["GET"]
    allowed_origins = ["https://example.com"]
  }
}

resource "aws_s3_bucket_intelligent_tiering_configuration" "example" {
  bucket = aws_s3_bucket.example.id
  name   = "iams3-tiering"

  tiering {
    access_tier = "ARCHIVE_ACCESS"
    days        = 90
  }
}

resource "aws_s3_bucket_inventory" "example" {
  bucket = aws_s3_bucket.example.id
  name   = "iams3-inventory"

  included_object_versions = "All"

  schedule {
    frequency = "Daily"
  }

  destination {
    bucket {
      format     = "CSV"
      bucket_arn = aws_s3_bucket.example.arn
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "example" {
  bucket = aws_s3_bucket.example.id

  rule {
    id     = "expire"
    status = "Enabled"

    filter {}

    expiration {
      days = 30
    }
  }
}

resource "aws_s3_bucket_metric" "example" {
  bucket = aws_s3_bucket.example.id
  name   = "iams3-metric"
}

# aws_s3_bucket_object has no request_payer argument, but the terraform
# provider's credentials resolve to the same account that created the bucket
# below, so it's exempt from the x-amz-request-payer header requirement as
# the bucket owner (see services/s3/requester_pays.go's owner exemption).
resource "aws_s3_bucket_object" "example" {
  bucket  = aws_s3_bucket.example.id
  key     = "iams3.txt"
  content = "iams3"
}

resource "aws_s3_bucket_request_payment_configuration" "example" {
  bucket = aws_s3_bucket.example.id
  payer  = "Requester"
}
