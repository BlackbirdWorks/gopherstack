##############################################################################
# OpenSearch: a VPC-based domain with a domain policy, SAML options, a
# package association, an authorized VPC endpoint, and an outbound/inbound
# cross-cluster search connection to a second domain.
##############################################################################

resource "aws_vpc" "mb38" {
  cidr_block = "10.201.0.0/16"

  tags = {
    Name = "mega-batch-38-vpc"
  }
}

resource "aws_subnet" "mb38_a" {
  vpc_id     = aws_vpc.mb38.id
  cidr_block = "10.201.1.0/24"

  tags = {
    Name = "mega-batch-38-subnet-a"
  }
}

resource "aws_security_group" "mb38_os" {
  name   = "mega-batch-38-os-sg"
  vpc_id = aws_vpc.mb38.id
}

resource "aws_opensearch_domain" "mb38" {
  domain_name    = "mb38-domain"
  engine_version = "OpenSearch_2.3"

  cluster_config {
    instance_type  = "t3.small.search"
    instance_count = 1
  }

  vpc_options {
    subnet_ids         = [aws_subnet.mb38_a.id]
    security_group_ids = [aws_security_group.mb38_os.id]
  }

  domain_endpoint_options {
    enforce_https       = true
    tls_security_policy = "Policy-Min-TLS-1-2-2019-07"
  }

  timeouts {
    create = "5s"
    delete = "5s"
    update = "5s"
  }
}

resource "aws_opensearch_domain" "mb38_remote" {
  domain_name    = "mb38-remote-domain"
  engine_version = "OpenSearch_2.3"

  timeouts {
    create = "5s"
    delete = "5s"
    update = "5s"
  }
}

resource "aws_opensearch_domain_policy" "mb38" {
  domain_name = aws_opensearch_domain.mb38.domain_name

  access_policies = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "es:*"
      Resource  = "${aws_opensearch_domain.mb38.arn}/*"
    }]
  })
}

resource "aws_opensearch_domain_saml_options" "mb38" {
  domain_name = aws_opensearch_domain.mb38.domain_name

  saml_options {
    enabled = true

    idp {
      entity_id        = "https://mega-batch-38-idp.example.com"
      metadata_content = "<EntityDescriptor entityID=\"https://mega-batch-38-idp.example.com\"></EntityDescriptor>"
    }
  }
}

resource "aws_s3_bucket" "mb38_pkg" {
  bucket = "mega-batch-38-pkg-bucket"
}

resource "aws_s3_object" "mb38_pkg" {
  bucket  = aws_s3_bucket.mb38_pkg.id
  key     = "mega-batch-38/dictionary.txt"
  content = "example custom dictionary"
}

resource "aws_opensearch_package" "mb38" {
  package_name = "mega-batch-38-dictionary"
  package_type = "TXT-DICTIONARY"

  package_source {
    s3_bucket_name = aws_s3_bucket.mb38_pkg.id
    s3_key         = aws_s3_object.mb38_pkg.key
  }
}

resource "aws_opensearch_package_association" "mb38" {
  package_id  = aws_opensearch_package.mb38.id
  domain_name = aws_opensearch_domain.mb38.domain_name
}

resource "aws_vpc" "mb38_ep" {
  cidr_block = "10.202.0.0/16"

  tags = {
    Name = "mega-batch-38-ep-vpc"
  }
}

resource "aws_subnet" "mb38_ep" {
  vpc_id     = aws_vpc.mb38_ep.id
  cidr_block = "10.202.1.0/24"

  tags = {
    Name = "mega-batch-38-ep-subnet"
  }
}

resource "aws_opensearch_vpc_endpoint" "mb38" {
  domain_arn = aws_opensearch_domain.mb38.arn

  vpc_options {
    subnet_ids = [aws_subnet.mb38_ep.id]
  }
}

resource "aws_opensearch_authorize_vpc_endpoint_access" "mb38" {
  domain_name = aws_opensearch_domain.mb38.domain_name
  account     = "000000000000"
}

resource "aws_opensearch_outbound_connection" "mb38" {
  connection_alias  = "mega-batch-38-connection"
  accept_connection = false

  local_domain_info {
    domain_name = aws_opensearch_domain.mb38.domain_name
    owner_id    = "000000000000"
    region      = "us-east-1"
  }

  remote_domain_info {
    domain_name = aws_opensearch_domain.mb38_remote.domain_name
    owner_id    = "000000000000"
    region      = "us-east-1"
  }
}

resource "aws_opensearch_inbound_connection_accepter" "mb38" {
  connection_id = aws_opensearch_outbound_connection.mb38.id
}

##############################################################################
# Shield: a subscription, a protection on an ALB, a protection group, DRT
# role/log-bucket associations, proactive engagement, and an application
# layer automatic response.
##############################################################################

resource "aws_vpc" "mb38_shield" {
  cidr_block = "10.203.0.0/16"

  tags = {
    Name = "mega-batch-38-shield-vpc"
  }
}

resource "aws_subnet" "mb38_shield_a" {
  vpc_id     = aws_vpc.mb38_shield.id
  cidr_block = "10.203.1.0/24"

  tags = {
    Name = "mega-batch-38-shield-subnet-a"
  }
}

resource "aws_subnet" "mb38_shield_b" {
  vpc_id     = aws_vpc.mb38_shield.id
  cidr_block = "10.203.2.0/24"

  tags = {
    Name = "mega-batch-38-shield-subnet-b"
  }
}

resource "aws_lb" "mb38_shield" {
  name               = "mega-batch-38-shield-alb"
  internal           = false
  load_balancer_type = "application"
  subnets            = [aws_subnet.mb38_shield_a.id, aws_subnet.mb38_shield_b.id]
}

resource "aws_shield_subscription" "mb38" {
  skip_destroy = true
}

resource "aws_shield_protection" "mb38" {
  name         = "mega-batch-38-protection"
  resource_arn = aws_lb.mb38_shield.arn
}

resource "aws_shield_protection_group" "mb38" {
  protection_group_id = "mega-batch-38-protection-group"
  aggregation         = "MAX"
  pattern             = "ARBITRARY"
  members             = [aws_lb.mb38_shield.arn]

  depends_on = [aws_shield_protection.mb38]
}

resource "aws_shield_proactive_engagement" "mb38" {
  enabled = true

  emergency_contact {
    contact_notes = "mega-batch-38 primary contact"
    email_address = "mega-batch-38-oncall@example.com"
    phone_number  = "+12025550123"
  }

  depends_on = [aws_shield_subscription.mb38]
}

resource "aws_iam_role" "mb38_drt" {
  name = "mega-batch-38-drt-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "drt.shield.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "mb38_drt" {
  role       = aws_iam_role.mb38_drt.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSShieldDRTAccessPolicy"
}

resource "aws_shield_drt_access_role_arn_association" "mb38" {
  role_arn = aws_iam_role.mb38_drt.arn

  depends_on = [aws_iam_role_policy_attachment.mb38_drt]

  # The provider's Delete polls DescribeDRTAccess waiting for it to report
  # "not found", but real AWS's DescribeDRTAccess always succeeds (it's a
  # per-account describe, never a 404) -- so this poll never converges
  # against any backend, real or emulated. Bound it short, as
  # mega-batch-32's FSx override does for a real 10-minute provider delay.
  timeouts {
    delete = "20s"
  }
}

resource "aws_s3_bucket" "mb38_drt_log" {
  bucket = "mega-batch-38-drt-log-bucket"
}

resource "aws_shield_drt_access_log_bucket_association" "mb38" {
  log_bucket              = aws_s3_bucket.mb38_drt_log.id
  role_arn_association_id = aws_shield_drt_access_role_arn_association.mb38.id
}

resource "aws_shield_application_layer_automatic_response" "mb38" {
  resource_arn = aws_lb.mb38_shield.arn
  action       = "COUNT"

  depends_on = [aws_shield_protection.mb38]
}
