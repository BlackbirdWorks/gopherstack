##############################################################################
# OpenSearch: a VPC-based domain with a domain policy, SAML options, a
# package association, an authorized VPC endpoint, and an outbound/inbound
# cross-cluster search connection to a second domain.
##############################################################################

resource "aws_vpc" "opsh" {
  cidr_block = "10.201.0.0/16"

  tags = {
    Name = "opsh-vpc"
  }
}

resource "aws_subnet" "opsh_a" {
  vpc_id     = aws_vpc.opsh.id
  cidr_block = "10.201.1.0/24"

  tags = {
    Name = "opsh-subnet-a"
  }
}

resource "aws_security_group" "opsh_os" {
  name   = "opsh-os-sg"
  vpc_id = aws_vpc.opsh.id
}

resource "aws_opensearch_domain" "opsh" {
  domain_name    = "opsh-domain"
  engine_version = "OpenSearch_2.3"

  cluster_config {
    instance_type  = "t3.small.search"
    instance_count = 1
  }

  vpc_options {
    subnet_ids         = [aws_subnet.opsh_a.id]
    security_group_ids = [aws_security_group.opsh_os.id]
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

resource "aws_opensearch_domain" "opsh_remote" {
  domain_name    = "opsh-remote-domain"
  engine_version = "OpenSearch_2.3"

  timeouts {
    create = "5s"
    delete = "5s"
    update = "5s"
  }
}

resource "aws_opensearch_domain_policy" "opsh" {
  domain_name = aws_opensearch_domain.opsh.domain_name

  access_policies = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "es:*"
      Resource  = "${aws_opensearch_domain.opsh.arn}/*"
    }]
  })
}

resource "aws_opensearch_domain_saml_options" "opsh" {
  domain_name = aws_opensearch_domain.opsh.domain_name

  saml_options {
    enabled = true

    idp {
      entity_id        = "https://opsh-idp.example.com"
      metadata_content = "<EntityDescriptor entityID=\"https://opsh-idp.example.com\"></EntityDescriptor>"
    }
  }
}

resource "aws_s3_bucket" "opsh_pkg" {
  bucket = "opsh-pkg-bucket"
}

resource "aws_s3_object" "opsh_pkg" {
  bucket  = aws_s3_bucket.opsh_pkg.id
  key     = "opsh/dictionary.txt"
  content = "example custom dictionary"
}

resource "aws_opensearch_package" "opsh" {
  package_name = "opsh-dictionary"
  package_type = "TXT-DICTIONARY"

  package_source {
    s3_bucket_name = aws_s3_bucket.opsh_pkg.id
    s3_key         = aws_s3_object.opsh_pkg.key
  }
}

resource "aws_opensearch_package_association" "opsh" {
  package_id  = aws_opensearch_package.opsh.id
  domain_name = aws_opensearch_domain.opsh.domain_name
}

resource "aws_vpc" "opsh_ep" {
  cidr_block = "10.202.0.0/16"

  tags = {
    Name = "opsh-ep-vpc"
  }
}

resource "aws_subnet" "opsh_ep" {
  vpc_id     = aws_vpc.opsh_ep.id
  cidr_block = "10.202.1.0/24"

  tags = {
    Name = "opsh-ep-subnet"
  }
}

resource "aws_opensearch_vpc_endpoint" "opsh" {
  domain_arn = aws_opensearch_domain.opsh.arn

  vpc_options {
    subnet_ids = [aws_subnet.opsh_ep.id]
  }
}

resource "aws_opensearch_authorize_vpc_endpoint_access" "opsh" {
  domain_name = aws_opensearch_domain.opsh.domain_name
  account     = "000000000000"
}

resource "aws_opensearch_outbound_connection" "opsh" {
  connection_alias  = "opsh-connection"
  accept_connection = false

  local_domain_info {
    domain_name = aws_opensearch_domain.opsh.domain_name
    owner_id    = "000000000000"
    region      = "us-east-1"
  }

  remote_domain_info {
    domain_name = aws_opensearch_domain.opsh_remote.domain_name
    owner_id    = "000000000000"
    region      = "us-east-1"
  }
}

resource "aws_opensearch_inbound_connection_accepter" "opsh" {
  connection_id = aws_opensearch_outbound_connection.opsh.id
}

##############################################################################
# Shield: a subscription, a protection on an ALB, a protection group, DRT
# role/log-bucket associations, proactive engagement, and an application
# layer automatic response.
##############################################################################

resource "aws_vpc" "opsh_shield" {
  cidr_block = "10.203.0.0/16"

  tags = {
    Name = "opsh-shield-vpc"
  }
}

resource "aws_subnet" "opsh_shield_a" {
  vpc_id     = aws_vpc.opsh_shield.id
  cidr_block = "10.203.1.0/24"

  tags = {
    Name = "opsh-shield-subnet-a"
  }
}

resource "aws_subnet" "opsh_shield_b" {
  vpc_id     = aws_vpc.opsh_shield.id
  cidr_block = "10.203.2.0/24"

  tags = {
    Name = "opsh-shield-subnet-b"
  }
}

resource "aws_lb" "opsh_shield" {
  name               = "opsh-shield-alb"
  internal           = false
  load_balancer_type = "application"
  subnets            = [aws_subnet.opsh_shield_a.id, aws_subnet.opsh_shield_b.id]
}

resource "aws_shield_subscription" "opsh" {
  skip_destroy = true
}

resource "aws_shield_protection" "opsh" {
  name         = "opsh-protection"
  resource_arn = aws_lb.opsh_shield.arn
}

resource "aws_shield_protection_group" "opsh" {
  protection_group_id = "opsh-protection-group"
  aggregation         = "MAX"
  pattern             = "ARBITRARY"
  members             = [aws_lb.opsh_shield.arn]

  depends_on = [aws_shield_protection.opsh]
}

resource "aws_shield_proactive_engagement" "opsh" {
  enabled = true

  emergency_contact {
    contact_notes = "opsh primary contact"
    email_address = "opsh-oncall@example.com"
    phone_number  = "+12025550123"
  }

  depends_on = [aws_shield_subscription.opsh]
}

resource "aws_iam_role" "opsh_drt" {
  name = "opsh-drt-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "drt.shield.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "opsh_drt" {
  role       = aws_iam_role.opsh_drt.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSShieldDRTAccessPolicy"
}

resource "aws_shield_drt_access_role_arn_association" "opsh" {
  role_arn = aws_iam_role.opsh_drt.arn

  depends_on = [aws_iam_role_policy_attachment.opsh_drt]

  # The provider's Delete polls DescribeDRTAccess waiting for it to report
  # "not found", but real AWS's DescribeDRTAccess always succeeds (it's a
  # per-account describe, never a 404) -- so this poll never converges
  # against any backend, real or emulated. Bound it short, as
  # fsx-file-systems's FSx override does for a real 10-minute provider delay.
  timeouts {
    delete = "20s"
  }
}

resource "aws_s3_bucket" "opsh_drt_log" {
  bucket = "opsh-drt-log-bucket"
}

resource "aws_shield_drt_access_log_bucket_association" "opsh" {
  log_bucket              = aws_s3_bucket.opsh_drt_log.id
  role_arn_association_id = aws_shield_drt_access_role_arn_association.opsh.id
}

resource "aws_shield_application_layer_automatic_response" "opsh" {
  resource_arn = aws_lb.opsh_shield.arn
  action       = "COUNT"

  depends_on = [aws_shield_protection.opsh]
}
