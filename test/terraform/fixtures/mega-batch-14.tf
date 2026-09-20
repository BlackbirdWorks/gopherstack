##############################################################################
# CloudFront
##############################################################################

resource "aws_cloudfront_public_key" "example" {
  name        = "mega-batch-14-public-key"
  comment     = "mega batch 14 public key"
  encoded_key = <<-EOT
  -----BEGIN PUBLIC KEY-----
  MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuKUWTWVo77dy9tJXJfTt
  DifuXwSwSQbVgQMGmzwJXAJRQ74jZu9ya9dVGasAuTRpWFR6rIH6MOqfLZck1on6
  lXuB4kQ0CMZgi5pvPKFmiXEQ0LdvbawB0z6pi6IiGMsyqY51eLKmi21wVodlFCjU
  fQMv40RrdE1j1RXufzgacxJFcoD86vD5ypv2rY4HrSSaQLaEPdVRFmX8BuRmXi0P
  Nui7wIw+6q+iliFG+m2Tm583GepFDbVm79mxjJ5nBA+pA5U+8jIAGMHXjkDudieW
  JxRXNyRdlV6MkFttIVr3ASixq9eQKIZ6X41DMRTfzYSkJj8rvpLsPEgYEMQwTQYs
  6QIDAQAB
  -----END PUBLIC KEY-----
  EOT
}

resource "aws_cloudfront_key_group" "example" {
  name    = "mega-batch-14-key-group"
  comment = "mega batch 14 key group"
  items   = [aws_cloudfront_public_key.example.id]
}

resource "aws_cloudfront_field_level_encryption_profile" "example" {
  name    = "mega-batch-14-fle-profile"
  comment = "mega batch 14 field level encryption profile"

  encryption_entities {
    items {
      public_key_id = aws_cloudfront_public_key.example.id
      provider_id   = "mega-batch-14-provider"

      field_patterns {
        items = ["DateOfBirth"]
      }
    }
  }
}

resource "aws_cloudfront_field_level_encryption_config" "example" {
  comment = "mega batch 14 field level encryption config"

  content_type_profile_config {
    forward_when_content_type_is_unknown = true

    content_type_profiles {
      items {
        content_type = "application/x-www-form-urlencoded"
        format       = "URLEncoded"
        profile_id   = aws_cloudfront_field_level_encryption_profile.example.id
      }
    }
  }

  query_arg_profile_config {
    forward_when_query_arg_profile_is_unknown = true

    query_arg_profiles {
      items {
        profile_id = aws_cloudfront_field_level_encryption_profile.example.id
        query_arg  = "Arg1"
      }
    }
  }
}

resource "aws_cloudfront_origin_access_control" "example" {
  name                              = "mega-batch-14-oac"
  description                       = "mega batch 14 origin access control"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_origin_request_policy" "example" {
  name    = "mega-batch-14-orp"
  comment = "mega batch 14 origin request policy"

  cookies_config {
    cookie_behavior = "none"
  }
  headers_config {
    header_behavior = "none"
  }
  query_strings_config {
    query_string_behavior = "none"
  }
}

resource "aws_cloudfront_response_headers_policy" "example" {
  name    = "mega-batch-14-rhp"
  comment = "mega batch 14 response headers policy"

  cors_config {
    access_control_allow_credentials = false
    origin_override                  = true

    access_control_allow_headers {
      items = ["*"]
    }
    access_control_allow_methods {
      items = ["GET", "HEAD"]
    }
    access_control_allow_origins {
      items = ["*"]
    }
  }
}

resource "aws_cloudfront_cache_policy" "example" {
  name        = "mega-batch-14-cache-policy"
  comment     = "mega batch 14 cache policy"
  default_ttl = 86400
  max_ttl     = 31536000
  min_ttl     = 1

  parameters_in_cache_key_and_forwarded_to_origin {
    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "none"
    }
    query_strings_config {
      query_string_behavior = "none"
    }
  }
}

resource "aws_cloudfront_function" "example" {
  name    = "mega-batch-14-function"
  runtime = "cloudfront-js-2.0"
  comment = "mega batch 14 function"
  publish = true

  code = <<-EOT
  function handler(event) {
    return event.request;
  }
  EOT
}

resource "aws_cloudfront_distribution" "staging" {
  enabled = true
  staging = true
  comment = "mega-batch-14 staging distribution"

  origin {
    domain_name = "origin.mega-batch-14.example.com"
    origin_id   = "mega-batch-14-origin"

    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
  }

  default_cache_behavior {
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "mega-batch-14-origin"
    viewer_protocol_policy = "redirect-to-https"

    forwarded_values {
      query_string = false
      cookies {
        forward = "none"
      }
    }
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}

resource "aws_cloudfront_continuous_deployment_policy" "example" {
  enabled = true

  staging_distribution_dns_names {
    items    = [aws_cloudfront_distribution.staging.domain_name]
    quantity = 1
  }

  traffic_config {
    type = "SingleWeight"
    single_weight_config {
      weight = "0.1"
    }
  }
}

resource "aws_cloudfront_monitoring_subscription" "example" {
  distribution_id = aws_cloudfront_distribution.staging.id

  monitoring_subscription {
    realtime_metrics_subscription_config {
      realtime_metrics_subscription_status = "Enabled"
    }
  }
}

resource "aws_kinesis_stream" "cf_logs" {
  name             = "mega-batch-14-cf-logs"
  shard_count      = 1
  retention_period = 24
}

resource "aws_iam_role" "cf_realtime_logs" {
  name = "mega-batch-14-cf-realtime-logs-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "cloudfront.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_cloudfront_realtime_log_config" "example" {
  name          = "mega-batch-14-realtime-log-config"
  sampling_rate = 75
  fields        = ["timestamp", "c-ip"]

  endpoint {
    stream_type = "Kinesis"

    kinesis_stream_config {
      role_arn   = aws_iam_role.cf_realtime_logs.arn
      stream_arn = aws_kinesis_stream.cf_logs.arn
    }
  }
}

resource "aws_vpc" "cfvpc" {
  cidr_block = "10.118.0.0/16"

  tags = {
    Name = "mega-batch-14-cf-vpc"
  }
}

resource "aws_subnet" "cfa" {
  vpc_id     = aws_vpc.cfvpc.id
  cidr_block = "10.118.1.0/24"

  tags = {
    Name = "mega-batch-14-cf-subnet-a"
  }
}

resource "aws_subnet" "cfb" {
  vpc_id     = aws_vpc.cfvpc.id
  cidr_block = "10.118.2.0/24"

  tags = {
    Name = "mega-batch-14-cf-subnet-b"
  }
}

resource "aws_lb" "cf_origin" {
  name               = "mega-batch-14-cf-alb"
  internal           = false
  load_balancer_type = "application"
  subnets            = [aws_subnet.cfa.id, aws_subnet.cfb.id]
}

resource "aws_cloudfront_vpc_origin" "example" {
  vpc_origin_endpoint_config {
    name                   = "mega-batch-14-vpc-origin"
    arn                    = aws_lb.cf_origin.arn
    http_port              = 8080
    https_port             = 8443
    origin_protocol_policy = "https-only"

    origin_ssl_protocols {
      items    = ["TLSv1.2"]
      quantity = 1
    }
  }
}

##############################################################################
# Route53
##############################################################################

resource "aws_route53_delegation_set" "example" {
  reference_name = "mega-batch-14"
}

resource "aws_route53_health_check" "example" {
  fqdn              = "mega-batch-14.example.com"
  port              = 80
  type              = "HTTP"
  resource_path     = "/healthz"
  failure_threshold = 3
  request_interval  = 30
}

resource "aws_route53_cidr_collection" "example" {
  name = "mega-batch-14-cidr-collection"
}

resource "aws_route53_cidr_location" "example" {
  cidr_collection_id = aws_route53_cidr_collection.example.id
  name               = "mb14-location"
  cidr_blocks        = ["10.114.32.0/24"]
}

resource "aws_route53_zone" "dnssec" {
  name = "mega-batch-14-dnssec.example.com"
}

resource "aws_kms_key" "dnssec" {
  customer_master_key_spec = "ECC_NIST_P256"
  deletion_window_in_days  = 7
  key_usage                = "SIGN_VERIFY"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowRoute53DNSSEC"
        Effect    = "Allow"
        Principal = { Service = "dnssec-route53.amazonaws.com" }
        Action    = ["kms:DescribeKey", "kms:GetPublicKey", "kms:Sign"]
        Resource  = "*"
      },
      {
        Sid       = "EnableIAM"
        Effect    = "Allow"
        Principal = { AWS = "*" }
        Action    = "kms:*"
        Resource  = "*"
      }
    ]
  })
}

resource "aws_route53_key_signing_key" "example" {
  hosted_zone_id             = aws_route53_zone.dnssec.id
  key_management_service_arn = aws_kms_key.dnssec.arn
  name                       = "mega_batch_14_ksk"
}

resource "aws_route53_hosted_zone_dnssec" "example" {
  depends_on     = [aws_route53_key_signing_key.example]
  hosted_zone_id = aws_route53_key_signing_key.example.hosted_zone_id
}

resource "aws_cloudwatch_log_group" "route53" {
  name = "/aws/route53/mega-batch-14"
}

resource "aws_route53_query_log" "example" {
  cloudwatch_log_group_arn = aws_cloudwatch_log_group.route53.arn
  zone_id                  = aws_route53_zone.dnssec.zone_id
}

resource "aws_route53_traffic_policy" "example" {
  name    = "mega-batch-14-traffic-policy"
  comment = "mega batch 14"
  document = jsonencode({
    AWSPolicyFormatVersion = "2015-10-01"
    RecordType             = "A"
    Endpoints = {
      endpoint-start = {
        Type  = "value"
        Value = "192.0.2.10"
      }
    }
    StartEndpoint = "endpoint-start"
  })
}

resource "aws_route53_traffic_policy_instance" "example" {
  name                   = "tp.mega-batch-14-dnssec.example.com"
  traffic_policy_id      = aws_route53_traffic_policy.example.id
  traffic_policy_version = aws_route53_traffic_policy.example.version
  hosted_zone_id         = aws_route53_zone.dnssec.zone_id
  ttl                    = 60
}

resource "aws_route53_zone" "excl" {
  name          = "mega-batch-14-excl.example.com"
  force_destroy = true
}

resource "aws_route53_records_exclusive" "example" {
  zone_id = aws_route53_zone.excl.zone_id

  resource_record_set {
    name = "sub.mega-batch-14-excl.example.com"
    type = "A"
    ttl  = 30

    resource_records {
      value = "192.0.2.1"
    }
  }
}

resource "aws_vpc" "r53a" {
  cidr_block           = "10.116.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "mega-batch-14-r53-vpc-a"
  }
}

resource "aws_vpc" "r53b" {
  cidr_block           = "10.117.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "mega-batch-14-r53-vpc-b"
  }
}

resource "aws_route53_zone" "private" {
  name = "mega-batch-14-private.internal"

  vpc {
    vpc_id = aws_vpc.r53a.id
  }
}

resource "aws_route53_vpc_association_authorization" "example" {
  zone_id = aws_route53_zone.private.zone_id
  vpc_id  = aws_vpc.r53b.id
}

resource "aws_route53_zone_association" "example" {
  zone_id = aws_route53_zone.private.zone_id
  vpc_id  = aws_vpc.r53b.id

  depends_on = [aws_route53_vpc_association_authorization.example]
}
