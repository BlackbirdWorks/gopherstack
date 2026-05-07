terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    route53    = var.gopherstack_endpoint
    cloudfront = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Route53: hosted zone
# ---------------------------------------------------------------------------

resource "aws_route53_zone" "primary" {
  name = "example-dns-cdn.com"
}

# ---------------------------------------------------------------------------
# Route53: A record
# ---------------------------------------------------------------------------

resource "aws_route53_record" "www" {
  zone_id = aws_route53_zone.primary.zone_id
  name    = "www.example-dns-cdn.com"
  type    = "A"
  ttl     = 300
  records = ["203.0.113.10"]
}

# ---------------------------------------------------------------------------
# Route53: CNAME record
# ---------------------------------------------------------------------------

resource "aws_route53_record" "api" {
  zone_id = aws_route53_zone.primary.zone_id
  name    = "api.example-dns-cdn.com"
  type    = "CNAME"
  ttl     = 300
  records = ["backend.example-dns-cdn.com"]
}

# ---------------------------------------------------------------------------
# Route53: alias record pointing to a CloudFront distribution
# ---------------------------------------------------------------------------

resource "aws_cloudfront_distribution" "main" {
  enabled             = true
  comment             = "dns-cdn-comprehensive test distribution"
  default_root_object = "index.html"

  origin {
    domain_name = "origin.example-dns-cdn.com"
    origin_id   = "primary-origin"

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
    target_origin_id       = "primary-origin"
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

resource "aws_route53_record" "cf_alias" {
  zone_id = aws_route53_zone.primary.zone_id
  name    = "cdn.example-dns-cdn.com"
  type    = "A"

  alias {
    name                   = aws_cloudfront_distribution.main.domain_name
    zone_id                = "Z2FDTNDATAQYW2" # CloudFront hosted zone ID
    evaluate_target_health = false
  }
}

# ---------------------------------------------------------------------------
# Route53: health check
# ---------------------------------------------------------------------------

resource "aws_route53_health_check" "web" {
  fqdn              = "www.example-dns-cdn.com"
  port              = 443
  type              = "HTTPS"
  resource_path     = "/health"
  failure_threshold = 3
  request_interval  = 30
}

# ---------------------------------------------------------------------------
# Route53: traffic policy
# ---------------------------------------------------------------------------

resource "aws_route53_traffic_policy" "weighted" {
  name     = "weighted-routing"
  comment  = "Weighted traffic policy for A/B routing"
  document = jsonencode({
    AWSPolicyFormatVersion = "2015-10-01"
    RecordType             = "A"
    Endpoints = {
      primary = {
        Type  = "value"
        Value = "203.0.113.10"
      }
      secondary = {
        Type  = "value"
        Value = "203.0.113.20"
      }
    }
    Rules = {
      weighted = {
        RuleType = "weighted"
        Items = [
          { EndpointReference = "primary", Weight = 80 },
          { EndpointReference = "secondary", Weight = 20 },
        ]
      }
    }
    StartRule = "weighted"
  })
}

resource "aws_route53_traffic_policy_instance" "weighted" {
  name                   = "weighted.example-dns-cdn.com"
  traffic_policy_id      = aws_route53_traffic_policy.weighted.id
  traffic_policy_version = aws_route53_traffic_policy.weighted.version
  hosted_zone_id         = aws_route53_zone.primary.zone_id
  ttl                    = 60
}

# ---------------------------------------------------------------------------
# Route53: CIDR collection
# ---------------------------------------------------------------------------

resource "aws_route53_cidr_collection" "offices" {
  name = "office-ranges"
}

resource "aws_route53_cidr_location" "us_east" {
  cidr_collection_id = aws_route53_cidr_collection.offices.id
  name               = "us-east"
  cidr_blocks        = ["203.0.113.0/24", "198.51.100.0/24"]
}

# ---------------------------------------------------------------------------
# Route53: query logging
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "dns_logs" {
  name              = "/aws/route53/example-dns-cdn.com"
  retention_in_days = 7
}

# ---------------------------------------------------------------------------
# CloudFront: cache policy
# ---------------------------------------------------------------------------

resource "aws_cloudfront_cache_policy" "standard" {
  name        = "dns-cdn-standard-cache"
  comment     = "Standard cache policy for dns-cdn-comprehensive"
  default_ttl = 86400
  max_ttl     = 31536000
  min_ttl     = 0

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

# ---------------------------------------------------------------------------
# CloudFront: origin access control
# ---------------------------------------------------------------------------

resource "aws_cloudfront_origin_access_control" "s3_oac" {
  name                              = "dns-cdn-s3-oac"
  description                       = "OAC for S3 origin"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

# ---------------------------------------------------------------------------
# CloudFront: function (viewer request)
# ---------------------------------------------------------------------------

resource "aws_cloudfront_function" "viewer_req" {
  name    = "dns-cdn-viewer-request"
  runtime = "cloudfront-js-2.0"
  comment = "Viewer request function for dns-cdn-comprehensive"
  publish = true
  code    = <<-EOF
    function handler(event) {
      return event.request;
    }
  EOF
}

# ---------------------------------------------------------------------------
# CloudFront: continuous deployment policy
# ---------------------------------------------------------------------------

resource "aws_cloudfront_continuous_deployment_policy" "canary" {
  enabled = true

  staging_distribution_dns_names {
    items    = [aws_cloudfront_distribution.main.domain_name]
    quantity = 1
  }

  traffic_config {
    type = "SingleWeight"

    single_weight_config {
      weight = "0.05"
    }
  }
}

# ---------------------------------------------------------------------------
# CloudFront: key value store
# ---------------------------------------------------------------------------

resource "aws_cloudfront_key_value_store" "config" {
  name    = "dns-cdn-config-store"
  comment = "Config KVS for dns-cdn-comprehensive"
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "hosted_zone_id" {
  value = aws_route53_zone.primary.zone_id
}

output "distribution_domain_name" {
  value = aws_cloudfront_distribution.main.domain_name
}

output "traffic_policy_id" {
  value = aws_route53_traffic_policy.weighted.id
}

output "cidr_collection_id" {
  value = aws_route53_cidr_collection.offices.id
}

output "cache_policy_id" {
  value = aws_cloudfront_cache_policy.standard.id
}

output "continuous_deployment_policy_id" {
  value = aws_cloudfront_continuous_deployment_policy.canary.id
}

output "key_value_store_id" {
  value = aws_cloudfront_key_value_store.config.id
}

output "cloudfront_function_arn" {
  value = aws_cloudfront_function.viewer_req.arn
}
