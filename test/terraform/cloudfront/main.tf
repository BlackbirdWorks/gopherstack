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
    cloudfront = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Cache policy
# ---------------------------------------------------------------------------

resource "aws_cloudfront_cache_policy" "example" {
  name        = "gopherstack-cache-policy"
  comment     = "Test cache policy for gopherstack"
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
# Origin access control
# ---------------------------------------------------------------------------

resource "aws_cloudfront_origin_access_control" "example" {
  name                              = "gopherstack-oac"
  description                       = "Test OAC for gopherstack"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

# ---------------------------------------------------------------------------
# Origin request policy
# ---------------------------------------------------------------------------

resource "aws_cloudfront_origin_request_policy" "example" {
  name    = "gopherstack-origin-request-policy"
  comment = "Test origin request policy"

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

# ---------------------------------------------------------------------------
# Response headers policy
# ---------------------------------------------------------------------------

resource "aws_cloudfront_response_headers_policy" "example" {
  name    = "gopherstack-response-headers"
  comment = "Test response headers policy"

  cors_config {
    access_control_allow_credentials = false
    access_control_allow_headers {
      items = ["*"]
    }
    access_control_allow_methods {
      items = ["GET", "HEAD"]
    }
    access_control_allow_origins {
      items = ["*"]
    }
    origin_override = true
  }
}

# ---------------------------------------------------------------------------
# CloudFront function
# ---------------------------------------------------------------------------

resource "aws_cloudfront_function" "example" {
  name    = "gopherstack-function"
  runtime = "cloudfront-js-2.0"
  comment = "Test CloudFront function"
  publish = true
  code    = <<-EOT
    function handler(event) {
      return event.request;
    }
  EOT
}

# ---------------------------------------------------------------------------
# Public key (for key group)
# ---------------------------------------------------------------------------

resource "aws_cloudfront_public_key" "example" {
  name        = "gopherstack-public-key"
  comment     = "Test public key"
  encoded_key = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA\n-----END PUBLIC KEY-----"
}

# ---------------------------------------------------------------------------
# Key group
# ---------------------------------------------------------------------------

resource "aws_cloudfront_key_group" "example" {
  name    = "gopherstack-key-group"
  comment = "Test key group"
  items   = [aws_cloudfront_public_key.example.id]
}

# ---------------------------------------------------------------------------
# Field level encryption profile
# ---------------------------------------------------------------------------

resource "aws_cloudfront_field_level_encryption_profile" "example" {
  name    = "gopherstack-fle-profile"
  comment = "Test FLE profile"

  encryption_entities {
    items {
      public_key_id = aws_cloudfront_public_key.example.id
      provider_id   = "gopherstack-provider"

      field_patterns {
        items = ["DateOfBirth"]
      }
    }
  }
}

# ---------------------------------------------------------------------------
# Field level encryption config
# ---------------------------------------------------------------------------

resource "aws_cloudfront_field_level_encryption_config" "example" {
  comment = "Test FLE config"

  content_type_profile_config {
    forward_when_content_type_is_unknown = true

    content_type_profiles {
      items {
        content_type = "application/x-www-form-urlencoded"
        format       = "URLEncoded"
      }
    }
  }

  query_arg_profile_config {
    forward_when_query_arg_profile_is_unknown = true
  }
}

# ---------------------------------------------------------------------------
# Distribution using the above resources
# ---------------------------------------------------------------------------

resource "aws_cloudfront_distribution" "example" {
  origin {
    domain_name              = "example-bucket.s3.us-east-1.amazonaws.com"
    origin_id                = "gopherstack-s3-origin"
    origin_access_control_id = aws_cloudfront_origin_access_control.example.id
  }

  enabled             = true
  comment             = "gopherstack test distribution"
  default_root_object = "index.html"

  default_cache_behavior {
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "gopherstack-s3-origin"
    cache_policy_id        = aws_cloudfront_cache_policy.example.id
    viewer_protocol_policy = "redirect-to-https"
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

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "distribution_id" {
  value = aws_cloudfront_distribution.example.id
}

output "distribution_domain_name" {
  value = aws_cloudfront_distribution.example.domain_name
}

output "cache_policy_id" {
  value = aws_cloudfront_cache_policy.example.id
}

output "oac_id" {
  value = aws_cloudfront_origin_access_control.example.id
}

output "public_key_id" {
  value = aws_cloudfront_public_key.example.id
}

output "key_group_id" {
  value = aws_cloudfront_key_group.example.id
}
