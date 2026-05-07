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
    route53 = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Public hosted zone
# ---------------------------------------------------------------------------

resource "aws_route53_zone" "public" {
  name = "gopherstack-test.example.com"

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

# ---------------------------------------------------------------------------
# A record
# ---------------------------------------------------------------------------

resource "aws_route53_record" "www" {
  zone_id = aws_route53_zone.public.zone_id
  name    = "www.gopherstack-test.example.com"
  type    = "A"
  ttl     = 300
  records = ["203.0.113.10"]
}

# ---------------------------------------------------------------------------
# CNAME record
# ---------------------------------------------------------------------------

resource "aws_route53_record" "alias" {
  zone_id = aws_route53_zone.public.zone_id
  name    = "alias.gopherstack-test.example.com"
  type    = "CNAME"
  ttl     = 60
  records = ["www.gopherstack-test.example.com"]
}

# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

resource "aws_route53_health_check" "web" {
  fqdn              = "www.gopherstack-test.example.com"
  port              = 80
  type              = "HTTP"
  resource_path     = "/health"
  failure_threshold = "3"
  request_interval  = "30"

  tags = {
    Name = "gopherstack-test-hc"
  }
}

# ---------------------------------------------------------------------------
# Reusable delegation set
# ---------------------------------------------------------------------------

resource "aws_route53_delegation_set" "main" {
  reference_name = "gopherstack-test"
}

# ---------------------------------------------------------------------------
# Private hosted zone with VPC association
# ---------------------------------------------------------------------------

resource "aws_route53_zone" "private" {
  name = "internal.gopherstack-test.example.com"

  vpc {
    vpc_id     = "vpc-12345678"
    vpc_region = "us-east-1"
  }

  tags = {
    Environment = "test"
    Visibility  = "private"
  }
}

# ---------------------------------------------------------------------------
# Query logging config (requires a CloudWatch log group ARN)
# ---------------------------------------------------------------------------

resource "aws_route53_query_log" "public" {
  depends_on = [aws_route53_zone.public]

  cloudwatch_log_group_arn = "arn:aws:logs:us-east-1:123456789012:log-group:/aws/route53/gopherstack-test"
  zone_id                  = aws_route53_zone.public.zone_id
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "public_zone_id" {
  value = aws_route53_zone.public.zone_id
}

output "private_zone_id" {
  value = aws_route53_zone.private.zone_id
}

output "health_check_id" {
  value = aws_route53_health_check.web.id
}

output "delegation_set_id" {
  value = aws_route53_delegation_set.main.id
}

output "query_log_id" {
  value = aws_route53_query_log.public.id
}
