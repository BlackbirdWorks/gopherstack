##############################################################################
# ELBv2 (application load balancer family, "alb" and "lb" aliases)
##############################################################################

resource "aws_vpc" "elbv2b" {
  cidr_block = "10.115.0.0/16"

  tags = {
    Name = "elae-vpc"
  }
}

resource "aws_subnet" "elbv2b_a" {
  vpc_id     = aws_vpc.elbv2b.id
  cidr_block = "10.115.1.0/24"

  tags = {
    Name = "elae-subnet-a"
  }
}

resource "aws_subnet" "elbv2b_b" {
  vpc_id     = aws_vpc.elbv2b.id
  cidr_block = "10.115.2.0/24"

  tags = {
    Name = "elae-subnet-b"
  }
}

resource "aws_acm_certificate" "default" {
  domain_name       = "elae-default.example.com"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_acm_certificate" "alb_extra" {
  domain_name       = "elae-alb-extra.example.com"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_acm_certificate" "lb_extra" {
  domain_name       = "elae-lb-extra.example.com"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_alb" "example" {
  name               = "elae-alb"
  internal           = false
  load_balancer_type = "application"
  subnets            = [aws_subnet.elbv2b_a.id, aws_subnet.elbv2b_b.id]
}

resource "aws_alb_target_group" "example" {
  name        = "elae-tg"
  port        = 80
  protocol    = "HTTP"
  vpc_id      = aws_vpc.elbv2b.id
  target_type = "ip"
}

resource "aws_alb_listener" "example" {
  load_balancer_arn = aws_alb.example.arn
  port              = 443
  protocol          = "HTTPS"
  certificate_arn   = aws_acm_certificate.default.arn

  default_action {
    type             = "forward"
    target_group_arn = aws_alb_target_group.example.arn
  }
}

resource "aws_alb_listener_certificate" "example" {
  listener_arn    = aws_alb_listener.example.arn
  certificate_arn = aws_acm_certificate.alb_extra.arn
}

resource "aws_lb_listener_certificate" "example" {
  listener_arn    = aws_alb_listener.example.arn
  certificate_arn = aws_acm_certificate.lb_extra.arn
}

resource "aws_alb_listener_rule" "example" {
  listener_arn = aws_alb_listener.example.arn
  priority     = 10

  action {
    type             = "forward"
    target_group_arn = aws_alb_target_group.example.arn
  }

  condition {
    path_pattern {
      values = ["/alb/*"]
    }
  }
}

resource "aws_lb_listener_rule" "example" {
  listener_arn = aws_alb_listener.example.arn
  priority     = 20

  action {
    type             = "forward"
    target_group_arn = aws_alb_target_group.example.arn
  }

  condition {
    path_pattern {
      values = ["/lb/*"]
    }
  }
}

resource "aws_alb_target_group_attachment" "example" {
  target_group_arn = aws_alb_target_group.example.arn
  target_id        = "10.115.1.5"
  port             = 80
}

resource "aws_lb_target_group_attachment" "example" {
  target_group_arn = aws_alb_target_group.example.arn
  target_id        = "10.115.1.6"
  port             = 80
}

resource "aws_lb_trust_store" "example" {
  name                             = "elae-ts"
  ca_certificates_bundle_s3_bucket = "elae-ts-bucket"
  ca_certificates_bundle_s3_key    = "ca-bundle.pem"
}

resource "aws_lb_trust_store_revocation" "example" {
  trust_store_arn       = aws_lb_trust_store.example.arn
  revocations_s3_bucket = "elae-ts-bucket"
  revocations_s3_key    = "revocation.crl"
}

##############################################################################
# Classic ELB (aws_lb_cookie_stickiness_policy / aws_lb_ssl_negotiation_policy
# attach to aws_elb, not elbv2, despite the "lb" name)
##############################################################################

resource "aws_elb" "classic" {
  name               = "elae-elb"
  availability_zones = ["us-east-1a"]

  listener {
    instance_port     = 8000
    instance_protocol = "http"
    lb_port           = 80
    lb_protocol       = "http"
  }

  listener {
    instance_port      = 8443
    instance_protocol  = "https"
    lb_port            = 443
    lb_protocol        = "https"
    ssl_certificate_id = aws_acm_certificate.default.arn
  }
}

resource "aws_lb_cookie_stickiness_policy" "example" {
  name                     = "elae-cookie-policy"
  load_balancer            = aws_elb.classic.id
  lb_port                  = 80
  cookie_expiration_period = 600
}

resource "aws_lb_ssl_negotiation_policy" "example" {
  name          = "elae-ssl-policy"
  load_balancer = aws_elb.classic.id
  lb_port       = 443

  attribute {
    name  = "Protocol-TLSv1.2"
    value = "true"
  }

  attribute {
    name  = "Server-Defined-Cipher-Order"
    value = "true"
  }
}

##############################################################################
# ECR
##############################################################################

resource "aws_ecr_repository" "example" {
  name = "elae-repo"
}

resource "aws_ecr_repository_policy" "example" {
  repository = aws_ecr_repository.example.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowPull"
      Effect    = "Allow"
      Principal = "*"
      Action    = ["ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage"]
    }]
  })
}

resource "aws_ecr_lifecycle_policy" "example" {
  repository = aws_ecr_repository.example.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Expire untagged images older than 14 days"
      selection = {
        tagStatus   = "untagged"
        countType   = "sinceImagePushed"
        countUnit   = "days"
        countNumber = 14
      }
      action = { type = "expire" }
    }]
  })
}

resource "aws_ecr_registry_policy" "example" {
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowReplication"
      Effect    = "Allow"
      Principal = "*"
      Action    = "ecr:ReplicateImage"
      Resource  = "arn:aws:ecr:us-east-1:*:repository/*"
    }]
  })
}

resource "aws_ecr_pull_through_cache_rule" "example" {
  ecr_repository_prefix = "elae-ptc"
  upstream_registry_url = "public.ecr.aws"
}

resource "aws_ecr_repository_creation_template" "example" {
  prefix               = "elae-tmpl"
  description          = "elae template"
  image_tag_mutability = "IMMUTABLE"

  applied_for = ["PULL_THROUGH_CACHE"]

  encryption_configuration {
    encryption_type = "AES256"
  }

  lifecycle_policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Expire untagged images"
      selection = {
        tagStatus   = "untagged"
        countType   = "sinceImagePushed"
        countUnit   = "days"
        countNumber = 14
      }
      action = { type = "expire" }
    }]
  })
}

resource "aws_ecr_account_setting" "example" {
  name  = "BASIC_SCAN_TYPE_VERSION"
  value = "AWS_NATIVE"
}
