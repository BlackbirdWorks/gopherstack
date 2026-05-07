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
    ec2 = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---- VPC ----

resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"

  tags = {
    Name = "ec2-networking-test"
  }
}

resource "aws_subnet" "a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = "us-east-1a"

  tags = {
    Name = "ec2-networking-subnet-a"
  }
}

# ---- Transit Gateway ----

resource "aws_ec2_transit_gateway" "tgw" {
  description = "ec2-networking-test-tgw"

  tags = {
    Name = "ec2-networking-tgw"
  }
}

resource "aws_ec2_transit_gateway_vpc_attachment" "attach" {
  transit_gateway_id = aws_ec2_transit_gateway.tgw.id
  vpc_id             = aws_vpc.main.id
  subnet_ids         = [aws_subnet.a.id]

  tags = {
    Name = "ec2-networking-tgw-attach"
  }
}

# ---- VPC Flow Logs ----

resource "aws_flow_log" "vpc_flow" {
  vpc_id          = aws_vpc.main.id
  traffic_type    = "ALL"
  iam_role_arn    = "arn:aws:iam::000000000000:role/flow-log-role"
  log_destination = "arn:aws:logs:us-east-1:000000000000:log-group:vpc-flow-logs"

  tags = {
    Name = "ec2-networking-flow-log"
  }
}

# ---- Network ACL ----

resource "aws_network_acl" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "ec2-networking-nacl"
  }
}

resource "aws_network_acl_rule" "allow_http_in" {
  network_acl_id = aws_network_acl.main.id
  rule_number    = 100
  egress         = false
  protocol       = "tcp"
  rule_action    = "allow"
  cidr_block     = "0.0.0.0/0"
  from_port      = 80
  to_port        = 80
}

# ---- DHCP Options ----

resource "aws_vpc_dhcp_options" "opts" {
  domain_name         = "example.internal"
  domain_name_servers = ["10.0.0.2"]

  tags = {
    Name = "ec2-networking-dhcp"
  }
}

resource "aws_vpc_dhcp_options_association" "assoc" {
  vpc_id          = aws_vpc.main.id
  dhcp_options_id = aws_vpc_dhcp_options.opts.id
}

# ---- Launch Template ----

resource "aws_launch_template" "lt" {
  name          = "ec2-networking-lt"
  image_id      = "ami-12345678"
  instance_type = "t3.micro"

  tags = {
    Name = "ec2-networking-lt"
  }
}

# ---- Outputs ----

output "vpc_id" {
  value = aws_vpc.main.id
}

output "transit_gateway_id" {
  value = aws_ec2_transit_gateway.tgw.id
}

output "tgw_attachment_id" {
  value = aws_ec2_transit_gateway_vpc_attachment.attach.id
}

output "flow_log_id" {
  value = aws_flow_log.vpc_flow.id
}

output "network_acl_id" {
  value = aws_network_acl.main.id
}

output "dhcp_options_id" {
  value = aws_vpc_dhcp_options.opts.id
}

output "launch_template_id" {
  value = aws_launch_template.lt.id
}
