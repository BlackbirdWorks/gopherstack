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
    iam = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---- VPC + subnets ----

resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"

  tags = {
    Name = "ec2-core-test"
  }
}

resource "aws_vpc_ipv4_cidr_block_association" "secondary" {
  vpc_id     = aws_vpc.main.id
  cidr_block = "10.1.0.0/16"
}

resource "aws_subnet" "a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = "us-east-1a"

  tags = {
    Name = "ec2-core-subnet-a"
  }
}

resource "aws_subnet" "b" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = "us-east-1b"

  tags = {
    Name = "ec2-core-subnet-b"
  }
}

# ---- Egress-only Internet Gateway (IPv6 egress) ----

resource "aws_egress_only_internet_gateway" "eigw" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "ec2-core-eigw"
  }
}

# ---- Route tables with ReplaceRouteTableAssociation ----

resource "aws_route_table" "rt_a" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "ec2-core-rt-a"
  }
}

resource "aws_route_table" "rt_b" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "ec2-core-rt-b"
  }
}

resource "aws_route_table_association" "assoc_a" {
  subnet_id      = aws_subnet.a.id
  route_table_id = aws_route_table.rt_a.id
}

# ---- Transit Gateway with Route Tables and Routes ----

resource "aws_ec2_transit_gateway" "tgw" {
  description = "ec2-core-test-tgw"

  tags = {
    Name = "ec2-core-tgw"
  }
}

resource "aws_ec2_transit_gateway_vpc_attachment" "attach" {
  transit_gateway_id = aws_ec2_transit_gateway.tgw.id
  vpc_id             = aws_vpc.main.id
  subnet_ids         = [aws_subnet.a.id]

  tags = {
    Name = "ec2-core-tgw-attach"
  }
}

resource "aws_ec2_transit_gateway_route_table" "tgw_rt" {
  transit_gateway_id = aws_ec2_transit_gateway.tgw.id

  tags = {
    Name = "ec2-core-tgw-rt"
  }
}

resource "aws_ec2_transit_gateway_route" "tgw_route" {
  destination_cidr_block         = "0.0.0.0/0"
  transit_gateway_attachment_id  = aws_ec2_transit_gateway_vpc_attachment.attach.id
  transit_gateway_route_table_id = aws_ec2_transit_gateway_route_table.tgw_rt.id
}

resource "aws_ec2_transit_gateway_route_table_association" "tgw_rt_assoc" {
  transit_gateway_attachment_id  = aws_ec2_transit_gateway_vpc_attachment.attach.id
  transit_gateway_route_table_id = aws_ec2_transit_gateway_route_table.tgw_rt.id
}

# ---- EC2 Instance with IAM Instance Profile ----

resource "aws_iam_role" "instance_role" {
  name = "ec2-core-instance-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_instance_profile" "instance_profile" {
  name = "ec2-core-instance-profile"
  role = aws_iam_role.instance_role.name
}

resource "aws_instance" "app" {
  ami           = "ami-12345678"
  instance_type = "t3.micro"
  subnet_id     = aws_subnet.a.id

  iam_instance_profile = aws_iam_instance_profile.instance_profile.name

  tags = {
    Name = "ec2-core-instance"
  }
}
