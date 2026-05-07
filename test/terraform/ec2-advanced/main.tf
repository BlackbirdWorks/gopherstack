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

# ---- VPC (required for VPN gateway attachment) ----

resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"

  tags = {
    Name = "ec2-advanced-test"
  }
}

# ---- VPN Gateways / Customer Gateways / VPN Connections ----

resource "aws_customer_gateway" "cgw" {
  bgp_asn    = 65000
  ip_address = "203.0.113.1"
  type       = "ipsec.1"

  tags = {
    Name = "ec2-advanced-cgw"
  }
}

resource "aws_vpn_gateway" "vgw" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "ec2-advanced-vgw"
  }
}

resource "aws_vpn_connection" "vpn" {
  vpn_gateway_id      = aws_vpn_gateway.vgw.id
  customer_gateway_id = aws_customer_gateway.cgw.id
  type                = "ipsec.1"

  tags = {
    Name = "ec2-advanced-vpn"
  }
}

# ---- VPC Peering ----

resource "aws_vpc" "peer" {
  cidr_block = "10.1.0.0/16"

  tags = {
    Name = "ec2-advanced-peer"
  }
}

resource "aws_vpc_peering_connection" "peer" {
  vpc_id      = aws_vpc.main.id
  peer_vpc_id = aws_vpc.peer.id
  auto_accept = true

  tags = {
    Name = "ec2-advanced-peering"
  }
}

# ---- VPC Endpoint Service Configuration ----

resource "aws_vpc_endpoint_service" "svc" {
  acceptance_required        = false
  network_load_balancer_arns = []

  tags = {
    Name = "ec2-advanced-endpoint-svc"
  }
}

# ---- IPAM ----

resource "aws_vpc_ipam" "ipam" {
  operating_regions {
    region_name = "us-east-1"
  }

  tags = {
    Name = "ec2-advanced-ipam"
  }
}

resource "aws_vpc_ipam_pool" "pool" {
  address_family = "ipv4"
  ipam_scope_id  = aws_vpc_ipam.ipam.private_default_scope_id
  locale         = "us-east-1"

  tags = {
    Name = "ec2-advanced-pool"
  }
}

resource "aws_vpc_ipam_pool_cidr" "cidr" {
  ipam_pool_id = aws_vpc_ipam_pool.pool.id
  cidr         = "10.128.0.0/16"
}

# ---- Outputs ----

output "vpn_connection_id" {
  value = aws_vpn_connection.vpn.id
}

output "vpn_gateway_id" {
  value = aws_vpn_gateway.vgw.id
}

output "customer_gateway_id" {
  value = aws_customer_gateway.cgw.id
}

output "peering_connection_id" {
  value = aws_vpc_peering_connection.peer.id
}

output "endpoint_service_id" {
  value = aws_vpc_endpoint_service.svc.id
}

output "ipam_id" {
  value = aws_vpc_ipam.ipam.id
}

output "ipam_pool_id" {
  value = aws_vpc_ipam_pool.pool.id
}
