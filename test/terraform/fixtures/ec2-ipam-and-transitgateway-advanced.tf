resource "aws_vpc" "example" {
  cidr_block = "{{.VPCCidr}}"

  tags = {
    Name = "eita-vpc"
  }
}

resource "aws_subnet" "example" {
  vpc_id            = aws_vpc.example.id
  cidr_block        = cidrsubnet(aws_vpc.example.cidr_block, 8, 1)
  availability_zone = "us-east-1a"

  tags = {
    Name = "eita-subnet"
  }
}

resource "aws_network_interface" "example" {
  subnet_id = aws_subnet.example.id

  tags = {
    Name = "eita-eni"
  }
}

resource "aws_instance" "source" {
  ami           = "ami-12345678"
  instance_type = "t2.micro"
  subnet_id     = aws_subnet.example.id

  tags = {
    Name = "eita-source"
  }
}

resource "aws_instance" "destination" {
  ami           = "ami-12345678"
  instance_type = "t2.micro"
  subnet_id     = aws_subnet.example.id

  tags = {
    Name = "eita-destination"
  }
}

# ---- IPAM ----

resource "aws_vpc_ipam" "example" {
  operating_regions {
    region_name = "us-east-1"
  }

  tags = {
    Name = "eita-ipam"
  }
}

resource "aws_vpc_ipam_scope" "example" {
  ipam_id     = aws_vpc_ipam.example.id
  description = "eita scope"
}

resource "aws_vpc_ipam_pool" "example" {
  address_family = "ipv4"
  ipam_scope_id  = aws_vpc_ipam.example.private_default_scope_id
  locale         = "us-east-1"

  tags = {
    Name = "eita-ipam-pool"
  }
}

resource "aws_vpc_ipam_pool_cidr" "example" {
  ipam_pool_id = aws_vpc_ipam_pool.example.id
  cidr         = "10.90.0.0/16"
}

resource "aws_vpc_ipam_pool_cidr_allocation" "example" {
  ipam_pool_id = aws_vpc_ipam_pool.example.id
  cidr         = "10.90.1.0/24"

  depends_on = [aws_vpc_ipam_pool_cidr.example]
}

resource "aws_vpc_ipam_resource_discovery" "example" {
  operating_regions {
    region_name = "us-east-1"
  }

  tags = {
    Name = "eita-ipam-rd"
  }
}

resource "aws_vpc_ipam_resource_discovery_association" "example" {
  ipam_id                    = aws_vpc_ipam.example.id
  ipam_resource_discovery_id = aws_vpc_ipam_resource_discovery.example.id
}

# ---- Traffic mirroring ----

resource "aws_ec2_traffic_mirror_filter" "example" {
  description = "eita filter"
}

resource "aws_ec2_traffic_mirror_filter_rule" "example" {
  description              = "eita rule"
  traffic_mirror_filter_id = aws_ec2_traffic_mirror_filter.example.id
  destination_cidr_block   = "0.0.0.0/0"
  source_cidr_block        = "0.0.0.0/0"
  rule_number              = 1
  rule_action              = "accept"
  traffic_direction        = "ingress"
}

resource "aws_ec2_traffic_mirror_target" "example" {
  network_interface_id = aws_network_interface.example.id
  description          = "eita target"
}

resource "aws_ec2_traffic_mirror_session" "example" {
  description              = "eita session"
  network_interface_id     = aws_instance.source.primary_network_interface_id
  traffic_mirror_filter_id = aws_ec2_traffic_mirror_filter.example.id
  traffic_mirror_target_id = aws_ec2_traffic_mirror_target.example.id
  session_number           = 1
}

# ---- Client VPN (needs an ACM cert) ----

resource "aws_acm_certificate" "vpn" {
  domain_name       = "eita.example.test"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_ec2_client_vpn_endpoint" "example" {
  description            = "eita client vpn"
  server_certificate_arn = aws_acm_certificate.vpn.arn
  client_cidr_block      = "10.200.0.0/22"

  authentication_options {
    type                       = "certificate-authentication"
    root_certificate_chain_arn = aws_acm_certificate.vpn.arn
  }

  connection_log_options {
    enabled = false
  }

  tags = {
    Name = "eita-cvpn"
  }
}

resource "aws_ec2_client_vpn_network_association" "example" {
  client_vpn_endpoint_id = aws_ec2_client_vpn_endpoint.example.id
  subnet_id              = aws_subnet.example.id
}

resource "aws_ec2_client_vpn_authorization_rule" "example" {
  client_vpn_endpoint_id = aws_ec2_client_vpn_endpoint.example.id
  target_network_cidr    = aws_vpc.example.cidr_block
  authorize_all_groups   = true
}

resource "aws_ec2_client_vpn_route" "example" {
  client_vpn_endpoint_id = aws_ec2_client_vpn_endpoint.example.id
  destination_cidr_block = "0.0.0.0/0"
  target_vpc_subnet_id   = aws_subnet.example.id

  depends_on = [aws_ec2_client_vpn_network_association.example]
}

# ---- Instance connect endpoint ----

resource "aws_security_group" "ice" {
  name   = "eita-ice-sg"
  vpc_id = aws_vpc.example.id
}

resource "aws_ec2_instance_connect_endpoint" "example" {
  subnet_id          = aws_subnet.example.id
  security_group_ids = [aws_security_group.ice.id]

  tags = {
    Name = "eita-ice"
  }
}

# ---- Network Insights ----

resource "aws_ec2_network_insights_path" "example" {
  source      = aws_instance.source.id
  destination = aws_instance.destination.id
  protocol    = "tcp"
}

resource "aws_ec2_network_insights_analysis" "example" {
  network_insights_path_id = aws_ec2_network_insights_path.example.id
}

# ---- Transit Gateway extras ----

resource "aws_ec2_transit_gateway" "example" {
  description = "eita tgw"

  tags = {
    Name = "eita-tgw"
  }
}

resource "aws_ec2_transit_gateway_vpc_attachment" "example" {
  transit_gateway_id = aws_ec2_transit_gateway.example.id
  vpc_id             = aws_vpc.example.id
  subnet_ids         = [aws_subnet.example.id]

  tags = {
    Name = "eita-tgw-attachment"
  }
}

resource "aws_ec2_transit_gateway_connect" "example" {
  transport_attachment_id = aws_ec2_transit_gateway_vpc_attachment.example.id
  transit_gateway_id      = aws_ec2_transit_gateway.example.id

  tags = {
    Name = "eita-tgw-connect"
  }
}

resource "aws_vpc" "peer" {
  cidr_block = "{{.PeerVPCCidr}}"

  tags = {
    Name = "eita-peer-vpc"
  }
}

resource "aws_ec2_transit_gateway" "peer" {
  description = "eita peer tgw"

  tags = {
    Name = "eita-peer-tgw"
  }
}

resource "aws_ec2_transit_gateway_peering_attachment" "example" {
  transit_gateway_id      = aws_ec2_transit_gateway.example.id
  peer_transit_gateway_id = aws_ec2_transit_gateway.peer.id
  peer_account_id         = aws_ec2_transit_gateway.peer.owner_id
  peer_region             = "us-east-1"

  tags = {
    Name = "eita-tgw-peering"
  }
}

resource "aws_ec2_transit_gateway_multicast_domain" "example" {
  transit_gateway_id = aws_ec2_transit_gateway.example.id

  tags = {
    Name = "eita-tgw-multicast"
  }
}

resource "aws_ec2_transit_gateway_policy_table" "example" {
  transit_gateway_id = aws_ec2_transit_gateway.example.id

  tags = {
    Name = "eita-tgw-policy-table"
  }
}
