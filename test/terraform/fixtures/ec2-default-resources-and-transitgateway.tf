resource "aws_default_vpc" "example" {
  tags = {
    Name = "edtg-default-vpc"
  }
}

resource "aws_default_subnet" "example" {
  availability_zone = "us-east-1a"

  tags = {
    Name = "edtg-default-subnet"
  }
}

resource "aws_default_route_table" "example" {
  default_route_table_id = aws_default_vpc.example.default_route_table_id

  tags = {
    Name = "edtg-default-rt"
  }
}

resource "aws_default_network_acl" "example" {
  default_network_acl_id = aws_default_vpc.example.default_network_acl_id
  subnet_ids             = [aws_default_subnet.example.id]

  tags = {
    Name = "edtg-default-nacl"
  }
}

resource "aws_default_security_group" "example" {
  vpc_id = aws_default_vpc.example.id

  tags = {
    Name = "edtg-default-sg"
  }
}

resource "aws_default_vpc_dhcp_options" "example" {
  tags = {
    Name = "edtg-default-dhcp-opts"
  }
}

resource "aws_vpc" "requester" {
  cidr_block = "{{.VPCCidr}}"

  tags = {
    Name = "edtg-requester"
  }
}

resource "aws_vpc" "accepter" {
  cidr_block = "{{.PeerVPCCidr}}"

  tags = {
    Name = "edtg-accepter"
  }
}

resource "aws_vpc_peering_connection" "example" {
  vpc_id      = aws_vpc.requester.id
  peer_vpc_id = aws_vpc.accepter.id
  auto_accept = false

  # No tags here: aws_vpc_peering_connection and aws_vpc_peering_connection_accepter
  # both target the same underlying resource, and setting tags on the
  # connection while the accepter's "tags" argument stays unset causes the
  # accepter to plan clearing them on every refresh (a known
  # terraform-provider-aws quirk for this resource pair, reproducible against
  # real AWS too -- not a gopherstack gap).
}

resource "aws_vpc_peering_connection_accepter" "example" {
  vpc_peering_connection_id = aws_vpc_peering_connection.example.id
  auto_accept               = true
}

resource "aws_vpc_peering_connection_options" "example" {
  vpc_peering_connection_id = aws_vpc_peering_connection_accepter.example.id

  accepter {
    allow_remote_vpc_dns_resolution = true
  }
}

resource "aws_ec2_carrier_gateway" "example" {
  vpc_id = aws_vpc.requester.id

  tags = {
    Name = "edtg-carrier-gw"
  }
}

resource "aws_customer_gateway" "example" {
  bgp_asn    = 65000
  ip_address = "203.0.113.10"
  type       = "ipsec.1"

  tags = {
    Name = "edtg-cgw"
  }
}

resource "aws_vpn_gateway" "example" {
  tags = {
    Name = "edtg-vgw"
  }
}

resource "aws_vpn_gateway_attachment" "example" {
  vpc_id         = aws_vpc.requester.id
  vpn_gateway_id = aws_vpn_gateway.example.id
}

resource "aws_vpn_connection" "example" {
  customer_gateway_id = aws_customer_gateway.example.id
  vpn_gateway_id      = aws_vpn_gateway.example.id
  type                = "ipsec.1"
  static_routes_only  = true

  tags = {
    Name = "edtg-vpn"
  }
}

resource "aws_vpn_connection_route" "example" {
  vpn_connection_id      = aws_vpn_connection.example.id
  destination_cidr_block = "172.20.0.0/24"
}

resource "aws_route_table" "vgw_propagation" {
  vpc_id = aws_vpc.requester.id
}

resource "aws_vpn_gateway_route_propagation" "example" {
  vpn_gateway_id = aws_vpn_gateway.example.id
  route_table_id = aws_route_table.vgw_propagation.id

  depends_on = [aws_vpn_gateway_attachment.example]
}

resource "aws_ec2_transit_gateway" "example" {
  description                     = "edtg tgw"
  default_route_table_association = "disable"
  default_route_table_propagation = "disable"

  tags = {
    Name = "edtg-tgw"
  }
}

resource "aws_subnet" "tgw_attach" {
  vpc_id            = aws_vpc.accepter.id
  cidr_block        = cidrsubnet(aws_vpc.accepter.cidr_block, 4, 1)
  availability_zone = "us-east-1a"

  tags = {
    Name = "edtg-tgw-subnet"
  }
}

resource "aws_ec2_transit_gateway_vpc_attachment" "example" {
  transit_gateway_id = aws_ec2_transit_gateway.example.id
  vpc_id             = aws_vpc.accepter.id
  subnet_ids         = [aws_subnet.tgw_attach.id]

  tags = {
    Name = "edtg-tgw-attachment"
  }
}

resource "aws_ec2_transit_gateway_route_table" "example" {
  transit_gateway_id = aws_ec2_transit_gateway.example.id

  tags = {
    Name = "edtg-tgw-rt"
  }
}

resource "aws_ec2_transit_gateway_default_route_table_association" "example" {
  transit_gateway_id             = aws_ec2_transit_gateway.example.id
  transit_gateway_route_table_id = aws_ec2_transit_gateway_route_table.example.id
}

resource "aws_ec2_transit_gateway_default_route_table_propagation" "example" {
  transit_gateway_id             = aws_ec2_transit_gateway.example.id
  transit_gateway_route_table_id = aws_ec2_transit_gateway_route_table.example.id
}

resource "aws_ec2_transit_gateway_route_table_association" "example" {
  transit_gateway_attachment_id  = aws_ec2_transit_gateway_vpc_attachment.example.id
  transit_gateway_route_table_id = aws_ec2_transit_gateway_route_table.example.id
}

resource "aws_ec2_transit_gateway_route_table_propagation" "example" {
  transit_gateway_attachment_id  = aws_ec2_transit_gateway_vpc_attachment.example.id
  transit_gateway_route_table_id = aws_ec2_transit_gateway_route_table.example.id
}

resource "aws_ec2_transit_gateway_route" "example" {
  destination_cidr_block         = "10.200.0.0/24"
  transit_gateway_route_table_id = aws_ec2_transit_gateway_route_table.example.id
  transit_gateway_attachment_id  = aws_ec2_transit_gateway_vpc_attachment.example.id
}
