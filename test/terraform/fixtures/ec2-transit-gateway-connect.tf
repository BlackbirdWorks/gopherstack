##############################################################################
# EC2 Transit Gateway Connect: a Connect attachment over a VPC attachment,
# a Connect peer with an explicit BGP ASN, and the Network Manager device
# association for that peer. The peer's create waiter needs a non-empty
# BgpConfigurations on the wire -- fixed in b63f7384c (gopherstack-zfrof).
##############################################################################

resource "aws_vpc" "tgcn" {
  cidr_block = "10.250.0.0/16"
}

resource "aws_subnet" "tgcn" {
  vpc_id            = aws_vpc.tgcn.id
  cidr_block        = "10.250.1.0/24"
  availability_zone = "us-east-1a"
}

resource "aws_ec2_transit_gateway" "tgcn" {
  description                 = "tgcn tgw"
  amazon_side_asn             = 64520
  transit_gateway_cidr_blocks = ["10.100.0.0/24"]

  tags = {
    Name = "tgcn-tgw"
  }
}

resource "aws_ec2_transit_gateway_vpc_attachment" "tgcn" {
  transit_gateway_id = aws_ec2_transit_gateway.tgcn.id
  vpc_id             = aws_vpc.tgcn.id
  subnet_ids         = [aws_subnet.tgcn.id]

  tags = {
    Name = "tgcn-tgw-attachment"
  }
}

resource "aws_ec2_transit_gateway_connect" "tgcn" {
  transport_attachment_id = aws_ec2_transit_gateway_vpc_attachment.tgcn.id
  transit_gateway_id      = aws_ec2_transit_gateway.tgcn.id

  tags = {
    Name = "tgcn-connect"
  }
}

resource "aws_ec2_transit_gateway_connect_peer" "tgcn" {
  transit_gateway_attachment_id = aws_ec2_transit_gateway_connect.tgcn.id
  peer_address                  = "192.0.2.10"
  inside_cidr_blocks            = ["169.254.100.0/29"]
  bgp_asn                       = "65001"

  tags = {
    Name = "tgcn-connect-peer"
  }
}

resource "aws_networkmanager_global_network" "tgcn" {
  description = "tgcn global network"
}

resource "aws_networkmanager_site" "tgcn" {
  global_network_id = aws_networkmanager_global_network.tgcn.id
}

resource "aws_networkmanager_device" "tgcn" {
  global_network_id = aws_networkmanager_global_network.tgcn.id
  site_id           = aws_networkmanager_site.tgcn.id
}

resource "aws_networkmanager_transit_gateway_registration" "tgcn" {
  global_network_id   = aws_networkmanager_global_network.tgcn.id
  transit_gateway_arn = aws_ec2_transit_gateway.tgcn.arn
}

resource "aws_networkmanager_transit_gateway_connect_peer_association" "tgcn" {
  global_network_id                = aws_networkmanager_global_network.tgcn.id
  device_id                        = aws_networkmanager_device.tgcn.id
  transit_gateway_connect_peer_arn = aws_ec2_transit_gateway_connect_peer.tgcn.arn

  depends_on = [aws_networkmanager_transit_gateway_registration.tgcn]
}
