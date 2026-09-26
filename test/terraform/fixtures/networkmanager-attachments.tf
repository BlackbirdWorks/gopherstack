resource "aws_networkmanager_global_network" "example" {
  description = "nmat global network"
}

resource "aws_networkmanager_core_network" "example" {
  global_network_id = aws_networkmanager_global_network.example.id
  description       = "nmat core network"
}

resource "aws_networkmanager_core_network_policy_attachment" "example" {
  core_network_id = aws_networkmanager_core_network.example.id

  policy_document = jsonencode({
    version = "2021.12"
    core-network-configuration = {
      asn-ranges = ["64512-64555"]
      edge-locations = [
        { location = "us-east-1" }
      ]
    }
    segments = [
      {
        name                          = "segment1"
        require-attachment-acceptance = true
      }
    ]
  })
}

resource "aws_vpc" "example" {
  cidr_block = "10.40.0.0/16"
}

resource "aws_subnet" "example" {
  vpc_id     = aws_vpc.example.id
  cidr_block = "10.40.1.0/24"
}

resource "aws_networkmanager_vpc_attachment" "example" {
  core_network_id = aws_networkmanager_core_network.example.id
  vpc_arn         = aws_vpc.example.arn
  subnet_arns     = [aws_subnet.example.arn]

  depends_on = [aws_networkmanager_core_network_policy_attachment.example]
}

resource "aws_networkmanager_attachment_accepter" "vpc" {
  attachment_id   = aws_networkmanager_vpc_attachment.example.id
  attachment_type = aws_networkmanager_vpc_attachment.example.attachment_type
}

resource "aws_networkmanager_connect_attachment" "example" {
  core_network_id         = aws_networkmanager_core_network.example.id
  transport_attachment_id = aws_networkmanager_vpc_attachment.example.id
  edge_location           = aws_networkmanager_vpc_attachment.example.edge_location

  options {
    protocol = "GRE"
  }

  depends_on = [aws_networkmanager_attachment_accepter.vpc]
}

resource "aws_networkmanager_attachment_accepter" "connect" {
  attachment_id   = aws_networkmanager_connect_attachment.example.id
  attachment_type = aws_networkmanager_connect_attachment.example.attachment_type
}

resource "aws_networkmanager_connect_peer" "example" {
  connect_attachment_id = aws_networkmanager_connect_attachment.example.id
  peer_address          = "10.40.2.1"

  bgp_options {
    peer_asn = 65100
  }

  depends_on = [aws_networkmanager_attachment_accepter.connect]
}

resource "aws_ec2_transit_gateway" "example" {
  description = "nmat tgw"
}

resource "aws_networkmanager_transit_gateway_registration" "example" {
  global_network_id   = aws_networkmanager_global_network.example.id
  transit_gateway_arn = aws_ec2_transit_gateway.example.arn
}

resource "aws_networkmanager_transit_gateway_peering" "example" {
  core_network_id     = aws_networkmanager_core_network.example.id
  transit_gateway_arn = aws_ec2_transit_gateway.example.arn

  depends_on = [aws_networkmanager_core_network_policy_attachment.example]
}

resource "aws_ec2_transit_gateway_route_table" "example" {
  transit_gateway_id = aws_ec2_transit_gateway.example.id
}

resource "aws_networkmanager_transit_gateway_route_table_attachment" "example" {
  peering_id                      = aws_networkmanager_transit_gateway_peering.example.id
  transit_gateway_route_table_arn = aws_ec2_transit_gateway_route_table.example.arn
}

resource "aws_networkmanager_attachment_accepter" "tgwrt" {
  attachment_id   = aws_networkmanager_transit_gateway_route_table_attachment.example.id
  attachment_type = aws_networkmanager_transit_gateway_route_table_attachment.example.attachment_type
}

resource "aws_customer_gateway" "example" {
  bgp_asn    = 65000
  ip_address = "175.45.176.10"
  type       = "ipsec.1"
}

resource "aws_networkmanager_site" "example" {
  global_network_id = aws_networkmanager_global_network.example.id
}

resource "aws_networkmanager_device" "example" {
  global_network_id = aws_networkmanager_global_network.example.id
  site_id           = aws_networkmanager_site.example.id
}

resource "aws_networkmanager_link" "example" {
  global_network_id = aws_networkmanager_global_network.example.id
  site_id           = aws_networkmanager_site.example.id

  bandwidth {
    upload_speed   = 10
    download_speed = 50
  }
}

resource "aws_networkmanager_customer_gateway_association" "example" {
  customer_gateway_arn = aws_customer_gateway.example.arn
  device_id            = aws_networkmanager_device.example.id
  global_network_id    = aws_networkmanager_global_network.example.id
}

resource "aws_networkmanager_link_association" "example" {
  device_id         = aws_networkmanager_device.example.id
  global_network_id = aws_networkmanager_global_network.example.id
  link_id           = aws_networkmanager_link.example.id
}

resource "aws_dx_gateway" "example" {
  name            = "nmat-dxgw"
  amazon_side_asn = 64515
}

resource "aws_networkmanager_dx_gateway_attachment" "example" {
  core_network_id            = aws_networkmanager_core_network.example.id
  direct_connect_gateway_arn = aws_dx_gateway.example.arn
  edge_locations             = ["us-east-1"]

  depends_on = [aws_networkmanager_core_network_policy_attachment.example]
}

resource "aws_networkmanager_attachment_accepter" "dxgw" {
  attachment_id   = aws_networkmanager_dx_gateway_attachment.example.id
  attachment_type = aws_networkmanager_dx_gateway_attachment.example.attachment_type
}

resource "aws_vpn_gateway" "example" {
  tags = {
    Name = "nmat-vgw"
  }
}

resource "aws_vpn_connection" "example" {
  customer_gateway_id = aws_customer_gateway.example.id
  vpn_gateway_id      = aws_vpn_gateway.example.id
  type                = "ipsec.1"
}

resource "aws_networkmanager_site_to_site_vpn_attachment" "example" {
  core_network_id    = aws_networkmanager_core_network.example.id
  vpn_connection_arn = aws_vpn_connection.example.arn

  depends_on = [aws_networkmanager_core_network_policy_attachment.example]
}

resource "aws_networkmanager_attachment_accepter" "vpn" {
  attachment_id   = aws_networkmanager_site_to_site_vpn_attachment.example.id
  attachment_type = aws_networkmanager_site_to_site_vpn_attachment.example.attachment_type
}

