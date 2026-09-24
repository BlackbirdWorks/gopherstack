resource "aws_vpc" "example" {
  cidr_block = "{{.VPCCidr}}"

  tags = {
    Name = "mega-batch-44-vpc"
  }
}

resource "aws_vpc_ipv4_cidr_block_association" "example" {
  vpc_id     = aws_vpc.example.id
  cidr_block = "10.99.0.0/16"
}

resource "aws_vpc_ipv6_cidr_block_association" "example" {
  vpc_id                           = aws_vpc.example.id
  assign_generated_ipv6_cidr_block = true
}

resource "aws_subnet" "example" {
  vpc_id            = aws_vpc.example.id
  cidr_block        = "{{.SubnetCidrA}}"
  availability_zone = "us-east-1a"

  tags = {
    Name = "mega-batch-44-subnet"
  }
}

resource "aws_subnet" "b" {
  vpc_id            = aws_vpc.example.id
  cidr_block        = "{{.SubnetCidrB}}"
  availability_zone = "us-east-1b"

  tags = {
    Name = "mega-batch-44-subnet-b"
  }
}

resource "aws_ec2_subnet_cidr_reservation" "example" {
  subnet_id        = aws_subnet.example.id
  cidr_block       = cidrsubnet(aws_subnet.example.cidr_block, 2, 0)
  reservation_type = "prefix"
}

resource "aws_vpc_dhcp_options" "example" {
  domain_name_servers = ["8.8.8.8", "8.8.4.4"]

  tags = {
    Name = "mega-batch-44-dhcp-opts"
  }
}

resource "aws_vpc_dhcp_options_association" "example" {
  vpc_id          = aws_vpc.example.id
  dhcp_options_id = aws_vpc_dhcp_options.example.id
}

resource "aws_internet_gateway" "example" {
  vpc_id = aws_vpc.example.id

  tags = {
    Name = "mega-batch-44-igw"
  }
}

resource "aws_eip" "example" {
  domain = "vpc"

  tags = {
    Name = "mega-batch-44-eip"
  }
}

resource "aws_eip_domain_name" "example" {
  allocation_id = aws_eip.example.id
  domain_name   = "mega-batch-44.example.com"
}

resource "aws_instance" "example" {
  ami           = "ami-12345678"
  instance_type = "t2.micro"
  subnet_id     = aws_subnet.example.id

  tags = {
    Name = "mega-batch-44-instance"
  }
}

resource "aws_eip_association" "example" {
  instance_id   = aws_instance.example.id
  allocation_id = aws_eip.example.id
}

resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name = "mega-batch-44-nat-eip"
  }
}

resource "aws_nat_gateway" "example" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.example.id

  tags = {
    Name = "mega-batch-44-natgw"
  }
}

resource "aws_route_table" "example" {
  vpc_id = aws_vpc.example.id

  tags = {
    Name = "mega-batch-44-rt"
  }
}

resource "aws_route" "example" {
  route_table_id         = aws_route_table.example.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.example.id
}

resource "aws_route_table_association" "example" {
  subnet_id      = aws_subnet.example.id
  route_table_id = aws_route_table.example.id
}

resource "aws_security_group" "example" {
  name   = "mega-batch-44-sg"
  vpc_id = aws_vpc.example.id
}

resource "aws_security_group" "rule" {
  name   = "mega-batch-44-sg-rule"
  vpc_id = aws_vpc.example.id
}

resource "aws_security_group_rule" "ingress" {
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["10.0.0.0/8"]
  security_group_id = aws_security_group.rule.id
}

resource "aws_vpc_security_group_ingress_rule" "example" {
  security_group_id = aws_security_group.example.id
  cidr_ipv4         = "192.168.0.0/16"
  from_port         = 443
  to_port           = 443
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "example" {
  security_group_id = aws_security_group.example.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_network_acl" "example" {
  vpc_id = aws_vpc.example.id

  tags = {
    Name = "mega-batch-44-nacl"
  }
}

resource "aws_network_acl_rule" "example" {
  network_acl_id = aws_network_acl.example.id
  rule_number    = 100
  egress         = false
  protocol       = "tcp"
  rule_action    = "allow"
  cidr_block     = "0.0.0.0/0"
  from_port      = 80
  to_port        = 80
}

resource "aws_ebs_volume" "example" {
  availability_zone = "us-east-1a"
  size              = 8

  tags = {
    Name = "mega-batch-44-volume"
  }
}

resource "aws_ebs_snapshot" "example" {
  volume_id = aws_ebs_volume.example.id

  tags = {
    Name = "mega-batch-44-snapshot"
  }
}

resource "aws_ebs_snapshot_block_public_access" "example" {
  state = "block-all-sharing"
}

resource "aws_vpc_block_public_access_options" "example" {
  internet_gateway_block_mode = "block-bidirectional"
}

resource "aws_vpc_block_public_access_exclusion" "example" {
  vpc_id                          = aws_vpc.example.id
  internet_gateway_exclusion_mode = "allow-bidirectional"

  depends_on = [aws_vpc_block_public_access_options.example]
}

resource "aws_ec2_instance_metadata_defaults" "example" {
  http_tokens                 = "required"
  http_put_response_hop_limit = 2
}

resource "aws_ec2_default_credit_specification" "example" {
  instance_family = "t2"
  cpu_credits     = "standard"
}

# VPC endpoint service family: a Network Load Balancer target is required by
# the real API shape, but our emulator does not validate its existence, so a
# bare ARN is accepted -- matches CreateVpcEndpointServiceConfiguration's
# actual required inputs (NetworkLoadBalancerArns is a list of ARN strings).
resource "aws_vpc_endpoint_service" "example" {
  acceptance_required        = false
  network_load_balancer_arns = ["arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/net/mega-batch-44-nlb/0123456789abcdef"]

  tags = {
    Name = "mega-batch-44-vpces"
  }
}

resource "aws_vpc_endpoint_service_allowed_principal" "example" {
  vpc_endpoint_service_id = aws_vpc_endpoint_service.example.id
  principal_arn           = "arn:aws:iam::123456789012:root"
}

resource "aws_sns_topic" "example" {
  name = "mega-batch-44-topic"
}

resource "aws_vpc_endpoint_connection_notification" "example" {
  vpc_endpoint_service_id     = aws_vpc_endpoint_service.example.id
  connection_notification_arn = aws_sns_topic.example.arn
  connection_events           = ["Accept", "Reject"]
}

resource "aws_vpc_endpoint" "gateway" {
  vpc_id       = aws_vpc.example.id
  service_name = "com.amazonaws.us-east-1.s3"

  tags = {
    Name = "mega-batch-44-vpce-gateway"
  }
}

resource "aws_vpc_endpoint_policy" "example" {
  vpc_endpoint_id = aws_vpc_endpoint.gateway.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = "*"
      Action    = "*"
      Resource  = "*"
    }]
  })
}

resource "aws_vpc_endpoint_route_table_association" "example" {
  vpc_endpoint_id = aws_vpc_endpoint.gateway.id
  route_table_id  = aws_route_table.example.id
}

resource "aws_vpc_endpoint" "interface" {
  vpc_id            = aws_vpc.example.id
  service_name      = aws_vpc_endpoint_service.example.service_name
  vpc_endpoint_type = "Interface"
  subnet_ids        = [aws_subnet.example.id]

  tags = {
    Name = "mega-batch-44-vpce-interface"
  }
}

resource "aws_vpc_endpoint_subnet_association" "example" {
  vpc_endpoint_id = aws_vpc_endpoint.interface.id
  subnet_id       = aws_subnet.b.id
}

resource "aws_vpc_endpoint_security_group_association" "example" {
  vpc_endpoint_id   = aws_vpc_endpoint.interface.id
  security_group_id = aws_security_group.example.id
}

resource "aws_vpc_endpoint_connection_accepter" "example" {
  vpc_endpoint_service_id = aws_vpc_endpoint_service.example.id
  vpc_endpoint_id         = aws_vpc_endpoint.interface.id
}

resource "aws_vpc_endpoint_private_dns" "example" {
  vpc_endpoint_id     = aws_vpc_endpoint.interface.id
  private_dns_enabled = true
}
