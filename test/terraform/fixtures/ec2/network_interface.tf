resource "aws_vpc" "this" {
  cidr_block = "{{.VPCCidr}}"
}

resource "aws_subnet" "this" {
  vpc_id     = aws_vpc.this.id
  cidr_block = "{{.SubnetCidrA}}"
}

resource "aws_network_interface" "this" {
  subnet_id   = aws_subnet.this.id
  description = "{{.ENIDescription}}"
}
