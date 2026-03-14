resource "aws_vpc" "this" {
  cidr_block = "10.0.0.0/16"
}

resource "aws_subnet" "this" {
  vpc_id     = aws_vpc.this.id
  cidr_block = "10.0.1.0/24"
}

resource "aws_network_interface" "this" {
  subnet_id   = aws_subnet.this.id
  description = "{{.ENIDescription}}"
}
