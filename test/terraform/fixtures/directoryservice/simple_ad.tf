resource "aws_vpc" "this" {
  cidr_block = "{{.VPCCidr}}"

  tags = {
    Name = "{{.VPCName}}"
  }
}

resource "aws_subnet" "a" {
  vpc_id            = aws_vpc.this.id
  cidr_block        = "{{.SubnetCidrA}}"
  availability_zone = "us-east-1a"
}

resource "aws_subnet" "b" {
  vpc_id            = aws_vpc.this.id
  cidr_block        = "{{.SubnetCidrB}}"
  availability_zone = "us-east-1b"
}

resource "aws_directory_service_directory" "this" {
  name     = "{{.DomainName}}"
  password = "{{.Password}}"
  type     = "SimpleAD"
  size     = "Small"

  vpc_settings {
    vpc_id     = aws_vpc.this.id
    subnet_ids = [aws_subnet.a.id, aws_subnet.b.id]
  }

  tags = {
    Environment = "test"
  }
}
