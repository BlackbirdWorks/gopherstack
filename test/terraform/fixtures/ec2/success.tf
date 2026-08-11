resource "aws_vpc" "this" {
  cidr_block = "{{.VPCCidr}}"

  tags = {
    Name = "test-vpc"
  }
}

resource "aws_subnet" "this" {
  vpc_id     = aws_vpc.this.id
  cidr_block = "{{.SubnetCidrA}}"

  tags = {
    Name = "test-subnet"
  }
}

resource "aws_security_group" "this" {
  name   = "{{.SGName}}"
  vpc_id = aws_vpc.this.id

  tags = {
    Name = "test-sg"
  }
}

resource "aws_instance" "this" {
  ami           = "ami-12345678"
  instance_type = "t2.micro"
  subnet_id     = aws_subnet.this.id

  tags = {
    Name = "test-instance"
  }
}
