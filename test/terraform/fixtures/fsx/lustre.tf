resource "aws_vpc" "this" {
  cidr_block = "{{.VPCCidr}}"

  tags = {
    Name = "{{.Name}}-vpc"
  }
}

resource "aws_subnet" "this" {
  vpc_id     = aws_vpc.this.id
  cidr_block = "{{.SubnetCidrA}}"

  tags = {
    Name = "{{.Name}}-subnet"
  }
}

resource "aws_fsx_lustre_file_system" "this" {
  storage_capacity = 1200
  subnet_ids       = [aws_subnet.this.id]
  deployment_type  = "SCRATCH_2"

  tags = {
    Name        = "{{.Name}}"
    Environment = "test"
  }
}
