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

  # See mega-batch-32.tf and fsx/PARITY.md (gopherstack-jtf4s): the pinned
  # hashicorp/aws v5.100.0 provider's waitFileSystemDeleted has a fixed
  # 10-minute pre-poll Delay for every FSx file system type, independent of
  # how fast the backend actually confirms deletion.
  timeouts {
    delete = "5s"
  }
}
