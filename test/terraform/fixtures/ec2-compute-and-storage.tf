resource "aws_vpc" "example" {
  cidr_block = "{{.VPCCidr}}"

  tags = {
    Name = "ecsu-vpc"
  }
}

resource "aws_subnet" "example" {
  vpc_id            = aws_vpc.example.id
  cidr_block        = "{{.SubnetCidrA}}"
  availability_zone = "us-east-1a"

  tags = {
    Name = "ecsu-subnet"
  }
}

resource "aws_security_group" "example" {
  name   = "ecsu-sg"
  vpc_id = aws_vpc.example.id
}

resource "aws_internet_gateway" "example" {}

resource "aws_internet_gateway_attachment" "example" {
  internet_gateway_id = aws_internet_gateway.example.id
  vpc_id              = aws_vpc.example.id
}

resource "aws_egress_only_internet_gateway" "example" {
  vpc_id = aws_vpc.example.id
}

resource "aws_route_table" "example" {
  vpc_id = aws_vpc.example.id
}

resource "aws_main_route_table_association" "example" {
  vpc_id         = aws_vpc.example.id
  route_table_id = aws_route_table.example.id
}

resource "aws_network_acl" "example" {
  vpc_id = aws_vpc.example.id
}

resource "aws_network_acl_association" "example" {
  network_acl_id = aws_network_acl.example.id
  subnet_id      = aws_subnet.example.id
}

resource "aws_key_pair" "example" {
  key_name   = "ecsu-key"
  public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDGwlIL9dCLoRUOVXkdaTB405ELPRUZ2q0T5vY/Sgo5+PsjhVXcSGGdVpvwsdt76PdaQIB0h4iX6yFyF+xIFEbWAYUeMEQxPz1sZK6iX1LSMR29Dt7SP5kA9wcbb8VbHVfR ecsu"
}

resource "aws_placement_group" "example" {
  name     = "ecsu-pg"
  strategy = "cluster"
}

resource "aws_ec2_host" "example" {
  instance_type     = "c5.large"
  availability_zone = "us-east-1a"
}

resource "aws_ec2_capacity_reservation" "example" {
  instance_type     = "t2.micro"
  instance_platform = "Linux/UNIX"
  availability_zone = "us-east-1a"
  instance_count    = 1
}

resource "aws_launch_template" "example" {
  name          = "ecsu-lt"
  image_id      = "ami-12345678"
  instance_type = "t2.micro"
  key_name      = aws_key_pair.example.key_name
}

resource "aws_instance" "example" {
  ami           = "ami-12345678"
  instance_type = "t2.micro"
  subnet_id     = aws_subnet.example.id

  tags = {
    Name = "ecsu-instance"
  }
}

resource "aws_ebs_volume" "example" {
  availability_zone = "us-east-1a"
  size              = 8

  tags = {
    Name = "ecsu-volume"
  }
}

resource "aws_volume_attachment" "example" {
  device_name = "/dev/sdh"
  volume_id   = aws_ebs_volume.example.id
  instance_id = aws_instance.example.id
}

resource "aws_ebs_snapshot" "example" {
  volume_id = aws_ebs_volume.example.id

  tags = {
    Name = "ecsu-snapshot"
  }
}

resource "aws_snapshot_create_volume_permission" "example" {
  snapshot_id = aws_ebs_snapshot.example.id
  account_id  = "123456789012"
}

resource "aws_ebs_snapshot_copy" "example" {
  source_snapshot_id = aws_ebs_snapshot.example.id
  source_region      = "us-east-1"

  tags = {
    Name = "ecsu-snapshot-copy"
  }
}

resource "aws_ami" "example" {
  name                = "ecsu-ami"
  virtualization_type = "hvm"
  root_device_name    = "/dev/xvda"

  ebs_block_device {
    device_name = "/dev/xvda"
    snapshot_id = aws_ebs_snapshot.example.id
  }
}

resource "aws_ami_copy" "example" {
  name              = "ecsu-ami-copy"
  source_ami_id     = aws_ami.example.id
  source_ami_region = "us-east-1"
}

resource "aws_ami_launch_permission" "example" {
  image_id   = aws_ami.example.id
  account_id = "123456789012"
}

resource "aws_kms_key" "example" {
  description = "ecsu EBS default key"
}

resource "aws_ebs_default_kms_key" "example" {
  key_arn = aws_kms_key.example.arn
}

resource "aws_ebs_encryption_by_default" "example" {
  enabled = true
}

resource "aws_ec2_image_block_public_access" "example" {
  state = "block-new-sharing"
}

resource "aws_ec2_serial_console_access" "example" {
  enabled = true
}

resource "aws_ec2_availability_zone_group" "example" {
  group_name    = "us-east-1-wl1-bos-wlz-1"
  opt_in_status = "opted-in"
}

resource "aws_ec2_instance_state" "example" {
  instance_id = aws_instance.example.id
  state       = "stopped"
}

resource "aws_ec2_managed_prefix_list" "example" {
  name           = "ecsu-pl"
  address_family = "IPv4"
  max_entries    = 5
}

resource "aws_ec2_managed_prefix_list_entry" "example" {
  prefix_list_id = aws_ec2_managed_prefix_list.example.id
  cidr           = "10.99.0.0/24"
  description    = "ecsu entry"
}

resource "aws_ec2_tag" "example" {
  resource_id = aws_vpc.example.id
  key         = "ecsu-tag"
  value       = "true"
}

resource "aws_cloudwatch_log_group" "example" {
  name = "/ecsu/flow-logs"
}

resource "aws_flow_log" "example" {
  vpc_id          = aws_vpc.example.id
  traffic_type    = "ALL"
  log_destination = aws_cloudwatch_log_group.example.arn
  iam_role_arn    = "arn:aws:iam::000000000000:role/ecsu-flowlog-role"
}

resource "aws_network_interface" "example" {
  subnet_id = aws_subnet.example.id

  tags = {
    Name = "ecsu-eni"
  }
}

resource "aws_instance" "attach_target" {
  ami           = "ami-12345678"
  instance_type = "t2.micro"
  subnet_id     = aws_subnet.example.id

  tags = {
    Name = "ecsu-instance-attach-target"
  }
}

resource "aws_network_interface_attachment" "example" {
  instance_id          = aws_instance.attach_target.id
  network_interface_id = aws_network_interface.example.id
  device_index         = 1
}

resource "aws_network_interface_sg_attachment" "example" {
  security_group_id    = aws_security_group.example.id
  network_interface_id = aws_network_interface.example.id
}

resource "aws_spot_instance_request" "example" {
  ami           = "ami-12345678"
  instance_type = "t2.micro"
  subnet_id     = aws_subnet.example.id

  tags = {
    Name = "ecsu-spot"
  }
}

resource "aws_ec2_fleet" "example" {
  launch_template_config {
    launch_template_specification {
      launch_template_id = aws_launch_template.example.id
      version            = aws_launch_template.example.latest_version
    }

    override {
      subnet_id     = aws_subnet.example.id
      instance_type = "t2.micro"
    }
  }

  target_capacity_specification {
    default_target_capacity_type = "on-demand"
    total_target_capacity        = 1
  }

  type                = "maintain"
  terminate_instances = true
}
