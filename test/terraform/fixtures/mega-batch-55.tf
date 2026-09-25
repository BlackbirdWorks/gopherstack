# FSx (Lustre, OpenZFS, ONTAP, Windows) + DataSync FSx locations.

resource "aws_vpc" "mb55" {
  cidr_block = "10.216.0.0/16"

  tags = {
    Name = "mega-batch-55-vpc"
  }
}

resource "aws_subnet" "mb55" {
  vpc_id            = aws_vpc.mb55.id
  cidr_block        = "10.216.1.0/24"
  availability_zone = "us-east-1a"

  tags = {
    Name = "mega-batch-55-subnet"
  }
}

resource "aws_security_group" "mb55" {
  name   = "mega-batch-55-sg"
  vpc_id = aws_vpc.mb55.id
}

# --- FSx: each of the four fixed-timeout resources feeds a DataSync FSx
# location. Every aws_fsx_* resource below sets timeouts.delete to a few
# seconds: the pinned hashicorp/aws v5.100.0 provider's FSx delete waiters use
# a long fixed pre-poll Delay regardless of how fast the underlying API
# confirms deletion, so a short delete timeout turns a multi-minute hang into
# a fast, harmless "timeout while waiting for resource to be gone" error
# without affecting apply or verify. See mega-batch-32.tf / fsx/PARITY.md.

resource "aws_fsx_lustre_file_system" "mb55" {
  storage_capacity            = 1200
  subnet_ids                  = [aws_subnet.mb55.id]
  deployment_type             = "PERSISTENT_2"
  per_unit_storage_throughput = 125

  tags = {
    Name = "mega-batch-55-lustre"
  }

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_openzfs_file_system" "mb55" {
  storage_capacity    = 64
  subnet_ids          = [aws_subnet.mb55.id]
  deployment_type     = "SINGLE_AZ_1"
  throughput_capacity = 64

  tags = {
    Name = "mega-batch-55-openzfs"
  }

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_ontap_file_system" "mb55" {
  storage_capacity    = 1024
  subnet_ids          = [aws_subnet.mb55.id]
  preferred_subnet_id = aws_subnet.mb55.id
  deployment_type     = "SINGLE_AZ_1"
  throughput_capacity = 128

  tags = {
    Name = "mega-batch-55-ontap"
  }

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_ontap_storage_virtual_machine" "mb55" {
  file_system_id = aws_fsx_ontap_file_system.mb55.id
  name           = "mb55svm"

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_windows_file_system" "mb55" {
  subnet_ids          = [aws_subnet.mb55.id]
  throughput_capacity = 32
  storage_capacity    = 32

  self_managed_active_directory {
    dns_ips     = ["10.216.0.10", "10.216.0.11"]
    domain_name = "mega-batch-55.example.test"
    password    = "MegaBatch55Password!"
    username    = "Admin"
  }

  tags = {
    Name = "mega-batch-55-windows"
  }

  timeouts {
    delete = "5s"
  }
}

resource "aws_datasync_location_fsx_lustre_file_system" "mb55" {
  fsx_filesystem_arn  = aws_fsx_lustre_file_system.mb55.arn
  security_group_arns = [aws_security_group.mb55.arn]
  subdirectory        = "/mb55"
}

resource "aws_datasync_location_fsx_openzfs_file_system" "mb55" {
  fsx_filesystem_arn  = aws_fsx_openzfs_file_system.mb55.arn
  security_group_arns = [aws_security_group.mb55.arn]

  protocol {
    nfs {
      mount_options {
        version = "NFS3"
      }
    }
  }
}

resource "aws_datasync_location_fsx_ontap_file_system" "mb55" {
  storage_virtual_machine_arn = aws_fsx_ontap_storage_virtual_machine.mb55.arn
  security_group_arns         = [aws_security_group.mb55.arn]

  protocol {
    nfs {
      mount_options {
        version = "NFS3"
      }
    }
  }
}

resource "aws_datasync_location_fsx_windows_file_system" "mb55" {
  fsx_filesystem_arn  = aws_fsx_windows_file_system.mb55.arn
  security_group_arns = [aws_security_group.mb55.arn]
  user                = "Admin"
  password            = "MegaBatch55Password!"
}
