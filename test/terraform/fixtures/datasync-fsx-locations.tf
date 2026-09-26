# FSx (Lustre, OpenZFS, ONTAP, Windows) + DataSync FSx locations.

resource "aws_vpc" "dsfx" {
  cidr_block = "10.216.0.0/16"

  tags = {
    Name = "dsfx-vpc"
  }
}

resource "aws_subnet" "dsfx" {
  vpc_id            = aws_vpc.dsfx.id
  cidr_block        = "10.216.1.0/24"
  availability_zone = "us-east-1a"

  tags = {
    Name = "dsfx-subnet"
  }
}

resource "aws_security_group" "dsfx" {
  name   = "dsfx-sg"
  vpc_id = aws_vpc.dsfx.id
}

# --- FSx: each of the four fixed-timeout resources feeds a DataSync FSx
# location. Every aws_fsx_* resource below sets timeouts.delete to a few
# seconds: the pinned hashicorp/aws v5.100.0 provider's FSx delete waiters use
# a long fixed pre-poll Delay regardless of how fast the underlying API
# confirms deletion, so a short delete timeout turns a multi-minute hang into
# a fast, harmless "timeout while waiting for resource to be gone" error
# without affecting apply or verify. See fsx-file-systems.tf / fsx/PARITY.md.

resource "aws_fsx_lustre_file_system" "dsfx" {
  storage_capacity            = 1200
  subnet_ids                  = [aws_subnet.dsfx.id]
  deployment_type             = "PERSISTENT_2"
  per_unit_storage_throughput = 125

  tags = {
    Name = "dsfx-lustre"
  }

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_openzfs_file_system" "dsfx" {
  storage_capacity    = 64
  subnet_ids          = [aws_subnet.dsfx.id]
  deployment_type     = "SINGLE_AZ_1"
  throughput_capacity = 64

  tags = {
    Name = "dsfx-openzfs"
  }

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_ontap_file_system" "dsfx" {
  storage_capacity    = 1024
  subnet_ids          = [aws_subnet.dsfx.id]
  preferred_subnet_id = aws_subnet.dsfx.id
  deployment_type     = "SINGLE_AZ_1"
  throughput_capacity = 128

  tags = {
    Name = "dsfx-ontap"
  }

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_ontap_storage_virtual_machine" "dsfx" {
  file_system_id = aws_fsx_ontap_file_system.dsfx.id
  name           = "dsfxsvm"

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_windows_file_system" "dsfx" {
  subnet_ids          = [aws_subnet.dsfx.id]
  throughput_capacity = 32
  storage_capacity    = 32

  self_managed_active_directory {
    dns_ips     = ["10.216.0.10", "10.216.0.11"]
    domain_name = "dsfx.example.test"
    password    = "DsfxPassword!"
    username    = "Admin"
  }

  tags = {
    Name = "dsfx-windows"
  }

  timeouts {
    delete = "5s"
  }
}

resource "aws_datasync_location_fsx_lustre_file_system" "dsfx" {
  fsx_filesystem_arn  = aws_fsx_lustre_file_system.dsfx.arn
  security_group_arns = [aws_security_group.dsfx.arn]
  subdirectory        = "/dsfx"
}

resource "aws_datasync_location_fsx_openzfs_file_system" "dsfx" {
  fsx_filesystem_arn  = aws_fsx_openzfs_file_system.dsfx.arn
  security_group_arns = [aws_security_group.dsfx.arn]

  protocol {
    nfs {
      mount_options {
        version = "NFS3"
      }
    }
  }
}

resource "aws_datasync_location_fsx_ontap_file_system" "dsfx" {
  storage_virtual_machine_arn = aws_fsx_ontap_storage_virtual_machine.dsfx.arn
  security_group_arns         = [aws_security_group.dsfx.arn]

  protocol {
    nfs {
      mount_options {
        version = "NFS3"
      }
    }
  }
}

resource "aws_datasync_location_fsx_windows_file_system" "dsfx" {
  fsx_filesystem_arn  = aws_fsx_windows_file_system.dsfx.arn
  security_group_arns = [aws_security_group.dsfx.arn]
  user                = "Admin"
  password            = "DsfxPassword!"
}
