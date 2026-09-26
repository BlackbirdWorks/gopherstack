# --- Shared networking ---

resource "aws_vpc" "fsxf" {
  cidr_block = "10.192.0.0/16"

  tags = {
    Name = "fsxf-vpc"
  }
}

resource "aws_subnet" "fsxf" {
  vpc_id            = aws_vpc.fsxf.id
  cidr_block        = "10.192.1.0/24"
  availability_zone = "us-east-1a"

  tags = {
    Name = "fsxf-subnet"
  }
}

# Every FSx resource below sets timeouts.delete to a few seconds. The pinned
# hashicorp/aws v5.100.0 provider's FSx delete waiters use a long fixed
# initial delay (matching real AWS, where deleting a file system can take
# many minutes) regardless of how fast the underlying API actually confirms
# deletion -- verified via a manual `tofu destroy` against a bare local
# server: aws_fsx_windows_file_system alone was still "Destroying..." past
# 4m41s elapsed. Terraform's own destroy step (applyTofu's t.Cleanup) is
# already non-fatal on error, so a short delete timeout turns a multi-minute
# hang into a fast, harmless "timeout while waiting for resource to be gone"
# error without affecting apply or verify. See fsx/PARITY.md.

# --- OpenZFS (file system, volume, snapshot, backup) ---

resource "aws_fsx_openzfs_file_system" "fsxf" {
  storage_capacity    = 64
  subnet_ids          = [aws_subnet.fsxf.id]
  deployment_type     = "SINGLE_AZ_1"
  throughput_capacity = 64

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_openzfs_volume" "fsxf" {
  name             = "fsxf-oz-vol"
  parent_volume_id = aws_fsx_openzfs_file_system.fsxf.root_volume_id

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_openzfs_snapshot" "fsxf" {
  name      = "fsxf-oz-snap"
  volume_id = aws_fsx_openzfs_file_system.fsxf.root_volume_id

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_backup" "fsxf" {
  file_system_id = aws_fsx_openzfs_file_system.fsxf.id

  timeouts {
    delete = "5s"
  }
}

# --- ONTAP (file system, storage virtual machine, volume) ---

resource "aws_fsx_ontap_file_system" "fsxf" {
  storage_capacity    = 1024
  subnet_ids          = [aws_subnet.fsxf.id]
  preferred_subnet_id = aws_subnet.fsxf.id
  deployment_type     = "SINGLE_AZ_1"
  throughput_capacity = 128

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_ontap_storage_virtual_machine" "fsxf" {
  file_system_id = aws_fsx_ontap_file_system.fsxf.id
  name           = "fsxfsvm"

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_ontap_volume" "fsxf" {
  name                       = "fsxfvol"
  junction_path              = "/fsxfvol"
  size_in_megabytes          = 1024
  storage_efficiency_enabled = true
  storage_virtual_machine_id = aws_fsx_ontap_storage_virtual_machine.fsxf.id

  timeouts {
    delete = "5s"
  }
}

# --- Windows ---

resource "aws_fsx_windows_file_system" "fsxf" {
  subnet_ids          = [aws_subnet.fsxf.id]
  throughput_capacity = 32
  storage_capacity    = 32

  self_managed_active_directory {
    dns_ips     = ["10.192.0.10", "10.192.0.11"]
    domain_name = "fsxf.example.test"
    password    = "FsxfPassword!"
    username    = "Admin"
  }

  timeouts {
    delete = "5s"
  }
}

# --- Lustre (prereq for data repository association + file cache) ---

resource "aws_s3_bucket" "fsxf_dra" {
  bucket        = "fsxf-dra-bucket"
  force_destroy = true
}

resource "aws_fsx_lustre_file_system" "fsxf" {
  storage_capacity            = 1200
  subnet_ids                  = [aws_subnet.fsxf.id]
  deployment_type             = "PERSISTENT_2"
  per_unit_storage_throughput = 125

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_data_repository_association" "fsxf" {
  file_system_id       = aws_fsx_lustre_file_system.fsxf.id
  data_repository_path = "s3://${aws_s3_bucket.fsxf_dra.bucket}"
  file_system_path     = "/fsxf"

  s3 {
    auto_export_policy {
      events = ["NEW", "CHANGED", "DELETED"]
    }

    auto_import_policy {
      events = ["NEW", "CHANGED", "DELETED"]
    }
  }

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_file_cache" "fsxf" {
  file_cache_type         = "LUSTRE"
  file_cache_type_version = "2.12"
  storage_capacity        = 1200
  subnet_ids              = [aws_subnet.fsxf.id]

  lustre_configuration {
    deployment_type             = "CACHE_1"
    per_unit_storage_throughput = 1000

    metadata_configuration {
      storage_capacity = 2400
    }
  }

  timeouts {
    delete = "5s"
  }
}
