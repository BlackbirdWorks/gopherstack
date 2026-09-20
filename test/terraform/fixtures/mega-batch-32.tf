# --- Shared networking ---

resource "aws_vpc" "mb32" {
  cidr_block = "10.192.0.0/16"

  tags = {
    Name = "mega-batch-32-vpc"
  }
}

resource "aws_subnet" "mb32" {
  vpc_id            = aws_vpc.mb32.id
  cidr_block        = "10.192.1.0/24"
  availability_zone = "us-east-1a"

  tags = {
    Name = "mega-batch-32-subnet"
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

resource "aws_fsx_openzfs_file_system" "mb32" {
  storage_capacity    = 64
  subnet_ids          = [aws_subnet.mb32.id]
  deployment_type     = "SINGLE_AZ_1"
  throughput_capacity = 64

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_openzfs_volume" "mb32" {
  name             = "mega-batch-32-oz-vol"
  parent_volume_id = aws_fsx_openzfs_file_system.mb32.root_volume_id

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_openzfs_snapshot" "mb32" {
  name      = "mega-batch-32-oz-snap"
  volume_id = aws_fsx_openzfs_file_system.mb32.root_volume_id

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_backup" "mb32" {
  file_system_id = aws_fsx_openzfs_file_system.mb32.id

  timeouts {
    delete = "5s"
  }
}

# --- ONTAP (file system, storage virtual machine, volume) ---

resource "aws_fsx_ontap_file_system" "mb32" {
  storage_capacity    = 1024
  subnet_ids          = [aws_subnet.mb32.id]
  preferred_subnet_id = aws_subnet.mb32.id
  deployment_type     = "SINGLE_AZ_1"
  throughput_capacity = 128

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_ontap_storage_virtual_machine" "mb32" {
  file_system_id = aws_fsx_ontap_file_system.mb32.id
  name           = "mb32svm"

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_ontap_volume" "mb32" {
  name                       = "mb32vol"
  junction_path              = "/mb32vol"
  size_in_megabytes          = 1024
  storage_efficiency_enabled = true
  storage_virtual_machine_id = aws_fsx_ontap_storage_virtual_machine.mb32.id

  timeouts {
    delete = "5s"
  }
}

# --- Windows ---

resource "aws_fsx_windows_file_system" "mb32" {
  subnet_ids          = [aws_subnet.mb32.id]
  throughput_capacity = 32
  storage_capacity    = 32

  self_managed_active_directory {
    dns_ips     = ["10.192.0.10", "10.192.0.11"]
    domain_name = "mega-batch-32.example.test"
    password    = "MegaBatch32Password!"
    username    = "Admin"
  }

  timeouts {
    delete = "5s"
  }
}

# --- Lustre (prereq for data repository association + file cache) ---

resource "aws_s3_bucket" "mb32_dra" {
  bucket        = "mega-batch-32-dra-bucket"
  force_destroy = true
}

resource "aws_fsx_lustre_file_system" "mb32" {
  storage_capacity            = 1200
  subnet_ids                  = [aws_subnet.mb32.id]
  deployment_type             = "PERSISTENT_2"
  per_unit_storage_throughput = 125

  timeouts {
    delete = "5s"
  }
}

resource "aws_fsx_data_repository_association" "mb32" {
  file_system_id       = aws_fsx_lustre_file_system.mb32.id
  data_repository_path = "s3://${aws_s3_bucket.mb32_dra.bucket}"
  file_system_path     = "/mb32"

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

resource "aws_fsx_file_cache" "mb32" {
  file_cache_type         = "LUSTRE"
  file_cache_type_version = "2.12"
  storage_capacity        = 1200
  subnet_ids              = [aws_subnet.mb32.id]

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
