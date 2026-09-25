resource "aws_iam_role" "trfm_transfer" {
  name = "trfm-transfer-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "transfer.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_s3_bucket" "trfm" {
  bucket        = "trfm-transfer-bucket"
  force_destroy = true
}

resource "aws_transfer_server" "trfm" {
  identity_provider_type = "SERVICE_MANAGED"
  protocols              = ["SFTP"]

  tags = {
    Name = "trfm-server"
  }
}

resource "aws_transfer_user" "trfm" {
  server_id      = aws_transfer_server.trfm.id
  user_name      = "trfm-user"
  role           = aws_iam_role.trfm_transfer.arn
  home_directory = "/${aws_s3_bucket.trfm.bucket}"
}

resource "aws_transfer_ssh_key" "trfm" {
  server_id = aws_transfer_server.trfm.id
  user_name = aws_transfer_user.trfm.user_name
  body      = "{{.SSHPublicKey}}"
}

resource "aws_transfer_access" "trfm" {
  server_id      = aws_transfer_server.trfm.id
  external_id    = "S-1-1-12-1234567890-123456789-1234567890-1234"
  role           = aws_iam_role.trfm_transfer.arn
  home_directory = "/${aws_s3_bucket.trfm.bucket}/trfm-access"
}

resource "aws_transfer_certificate" "trfm" {
  usage       = "SIGNING"
  certificate = "{{.CertPEM}}"
}

resource "aws_transfer_profile" "trfm_local" {
  as2_id       = "TRFMLOCAL"
  profile_type = "LOCAL"
}

resource "aws_transfer_profile" "trfm_partner" {
  as2_id       = "TRFMPARTNER"
  profile_type = "PARTNER"
}

resource "aws_transfer_agreement" "trfm" {
  server_id          = aws_transfer_server.trfm.id
  access_role        = aws_iam_role.trfm_transfer.arn
  base_directory     = "/${aws_s3_bucket.trfm.bucket}"
  local_profile_id   = aws_transfer_profile.trfm_local.profile_id
  partner_profile_id = aws_transfer_profile.trfm_partner.profile_id
}

resource "aws_transfer_connector" "trfm" {
  url         = "https://trfm.example.com/as2"
  access_role = aws_iam_role.trfm_transfer.arn

  as2_config {
    compression           = "DISABLED"
    encryption_algorithm  = "AES192_CBC"
    signing_algorithm     = "NONE"
    mdn_signing_algorithm = "NONE"
    mdn_response          = "NONE"
    message_subject       = "trfm AS2 message"
    local_profile_id      = aws_transfer_profile.trfm_local.profile_id
    partner_profile_id    = aws_transfer_profile.trfm_partner.profile_id
  }
}

resource "aws_transfer_workflow" "trfm" {
  description = "trfm workflow"

  steps {
    type = "COPY"

    copy_step_details {
      name = "trfm-copy-step"

      destination_file_location {
        s3_file_location {
          bucket = aws_s3_bucket.trfm.bucket
          key    = "trfm-workflow-output/"
        }
      }
    }
  }
}

resource "aws_transfer_tag" "trfm" {
  resource_arn = aws_transfer_server.trfm.arn
  key          = "Environment"
  value        = "transferfamily-and-msk"
}

resource "aws_vpc" "trfm" {
  cidr_block = "10.171.0.0/16"

  tags = {
    Name = "trfm-vpc"
  }
}

resource "aws_subnet" "trfma" {
  vpc_id     = aws_vpc.trfm.id
  cidr_block = "10.171.1.0/24"

  tags = {
    Name = "trfm-subnet-a"
  }
}

resource "aws_subnet" "trfmb" {
  vpc_id     = aws_vpc.trfm.id
  cidr_block = "10.171.2.0/24"

  tags = {
    Name = "trfm-subnet-b"
  }
}

resource "aws_security_group" "trfm" {
  name   = "trfm-sg"
  vpc_id = aws_vpc.trfm.id
}

resource "aws_msk_cluster" "trfm_source" {
  cluster_name           = "trfm-source"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 1

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = [aws_subnet.trfma.id, aws_subnet.trfmb.id]
    security_groups = [aws_security_group.trfm.id]

    storage_info {
      ebs_storage_info {
        volume_size = 20
      }
    }
  }
}

resource "aws_msk_cluster" "trfm_target" {
  cluster_name           = "trfm-target"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 1

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = [aws_subnet.trfma.id, aws_subnet.trfmb.id]
    security_groups = [aws_security_group.trfm.id]

    storage_info {
      ebs_storage_info {
        volume_size = 20
      }
    }
  }
}

resource "aws_msk_cluster_policy" "trfm" {
  cluster_arn = aws_msk_cluster.trfm_target.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "TrfmClusterPolicy"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::000000000000:root" }
      Action    = "kafka:GetBootstrapBrokers"
      Resource  = aws_msk_cluster.trfm_target.arn
    }]
  })
}

resource "aws_msk_vpc_connection" "trfm" {
  target_cluster_arn = aws_msk_cluster.trfm_target.arn
  authentication     = "SASL_IAM"
  vpc_id             = aws_vpc.trfm.id
  client_subnets     = [aws_subnet.trfma.id, aws_subnet.trfmb.id]
  security_groups    = [aws_security_group.trfm.id]
}

resource "aws_msk_serverless_cluster" "trfm" {
  cluster_name = "trfm-serverless"

  vpc_config {
    subnet_ids         = [aws_subnet.trfma.id, aws_subnet.trfmb.id]
    security_group_ids = [aws_security_group.trfm.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}

resource "aws_secretsmanager_secret" "trfm_scram" {
  name = "AmazonMSK_trfm-scram"
}

resource "aws_secretsmanager_secret_version" "trfm_scram" {
  secret_id     = aws_secretsmanager_secret.trfm_scram.id
  secret_string = jsonencode({ username = "transferfamily-and-msk", password = "trfm-password" })
}

resource "aws_msk_scram_secret_association" "trfm" {
  cluster_arn     = aws_msk_cluster.trfm_target.arn
  secret_arn_list = [aws_secretsmanager_secret.trfm_scram.arn]

  depends_on = [aws_secretsmanager_secret_version.trfm_scram]
}

resource "aws_secretsmanager_secret" "trfm_scram_single" {
  name = "AmazonMSK_trfm-scram-single"
}

resource "aws_secretsmanager_secret_version" "trfm_scram_single" {
  secret_id     = aws_secretsmanager_secret.trfm_scram_single.id
  secret_string = jsonencode({ username = "trfm-single", password = "trfm-single-password" })
}

resource "aws_msk_single_scram_secret_association" "trfm" {
  cluster_arn = aws_msk_cluster.trfm_source.arn
  secret_arn  = aws_secretsmanager_secret.trfm_scram_single.arn

  depends_on = [aws_secretsmanager_secret_version.trfm_scram_single]
}

resource "aws_iam_role" "trfm_replicator" {
  name = "trfm-replicator-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "kafka.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_msk_replicator" "trfm" {
  replicator_name            = "trfm-replicator"
  description                = "trfm replicator"
  service_execution_role_arn = aws_iam_role.trfm_replicator.arn

  kafka_cluster {
    amazon_msk_cluster {
      msk_cluster_arn = aws_msk_cluster.trfm_source.arn
    }

    vpc_config {
      subnet_ids          = [aws_subnet.trfma.id, aws_subnet.trfmb.id]
      security_groups_ids = [aws_security_group.trfm.id]
    }
  }

  kafka_cluster {
    amazon_msk_cluster {
      msk_cluster_arn = aws_msk_cluster.trfm_target.arn
    }

    vpc_config {
      subnet_ids          = [aws_subnet.trfma.id, aws_subnet.trfmb.id]
      security_groups_ids = [aws_security_group.trfm.id]
    }
  }

  replication_info_list {
    source_kafka_cluster_arn = aws_msk_cluster.trfm_source.arn
    target_kafka_cluster_arn = aws_msk_cluster.trfm_target.arn
    target_compression_type  = "NONE"

    topic_replication {
      topics_to_replicate = [".*"]
    }

    consumer_group_replication {
      consumer_groups_to_replicate = [".*"]
    }
  }
}
