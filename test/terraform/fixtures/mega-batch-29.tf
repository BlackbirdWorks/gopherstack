resource "aws_iam_role" "mb29_transfer" {
  name = "mega-batch-29-transfer-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "transfer.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_s3_bucket" "mb29" {
  bucket        = "mega-batch-29-transfer-bucket"
  force_destroy = true
}

resource "aws_transfer_server" "mb29" {
  identity_provider_type = "SERVICE_MANAGED"
  protocols               = ["SFTP"]

  tags = {
    Name = "mega-batch-29-server"
  }
}

resource "aws_transfer_user" "mb29" {
  server_id      = aws_transfer_server.mb29.id
  user_name      = "mega-batch-29-user"
  role           = aws_iam_role.mb29_transfer.arn
  home_directory = "/${aws_s3_bucket.mb29.bucket}"
}

resource "aws_transfer_ssh_key" "mb29" {
  server_id = aws_transfer_server.mb29.id
  user_name = aws_transfer_user.mb29.user_name
  body      = "{{.SSHPublicKey}}"
}

resource "aws_transfer_access" "mb29" {
  server_id      = aws_transfer_server.mb29.id
  external_id    = "S-1-1-12-1234567890-123456789-1234567890-1234"
  role           = aws_iam_role.mb29_transfer.arn
  home_directory = "/${aws_s3_bucket.mb29.bucket}/mega-batch-29-access"
}

resource "aws_transfer_certificate" "mb29" {
  usage       = "SIGNING"
  certificate = "{{.CertPEM}}"
}

resource "aws_transfer_profile" "mb29_local" {
  as2_id       = "MEGABATCH29LOCAL"
  profile_type = "LOCAL"
}

resource "aws_transfer_profile" "mb29_partner" {
  as2_id       = "MEGABATCH29PARTNER"
  profile_type = "PARTNER"
}

resource "aws_transfer_agreement" "mb29" {
  server_id           = aws_transfer_server.mb29.id
  access_role         = aws_iam_role.mb29_transfer.arn
  base_directory      = "/${aws_s3_bucket.mb29.bucket}"
  local_profile_id    = aws_transfer_profile.mb29_local.profile_id
  partner_profile_id  = aws_transfer_profile.mb29_partner.profile_id
}

resource "aws_transfer_connector" "mb29" {
  url         = "https://mega-batch-29.example.com/as2"
  access_role = aws_iam_role.mb29_transfer.arn

  as2_config {
    compression          = "DISABLED"
    encryption_algorithm = "AES192_CBC"
    signing_algorithm    = "NONE"
    mdn_signing_algorithm = "NONE"
    mdn_response         = "NONE"
    message_subject       = "mega-batch-29 AS2 message"
    local_profile_id      = aws_transfer_profile.mb29_local.profile_id
    partner_profile_id    = aws_transfer_profile.mb29_partner.profile_id
  }
}

resource "aws_transfer_workflow" "mb29" {
  description = "mega-batch-29 workflow"

  steps {
    type = "COPY"

    copy_step_details {
      name = "mega-batch-29-copy-step"

      destination_file_location {
        s3_file_location {
          bucket = aws_s3_bucket.mb29.bucket
          key    = "mega-batch-29-workflow-output/"
        }
      }
    }
  }
}

resource "aws_transfer_tag" "mb29" {
  resource_arn = aws_transfer_server.mb29.arn
  key          = "Environment"
  value        = "mega-batch-29"
}

resource "aws_vpc" "mb29" {
  cidr_block = "10.171.0.0/16"

  tags = {
    Name = "mega-batch-29-vpc"
  }
}

resource "aws_subnet" "mb29a" {
  vpc_id     = aws_vpc.mb29.id
  cidr_block = "10.171.1.0/24"

  tags = {
    Name = "mega-batch-29-subnet-a"
  }
}

resource "aws_subnet" "mb29b" {
  vpc_id     = aws_vpc.mb29.id
  cidr_block = "10.171.2.0/24"

  tags = {
    Name = "mega-batch-29-subnet-b"
  }
}

resource "aws_security_group" "mb29" {
  name   = "mega-batch-29-sg"
  vpc_id = aws_vpc.mb29.id
}

resource "aws_msk_cluster" "mb29_source" {
  cluster_name           = "mega-batch-29-source"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 1

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = [aws_subnet.mb29a.id, aws_subnet.mb29b.id]
    security_groups = [aws_security_group.mb29.id]

    storage_info {
      ebs_storage_info {
        volume_size = 20
      }
    }
  }
}

resource "aws_msk_cluster" "mb29_target" {
  cluster_name           = "mega-batch-29-target"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 1

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = [aws_subnet.mb29a.id, aws_subnet.mb29b.id]
    security_groups = [aws_security_group.mb29.id]

    storage_info {
      ebs_storage_info {
        volume_size = 20
      }
    }
  }
}

resource "aws_msk_cluster_policy" "mb29" {
  cluster_arn = aws_msk_cluster.mb29_target.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "MegaBatch29ClusterPolicy"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::000000000000:root" }
      Action    = "kafka:GetBootstrapBrokers"
      Resource  = aws_msk_cluster.mb29_target.arn
    }]
  })
}

resource "aws_msk_vpc_connection" "mb29" {
  target_cluster_arn = aws_msk_cluster.mb29_target.arn
  authentication      = "SASL_IAM"
  vpc_id              = aws_vpc.mb29.id
  client_subnets      = [aws_subnet.mb29a.id, aws_subnet.mb29b.id]
  security_groups     = [aws_security_group.mb29.id]
}

resource "aws_msk_serverless_cluster" "mb29" {
  cluster_name = "mega-batch-29-serverless"

  vpc_config {
    subnet_ids         = [aws_subnet.mb29a.id, aws_subnet.mb29b.id]
    security_group_ids = [aws_security_group.mb29.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}

resource "aws_secretsmanager_secret" "mb29_scram" {
  name = "AmazonMSK_mega-batch-29-scram"
}

resource "aws_secretsmanager_secret_version" "mb29_scram" {
  secret_id     = aws_secretsmanager_secret.mb29_scram.id
  secret_string = jsonencode({ username = "mega-batch-29", password = "mega-batch-29-password" })
}

resource "aws_msk_scram_secret_association" "mb29" {
  cluster_arn     = aws_msk_cluster.mb29_target.arn
  secret_arn_list = [aws_secretsmanager_secret.mb29_scram.arn]

  depends_on = [aws_secretsmanager_secret_version.mb29_scram]
}

resource "aws_secretsmanager_secret" "mb29_scram_single" {
  name = "AmazonMSK_mega-batch-29-scram-single"
}

resource "aws_secretsmanager_secret_version" "mb29_scram_single" {
  secret_id     = aws_secretsmanager_secret.mb29_scram_single.id
  secret_string = jsonencode({ username = "mega-batch-29-single", password = "mega-batch-29-single-password" })
}

resource "aws_msk_single_scram_secret_association" "mb29" {
  cluster_arn = aws_msk_cluster.mb29_source.arn
  secret_arn  = aws_secretsmanager_secret.mb29_scram_single.arn

  depends_on = [aws_secretsmanager_secret_version.mb29_scram_single]
}

resource "aws_iam_role" "mb29_replicator" {
  name = "mega-batch-29-replicator-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "kafka.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_msk_replicator" "mb29" {
  replicator_name             = "mega-batch-29-replicator"
  description                 = "mega-batch-29 replicator"
  service_execution_role_arn = aws_iam_role.mb29_replicator.arn

  kafka_cluster {
    amazon_msk_cluster {
      msk_cluster_arn = aws_msk_cluster.mb29_source.arn
    }

    vpc_config {
      subnet_ids          = [aws_subnet.mb29a.id, aws_subnet.mb29b.id]
      security_groups_ids = [aws_security_group.mb29.id]
    }
  }

  kafka_cluster {
    amazon_msk_cluster {
      msk_cluster_arn = aws_msk_cluster.mb29_target.arn
    }

    vpc_config {
      subnet_ids          = [aws_subnet.mb29a.id, aws_subnet.mb29b.id]
      security_groups_ids = [aws_security_group.mb29.id]
    }
  }

  replication_info_list {
    source_kafka_cluster_arn = aws_msk_cluster.mb29_source.arn
    target_kafka_cluster_arn = aws_msk_cluster.mb29_target.arn
    target_compression_type  = "NONE"

    topic_replication {
      topics_to_replicate = [".*"]
    }

    consumer_group_replication {
      consumer_groups_to_replicate = [".*"]
    }
  }
}
