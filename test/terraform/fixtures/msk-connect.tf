resource "aws_vpc" "mskc" {
  cidr_block = "10.201.0.0/16"
}

resource "aws_subnet" "mskc_a" {
  vpc_id     = aws_vpc.mskc.id
  cidr_block = "10.201.1.0/24"
}

resource "aws_subnet" "mskc_b" {
  vpc_id     = aws_vpc.mskc.id
  cidr_block = "10.201.2.0/24"
}

resource "aws_security_group" "mskc" {
  name   = "mskc-sg"
  vpc_id = aws_vpc.mskc.id
}

resource "aws_iam_role" "mskc_connect" {
  name = "mskc-connect-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "kafkaconnect.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_s3_bucket" "mskc" {
  bucket        = "{{.BucketName}}"
  force_destroy = true
}

resource "aws_s3_object" "mskc_plugin" {
  bucket  = aws_s3_bucket.mskc.id
  key     = "plugins/mskc-plugin.zip"
  content = "mskc fake plugin bytes"
}

resource "aws_mskconnect_custom_plugin" "mskc" {
  name         = "{{.PluginName}}"
  content_type = "ZIP"

  location {
    s3 {
      bucket_arn = aws_s3_bucket.mskc.arn
      file_key   = aws_s3_object.mskc_plugin.key
    }
  }
}

resource "aws_mskconnect_worker_configuration" "mskc" {
  name = "{{.WorkerConfigName}}"

  properties_file_content = <<EOT
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
EOT
}

resource "aws_mskconnect_connector" "mskc" {
  name                 = "{{.ConnectorName}}"
  kafkaconnect_version = "2.7.1"

  capacity {
    provisioned_capacity {
      mcu_count    = 1
      worker_count = 1
    }
  }

  connector_configuration = {
    "connector.class" = "com.example.MskConnectTestConnector"
    "tasks.max"        = "1"
  }

  kafka_cluster {
    apache_kafka_cluster {
      bootstrap_servers = "broker1.example.com:9092,broker2.example.com:9092"

      vpc {
        security_groups = [aws_security_group.mskc.id]
        subnets          = [aws_subnet.mskc_a.id, aws_subnet.mskc_b.id]
      }
    }
  }

  kafka_cluster_client_authentication {
    authentication_type = "NONE"
  }

  kafka_cluster_encryption_in_transit {
    encryption_type = "PLAINTEXT"
  }

  plugin {
    custom_plugin {
      arn      = aws_mskconnect_custom_plugin.mskc.arn
      revision = aws_mskconnect_custom_plugin.mskc.latest_revision
    }
  }

  worker_configuration {
    arn      = aws_mskconnect_worker_configuration.mskc.arn
    revision = aws_mskconnect_worker_configuration.mskc.latest_revision
  }

  service_execution_role_arn = aws_iam_role.mskc_connect.arn

  tags = {
    Environment = "test"
    Owner       = "terraform"
  }
}
