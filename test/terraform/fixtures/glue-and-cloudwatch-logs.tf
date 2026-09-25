resource "aws_iam_role" "glue" {
  name = "glcw-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_vpc" "glue" {
  cidr_block = "10.120.0.0/16"

  tags = {
    Name = "glcw-glue-vpc"
  }
}

resource "aws_subnet" "glue" {
  vpc_id     = aws_vpc.glue.id
  cidr_block = "10.120.1.0/24"

  tags = {
    Name = "glcw-glue-subnet"
  }
}

resource "aws_security_group" "glue" {
  name   = "glcw-glue-sg"
  vpc_id = aws_vpc.glue.id
}

resource "aws_glue_catalog_database" "example" {
  name = "glcw_db"
}

resource "aws_glue_catalog_table" "example" {
  name          = "glcw_table"
  database_name = aws_glue_catalog_database.example.name

  partition_keys {
    name = "year"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://glcw-bucket/data/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
    }

    columns {
      name = "id"
      type = "string"
    }
  }
}

resource "aws_glue_classifier" "example" {
  name = "glcw-classifier"

  csv_classifier {
    delimiter = ","
  }
}

resource "aws_glue_connection" "example" {
  name            = "glcw-connection"
  catalog_id      = "000000000000"
  connection_type = "NETWORK"

  physical_connection_requirements {
    availability_zone      = "us-east-1a"
    security_group_id_list = [aws_security_group.glue.id]
    subnet_id              = aws_subnet.glue.id
  }
}

resource "aws_glue_data_catalog_encryption_settings" "example" {
  catalog_id = "000000000000"

  data_catalog_encryption_settings {
    encryption_at_rest {
      catalog_encryption_mode = "DISABLED"
    }

    connection_password_encryption {
      return_connection_password_encrypted = false
    }
  }
}

resource "aws_glue_data_quality_ruleset" "example" {
  name    = "glcw-dq-ruleset"
  ruleset = "Rules = [ ColumnCount > 0 ]"
}

resource "aws_glue_dev_endpoint" "example" {
  name     = "glcw-dev-endpoint"
  role_arn = aws_iam_role.glue.arn
}

resource "aws_glue_ml_transform" "example" {
  name     = "glcw-ml-transform"
  role_arn = aws_iam_role.glue.arn

  input_record_tables {
    database_name = aws_glue_catalog_table.example.database_name
    table_name    = aws_glue_catalog_table.example.name
  }

  parameters {
    transform_type = "FIND_MATCHES"

    find_matches_parameters {
      primary_key_column_name = "id"
    }
  }
}

resource "aws_glue_partition" "example" {
  database_name    = aws_glue_catalog_database.example.name
  table_name       = aws_glue_catalog_table.example.name
  partition_values = ["2024"]

  storage_descriptor {
    location      = "s3://glcw-bucket/data/2024/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
  }
}

resource "aws_glue_partition_index" "example" {
  database_name = aws_glue_catalog_database.example.name
  table_name    = aws_glue_catalog_table.example.name

  partition_index {
    index_name = "year-index"
    keys       = ["year"]
  }

  depends_on = [aws_glue_partition.example]
}

resource "aws_glue_registry" "example" {
  registry_name = "glcw-registry"
}

resource "aws_glue_schema" "example" {
  schema_name   = "glcw-schema"
  registry_arn  = aws_glue_registry.example.arn
  data_format   = "AVRO"
  compatibility = "BACKWARD"

  schema_definition = jsonencode({
    type   = "record"
    name   = "example"
    fields = [{ name = "id", type = "string" }]
  })
}

resource "aws_glue_security_configuration" "example" {
  name = "glcw-secconfig"

  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "DISABLED"
    }

    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "DISABLED"
    }

    s3_encryption {
      s3_encryption_mode = "DISABLED"
    }
  }
}

resource "aws_glue_resource_policy" "example" {
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::000000000000:root" }
      Action    = "glue:GetTable"
      Resource  = "arn:aws:glue:us-east-1:000000000000:table/glcw_db/*"
    }]
  })
}

resource "aws_glue_job" "example" {
  name     = "glcw-job"
  role_arn = aws_iam_role.glue.arn

  command {
    script_location = "s3://glcw-bucket/scripts/job.py"
  }
}

resource "aws_glue_trigger" "example" {
  name = "glcw-trigger"
  type = "ON_DEMAND"

  actions {
    job_name = aws_glue_job.example.name
  }
}

resource "aws_glue_user_defined_function" "example" {
  name          = "glcw-udf"
  database_name = aws_glue_catalog_database.example.name
  class_name    = "com.example.MyUDF"
  owner_name    = "glcw-owner"
  owner_type    = "USER"

  resource_uris {
    resource_type = "JAR"
    uri           = "s3://glcw-bucket/udf.jar"
  }
}

resource "aws_glue_workflow" "example" {
  name = "glcw-workflow"
}

resource "aws_glue_catalog_table_optimizer" "example" {
  catalog_id    = "000000000000"
  database_name = aws_glue_catalog_database.example.name
  table_name    = aws_glue_catalog_table.example.name
  type          = "compaction"

  configuration {
    role_arn = aws_iam_role.glue.arn
    enabled  = true
  }
}

resource "aws_kinesis_stream" "cwl" {
  name             = "glcw-cwl-stream"
  shard_count      = 1
  retention_period = 24
}

resource "aws_iam_role" "cwl" {
  name = "glcw-cwl-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "logs.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_cloudwatch_log_account_policy" "example" {
  policy_name = "glcw-account-policy"
  policy_type = "SUBSCRIPTION_FILTER_POLICY"
  scope       = "ALL"

  policy_document = jsonencode({
    DestinationArn = aws_kinesis_stream.cwl.arn
    RoleArn        = aws_iam_role.cwl.arn
    FilterPattern  = ""
    Distribution   = "Random"
  })
}

resource "aws_cloudwatch_log_group" "example" {
  name = "/glcw/loggroup"
}

resource "aws_cloudwatch_log_anomaly_detector" "example" {
  detector_name           = "glcw-detector"
  log_group_arn_list      = [aws_cloudwatch_log_group.example.arn]
  anomaly_visibility_time = 7
  enabled                 = true
}

resource "aws_cloudwatch_log_data_protection_policy" "example" {
  log_group_name = aws_cloudwatch_log_group.example.name

  policy_document = jsonencode({
    Name    = "glcw-dpp"
    Version = "2021-06-01"
    Statement = [
      {
        Sid            = "audit-policy"
        DataIdentifier = ["arn:aws:dataprotection::aws:data-identifier/EmailAddress"]
        Operation = {
          Audit = {
            FindingsDestination = {}
          }
        }
      },
      {
        Sid            = "redact-policy"
        DataIdentifier = ["arn:aws:dataprotection::aws:data-identifier/EmailAddress"]
        Operation = {
          Deidentify = {
            MaskConfig = {}
          }
        }
      }
    ]
  })
}

resource "aws_cloudwatch_log_delivery_destination" "example" {
  name = "glcw-delivery-destination"

  delivery_destination_configuration {
    destination_resource_arn = aws_cloudwatch_log_group.example.arn
  }
}

resource "aws_cloudwatch_log_delivery_destination_policy" "example" {
  delivery_destination_name = aws_cloudwatch_log_delivery_destination.example.name

  delivery_destination_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "glcw-delivery-dest-policy"
      Effect    = "Allow"
      Principal = { Service = "delivery.logs.amazonaws.com" }
      Action    = "logs:CreateDelivery"
      Resource  = "*"
    }]
  })
}

resource "aws_cloudwatch_log_delivery_source" "example" {
  name         = "glcw-delivery-source"
  log_type     = "APPLICATION_LOGS"
  resource_arn = "arn:aws:bedrock:us-east-1:000000000000:knowledge-base/glcw-kb"
}

resource "aws_cloudwatch_log_delivery" "example" {
  delivery_source_name     = aws_cloudwatch_log_delivery_source.example.name
  delivery_destination_arn = aws_cloudwatch_log_delivery_destination.example.arn
}

resource "aws_cloudwatch_log_destination" "example" {
  name       = "glcw-destination"
  role_arn   = aws_iam_role.cwl.arn
  target_arn = aws_kinesis_stream.cwl.arn
}

resource "aws_cloudwatch_log_destination_policy" "example" {
  destination_name = aws_cloudwatch_log_destination.example.name

  access_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "glcw-destination-policy"
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "logs:PutSubscriptionFilter"
      Resource  = aws_cloudwatch_log_destination.example.arn
    }]
  })
}

resource "aws_cloudwatch_log_index_policy" "example" {
  log_group_name = aws_cloudwatch_log_group.example.name

  policy_document = jsonencode({
    Fields = ["eventName"]
  })
}

resource "aws_cloudwatch_log_resource_policy" "example" {
  policy_name = "glcw-resource-policy"

  policy_document = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "glcw-resource-policy"
      Effect    = "Allow"
      Principal = { Service = "route53.amazonaws.com" }
      Action    = ["logs:PutLogEvents", "logs:CreateLogStream"]
      Resource  = "arn:aws:logs:us-east-1:000000000000:log-group:/glcw/*"
    }]
  })
}

resource "aws_cloudwatch_query_definition" "example" {
  name = "glcw-query-definition"

  log_group_names = [aws_cloudwatch_log_group.example.name]
  query_string    = <<EOF
fields @timestamp, @message
| sort @timestamp desc
| limit 20
EOF
}
