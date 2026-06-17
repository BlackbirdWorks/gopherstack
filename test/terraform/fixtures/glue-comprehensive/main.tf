resource "aws_iam_role" "glue" {
  name = "{{.Prefix}}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_s3_bucket" "data" {
  bucket        = "{{.Prefix}}-data"
  force_destroy = true
}

resource "aws_glue_catalog_database" "this" {
  name        = "{{.Prefix}}_db"
  description = "Comprehensive Glue test database"
}

resource "aws_glue_catalog_table" "orders" {
  name          = "orders"
  database_name = aws_glue_catalog_database.this.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data.bucket}/orders/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "orders"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "id"
      type = "string"
    }

    columns {
      name = "amount"
      type = "double"
    }

    columns {
      name = "created_at"
      type = "timestamp"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }

  partition_keys {
    name = "month"
    type = "string"
  }
}

resource "aws_glue_crawler" "orders" {
  database_name = aws_glue_catalog_database.this.name
  name          = "{{.Prefix}}-orders-crawler"
  role          = aws_iam_role.glue.arn
  description   = "Crawls the orders S3 prefix"

  s3_target {
    path = "s3://${aws_s3_bucket.data.bucket}/orders/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  tags = {
    Environment = "test"
  }
}

resource "aws_glue_job" "etl" {
  name         = "{{.Prefix}}-etl-job"
  role_arn     = aws_iam_role.glue.arn
  description  = "Comprehensive ETL job"
  glue_version = "4.0"

  command {
    script_location = "s3://${aws_s3_bucket.data.bucket}/scripts/etl.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option" = "job-bookmark-enable"
    "--enable-metrics"      = ""
  }

  max_retries      = 1
  number_of_workers = 2
  worker_type      = "G.1X"

  tags = {
    Environment = "test"
  }
}

resource "aws_glue_trigger" "daily" {
  name     = "{{.Prefix}}-daily-trigger"
  type     = "SCHEDULED"
  schedule = "cron(0 2 * * ? *)"
  enabled  = false

  actions {
    job_name = aws_glue_job.etl.name
  }

  tags = {
    Environment = "test"
  }
}
