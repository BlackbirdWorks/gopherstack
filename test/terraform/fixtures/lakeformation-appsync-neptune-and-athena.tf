##############################################################################
# Lake Formation: register an S3 bucket, tag a Glue database, grant
# permissions, opt in, and apply a data cells filter to a Glue table.
##############################################################################

resource "aws_s3_bucket" "mb39_lf" {
  bucket        = "mega-batch-39-lf-bucket"
  force_destroy = true
}

resource "aws_glue_catalog_database" "mb39" {
  name = "mega_batch_39_db"
}

resource "aws_glue_catalog_table" "mb39" {
  name          = "mega_batch_39_table"
  database_name = aws_glue_catalog_database.mb39.name

  storage_descriptor {
    location = "s3://${aws_s3_bucket.mb39_lf.bucket}/data/"

    columns {
      name = "id"
      type = "string"
    }
  }
}

resource "aws_iam_role" "mb39_lf" {
  name = "mega-batch-39-lf-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lakeformation.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_lakeformation_resource" "mb39" {
  arn = aws_s3_bucket.mb39_lf.arn
}

resource "aws_lakeformation_lf_tag" "mb39" {
  key    = "mega-batch-39-tag"
  values = ["blue", "green"]
}

resource "aws_lakeformation_resource_lf_tags" "mb39" {
  database {
    name = aws_glue_catalog_database.mb39.name
  }

  lf_tag {
    key   = aws_lakeformation_lf_tag.mb39.key
    value = "blue"
  }
}

resource "aws_lakeformation_permissions" "mb39" {
  principal   = aws_iam_role.mb39_lf.arn
  permissions = ["DATA_LOCATION_ACCESS"]

  data_location {
    arn = aws_lakeformation_resource.mb39.arn
  }
}

resource "aws_lakeformation_opt_in" "mb39" {
  principal {
    data_lake_principal_identifier = aws_iam_role.mb39_lf.arn
  }

  resource_data {
    database {
      name = aws_glue_catalog_database.mb39.name
    }
  }
}

resource "aws_lakeformation_data_cells_filter" "mb39" {
  table_data {
    database_name    = aws_glue_catalog_database.mb39.name
    name             = "mega-batch-39-filter"
    table_catalog_id = "000000000000"
    table_name       = aws_glue_catalog_table.mb39.name

    column_names = ["id"]

    row_filter {
      all_rows_wildcard {}
    }
  }
}

##############################################################################
# AppSync: source/merged GraphQL APIs with a source API association, an
# API cache, a custom domain name + association, a pipeline function, and
# a custom type.
##############################################################################

resource "aws_appsync_graphql_api" "mb39_source" {
  name                 = "mega-batch-39-source-api"
  authentication_type  = "API_KEY"
}

resource "aws_iam_role" "mb39_appsync_merge" {
  name = "mega-batch-39-appsync-merge-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "appsync.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_appsync_graphql_api" "mb39_merged" {
  name                           = "mega-batch-39-merged-api"
  authentication_type            = "API_KEY"
  api_type                       = "MERGED"
  merged_api_execution_role_arn  = aws_iam_role.mb39_appsync_merge.arn
}

resource "aws_appsync_source_api_association" "mb39" {
  description   = "mega-batch-39 source association"
  merged_api_id = aws_appsync_graphql_api.mb39_merged.id
  source_api_id = aws_appsync_graphql_api.mb39_source.id
}

resource "aws_appsync_api_cache" "mb39" {
  api_id               = aws_appsync_graphql_api.mb39_source.id
  api_caching_behavior = "FULL_REQUEST_CACHING"
  type                 = "SMALL"
  ttl                  = 900
}

resource "aws_acm_certificate" "mb39" {
  domain_name       = "mega-batch-39.example.test"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_appsync_domain_name" "mb39" {
  domain_name     = "mega-batch-39.example.test"
  certificate_arn = aws_acm_certificate.mb39.arn
}

resource "aws_appsync_domain_name_api_association" "mb39" {
  api_id      = aws_appsync_graphql_api.mb39_source.id
  domain_name = aws_appsync_domain_name.mb39.domain_name
}

resource "aws_appsync_datasource" "mb39" {
  api_id = aws_appsync_graphql_api.mb39_source.id
  name   = "mega_batch_39_ds"
  type   = "NONE"
}

resource "aws_appsync_function" "mb39" {
  api_id      = aws_appsync_graphql_api.mb39_source.id
  data_source = aws_appsync_datasource.mb39.name
  name        = "mega_batch_39_function"

  request_mapping_template  = "{}"
  response_mapping_template = "$util.toJson($ctx.result)"
}

resource "aws_appsync_type" "mb39" {
  api_id = aws_appsync_graphql_api.mb39_source.id
  format = "SDL"

  definition = <<EOF
type MegaBatch39Widget {
  id: ID!
}
EOF
}

##############################################################################
# Neptune: a cluster with a custom endpoint, a cluster parameter group, an
# instance-level parameter group, a cluster snapshot, an event subscription,
# and a standalone global cluster.
##############################################################################

resource "aws_neptune_subnet_group" "mb39" {
  name       = "mega-batch-39-neptune-sg"
  subnet_ids = ["subnet-mb39a", "subnet-mb39b"]
}

resource "aws_neptune_cluster_parameter_group" "mb39" {
  name        = "mega-batch-39-neptune-cpg"
  family      = "neptune1"
  description = "mega-batch-39 cluster parameter group"

  parameter {
    name  = "neptune_enable_audit_log"
    value = "1"
  }
}

resource "aws_neptune_parameter_group" "mb39" {
  name   = "mega-batch-39-neptune-pg"
  family = "neptune1"

  parameter {
    name  = "neptune_query_timeout"
    value = "25"
  }
}

resource "aws_neptune_cluster" "mb39" {
  cluster_identifier                  = "mega-batch-39-neptune-cluster"
  engine                               = "neptune"
  skip_final_snapshot                  = true
  neptune_subnet_group_name            = aws_neptune_subnet_group.mb39.name
  neptune_cluster_parameter_group_name = aws_neptune_cluster_parameter_group.mb39.name
  apply_immediately                    = true
}

resource "aws_neptune_cluster_endpoint" "mb39" {
  cluster_identifier          = aws_neptune_cluster.mb39.cluster_identifier
  cluster_endpoint_identifier = "mega-batch-39-endpoint"
  endpoint_type               = "READER"
}

resource "aws_neptune_cluster_snapshot" "mb39" {
  db_cluster_identifier          = aws_neptune_cluster.mb39.id
  db_cluster_snapshot_identifier = "mega-batch-39-snapshot"
}

resource "aws_sns_topic" "mb39_neptune" {
  name = "mega-batch-39-neptune-events"
}

resource "aws_neptune_event_subscription" "mb39" {
  name          = "mega-batch-39-neptune-sub"
  sns_topic_arn = aws_sns_topic.mb39_neptune.arn
  source_type   = "db-cluster"
  source_ids    = [aws_neptune_cluster.mb39.id]

  event_categories = ["maintenance", "failure"]
}

resource "aws_neptune_global_cluster" "mb39" {
  global_cluster_identifier = "mega-batch-39-global"
  engine                    = "neptune"
}

##############################################################################
# Athena: a capacity reservation, a Glue-backed data catalog, a bucket-backed
# database, a named query, and a prepared statement on a workgroup.
##############################################################################

resource "aws_athena_capacity_reservation" "mb39" {
  name        = "mega-batch-39-reservation"
  target_dpus = 24
}

resource "aws_athena_data_catalog" "mb39" {
  name        = "mega-batch-39-catalog"
  description = "mega-batch-39 Glue data catalog"
  type        = "GLUE"

  parameters = {
    "catalog-id" = "000000000000"
  }
}

resource "aws_s3_bucket" "mb39_athena" {
  bucket        = "mega-batch-39-athena-bucket"
  force_destroy = true
}

resource "aws_athena_workgroup" "mb39" {
  name = "mega-batch-39-workgroup"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.mb39_athena.bucket}/results/"
    }
  }
}

resource "aws_athena_database" "mb39" {
  name   = "mega_batch_39_athena_db"
  bucket = aws_s3_bucket.mb39_athena.id
}

resource "aws_athena_named_query" "mb39" {
  name      = "mega-batch-39-named-query"
  workgroup = aws_athena_workgroup.mb39.id
  database  = aws_athena_database.mb39.name
  query     = "SELECT * FROM ${aws_athena_database.mb39.name} limit 10;"
}

resource "aws_athena_prepared_statement" "mb39" {
  name            = "mega_batch_39_prepared"
  workgroup       = aws_athena_workgroup.mb39.name
  query_statement = "SELECT * FROM ${aws_athena_database.mb39.name} WHERE x = ?"
}

##############################################################################
# Directory Service: a SimpleAD directory in its own VPC with a conditional
# forwarder, a CloudWatch log subscription, and RADIUS MFA settings.
##############################################################################

resource "aws_vpc" "mb39" {
  cidr_block = "{{.VPCCidr}}"

  tags = {
    Name = "mega-batch-39-vpc"
  }
}

resource "aws_subnet" "mb39_a" {
  vpc_id            = aws_vpc.mb39.id
  cidr_block        = "{{.SubnetCidrA}}"
  availability_zone = "us-east-1a"
}

resource "aws_subnet" "mb39_b" {
  vpc_id            = aws_vpc.mb39.id
  cidr_block        = "{{.SubnetCidrB}}"
  availability_zone = "us-east-1b"
}

resource "aws_directory_service_directory" "mb39" {
  name     = "mega-batch-39.test"
  password = "MegaBatch39Passw0rd!"
  type     = "SimpleAD"
  size     = "Small"

  vpc_settings {
    vpc_id     = aws_vpc.mb39.id
    subnet_ids = [aws_subnet.mb39_a.id, aws_subnet.mb39_b.id]
  }
}

resource "aws_directory_service_conditional_forwarder" "mb39" {
  directory_id        = aws_directory_service_directory.mb39.id
  remote_domain_name  = "mega-batch-39-remote.test"
  dns_ips             = ["10.0.0.10", "10.0.0.11"]
}

resource "aws_cloudwatch_log_group" "mb39" {
  name              = "/aws/directoryservice/mega-batch-39"
  retention_in_days = 14
}

resource "aws_directory_service_log_subscription" "mb39" {
  directory_id   = aws_directory_service_directory.mb39.id
  log_group_name = aws_cloudwatch_log_group.mb39.name
}

resource "aws_directory_service_radius_settings" "mb39" {
  directory_id = aws_directory_service_directory.mb39.id

  authentication_protocol = "PAP"
  display_label           = "mega-batch-39-radius"
  radius_port              = 1812
  radius_retries           = 4
  radius_servers           = ["10.0.0.20"]
  radius_timeout           = 1
  shared_secret            = "mega-batch-39-secret"
}

##############################################################################
# Directory Service: a Microsoft AD (Enterprise) directory, shared to another
# account (owner side only -- see PARITY.md for aws_directory_service_trust,
# _region, and _shared_directory_accepter, all left out after one real
# attempt each).
##############################################################################

resource "aws_directory_service_directory" "mb39_msad" {
  name     = "mega-batch-39-msad.test"
  password = "MegaBatch39MsadPassw0rd!"
  edition  = "Enterprise"
  type     = "MicrosoftAD"

  vpc_settings {
    vpc_id     = aws_vpc.mb39.id
    subnet_ids = [aws_subnet.mb39_a.id, aws_subnet.mb39_b.id]
  }
}

resource "aws_directory_service_shared_directory" "mb39" {
  directory_id = aws_directory_service_directory.mb39_msad.id
  notes        = "mega-batch-39 shared directory"

  target {
    id = "999999999999"
  }
}
