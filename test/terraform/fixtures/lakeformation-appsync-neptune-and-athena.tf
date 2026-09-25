##############################################################################
# Lake Formation: register an S3 bucket, tag a Glue database, grant
# permissions, opt in, and apply a data cells filter to a Glue table.
##############################################################################

resource "aws_s3_bucket" "lana_lf" {
  bucket        = "lana-lf-bucket"
  force_destroy = true
}

resource "aws_glue_catalog_database" "lana" {
  name = "lana_db"
}

resource "aws_glue_catalog_table" "lana" {
  name          = "lana_table"
  database_name = aws_glue_catalog_database.lana.name

  storage_descriptor {
    location = "s3://${aws_s3_bucket.lana_lf.bucket}/data/"

    columns {
      name = "id"
      type = "string"
    }
  }
}

resource "aws_iam_role" "lana_lf" {
  name = "lana-lf-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lakeformation.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_lakeformation_resource" "lana" {
  arn = aws_s3_bucket.lana_lf.arn
}

resource "aws_lakeformation_lf_tag" "lana" {
  key    = "lana-tag"
  values = ["blue", "green"]
}

resource "aws_lakeformation_resource_lf_tags" "lana" {
  database {
    name = aws_glue_catalog_database.lana.name
  }

  lf_tag {
    key   = aws_lakeformation_lf_tag.lana.key
    value = "blue"
  }
}

resource "aws_lakeformation_permissions" "lana" {
  principal   = aws_iam_role.lana_lf.arn
  permissions = ["DATA_LOCATION_ACCESS"]

  data_location {
    arn = aws_lakeformation_resource.lana.arn
  }
}

resource "aws_lakeformation_opt_in" "lana" {
  principal {
    data_lake_principal_identifier = aws_iam_role.lana_lf.arn
  }

  resource_data {
    database {
      name = aws_glue_catalog_database.lana.name
    }
  }
}

resource "aws_lakeformation_data_cells_filter" "lana" {
  table_data {
    database_name    = aws_glue_catalog_database.lana.name
    name             = "lana-filter"
    table_catalog_id = "000000000000"
    table_name       = aws_glue_catalog_table.lana.name

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

resource "aws_appsync_graphql_api" "lana_source" {
  name                = "lana-source-api"
  authentication_type = "API_KEY"
}

resource "aws_iam_role" "lana_appsync_merge" {
  name = "lana-appsync-merge-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "appsync.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_appsync_graphql_api" "lana_merged" {
  name                          = "lana-merged-api"
  authentication_type           = "API_KEY"
  api_type                      = "MERGED"
  merged_api_execution_role_arn = aws_iam_role.lana_appsync_merge.arn
}

resource "aws_appsync_source_api_association" "lana" {
  description   = "lana source association"
  merged_api_id = aws_appsync_graphql_api.lana_merged.id
  source_api_id = aws_appsync_graphql_api.lana_source.id
}

resource "aws_appsync_api_cache" "lana" {
  api_id               = aws_appsync_graphql_api.lana_source.id
  api_caching_behavior = "FULL_REQUEST_CACHING"
  type                 = "SMALL"
  ttl                  = 900
}

resource "aws_acm_certificate" "lana" {
  domain_name       = "lana.example.test"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_appsync_domain_name" "lana" {
  domain_name     = "lana.example.test"
  certificate_arn = aws_acm_certificate.lana.arn
}

resource "aws_appsync_domain_name_api_association" "lana" {
  api_id      = aws_appsync_graphql_api.lana_source.id
  domain_name = aws_appsync_domain_name.lana.domain_name
}

resource "aws_appsync_datasource" "lana" {
  api_id = aws_appsync_graphql_api.lana_source.id
  name   = "lana_ds"
  type   = "NONE"
}

resource "aws_appsync_function" "lana" {
  api_id      = aws_appsync_graphql_api.lana_source.id
  data_source = aws_appsync_datasource.lana.name
  name        = "lana_function"

  request_mapping_template  = "{}"
  response_mapping_template = "$util.toJson($ctx.result)"
}

resource "aws_appsync_type" "lana" {
  api_id = aws_appsync_graphql_api.lana_source.id
  format = "SDL"

  definition = <<EOF
type LakeformationAppsyncNeptuneAndAthenaWidget {
  id: ID!
}
EOF
}

##############################################################################
# Neptune: a cluster with a custom endpoint, a cluster parameter group, an
# instance-level parameter group, a cluster snapshot, an event subscription,
# and a standalone global cluster.
##############################################################################

resource "aws_neptune_subnet_group" "lana" {
  name       = "lana-neptune-sg"
  subnet_ids = ["subnet-lanaa", "subnet-lanab"]
}

resource "aws_neptune_cluster_parameter_group" "lana" {
  name        = "lana-neptune-cpg"
  family      = "neptune1"
  description = "lana cluster parameter group"

  parameter {
    name  = "neptune_enable_audit_log"
    value = "1"
  }
}

resource "aws_neptune_parameter_group" "lana" {
  name   = "lana-neptune-pg"
  family = "neptune1"

  parameter {
    name  = "neptune_query_timeout"
    value = "25"
  }
}

resource "aws_neptune_cluster" "lana" {
  cluster_identifier                   = "lana-neptune-cluster"
  engine                               = "neptune"
  skip_final_snapshot                  = true
  neptune_subnet_group_name            = aws_neptune_subnet_group.lana.name
  neptune_cluster_parameter_group_name = aws_neptune_cluster_parameter_group.lana.name
  apply_immediately                    = true
}

resource "aws_neptune_cluster_endpoint" "lana" {
  cluster_identifier          = aws_neptune_cluster.lana.cluster_identifier
  cluster_endpoint_identifier = "lana-endpoint"
  endpoint_type               = "READER"
}

resource "aws_neptune_cluster_snapshot" "lana" {
  db_cluster_identifier          = aws_neptune_cluster.lana.id
  db_cluster_snapshot_identifier = "lana-snapshot"
}

resource "aws_sns_topic" "lana_neptune" {
  name = "lana-neptune-events"
}

resource "aws_neptune_event_subscription" "lana" {
  name          = "lana-neptune-sub"
  sns_topic_arn = aws_sns_topic.lana_neptune.arn
  source_type   = "db-cluster"
  source_ids    = [aws_neptune_cluster.lana.id]

  event_categories = ["maintenance", "failure"]
}

resource "aws_neptune_global_cluster" "lana" {
  global_cluster_identifier = "lana-global"
  engine                    = "neptune"
}

##############################################################################
# Athena: a capacity reservation, a Glue-backed data catalog, a bucket-backed
# database, a named query, and a prepared statement on a workgroup.
##############################################################################

resource "aws_athena_capacity_reservation" "lana" {
  name        = "lana-reservation"
  target_dpus = 24
}

resource "aws_athena_data_catalog" "lana" {
  name        = "lana-catalog"
  description = "lana Glue data catalog"
  type        = "GLUE"

  parameters = {
    "catalog-id" = "000000000000"
  }
}

resource "aws_s3_bucket" "lana_athena" {
  bucket        = "lana-athena-bucket"
  force_destroy = true
}

resource "aws_athena_workgroup" "lana" {
  name = "lana-workgroup"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lana_athena.bucket}/results/"
    }
  }
}

resource "aws_athena_database" "lana" {
  name   = "lana_athena_db"
  bucket = aws_s3_bucket.lana_athena.id
}

resource "aws_athena_named_query" "lana" {
  name      = "lana-named-query"
  workgroup = aws_athena_workgroup.lana.id
  database  = aws_athena_database.lana.name
  query     = "SELECT * FROM ${aws_athena_database.lana.name} limit 10;"
}

resource "aws_athena_prepared_statement" "lana" {
  name            = "lana_prepared"
  workgroup       = aws_athena_workgroup.lana.name
  query_statement = "SELECT * FROM ${aws_athena_database.lana.name} WHERE x = ?"
}

##############################################################################
# Directory Service: a SimpleAD directory in its own VPC with a conditional
# forwarder, a CloudWatch log subscription, and RADIUS MFA settings.
##############################################################################

resource "aws_vpc" "lana" {
  cidr_block = "{{.VPCCidr}}"

  tags = {
    Name = "lana-vpc"
  }
}

resource "aws_subnet" "lana_a" {
  vpc_id            = aws_vpc.lana.id
  cidr_block        = "{{.SubnetCidrA}}"
  availability_zone = "us-east-1a"
}

resource "aws_subnet" "lana_b" {
  vpc_id            = aws_vpc.lana.id
  cidr_block        = "{{.SubnetCidrB}}"
  availability_zone = "us-east-1b"
}

resource "aws_directory_service_directory" "lana" {
  name     = "lana.test"
  password = "LakeformationAppsyncNeptuneAndAthenaPassw0rd!"
  type     = "SimpleAD"
  size     = "Small"

  vpc_settings {
    vpc_id     = aws_vpc.lana.id
    subnet_ids = [aws_subnet.lana_a.id, aws_subnet.lana_b.id]
  }
}

resource "aws_directory_service_conditional_forwarder" "lana" {
  directory_id       = aws_directory_service_directory.lana.id
  remote_domain_name = "lana-remote.test"
  dns_ips            = ["10.0.0.10", "10.0.0.11"]
}

resource "aws_cloudwatch_log_group" "lana" {
  name              = "/aws/directoryservice/lana"
  retention_in_days = 14
}

resource "aws_directory_service_log_subscription" "lana" {
  directory_id   = aws_directory_service_directory.lana.id
  log_group_name = aws_cloudwatch_log_group.lana.name
}

resource "aws_directory_service_radius_settings" "lana" {
  directory_id = aws_directory_service_directory.lana.id

  authentication_protocol = "PAP"
  display_label           = "lana-radius"
  radius_port             = 1812
  radius_retries          = 4
  radius_servers          = ["10.0.0.20"]
  radius_timeout          = 1
  shared_secret           = "lana-secret"
}

##############################################################################
# Directory Service: a Microsoft AD (Enterprise) directory, shared to another
# account (owner side only -- see PARITY.md for aws_directory_service_trust,
# _region, and _shared_directory_accepter, all left out after one real
# attempt each).
##############################################################################

resource "aws_directory_service_directory" "lana_msad" {
  name     = "lana-msad.test"
  password = "LakeformationAppsyncNeptuneAndAthenaMsadPassw0rd!"
  edition  = "Enterprise"
  type     = "MicrosoftAD"

  vpc_settings {
    vpc_id     = aws_vpc.lana.id
    subnet_ids = [aws_subnet.lana_a.id, aws_subnet.lana_b.id]
  }
}

resource "aws_directory_service_shared_directory" "lana" {
  directory_id = aws_directory_service_directory.lana_msad.id
  notes        = "lana shared directory"

  target {
    id = "999999999999"
  }
}
