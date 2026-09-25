# ACM: certificate + validation.

resource "aws_acm_certificate" "aswm" {
  domain_name       = "aswm.example.com"
  validation_method = "DNS"
}

resource "aws_route53_zone" "aswm" {
  name = "aswm.example.com"
}

resource "aws_route53_record" "aswm_validation" {
  for_each = {
    for dvo in aws_acm_certificate.aswm.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  }

  allow_overwrite = true
  name            = each.value.name
  records         = [each.value.record]
  ttl             = 60
  type            = each.value.type
  zone_id         = aws_route53_zone.aswm.zone_id
}

resource "aws_acm_certificate_validation" "aswm" {
  certificate_arn         = aws_acm_certificate.aswm.arn
  validation_record_fqdns = [for r in aws_route53_record.aswm_validation : r.fqdn]
}

# API Gateway: domain name + VPC endpoint + access association.

resource "aws_vpc" "aswm" {
  cidr_block = "10.214.0.0/16"
}

resource "aws_subnet" "aswm" {
  vpc_id            = aws_vpc.aswm.id
  cidr_block        = "10.214.1.0/24"
  availability_zone = "us-east-1a"
}

resource "aws_security_group" "aswm_vpce" {
  vpc_id = aws_vpc.aswm.id
}

resource "aws_vpc_endpoint" "aswm" {
  vpc_id             = aws_vpc.aswm.id
  service_name       = "com.amazonaws.us-east-1.execute-api"
  vpc_endpoint_type  = "Interface"
  subnet_ids         = [aws_subnet.aswm.id]
  security_group_ids = [aws_security_group.aswm_vpce.id]
}

resource "aws_api_gateway_domain_name" "aswm" {
  domain_name              = "aswm-api.example.com"
  regional_certificate_arn = aws_acm_certificate_validation.aswm.certificate_arn

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_api_gateway_domain_name_access_association" "aswm" {
  access_association_source      = aws_vpc_endpoint.aswm.id
  access_association_source_type = "VPCE"
  domain_name_arn                = aws_api_gateway_domain_name.aswm.arn
}

# App Mesh: virtual gateway + gateway route.

resource "aws_appmesh_mesh" "aswm" {
  name = "aswm-mesh"
}

resource "aws_appmesh_virtual_gateway" "aswm" {
  name      = "aswm-vgw"
  mesh_name = aws_appmesh_mesh.aswm.id

  spec {
    listener {
      port_mapping {
        port     = 8080
        protocol = "http"
      }
    }
  }
}

resource "aws_appmesh_virtual_service" "aswm" {
  name      = "aswm.svc.local"
  mesh_name = aws_appmesh_mesh.aswm.id

  spec {}
}

resource "aws_appmesh_gateway_route" "aswm" {
  name                 = "aswm-gw-route"
  mesh_name            = aws_appmesh_mesh.aswm.id
  virtual_gateway_name = aws_appmesh_virtual_gateway.aswm.name

  spec {
    http_route {
      action {
        target {
          virtual_service {
            virtual_service_name = aws_appmesh_virtual_service.aswm.name
          }
        }
      }

      match {
        prefix = "/"
      }
    }
  }
}

# CloudFront KeyValueStore: exclusive keys.

resource "aws_cloudfront_key_value_store" "aswm" {
  name    = "aswm-kvs"
  comment = "aswm keyvaluestore"
}

resource "aws_cloudfrontkeyvaluestore_keys_exclusive" "aswm" {
  key_value_store_arn = aws_cloudfront_key_value_store.aswm.arn

  resource_key_value_pair {
    key   = "aswm-key-a"
    value = "aswm-value-a"
  }

  resource_key_value_pair {
    key   = "aswm-key-b"
    value = "aswm-value-b"
  }
}

# CodePipeline: webhook.

resource "aws_s3_bucket" "aswm_pipeline" {
  bucket = "aswm-pipeline-artifacts"
}

resource "aws_iam_role" "aswm_pipeline" {
  name = "aswm-pipeline-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "codepipeline.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_codepipeline" "aswm" {
  name     = "aswm-pipeline"
  role_arn = aws_iam_role.aswm_pipeline.arn

  artifact_store {
    location = aws_s3_bucket.aswm_pipeline.id
    type     = "S3"
  }

  stage {
    name = "Source"

    action {
      name             = "Source"
      category         = "Source"
      owner            = "AWS"
      provider         = "S3"
      version          = "1"
      output_artifacts = ["source_output"]

      configuration = {
        S3Bucket    = aws_s3_bucket.aswm_pipeline.id
        S3ObjectKey = "source.zip"
      }
    }
  }

  stage {
    name = "Deploy"

    action {
      name            = "Deploy"
      category        = "Deploy"
      owner           = "AWS"
      provider        = "S3"
      version         = "1"
      input_artifacts = ["source_output"]

      configuration = {
        BucketName = aws_s3_bucket.aswm_pipeline.id
        Extract    = "true"
      }
    }
  }
}

resource "aws_codepipeline_webhook" "aswm" {
  name            = "aswm-webhook"
  authentication  = "UNAUTHENTICATED"
  target_action   = "Source"
  target_pipeline = aws_codepipeline.aswm.name

  authentication_configuration {}

  filter {
    json_path    = "$.ref"
    match_equals = "refs/heads/{Branch}"
  }
}

# Cognito Identity Pool: provider principal tag.

resource "aws_cognito_identity_pool" "aswm" {
  identity_pool_name               = "aswm_pool"
  allow_unauthenticated_identities = true

  supported_login_providers = {
    "graph.facebook.com" = "aswm-fb-app-id"
  }
}

resource "aws_cognito_identity_pool_provider_principal_tag" "aswm" {
  identity_pool_id       = aws_cognito_identity_pool.aswm.id
  identity_provider_name = "graph.facebook.com"
  use_defaults           = true
}

# EC2: IPAM preview next CIDR, network performance metric subscription,
# security group VPC association, spot fleet request.

resource "aws_vpc_ipam" "aswm" {
  operating_regions {
    region_name = "us-east-1"
  }
}

resource "aws_vpc_ipam_pool" "aswm" {
  address_family = "ipv4"
  ipam_scope_id  = aws_vpc_ipam.aswm.private_default_scope_id
  locale         = "us-east-1"
}

resource "aws_vpc_ipam_pool_cidr" "aswm" {
  ipam_pool_id = aws_vpc_ipam_pool.aswm.id
  cidr         = "10.220.0.0/16"
}

resource "aws_vpc_ipam_preview_next_cidr" "aswm" {
  ipam_pool_id   = aws_vpc_ipam_pool.aswm.id
  netmask_length = 24

  depends_on = [aws_vpc_ipam_pool_cidr.aswm]
}

resource "aws_vpc_network_performance_metric_subscription" "aswm" {
  source      = "us-east-1"
  destination = "us-west-2"
  metric      = "aggregate-latency"
  statistic   = "p50"
}

resource "aws_vpc_security_group_vpc_association" "aswm" {
  security_group_id = aws_security_group.aswm_vpce.id
  vpc_id            = aws_vpc.aswm.id
}

# Shield: protection health check association.

resource "aws_eip" "aswm" {
  domain = "vpc"
}

resource "aws_shield_subscription" "aswm" {
  auto_renew = "ENABLED"
}

resource "aws_shield_protection" "aswm" {
  name         = "aswm-shield-protection"
  resource_arn = "arn:aws:ec2:us-east-1:000000000000:eip-allocation/${aws_eip.aswm.id}"

  depends_on = [aws_shield_subscription.aswm]
}

resource "aws_route53_health_check" "aswm" {
  fqdn              = "aswm-health.example.com"
  port              = 443
  type              = "HTTPS"
  resource_path     = "/"
  failure_threshold = 3
  request_interval  = 30
}

resource "aws_shield_protection_health_check_association" "aswm" {
  health_check_arn     = aws_route53_health_check.aswm.arn
  shield_protection_id = aws_shield_protection.aswm.id
}

# Timestream Query: scheduled query.

resource "aws_iam_role" "aswm_timestream" {
  name = "aswm-timestream-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "timestream.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_sns_topic" "aswm_timestream" {
  name = "aswm-timestream-notify"
}

resource "aws_s3_bucket" "aswm_timestream" {
  bucket = "aswm-timestream-errors"
}

resource "aws_timestreamwrite_database" "aswm" {
  database_name = "aswm_db"
}

resource "aws_timestreamwrite_table" "aswm" {
  database_name = aws_timestreamwrite_database.aswm.database_name
  table_name    = "aswm_table"
}

resource "aws_timestreamquery_scheduled_query" "aswm" {
  name               = "aswm-scheduled-query"
  query_string       = "SELECT 1"
  execution_role_arn = aws_iam_role.aswm_timestream.arn

  schedule_configuration {
    schedule_expression = "rate(1 hour)"
  }

  notification_configuration {
    sns_configuration {
      topic_arn = aws_sns_topic.aswm_timestream.arn
    }
  }

  target_configuration {
    timestream_configuration {
      database_name = aws_timestreamwrite_database.aswm.database_name
      table_name    = aws_timestreamwrite_table.aswm.table_name

      time_column = "time"
      dimension_mapping {
        name                 = "region"
        dimension_value_type = "VARCHAR"
      }
      multi_measure_mappings {
        target_multi_measure_name = "metrics"

        multi_measure_attribute_mapping {
          source_column      = "value"
          measure_value_type = "DOUBLE"
        }
      }
    }
  }

  error_report_configuration {
    s3_configuration {
      bucket_name = aws_s3_bucket.aswm_timestream.id
    }
  }
}

# WorkSpaces: directory + workspace.

resource "aws_directory_service_directory" "aswm" {
  name     = "aswm.example.com"
  password = "AppmeshShieldAndWorkspacesPass!"
  size     = "Small"
  type     = "SimpleAD"

  vpc_settings {
    vpc_id     = aws_vpc.aswm.id
    subnet_ids = [aws_subnet.aswm.id, aws_subnet.aswm_b.id]
  }

  # This emulator deletes directories synchronously; the provider's own
  # delete waiter still has a long fixed poll cadence, so cap it short.
  timeouts {
    delete = "90s"
  }
}

resource "aws_subnet" "aswm_b" {
  vpc_id            = aws_vpc.aswm.id
  cidr_block        = "10.214.2.0/24"
  availability_zone = "us-east-1b"
}

resource "aws_workspaces_directory" "aswm" {
  directory_id = aws_directory_service_directory.aswm.id

  workspace_creation_properties {
    enable_internet_access = false
  }
}

resource "aws_workspaces_workspace" "aswm" {
  directory_id = aws_workspaces_directory.aswm.id
  bundle_id    = data.aws_workspaces_bundle.aswm.id
  user_name    = "Administrator"

  workspace_properties {
    compute_type_name                         = "STANDARD"
    root_volume_size_gib                      = 80
    user_volume_size_gib                      = 50
    running_mode                              = "AUTO_STOP"
    running_mode_auto_stop_timeout_in_minutes = 60
  }
}

data "aws_workspaces_bundle" "aswm" {
  bundle_id = "wsb-bh8rsxt14"
}

# RDS: custom DB engine version.

resource "aws_s3_bucket" "aswm_rds" {
  bucket = "aswm-rds-custom-media"
}

resource "aws_s3_object" "aswm_rds" {
  bucket  = aws_s3_bucket.aswm_rds.id
  key     = "aswm-oracle-media.zip"
  content = "aswm fake install media"
}

resource "aws_rds_custom_db_engine_version" "aswm" {
  engine         = "custom-oracle-ee"
  engine_version = "19.aswm.1"
  manifest = jsonencode({
    mediaImportTemplateVersion = "2020-08-14"
  })

  database_installation_files_s3_bucket_name = aws_s3_bucket.aswm_rds.id
  database_installation_files_s3_prefix      = "aswm-media"

  depends_on = [aws_s3_object.aswm_rds]
}

# Kinesis Analytics v2: application snapshot.

resource "aws_iam_role" "aswm_kda" {
  name = "aswm-kda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "kinesisanalytics.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_kinesisanalyticsv2_application" "aswm" {
  name                   = "aswm-kda-app"
  runtime_environment    = "SQL-1_0"
  service_execution_role = aws_iam_role.aswm_kda.arn
  start_application      = true

  application_configuration {
    application_code_configuration {
      code_content_type = "PLAINTEXT"

      code_content {
        text_content = "SELECT 1;"
      }
    }
  }
}

resource "aws_kinesisanalyticsv2_application_snapshot" "aswm" {
  application_name = aws_kinesisanalyticsv2_application.aswm.name
  snapshot_name    = "aswm-snapshot"
}

# Directory Service: trust.

resource "aws_directory_service_directory" "aswm_msad" {
  name     = "aswmmsad.example.com"
  password = "AppmeshShieldAndWorkspacesMsadPass!"
  edition  = "Standard"
  type     = "MicrosoftAD"

  vpc_settings {
    vpc_id     = aws_vpc.aswm.id
    subnet_ids = [aws_subnet.aswm.id, aws_subnet.aswm_b.id]
  }

  timeouts {
    delete = "90s"
  }
}

resource "aws_directory_service_trust" "aswm" {
  directory_id       = aws_directory_service_directory.aswm_msad.id
  remote_domain_name = "remote-aswm.example.com"
  trust_password     = "AppmeshShieldAndWorkspacesTrustPass!"
  trust_direction    = "One-Way: Outgoing"
}
