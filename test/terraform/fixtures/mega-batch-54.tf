# ACM: certificate + validation.

resource "aws_acm_certificate" "mb54" {
  domain_name       = "mega-batch-54.example.com"
  validation_method = "DNS"
}

resource "aws_route53_zone" "mb54" {
  name = "mega-batch-54.example.com"
}

resource "aws_route53_record" "mb54_validation" {
  for_each = {
    for dvo in aws_acm_certificate.mb54.domain_validation_options : dvo.domain_name => {
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
  zone_id         = aws_route53_zone.mb54.zone_id
}

resource "aws_acm_certificate_validation" "mb54" {
  certificate_arn         = aws_acm_certificate.mb54.arn
  validation_record_fqdns = [for r in aws_route53_record.mb54_validation : r.fqdn]
}

# API Gateway: domain name + VPC endpoint + access association.

resource "aws_vpc" "mb54" {
  cidr_block = "10.214.0.0/16"
}

resource "aws_subnet" "mb54" {
  vpc_id            = aws_vpc.mb54.id
  cidr_block        = "10.214.1.0/24"
  availability_zone = "us-east-1a"
}

resource "aws_security_group" "mb54_vpce" {
  vpc_id = aws_vpc.mb54.id
}

resource "aws_vpc_endpoint" "mb54" {
  vpc_id             = aws_vpc.mb54.id
  service_name       = "com.amazonaws.us-east-1.execute-api"
  vpc_endpoint_type  = "Interface"
  subnet_ids         = [aws_subnet.mb54.id]
  security_group_ids = [aws_security_group.mb54_vpce.id]
}

resource "aws_api_gateway_domain_name" "mb54" {
  domain_name              = "mb54-api.example.com"
  regional_certificate_arn = aws_acm_certificate_validation.mb54.certificate_arn

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_api_gateway_domain_name_access_association" "mb54" {
  access_association_source      = aws_vpc_endpoint.mb54.id
  access_association_source_type = "VPCE"
  domain_name_arn                = aws_api_gateway_domain_name.mb54.arn
}

# App Mesh: virtual gateway + gateway route.

resource "aws_appmesh_mesh" "mb54" {
  name = "mega-batch-54-mesh"
}

resource "aws_appmesh_virtual_gateway" "mb54" {
  name      = "mega-batch-54-vgw"
  mesh_name = aws_appmesh_mesh.mb54.id

  spec {
    listener {
      port_mapping {
        port     = 8080
        protocol = "http"
      }
    }
  }
}

resource "aws_appmesh_virtual_service" "mb54" {
  name      = "mega-batch-54.svc.local"
  mesh_name = aws_appmesh_mesh.mb54.id

  spec {}
}

resource "aws_appmesh_gateway_route" "mb54" {
  name                 = "mega-batch-54-gw-route"
  mesh_name            = aws_appmesh_mesh.mb54.id
  virtual_gateway_name = aws_appmesh_virtual_gateway.mb54.name

  spec {
    http_route {
      action {
        target {
          virtual_service {
            virtual_service_name = aws_appmesh_virtual_service.mb54.name
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

resource "aws_cloudfront_key_value_store" "mb54" {
  name    = "mega-batch-54-kvs"
  comment = "mega-batch-54 keyvaluestore"
}

resource "aws_cloudfrontkeyvaluestore_keys_exclusive" "mb54" {
  key_value_store_arn = aws_cloudfront_key_value_store.mb54.arn

  resource_key_value_pair {
    key   = "mb54-key-a"
    value = "mb54-value-a"
  }

  resource_key_value_pair {
    key   = "mb54-key-b"
    value = "mb54-value-b"
  }
}

# CodePipeline: webhook.

resource "aws_s3_bucket" "mb54_pipeline" {
  bucket = "mega-batch-54-pipeline-artifacts"
}

resource "aws_iam_role" "mb54_pipeline" {
  name = "mega-batch-54-pipeline-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "codepipeline.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_codepipeline" "mb54" {
  name     = "mega-batch-54-pipeline"
  role_arn = aws_iam_role.mb54_pipeline.arn

  artifact_store {
    location = aws_s3_bucket.mb54_pipeline.id
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
        S3Bucket    = aws_s3_bucket.mb54_pipeline.id
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
        BucketName = aws_s3_bucket.mb54_pipeline.id
        Extract    = "true"
      }
    }
  }
}

resource "aws_codepipeline_webhook" "mb54" {
  name            = "mega-batch-54-webhook"
  authentication  = "UNAUTHENTICATED"
  target_action   = "Source"
  target_pipeline = aws_codepipeline.mb54.name

  authentication_configuration {}

  filter {
    json_path    = "$.ref"
    match_equals = "refs/heads/{Branch}"
  }
}

# Cognito Identity Pool: provider principal tag.

resource "aws_cognito_identity_pool" "mb54" {
  identity_pool_name               = "mega_batch_54_pool"
  allow_unauthenticated_identities = true

  supported_login_providers = {
    "graph.facebook.com" = "mb54-fb-app-id"
  }
}

resource "aws_cognito_identity_pool_provider_principal_tag" "mb54" {
  identity_pool_id       = aws_cognito_identity_pool.mb54.id
  identity_provider_name = "graph.facebook.com"
  use_defaults           = true
}

# EC2: IPAM preview next CIDR, network performance metric subscription,
# security group VPC association, spot fleet request.

resource "aws_vpc_ipam" "mb54" {
  operating_regions {
    region_name = "us-east-1"
  }
}

resource "aws_vpc_ipam_pool" "mb54" {
  address_family = "ipv4"
  ipam_scope_id  = aws_vpc_ipam.mb54.private_default_scope_id
  locale         = "us-east-1"
}

resource "aws_vpc_ipam_pool_cidr" "mb54" {
  ipam_pool_id = aws_vpc_ipam_pool.mb54.id
  cidr         = "10.220.0.0/16"
}

resource "aws_vpc_ipam_preview_next_cidr" "mb54" {
  ipam_pool_id   = aws_vpc_ipam_pool.mb54.id
  netmask_length = 24

  depends_on = [aws_vpc_ipam_pool_cidr.mb54]
}

resource "aws_vpc_network_performance_metric_subscription" "mb54" {
  source      = "us-east-1"
  destination = "us-west-2"
  metric      = "aggregate-latency"
  statistic   = "p50"
}

resource "aws_vpc_security_group_vpc_association" "mb54" {
  security_group_id = aws_security_group.mb54_vpce.id
  vpc_id            = aws_vpc.mb54.id
}

# Shield: protection health check association.

resource "aws_eip" "mb54" {
  domain = "vpc"
}

resource "aws_shield_subscription" "mb54" {
  auto_renew = "ENABLED"
}

resource "aws_shield_protection" "mb54" {
  name         = "mega-batch-54-shield-protection"
  resource_arn = "arn:aws:ec2:us-east-1:000000000000:eip-allocation/${aws_eip.mb54.id}"

  depends_on = [aws_shield_subscription.mb54]
}

resource "aws_route53_health_check" "mb54" {
  fqdn              = "mb54-health.example.com"
  port              = 443
  type              = "HTTPS"
  resource_path     = "/"
  failure_threshold = 3
  request_interval  = 30
}

resource "aws_shield_protection_health_check_association" "mb54" {
  health_check_arn     = aws_route53_health_check.mb54.arn
  shield_protection_id = aws_shield_protection.mb54.id
}

# Timestream Query: scheduled query.

resource "aws_iam_role" "mb54_timestream" {
  name = "mega-batch-54-timestream-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "timestream.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_sns_topic" "mb54_timestream" {
  name = "mega-batch-54-timestream-notify"
}

resource "aws_s3_bucket" "mb54_timestream" {
  bucket = "mega-batch-54-timestream-errors"
}

resource "aws_timestreamwrite_database" "mb54" {
  database_name = "mega_batch_54_db"
}

resource "aws_timestreamwrite_table" "mb54" {
  database_name = aws_timestreamwrite_database.mb54.database_name
  table_name    = "mega_batch_54_table"
}

resource "aws_timestreamquery_scheduled_query" "mb54" {
  name               = "mega-batch-54-scheduled-query"
  query_string       = "SELECT 1"
  execution_role_arn = aws_iam_role.mb54_timestream.arn

  schedule_configuration {
    schedule_expression = "rate(1 hour)"
  }

  notification_configuration {
    sns_configuration {
      topic_arn = aws_sns_topic.mb54_timestream.arn
    }
  }

  target_configuration {
    timestream_configuration {
      database_name = aws_timestreamwrite_database.mb54.database_name
      table_name    = aws_timestreamwrite_table.mb54.table_name

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
      bucket_name = aws_s3_bucket.mb54_timestream.id
    }
  }
}

# WorkSpaces: directory + workspace.

resource "aws_directory_service_directory" "mb54" {
  name     = "mb54.example.com"
  password = "MegaBatch54Pass!"
  size     = "Small"
  type     = "SimpleAD"

  vpc_settings {
    vpc_id     = aws_vpc.mb54.id
    subnet_ids = [aws_subnet.mb54.id, aws_subnet.mb54_b.id]
  }

  # This emulator deletes directories synchronously; the provider's own
  # delete waiter still has a long fixed poll cadence, so cap it short.
  timeouts {
    delete = "90s"
  }
}

resource "aws_subnet" "mb54_b" {
  vpc_id            = aws_vpc.mb54.id
  cidr_block        = "10.214.2.0/24"
  availability_zone = "us-east-1b"
}

resource "aws_workspaces_directory" "mb54" {
  directory_id = aws_directory_service_directory.mb54.id

  workspace_creation_properties {
    enable_internet_access = false
  }
}

resource "aws_workspaces_workspace" "mb54" {
  directory_id = aws_workspaces_directory.mb54.id
  bundle_id    = data.aws_workspaces_bundle.mb54.id
  user_name    = "Administrator"

  workspace_properties {
    compute_type_name                         = "STANDARD"
    root_volume_size_gib                      = 80
    user_volume_size_gib                      = 50
    running_mode                              = "AUTO_STOP"
    running_mode_auto_stop_timeout_in_minutes = 60
  }
}

data "aws_workspaces_bundle" "mb54" {
  bundle_id = "wsb-bh8rsxt14"
}

# RDS: custom DB engine version.

resource "aws_s3_bucket" "mb54_rds" {
  bucket = "mega-batch-54-rds-custom-media"
}

resource "aws_s3_object" "mb54_rds" {
  bucket  = aws_s3_bucket.mb54_rds.id
  key     = "mb54-oracle-media.zip"
  content = "mega-batch-54 fake install media"
}

resource "aws_rds_custom_db_engine_version" "mb54" {
  engine         = "custom-oracle-ee"
  engine_version = "19.mb54.1"
  manifest = jsonencode({
    mediaImportTemplateVersion = "2020-08-14"
  })

  database_installation_files_s3_bucket_name = aws_s3_bucket.mb54_rds.id
  database_installation_files_s3_prefix      = "mb54-media"

  depends_on = [aws_s3_object.mb54_rds]
}

# Kinesis Analytics v2: application snapshot.

resource "aws_iam_role" "mb54_kda" {
  name = "mega-batch-54-kda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "kinesisanalytics.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_kinesisanalyticsv2_application" "mb54" {
  name                   = "mega-batch-54-kda-app"
  runtime_environment    = "SQL-1_0"
  service_execution_role = aws_iam_role.mb54_kda.arn
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

resource "aws_kinesisanalyticsv2_application_snapshot" "mb54" {
  application_name = aws_kinesisanalyticsv2_application.mb54.name
  snapshot_name    = "mega-batch-54-snapshot"
}

# Directory Service: trust.

resource "aws_directory_service_directory" "mb54_msad" {
  name     = "mb54msad.example.com"
  password = "MegaBatch54MsadPass!"
  edition  = "Standard"
  type     = "MicrosoftAD"

  vpc_settings {
    vpc_id     = aws_vpc.mb54.id
    subnet_ids = [aws_subnet.mb54.id, aws_subnet.mb54_b.id]
  }

  timeouts {
    delete = "90s"
  }
}

resource "aws_directory_service_trust" "mb54" {
  directory_id       = aws_directory_service_directory.mb54_msad.id
  remote_domain_name = "remote-mb54.example.com"
  trust_password     = "MegaBatch54TrustPass!"
  trust_direction    = "One-Way: Outgoing"
}
