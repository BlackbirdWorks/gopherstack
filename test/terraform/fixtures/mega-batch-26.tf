resource "aws_iam_role" "sagemaker" {
  name = "mega-batch-26-sagemaker-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "sagemaker.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_vpc" "sagemaker" {
  cidr_block = "10.130.0.0/16"

  tags = {
    Name = "mega-batch-26-sagemaker-vpc"
  }
}

resource "aws_subnet" "sagemaker" {
  vpc_id     = aws_vpc.sagemaker.id
  cidr_block = "10.130.1.0/24"

  tags = {
    Name = "mega-batch-26-sagemaker-subnet"
  }
}

resource "aws_security_group" "sagemaker" {
  name   = "mega-batch-26-sagemaker-sg"
  vpc_id = aws_vpc.sagemaker.id
}

resource "aws_s3_bucket" "sagemaker" {
  bucket = "mega-batch-26-sagemaker-bucket"
}

resource "aws_ecr_repository" "sagemaker" {
  name = "mega-batch-26-sagemaker-repo"
}

resource "aws_sagemaker_code_repository" "example" {
  code_repository_name = "mega-batch-26-code-repo"

  git_config {
    repository_url = "https://github.com/example/mega-batch-26-repo.git"
  }
}

resource "aws_sagemaker_notebook_instance_lifecycle_configuration" "example" {
  name      = "mega-batch-26-nb-lifecycle"
  on_create = base64encode("echo create")
  on_start  = base64encode("echo start")
}

resource "aws_sagemaker_notebook_instance" "example" {
  name                   = "mega-batch-26-notebook"
  role_arn               = aws_iam_role.sagemaker.arn
  instance_type          = "ml.t2.medium"
  lifecycle_config_name  = aws_sagemaker_notebook_instance_lifecycle_configuration.example.name
}

resource "aws_sagemaker_app_image_config" "example" {
  app_image_config_name = "mega-batch-26-app-image-config"

  kernel_gateway_image_config {
    kernel_spec {
      name = "python3"
    }
  }
}

resource "aws_sagemaker_studio_lifecycle_config" "example" {
  studio_lifecycle_config_name     = "mega-batch-26-studio-lifecycle"
  studio_lifecycle_config_app_type = "JupyterServer"
  studio_lifecycle_config_content  = base64encode("echo hello")
}

resource "aws_sagemaker_domain" "example" {
  domain_name = "mega-batch-26-domain"
  auth_mode   = "IAM"
  vpc_id      = aws_vpc.sagemaker.id
  subnet_ids  = [aws_subnet.sagemaker.id]

  default_user_settings {
    execution_role = aws_iam_role.sagemaker.arn
  }
}

resource "aws_sagemaker_user_profile" "example" {
  domain_id         = aws_sagemaker_domain.example.id
  user_profile_name = "mega-batch-26-user-profile"

  user_settings {
    execution_role = aws_iam_role.sagemaker.arn
  }
}

resource "aws_sagemaker_space" "example" {
  domain_id  = aws_sagemaker_domain.example.id
  space_name = "mega-batch-26-space"
}

resource "aws_sagemaker_app" "example" {
  domain_id         = aws_sagemaker_domain.example.id
  user_profile_name = aws_sagemaker_user_profile.example.user_profile_name
  app_name          = "mega-batch-26-app"
  app_type          = "JupyterServer"
}

resource "aws_sagemaker_image" "example" {
  image_name = "mega-batch-26-image"
  role_arn   = aws_iam_role.sagemaker.arn
}

resource "aws_sagemaker_image_version" "example" {
  image_name = aws_sagemaker_image.example.id
  base_image = "${aws_ecr_repository.sagemaker.repository_url}:latest"
}

resource "aws_sagemaker_model_package_group" "example" {
  model_package_group_name = "mega-batch-26-model-package-group"
}

resource "aws_sagemaker_model_package_group_policy" "example" {
  model_package_group_name = aws_sagemaker_model_package_group.example.model_package_group_name

  resource_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AddPermModelPackageGroup"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::000000000000:root" }
      Action    = ["sagemaker:DescribeModelPackage", "sagemaker:ListModelPackages"]
      Resource  = "*"
    }]
  })
}

resource "aws_sagemaker_workforce" "example" {
  workforce_name = "mega-batch-26-workforce"

  cognito_config {
    client_id = "abcd1234abcd1234abcd1234ab"
    user_pool = "us-east-1_abcdefghi"
  }
}

resource "aws_sagemaker_workteam" "example" {
  workteam_name  = "mega-batch-26-workteam"
  workforce_name = aws_sagemaker_workforce.example.id
  description    = "mega-batch-26 workteam"

  member_definition {
    cognito_member_definition {
      client_id  = "abcd1234abcd1234abcd1234ab"
      user_pool  = "us-east-1_abcdefghi"
      user_group = "mega-batch-26-group"
    }
  }
}

resource "aws_sagemaker_human_task_ui" "example" {
  human_task_ui_name = "mega-batch-26-human-task-ui"

  ui_template {
    content = <<EOF
<html><body><crowd-form></crowd-form></body></html>
EOF
  }
}

resource "aws_sagemaker_flow_definition" "example" {
  flow_definition_name = "mega-batch-26-flow-definition"
  role_arn              = aws_iam_role.sagemaker.arn

  human_loop_config {
    human_task_ui_arn                     = aws_sagemaker_human_task_ui.example.arn
    task_availability_lifetime_in_seconds = 3600
    task_count                            = 1
    task_description                      = "mega-batch-26 task"
    task_title                            = "mega-batch-26 title"
    workteam_arn                          = aws_sagemaker_workteam.example.arn
  }

  output_config {
    s3_output_path = "s3://${aws_s3_bucket.sagemaker.bucket}/flow-output/"
  }
}

resource "aws_sagemaker_hub" "example" {
  hub_name          = "mega-batch-26-hub"
  hub_description   = "mega-batch-26 hub"
  hub_display_name  = "Mega Batch 26 Hub"
}

resource "aws_sagemaker_servicecatalog_portfolio_status" "example" {
  status = "Enabled"
}

resource "aws_sagemaker_device_fleet" "example" {
  device_fleet_name = "mega-batch-26-device-fleet"
  role_arn          = aws_iam_role.sagemaker.arn

  output_config {
    s3_output_location = "s3://${aws_s3_bucket.sagemaker.bucket}/device-fleet-output/"
  }
}

resource "aws_sagemaker_device" "example" {
  device_fleet_name = aws_sagemaker_device_fleet.example.device_fleet_name

  device {
    device_name = "mega-batch-26-device"
  }
}

resource "aws_sagemaker_model" "example" {
  name               = "mega-batch-26-model"
  execution_role_arn = aws_iam_role.sagemaker.arn

  primary_container {
    image = "${aws_ecr_repository.sagemaker.repository_url}:latest"
  }
}

resource "aws_sagemaker_endpoint_configuration" "example" {
  name = "mega-batch-26-endpoint-config"

  production_variants {
    variant_name           = "variant-1"
    model_name             = aws_sagemaker_model.example.name
    initial_instance_count = 1
    instance_type          = "ml.t2.medium"
  }
}

resource "aws_sagemaker_endpoint" "example" {
  name                 = "mega-batch-26-endpoint"
  endpoint_config_name = aws_sagemaker_endpoint_configuration.example.name
}

resource "aws_sagemaker_pipeline" "example" {
  pipeline_name         = "mega-batch-26-pipeline"
  pipeline_display_name = "MegaBatch26Pipeline"
  role_arn              = aws_iam_role.sagemaker.arn

  pipeline_definition = jsonencode({
    Version = "2020-12-01"
    Steps   = []
  })
}

resource "aws_sagemaker_project" "example" {
  project_name        = "mega-batch-26-project"
  project_description = "mega-batch-26 project"

  service_catalog_provisioning_details {
    product_id = "prod-abcdefghijklm"
  }
}

resource "aws_sagemaker_feature_group" "example" {
  feature_group_name             = "mega-batch-26-feature-group"
  record_identifier_feature_name = "id"
  event_time_feature_name        = "event_time"
  role_arn                       = aws_iam_role.sagemaker.arn

  feature_definition {
    feature_name = "id"
    feature_type = "String"
  }

  feature_definition {
    feature_name = "event_time"
    feature_type = "String"
  }

  online_store_config {
    enable_online_store = true
  }
}

resource "aws_sagemaker_mlflow_tracking_server" "example" {
  tracking_server_name = "mega-batch-26-mlflow"
  artifact_store_uri   = "s3://${aws_s3_bucket.sagemaker.bucket}/mlflow/"
  role_arn              = aws_iam_role.sagemaker.arn
}

resource "aws_sagemaker_data_quality_job_definition" "example" {
  name     = "mega-batch-26-data-quality-job"
  role_arn = aws_iam_role.sagemaker.arn

  data_quality_app_specification {
    image_uri = "${aws_ecr_repository.sagemaker.repository_url}:latest"
  }

  data_quality_job_input {
    endpoint_input {
      endpoint_name       = aws_sagemaker_endpoint.example.name
      local_path          = "/opt/ml/processing/input"
    }
  }

  data_quality_job_output_config {
    monitoring_outputs {
      s3_output {
        s3_uri = "s3://${aws_s3_bucket.sagemaker.bucket}/data-quality-output/"
      }
    }
  }

  job_resources {
    cluster_config {
      instance_count    = 1
      instance_type     = "ml.t3.medium"
      volume_size_in_gb = 20
    }
  }
}

resource "aws_sagemaker_monitoring_schedule" "example" {
  name = "mega-batch-26-monitoring-schedule"

  monitoring_schedule_config {
    monitoring_job_definition_name = aws_sagemaker_data_quality_job_definition.example.name
    monitoring_type                = "DataQuality"
  }
}
