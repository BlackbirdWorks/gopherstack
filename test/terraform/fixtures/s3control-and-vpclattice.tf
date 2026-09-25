resource "aws_s3control_access_grants_instance" "example" {
  account_id = "000000000000"
}

resource "aws_iam_role" "access_grants" {
  name = "s3vl-access-grants-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "access-grants.s3.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_s3control_access_grants_instance_resource_policy" "example" {
  depends_on = [aws_s3control_access_grants_instance.example]

  account_id = "000000000000"

  policy = jsonencode({
    Version = "2012-10-17"
    Id      = "s3vl-agi-policy"
    Statement = [{
      Sid       = "AllowAccessToS3AccessGrants"
      Effect    = "Allow"
      Principal = { AWS = "000000000000" }
      Action    = ["s3:ListAccessGrants", "s3:GetDataAccess"]
      Resource  = aws_s3control_access_grants_instance.example.access_grants_instance_arn
    }]
  })
}

resource "aws_s3_bucket" "grants" {
  bucket = "s3vl-grants-bucket"
}

resource "aws_s3control_access_grants_location" "example" {
  depends_on = [aws_s3control_access_grants_instance.example]

  account_id     = "000000000000"
  iam_role_arn   = aws_iam_role.access_grants.arn
  location_scope = "s3://${aws_s3_bucket.grants.bucket}/prefixA*"
}

resource "aws_s3control_access_grant" "example" {
  access_grants_location_id = aws_s3control_access_grants_location.example.access_grants_location_id
  account_id                = "000000000000"
  permission                = "READ"

  access_grants_location_configuration {
    s3_sub_prefix = "prefixB*"
  }

  grantee {
    grantee_type       = "IAM"
    grantee_identifier = aws_iam_role.access_grants.arn
  }
}

resource "aws_s3_bucket" "ap" {
  bucket = "s3vl-ap-bucket"
}

resource "aws_s3_access_point" "example" {
  bucket     = aws_s3_bucket.ap.id
  name       = "s3vl-ap"
  account_id = "000000000000"

  lifecycle {
    ignore_changes = [policy]
  }
}

resource "aws_s3control_access_point_policy" "example" {
  access_point_arn = aws_s3_access_point.example.arn

  policy = jsonencode({
    Version = "2008-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "s3:GetObjectTagging"
      Principal = { AWS = "*" }
      Resource  = "${aws_s3_access_point.example.arn}/object/*"
    }]
  })
}

resource "aws_s3_bucket" "mrap_a" {
  bucket = "s3vl-mrap-a"
}

resource "aws_s3_bucket" "mrap_b" {
  bucket = "s3vl-mrap-b"
}

resource "aws_s3control_multi_region_access_point" "example" {
  account_id = "000000000000"

  details {
    name = "s3vl-mrap"

    region {
      bucket = aws_s3_bucket.mrap_a.id
    }

    region {
      bucket = aws_s3_bucket.mrap_b.id
    }
  }
}

resource "aws_s3control_multi_region_access_point_policy" "example" {
  account_id = "000000000000"

  details {
    name = element(split(":", aws_s3control_multi_region_access_point.example.id), 1)

    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [{
        Sid       = "S3controlAndVpclatticeMRAPPolicy"
        Effect    = "Allow"
        Principal = { AWS = "000000000000" }
        Action    = ["s3:GetObject", "s3:PutObject"]
        Resource  = "arn:aws:s3::000000000000:accesspoint/${aws_s3control_multi_region_access_point.example.alias}/object/*"
      }]
    })
  }
}

resource "aws_s3_bucket" "ol" {
  bucket = "s3vl-ol-bucket"
}

resource "aws_s3_access_point" "ol" {
  bucket     = aws_s3_bucket.ol.id
  name       = "s3vl-ol-ap"
  account_id = "000000000000"

  lifecycle {
    ignore_changes = [policy]
  }
}

resource "aws_iam_role" "lambda" {
  name = "s3vl-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_lambda_function" "ol" {
  filename         = "{{.FunctionZip}}"
  function_name    = "s3vl-ol-function"
  role             = aws_iam_role.lambda.arn
  handler          = "index.handler"
  runtime          = "python3.12"
  source_code_hash = filebase64sha256("{{.FunctionZip}}")
}

resource "aws_s3control_object_lambda_access_point" "example" {
  name       = "s3vl-ol"
  account_id = "000000000000"

  configuration {
    supporting_access_point = aws_s3_access_point.ol.arn

    transformation_configuration {
      actions = ["GetObject"]

      content_transformation {
        aws_lambda {
          function_arn = aws_lambda_function.ol.arn
        }
      }
    }
  }
}

resource "aws_s3control_object_lambda_access_point_policy" "example" {
  name       = aws_s3control_object_lambda_access_point.example.name
  account_id = "000000000000"

  policy = jsonencode({
    Version = "2008-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "s3-object-lambda:GetObject"
      Principal = { AWS = "000000000000" }
      Resource  = aws_s3control_object_lambda_access_point.example.arn
    }]
  })
}

resource "aws_s3_bucket" "lens_target" {
  bucket = "s3vl-lens-target"
}

resource "aws_s3control_storage_lens_configuration" "example" {
  account_id = "000000000000"
  config_id  = "s3vl-lens"

  storage_lens_configuration {
    enabled = true

    account_level {
      activity_metrics {
        enabled = true
      }

      bucket_level {
        activity_metrics {
          enabled = true
        }
      }
    }
  }
}

resource "aws_vpc" "lattice" {
  cidr_block = "10.121.0.0/16"

  tags = {
    Name = "s3vl-lattice-vpc"
  }
}

resource "aws_subnet" "lattice" {
  vpc_id     = aws_vpc.lattice.id
  cidr_block = "10.121.1.0/24"

  tags = {
    Name = "s3vl-lattice-subnet"
  }
}

resource "aws_security_group" "lattice" {
  name   = "s3vl-lattice-sg"
  vpc_id = aws_vpc.lattice.id
}

resource "aws_vpclattice_service_network" "example" {
  name = "s3vl-service-network"
}

resource "aws_vpclattice_service" "example" {
  name      = "s3vl-service"
  auth_type = "AWS_IAM"
}

resource "aws_vpclattice_target_group" "example" {
  name = "s3vl-target-group"
  type = "IP"

  config {
    vpc_identifier = aws_vpc.lattice.id
    port           = 80
    protocol       = "HTTP"
  }
}

resource "aws_vpclattice_target_group_attachment" "example" {
  target_group_identifier = aws_vpclattice_target_group.example.id

  target {
    id   = "10.121.1.10"
    port = 80
  }
}

resource "aws_vpclattice_listener" "example" {
  name               = "s3vl-listener"
  protocol           = "HTTP"
  service_identifier = aws_vpclattice_service.example.id

  default_action {
    forward {
      target_groups {
        target_group_identifier = aws_vpclattice_target_group.example.id
      }
    }
  }
}

resource "aws_vpclattice_listener_rule" "example" {
  name                = "s3vl-listener-rule"
  listener_identifier = aws_vpclattice_listener.example.listener_id
  service_identifier  = aws_vpclattice_service.example.id
  priority            = 10

  match {
    http_match {
      path_match {
        case_sensitive = false

        match {
          exact = "/s3vl"
        }
      }
    }
  }

  action {
    fixed_response {
      status_code = 404
    }
  }
}

resource "aws_vpclattice_auth_policy" "example" {
  resource_identifier = aws_vpclattice_service.example.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "*"
      Effect    = "Allow"
      Principal = "*"
      Resource  = "*"
      Condition = {
        StringNotEqualsIgnoreCase = {
          "aws:PrincipalType" = "anonymous"
        }
      }
    }]
  })
}

resource "aws_vpclattice_resource_policy" "example" {
  resource_arn = aws_vpclattice_service_network.example.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "s3vl-resource-policy"
      Effect = "Allow"
      Principal = {
        AWS = "arn:aws:iam::000000000000:root"
      }
      Action = [
        "vpc-lattice:CreateServiceNetworkVpcAssociation",
        "vpc-lattice:GetServiceNetwork"
      ]
      Resource = aws_vpclattice_service_network.example.arn
    }]
  })
}

resource "aws_s3_bucket" "access_logs" {
  bucket = "s3vl-access-logs"
}

resource "aws_vpclattice_access_log_subscription" "example" {
  resource_identifier = aws_vpclattice_service_network.example.id
  destination_arn     = aws_s3_bucket.access_logs.arn
}

resource "aws_vpclattice_service_network_vpc_association" "example" {
  vpc_identifier             = aws_vpc.lattice.id
  service_network_identifier = aws_vpclattice_service_network.example.id
  security_group_ids         = [aws_security_group.lattice.id]
}

resource "aws_vpclattice_service_network_service_association" "example" {
  service_identifier         = aws_vpclattice_service.example.id
  service_network_identifier = aws_vpclattice_service_network.example.id
}

resource "aws_vpclattice_resource_gateway" "example" {
  name       = "s3vl-resource-gateway"
  vpc_id     = aws_vpc.lattice.id
  subnet_ids = [aws_subnet.lattice.id]
}

resource "aws_vpclattice_resource_configuration" "example" {
  name = "s3vl-resource-configuration"

  resource_gateway_identifier = aws_vpclattice_resource_gateway.example.id

  port_ranges = ["80"]
  protocol    = "TCP"

  resource_configuration_definition {
    ip_resource {
      ip_address = "10.121.1.20"
    }
  }
}

resource "aws_vpclattice_service_network_resource_association" "example" {
  resource_configuration_identifier = aws_vpclattice_resource_configuration.example.id
  service_network_identifier        = aws_vpclattice_service_network.example.id
}
