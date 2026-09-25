# A second, aliased provider in a different region, pointed at the same
# emulator endpoint, for the KMS multi-region replica resources below.
provider "aws" {
  alias                       = "replica"
  region                      = "us-west-2"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  s3_use_path_style           = true

  endpoints {
    kms = "{{.Endpoint}}"
    sts = "{{.Endpoint}}"
    iam = "{{.Endpoint}}"
  }
}

# --- Auto Scaling ----------------------------------------------------------

resource "aws_vpc" "example" {
  cidr_block = "{{.VPCCidr}}"

  tags = {
    Name = "adkc-vpc"
  }
}

resource "aws_subnet" "a" {
  vpc_id            = aws_vpc.example.id
  cidr_block        = "{{.SubnetCidrA}}"
  availability_zone = "us-east-1a"

  tags = {
    Name = "adkc-subnet-a"
  }
}

resource "aws_subnet" "b" {
  vpc_id            = aws_vpc.example.id
  cidr_block        = "{{.SubnetCidrB}}"
  availability_zone = "us-east-1b"

  tags = {
    Name = "adkc-subnet-b"
  }
}

resource "aws_launch_template" "example" {
  name          = "adkc-lt"
  image_id      = "ami-12345678"
  instance_type = "t2.micro"
}

resource "aws_autoscaling_group" "example" {
  name                = "adkc-asg"
  min_size            = 1
  max_size            = 2
  desired_capacity    = 1
  vpc_zone_identifier = [aws_subnet.a.id, aws_subnet.b.id]

  launch_template {
    id      = aws_launch_template.example.id
    version = "$Latest"
  }
}

resource "aws_lb" "nlb" {
  name               = "adkc-nlb"
  internal           = true
  load_balancer_type = "network"
  subnets            = [aws_subnet.a.id, aws_subnet.b.id]
}

resource "aws_lb_target_group" "attach" {
  name     = "adkc-tg-attach"
  port     = 80
  protocol = "TCP"
  vpc_id   = aws_vpc.example.id
}

resource "aws_lb_target_group" "traffic" {
  name     = "adkc-tg-traffic"
  port     = 80
  protocol = "TCP"
  vpc_id   = aws_vpc.example.id
}

resource "aws_autoscaling_attachment" "example" {
  autoscaling_group_name = aws_autoscaling_group.example.name
  lb_target_group_arn    = aws_lb_target_group.attach.arn
}

resource "aws_autoscaling_traffic_source_attachment" "example" {
  autoscaling_group_name = aws_autoscaling_group.example.name

  traffic_source {
    identifier = aws_lb_target_group.traffic.arn
    type       = "elbv2"
  }
}

resource "aws_autoscaling_group_tag" "example" {
  autoscaling_group_name = aws_autoscaling_group.example.name

  tag {
    key                 = "adkc-tag"
    value               = "true"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_lifecycle_hook" "example" {
  name                   = "adkc-hook"
  autoscaling_group_name = aws_autoscaling_group.example.name
  default_result         = "CONTINUE"
  heartbeat_timeout      = 60
  lifecycle_transition   = "autoscaling:EC2_INSTANCE_LAUNCHING"
}

resource "aws_sns_topic" "asg_notify" {
  name = "adkc-asg-topic"
}

resource "aws_autoscaling_notification" "example" {
  group_names = [aws_autoscaling_group.example.name]
  notifications = [
    "autoscaling:EC2_INSTANCE_LAUNCH",
    "autoscaling:EC2_INSTANCE_TERMINATE",
  ]
  topic_arn = aws_sns_topic.asg_notify.arn
}

resource "aws_autoscaling_policy" "example" {
  name                   = "adkc-policy"
  autoscaling_group_name = aws_autoscaling_group.example.name
  adjustment_type        = "ChangeInCapacity"
  scaling_adjustment     = 1
  cooldown               = 300
}

resource "aws_autoscaling_schedule" "example" {
  scheduled_action_name  = "adkc-schedule"
  autoscaling_group_name = aws_autoscaling_group.example.name
  min_size               = 1
  max_size               = 3
  desired_capacity       = 2
  start_time             = "2030-01-01T00:00:00Z"
}

# --- DynamoDB ---------------------------------------------------------------

resource "aws_dynamodb_table" "example" {
  name             = "adkc-table"
  billing_mode     = "PAY_PER_REQUEST"
  hash_key         = "id"
  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  attribute {
    name = "id"
    type = "S"
  }
}

resource "aws_dynamodb_contributor_insights" "example" {
  table_name = aws_dynamodb_table.example.name
}

resource "aws_kinesis_stream" "example" {
  name             = "adkc-stream"
  shard_count      = 1
  retention_period = 24
}

resource "aws_dynamodb_kinesis_streaming_destination" "example" {
  table_name = aws_dynamodb_table.example.name
  stream_arn = aws_kinesis_stream.example.arn
}

resource "aws_dynamodb_resource_policy" "example" {
  resource_arn = aws_dynamodb_table.example.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "adkc-resource-policy"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::000000000000:root" }
      Action    = "dynamodb:DescribeTable"
      Resource  = "*"
    }]
  })
}

resource "aws_dynamodb_table_item" "example" {
  table_name = aws_dynamodb_table.example.name
  hash_key   = aws_dynamodb_table.example.hash_key

  item = jsonencode({
    id   = { S = "adkc-item" }
    name = { S = "widget" }
  })
}

resource "aws_dynamodb_tag" "example" {
  resource_arn = aws_dynamodb_table.example.arn
  key          = "adkc-tag"
  value        = "true"
}

resource "aws_s3_bucket" "export" {
  bucket        = "adkc-export-bucket"
  force_destroy = true
}

resource "aws_dynamodb_table_export" "example" {
  s3_bucket = aws_s3_bucket.export.id
  table_arn = aws_dynamodb_table.example.arn
}

# --- KMS ---------------------------------------------------------------------

resource "aws_kms_key" "example" {
  description = "adkc kms key"
}

resource "aws_kms_ciphertext" "example" {
  key_id    = aws_kms_key.example.key_id
  plaintext = "adkc-secret"
}

resource "aws_kms_grant" "example" {
  name              = "adkc-grant"
  key_id            = aws_kms_key.example.key_id
  grantee_principal = "arn:aws:iam::000000000000:role/adkc-grantee"
  operations        = ["Encrypt", "Decrypt"]
}

resource "aws_kms_key_policy" "example" {
  key_id = aws_kms_key.example.key_id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "adkc-key-policy"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::000000000000:root" }
      Action    = "kms:*"
      Resource  = "*"
    }]
  })
}

resource "aws_kms_external_key" "example" {
  description             = "adkc external key"
  deletion_window_in_days = 7
}

resource "aws_kms_custom_key_store" "example" {
  custom_key_store_name    = "adkc-cks"
  cloud_hsm_cluster_id     = "cluster-adkc"
  key_store_password       = "adkcPassword"
  trust_anchor_certificate = "-----BEGIN CERTIFICATE-----\nMIIBogIBAAKC\n-----END CERTIFICATE-----"
}

resource "aws_kms_key" "replica_primary" {
  description  = "adkc replica primary key"
  multi_region = true
}

resource "aws_kms_replica_key" "example" {
  provider        = aws.replica
  primary_key_arn = aws_kms_key.replica_primary.arn
  description     = "adkc replica key"
}

resource "aws_kms_external_key" "replica_primary" {
  description         = "adkc replica primary external key"
  multi_region        = true
  key_material_base64 = base64encode("0123456789abcdef0123456789abcdef")
  enabled             = true
}

resource "aws_kms_replica_external_key" "example" {
  provider            = aws.replica
  primary_key_arn     = aws_kms_external_key.replica_primary.arn
  description         = "adkc replica external key"
  key_material_base64 = base64encode("0123456789abcdef0123456789abcdef")
  enabled             = true
}

# --- CloudWatch ----------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "leaf_a" {
  alarm_name          = "adkc-leaf-a"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 60
  statistic           = "Average"
  threshold           = 80
}

resource "aws_cloudwatch_metric_alarm" "leaf_b" {
  alarm_name          = "adkc-leaf-b"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "MemoryUtilization"
  namespace           = "AWS/EC2"
  period              = 60
  statistic           = "Average"
  threshold           = 90
}

resource "aws_cloudwatch_composite_alarm" "example" {
  alarm_name = "adkc-composite-alarm"
  alarm_rule = join(" OR ", [
    "ALARM(\"${aws_cloudwatch_metric_alarm.leaf_a.alarm_name}\")",
    "ALARM(\"${aws_cloudwatch_metric_alarm.leaf_b.alarm_name}\")",
  ])
}

resource "aws_cloudwatch_contributor_insight_rule" "example" {
  rule_name  = "adkc-insight-rule"
  rule_state = "ENABLED"

  rule_definition = jsonencode({
    Schema = {
      Name    = "CloudWatchLogRule"
      Version = 1
    }
    LogGroupNames = ["adkc-log-group"]
    LogFormat     = "JSON"
    Contribution = {
      Keys = ["$.ip"]
    }
    AggregateOn = "Count"
  })
}

resource "aws_cloudwatch_contributor_managed_insight_rule" "example" {
  resource_arn  = aws_dynamodb_table.example.arn
  template_name = "DynamoDBContributorInsights"
  state         = "ENABLED"
}

resource "aws_iam_role" "firehose" {
  name = "adkc-firehose-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "firehose.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_s3_bucket" "firehose" {
  bucket        = "adkc-firehose-bucket"
  force_destroy = true
}

resource "aws_kinesis_firehose_delivery_stream" "example" {
  name        = "adkc-metric-stream-dest"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = aws_s3_bucket.firehose.arn
  }
}

resource "aws_iam_role" "metric_stream" {
  name = "adkc-metric-stream-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "streams.metrics.cloudwatch.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_cloudwatch_metric_stream" "example" {
  name          = "adkc-metric-stream"
  role_arn      = aws_iam_role.metric_stream.arn
  firehose_arn  = aws_kinesis_firehose_delivery_stream.example.arn
  output_format = "json"

  include_filter {
    namespace = "AWS/EC2"
  }
}
