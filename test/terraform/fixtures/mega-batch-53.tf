##############################################################################
# EC2: spot datafeed subscription, network interface permission, EBS fast
# snapshot restore, EBS snapshot import (from S3), and an AMI created from a
# running instance.
##############################################################################

resource "aws_s3_bucket" "mb53_datafeed" {
  bucket = "mega-batch-53-datafeed"
}

resource "aws_spot_datafeed_subscription" "mb53" {
  bucket = aws_s3_bucket.mb53_datafeed.id
}

resource "aws_vpc" "mb53_ec2" {
  cidr_block = "10.213.0.0/16"
}

resource "aws_subnet" "mb53_ec2" {
  vpc_id            = aws_vpc.mb53_ec2.id
  cidr_block        = "10.213.1.0/24"
  availability_zone = "us-east-1a"
}

resource "aws_instance" "mb53" {
  ami           = "ami-12345678"
  instance_type = "t2.micro"
  subnet_id     = aws_subnet.mb53_ec2.id

  tags = {
    Name = "mega-batch-53-instance"
  }
}

resource "aws_network_interface" "mb53" {
  subnet_id = aws_subnet.mb53_ec2.id
}

resource "aws_network_interface_permission" "mb53" {
  network_interface_id = aws_network_interface.mb53.id
  aws_account_id       = "000000000000"
  permission           = "INSTANCE-ATTACH"
}

resource "aws_ebs_volume" "mb53" {
  availability_zone = "us-east-1a"
  size              = 1
}

resource "aws_ebs_snapshot" "mb53" {
  volume_id = aws_ebs_volume.mb53.id
}

resource "aws_ebs_fast_snapshot_restore" "mb53" {
  availability_zone = "us-east-1a"
  snapshot_id       = aws_ebs_snapshot.mb53.id
}

resource "aws_ebs_snapshot_import" "mb53" {
  description = "mega-batch-53 imported snapshot"

  disk_container {
    format      = "VHD"
    description = "mega-batch-53 disk"

    user_bucket {
      s3_bucket = aws_s3_bucket.mb53_datafeed.id
      s3_key    = "mega-batch-53-disk.vhd"
    }
  }
}

resource "aws_ami_from_instance" "mb53" {
  name               = "mega-batch-53-ami"
  source_instance_id = aws_instance.mb53.id
}

##############################################################################
# EC2: VPC Route Server family (route server, its VPC association, an
# endpoint, a BGP peer on that endpoint, and route table propagation).
##############################################################################

resource "aws_vpc_route_server" "mb53" {
  amazon_side_asn = 4210000001
}

resource "aws_vpc_route_server_vpc_association" "mb53" {
  route_server_id = aws_vpc_route_server.mb53.route_server_id
  vpc_id          = aws_vpc.mb53_ec2.id
}

resource "aws_vpc_route_server_endpoint" "mb53" {
  route_server_id = aws_vpc_route_server.mb53.route_server_id
  subnet_id       = aws_subnet.mb53_ec2.id

  depends_on = [aws_vpc_route_server_vpc_association.mb53]
}

resource "aws_vpc_route_server_peer" "mb53" {
  route_server_endpoint_id = aws_vpc_route_server_endpoint.mb53.route_server_endpoint_id
  peer_address             = "10.213.1.100"

  bgp_options {
    peer_asn = 4210000002
  }
}

resource "aws_route_table" "mb53" {
  vpc_id = aws_vpc.mb53_ec2.id
}

resource "aws_vpc_route_server_propagation" "mb53" {
  route_server_id = aws_vpc_route_server.mb53.route_server_id
  route_table_id  = aws_route_table.mb53.id

  depends_on = [aws_vpc_route_server_vpc_association.mb53]
}

##############################################################################
# EC2: Transit Gateway VPC attachment accepter, multicast domain association
# plus group member/source, policy table association, and a managed prefix
# list reference on the TGW's own route table.
##############################################################################

resource "aws_ec2_transit_gateway" "mb53" {
  description = "mega-batch-53-tgw"
}

resource "aws_ec2_transit_gateway_vpc_attachment" "mb53" {
  transit_gateway_id = aws_ec2_transit_gateway.mb53.id
  vpc_id             = aws_vpc.mb53_ec2.id
  subnet_ids         = [aws_subnet.mb53_ec2.id]
}

resource "aws_ec2_transit_gateway_vpc_attachment_accepter" "mb53" {
  transit_gateway_attachment_id = aws_ec2_transit_gateway_vpc_attachment.mb53.id
}

resource "aws_ec2_transit_gateway_multicast_domain" "mb53" {
  transit_gateway_id = aws_ec2_transit_gateway.mb53.id
}

resource "aws_ec2_transit_gateway_multicast_domain_association" "mb53" {
  subnet_id                           = aws_subnet.mb53_ec2.id
  transit_gateway_attachment_id       = aws_ec2_transit_gateway_vpc_attachment.mb53.id
  transit_gateway_multicast_domain_id = aws_ec2_transit_gateway_multicast_domain.mb53.id
}

resource "aws_network_interface" "mb53_mcast" {
  subnet_id = aws_subnet.mb53_ec2.id
}

resource "aws_ec2_transit_gateway_multicast_group_member" "mb53" {
  group_ip_address                    = "224.0.1.1"
  network_interface_id                = aws_network_interface.mb53_mcast.id
  transit_gateway_multicast_domain_id = aws_ec2_transit_gateway_multicast_domain.mb53.id

  depends_on = [aws_ec2_transit_gateway_multicast_domain_association.mb53]
}

resource "aws_ec2_transit_gateway_multicast_group_source" "mb53" {
  group_ip_address                    = "224.0.1.1"
  network_interface_id                = aws_network_interface.mb53_mcast.id
  transit_gateway_multicast_domain_id = aws_ec2_transit_gateway_multicast_domain.mb53.id

  depends_on = [aws_ec2_transit_gateway_multicast_domain_association.mb53]
}

resource "aws_ec2_transit_gateway_policy_table" "mb53" {
  transit_gateway_id = aws_ec2_transit_gateway.mb53.id
}

resource "aws_ec2_transit_gateway_policy_table_association" "mb53" {
  transit_gateway_attachment_id   = aws_ec2_transit_gateway_vpc_attachment.mb53.id
  transit_gateway_policy_table_id = aws_ec2_transit_gateway_policy_table.mb53.id
}

resource "aws_ec2_managed_prefix_list" "mb53" {
  name           = "mega-batch-53-pl"
  address_family = "IPv4"
  max_entries    = 5

  entry {
    cidr = "10.213.9.0/24"
  }
}

resource "aws_ec2_transit_gateway_route_table" "mb53" {
  transit_gateway_id = aws_ec2_transit_gateway.mb53.id
}

resource "aws_ec2_transit_gateway_prefix_list_reference" "mb53" {
  transit_gateway_route_table_id = aws_ec2_transit_gateway_route_table.mb53.id
  prefix_list_id                 = aws_ec2_managed_prefix_list.mb53.id
  transit_gateway_attachment_id  = aws_ec2_transit_gateway_vpc_attachment.mb53.id
}

##############################################################################
# Batch job definition, CodeDeploy application + deployment group, and
# CodeConnections / CodeStarConnections hosts.
##############################################################################

resource "aws_iam_role" "mb53_svc" {
  name = "mega-batch-53-svc-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = [
          "batch.amazonaws.com",
          "codedeploy.amazonaws.com",
          "transcribe.amazonaws.com",
        ]
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_batch_job_definition" "mb53" {
  name = "mega-batch-53-job-def"
  type = "container"

  container_properties = jsonencode({
    image      = "busybox"
    command    = ["echo", "mb53"]
    vcpus      = 1
    memory     = 512
    jobRoleArn = aws_iam_role.mb53_svc.arn
  })
}

resource "aws_codedeploy_app" "mb53" {
  name = "mega-batch-53-app"
}

resource "aws_codedeploy_deployment_group" "mb53" {
  app_name              = aws_codedeploy_app.mb53.name
  deployment_group_name = "mega-batch-53-dg"
  service_role_arn      = aws_iam_role.mb53_svc.arn

  ec2_tag_filter {
    key   = "Name"
    type  = "KEY_AND_VALUE"
    value = "mega-batch-53-instance"
  }
}

resource "aws_codeconnections_host" "mb53" {
  name              = "mega-batch-53-cc-host"
  provider_endpoint = "https://ghe.mega-batch-53.example.com"
  provider_type     = "GitHubEnterpriseServer"
}

resource "aws_codestarconnections_host" "mb53" {
  name              = "mega-batch-53-csc-host"
  provider_endpoint = "https://ghe2.mega-batch-53.example.com"
  provider_type     = "GitHubEnterpriseServer"
}

##############################################################################
# Service Discovery: HTTP namespace + service + registered instance.
##############################################################################

resource "aws_service_discovery_http_namespace" "mb53" {
  name = "mega-batch-53-http-ns"
}

resource "aws_service_discovery_service" "mb53" {
  name = "mega-batch-53-svc"
  type = "HTTP"

  namespace_id = aws_service_discovery_http_namespace.mb53.id
}

resource "aws_service_discovery_instance" "mb53" {
  instance_id = "mega-batch-53-svc-instance"
  service_id  = aws_service_discovery_service.mb53.id

  attributes = {
    AWS_INSTANCE_IPV4 = "10.213.9.9"
  }
}

##############################################################################
# Step Functions: state machine (published) + an alias routed to that
# version.
##############################################################################

resource "aws_sfn_state_machine" "mb53" {
  name     = "mega-batch-53-sm"
  role_arn = aws_iam_role.mb53_svc.arn
  publish  = true

  definition = jsonencode({
    Comment = "mega-batch-53"
    StartAt = "Pass"
    States = {
      Pass = {
        Type = "Pass"
        End  = true
      }
    }
  })
}

resource "aws_sfn_alias" "mb53" {
  name = "mega-batch-53-alias"

  routing_configuration {
    state_machine_version_arn = aws_sfn_state_machine.mb53.state_machine_version_arn
    weight                    = 100
  }
}

##############################################################################
# Transcribe custom language model.
##############################################################################

resource "aws_transcribe_language_model" "mb53" {
  model_name      = "mega-batch-53-lm"
  base_model_name = "NarrowBand"
  language_code   = "en-US"

  input_data_config {
    data_access_role_arn = aws_iam_role.mb53_svc.arn
    s3_uri               = "s3://${aws_s3_bucket.mb53_datafeed.id}/training/"
  }
}

##############################################################################
# WorkSpaces connection alias.
##############################################################################

resource "aws_workspaces_connection_alias" "mb53" {
  connection_string = "mega-batch-53.workspaces.example.com"
}

##############################################################################
# Network Manager: global network + two devices + a connection between them.
##############################################################################

resource "aws_networkmanager_global_network" "mb53" {
  description = "mega-batch-53-global-network"
}

resource "aws_networkmanager_device" "mb53_a" {
  global_network_id = aws_networkmanager_global_network.mb53.id
  description       = "mega-batch-53-device-a"
}

resource "aws_networkmanager_device" "mb53_b" {
  global_network_id = aws_networkmanager_global_network.mb53.id
  description       = "mega-batch-53-device-b"
}

resource "aws_networkmanager_connection" "mb53" {
  global_network_id   = aws_networkmanager_global_network.mb53.id
  device_id           = aws_networkmanager_device.mb53_a.id
  connected_device_id = aws_networkmanager_device.mb53_b.id
}

##############################################################################
# CloudWatch Network Monitor: monitor + a TCP probe against our own subnet.
##############################################################################

resource "aws_networkmonitor_monitor" "mb53" {
  monitor_name = "mega-batch-53-monitor"
}

resource "aws_networkmonitor_probe" "mb53" {
  monitor_name     = aws_networkmonitor_monitor.mb53.monitor_name
  destination      = "10.213.1.50"
  destination_port = 443
  protocol         = "TCP"
  source_arn       = aws_subnet.mb53_ec2.arn
}

##############################################################################
# Elastic Beanstalk: application version (from an S3 object) and a
# configuration template.
##############################################################################

resource "aws_elastic_beanstalk_application" "mb53" {
  name = "mega-batch-53-eb-app"
}

resource "aws_elastic_beanstalk_application_version" "mb53" {
  name        = "mega-batch-53-eb-version"
  application = aws_elastic_beanstalk_application.mb53.name
  bucket      = aws_s3_bucket.mb53_datafeed.id
  key         = "eb/mega-batch-53.zip"
}

resource "aws_elastic_beanstalk_configuration_template" "mb53" {
  name                = "mega-batch-53-eb-template"
  application         = aws_elastic_beanstalk_application.mb53.name
  solution_stack_name = "64bit Amazon Linux 2023 v4.1.1 running Python 3.11"
}

##############################################################################
# RDS: Aurora cluster + a manual cluster snapshot + a same-account copy of
# that snapshot.
##############################################################################

resource "aws_rds_cluster" "mb53" {
  cluster_identifier  = "mega-batch-53-rds-cluster"
  engine              = "aurora-postgresql"
  master_username     = "admin"
  master_password     = "MegaBatch53Pass!"
  skip_final_snapshot = true
}

resource "aws_db_cluster_snapshot" "mb53" {
  db_cluster_identifier          = aws_rds_cluster.mb53.id
  db_cluster_snapshot_identifier = "mega-batch-53-rds-snap"
}

resource "aws_rds_cluster_snapshot_copy" "mb53" {
  source_db_cluster_snapshot_identifier = aws_db_cluster_snapshot.mb53.db_cluster_snapshot_arn
  target_db_cluster_snapshot_identifier = "mega-batch-53-rds-snap-copy"
}

##############################################################################
# EKS: cluster + an OIDC identity provider config + a pod identity
# association.
##############################################################################

resource "aws_iam_role" "mb53_eks" {
  name = "mega-batch-53-eks-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_subnet" "mb53_eks_b" {
  vpc_id            = aws_vpc.mb53_ec2.id
  cidr_block        = "10.213.3.0/24"
  availability_zone = "us-east-1b"
}

resource "aws_eks_cluster" "mb53" {
  name     = "mega-batch-53-eks"
  role_arn = aws_iam_role.mb53_eks.arn

  vpc_config {
    subnet_ids = [aws_subnet.mb53_ec2.id, aws_subnet.mb53_eks_b.id]
  }
}

resource "aws_eks_identity_provider_config" "mb53" {
  cluster_name = aws_eks_cluster.mb53.name

  oidc {
    client_id                     = "mega-batch-53-client"
    identity_provider_config_name = "mega-batch-53-idp"
    issuer_url                    = "https://oidc.mega-batch-53.example.com"
  }
}

resource "aws_iam_role" "mb53_pod" {
  name = "mega-batch-53-pod-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "pods.eks.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:TagSession"]
    }]
  })
}

resource "aws_eks_pod_identity_association" "mb53" {
  cluster_name    = aws_eks_cluster.mb53.name
  namespace       = "default"
  service_account = "mega-batch-53-sa"
  role_arn        = aws_iam_role.mb53_pod.arn
}
