##############################################################################
# EC2: spot datafeed subscription, network interface permission, EBS fast
# snapshot restore, EBS snapshot import (from S3), and an AMI created from a
# running instance.
##############################################################################

resource "aws_s3_bucket" "tgms_datafeed" {
  bucket = "tgms-datafeed"
}

resource "aws_spot_datafeed_subscription" "tgms" {
  bucket = aws_s3_bucket.tgms_datafeed.id
}

resource "aws_vpc" "tgms_ec2" {
  cidr_block = "10.213.0.0/16"
}

resource "aws_subnet" "tgms_ec2" {
  vpc_id            = aws_vpc.tgms_ec2.id
  cidr_block        = "10.213.1.0/24"
  availability_zone = "us-east-1a"
}

resource "aws_instance" "tgms" {
  ami           = "ami-12345678"
  instance_type = "t2.micro"
  subnet_id     = aws_subnet.tgms_ec2.id

  tags = {
    Name = "tgms-instance"
  }
}

resource "aws_network_interface" "tgms" {
  subnet_id = aws_subnet.tgms_ec2.id
}

resource "aws_network_interface_permission" "tgms" {
  network_interface_id = aws_network_interface.tgms.id
  aws_account_id       = "000000000000"
  permission           = "INSTANCE-ATTACH"
}

resource "aws_ebs_volume" "tgms" {
  availability_zone = "us-east-1a"
  size              = 1
}

resource "aws_ebs_snapshot" "tgms" {
  volume_id = aws_ebs_volume.tgms.id
}

resource "aws_ebs_fast_snapshot_restore" "tgms" {
  availability_zone = "us-east-1a"
  snapshot_id       = aws_ebs_snapshot.tgms.id
}

resource "aws_ebs_snapshot_import" "tgms" {
  description = "tgms imported snapshot"

  disk_container {
    format      = "VHD"
    description = "tgms disk"

    user_bucket {
      s3_bucket = aws_s3_bucket.tgms_datafeed.id
      s3_key    = "tgms-disk.vhd"
    }
  }
}

resource "aws_ami_from_instance" "tgms" {
  name               = "tgms-ami"
  source_instance_id = aws_instance.tgms.id
}

##############################################################################
# EC2: VPC Route Server family (route server, its VPC association, an
# endpoint, a BGP peer on that endpoint, and route table propagation).
##############################################################################

resource "aws_vpc_route_server" "tgms" {
  amazon_side_asn = 4210000001
}

resource "aws_vpc_route_server_vpc_association" "tgms" {
  route_server_id = aws_vpc_route_server.tgms.route_server_id
  vpc_id          = aws_vpc.tgms_ec2.id
}

resource "aws_vpc_route_server_endpoint" "tgms" {
  route_server_id = aws_vpc_route_server.tgms.route_server_id
  subnet_id       = aws_subnet.tgms_ec2.id

  depends_on = [aws_vpc_route_server_vpc_association.tgms]
}

resource "aws_vpc_route_server_peer" "tgms" {
  route_server_endpoint_id = aws_vpc_route_server_endpoint.tgms.route_server_endpoint_id
  peer_address             = "10.213.1.100"

  bgp_options {
    peer_asn = 4210000002
  }
}

resource "aws_route_table" "tgms" {
  vpc_id = aws_vpc.tgms_ec2.id
}

resource "aws_vpc_route_server_propagation" "tgms" {
  route_server_id = aws_vpc_route_server.tgms.route_server_id
  route_table_id  = aws_route_table.tgms.id

  depends_on = [aws_vpc_route_server_vpc_association.tgms]
}

##############################################################################
# EC2: Transit Gateway VPC attachment accepter, multicast domain association
# plus group member/source, policy table association, and a managed prefix
# list reference on the TGW's own route table.
##############################################################################

resource "aws_ec2_transit_gateway" "tgms" {
  description = "tgms-tgw"
}

resource "aws_ec2_transit_gateway_vpc_attachment" "tgms" {
  transit_gateway_id = aws_ec2_transit_gateway.tgms.id
  vpc_id             = aws_vpc.tgms_ec2.id
  subnet_ids         = [aws_subnet.tgms_ec2.id]
}

resource "aws_ec2_transit_gateway_vpc_attachment_accepter" "tgms" {
  transit_gateway_attachment_id = aws_ec2_transit_gateway_vpc_attachment.tgms.id
}

resource "aws_ec2_transit_gateway_multicast_domain" "tgms" {
  transit_gateway_id = aws_ec2_transit_gateway.tgms.id
}

resource "aws_ec2_transit_gateway_multicast_domain_association" "tgms" {
  subnet_id                           = aws_subnet.tgms_ec2.id
  transit_gateway_attachment_id       = aws_ec2_transit_gateway_vpc_attachment.tgms.id
  transit_gateway_multicast_domain_id = aws_ec2_transit_gateway_multicast_domain.tgms.id
}

resource "aws_network_interface" "tgms_mcast" {
  subnet_id = aws_subnet.tgms_ec2.id
}

resource "aws_ec2_transit_gateway_multicast_group_member" "tgms" {
  group_ip_address                    = "224.0.1.1"
  network_interface_id                = aws_network_interface.tgms_mcast.id
  transit_gateway_multicast_domain_id = aws_ec2_transit_gateway_multicast_domain.tgms.id

  depends_on = [aws_ec2_transit_gateway_multicast_domain_association.tgms]
}

resource "aws_ec2_transit_gateway_multicast_group_source" "tgms" {
  group_ip_address                    = "224.0.1.1"
  network_interface_id                = aws_network_interface.tgms_mcast.id
  transit_gateway_multicast_domain_id = aws_ec2_transit_gateway_multicast_domain.tgms.id

  depends_on = [aws_ec2_transit_gateway_multicast_domain_association.tgms]
}

resource "aws_ec2_transit_gateway_policy_table" "tgms" {
  transit_gateway_id = aws_ec2_transit_gateway.tgms.id
}

resource "aws_ec2_transit_gateway_policy_table_association" "tgms" {
  transit_gateway_attachment_id   = aws_ec2_transit_gateway_vpc_attachment.tgms.id
  transit_gateway_policy_table_id = aws_ec2_transit_gateway_policy_table.tgms.id
}

resource "aws_ec2_managed_prefix_list" "tgms" {
  name           = "tgms-pl"
  address_family = "IPv4"
  max_entries    = 5

  entry {
    cidr = "10.213.9.0/24"
  }
}

resource "aws_ec2_transit_gateway_route_table" "tgms" {
  transit_gateway_id = aws_ec2_transit_gateway.tgms.id
}

resource "aws_ec2_transit_gateway_prefix_list_reference" "tgms" {
  transit_gateway_route_table_id = aws_ec2_transit_gateway_route_table.tgms.id
  prefix_list_id                 = aws_ec2_managed_prefix_list.tgms.id
  transit_gateway_attachment_id  = aws_ec2_transit_gateway_vpc_attachment.tgms.id
}

##############################################################################
# Batch job definition, CodeDeploy application + deployment group, and
# CodeConnections / CodeStarConnections hosts.
##############################################################################

resource "aws_iam_role" "tgms_svc" {
  name = "tgms-svc-role"
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

resource "aws_batch_job_definition" "tgms" {
  name = "tgms-job-def"
  type = "container"

  container_properties = jsonencode({
    image      = "busybox"
    command    = ["echo", "tgms"]
    vcpus      = 1
    memory     = 512
    jobRoleArn = aws_iam_role.tgms_svc.arn
  })
}

resource "aws_codedeploy_app" "tgms" {
  name = "tgms-app"
}

resource "aws_codedeploy_deployment_group" "tgms" {
  app_name              = aws_codedeploy_app.tgms.name
  deployment_group_name = "tgms-dg"
  service_role_arn      = aws_iam_role.tgms_svc.arn

  ec2_tag_filter {
    key   = "Name"
    type  = "KEY_AND_VALUE"
    value = "tgms-instance"
  }
}

resource "aws_codeconnections_host" "tgms" {
  name              = "tgms-cc-host"
  provider_endpoint = "https://ghe.tgms.example.com"
  provider_type     = "GitHubEnterpriseServer"
}

resource "aws_codestarconnections_host" "tgms" {
  name              = "tgms-csc-host"
  provider_endpoint = "https://ghe2.tgms.example.com"
  provider_type     = "GitHubEnterpriseServer"
}

##############################################################################
# Service Discovery: HTTP namespace + service + registered instance.
##############################################################################

resource "aws_service_discovery_http_namespace" "tgms" {
  name = "tgms-http-ns"
}

resource "aws_service_discovery_service" "tgms" {
  name = "tgms-svc"
  type = "HTTP"

  namespace_id = aws_service_discovery_http_namespace.tgms.id
}

resource "aws_service_discovery_instance" "tgms" {
  instance_id = "tgms-svc-instance"
  service_id  = aws_service_discovery_service.tgms.id

  attributes = {
    AWS_INSTANCE_IPV4 = "10.213.9.9"
  }
}

##############################################################################
# Step Functions: state machine (published) + an alias routed to that
# version.
##############################################################################

resource "aws_sfn_state_machine" "tgms" {
  name     = "tgms-sm"
  role_arn = aws_iam_role.tgms_svc.arn
  publish  = true

  definition = jsonencode({
    Comment = "ec2-transit-gateway-multicast-route-server"
    StartAt = "Pass"
    States = {
      Pass = {
        Type = "Pass"
        End  = true
      }
    }
  })
}

resource "aws_sfn_alias" "tgms" {
  name = "tgms-alias"

  routing_configuration {
    state_machine_version_arn = aws_sfn_state_machine.tgms.state_machine_version_arn
    weight                    = 100
  }
}

##############################################################################
# Transcribe custom language model.
##############################################################################

resource "aws_transcribe_language_model" "tgms" {
  model_name      = "tgms-lm"
  base_model_name = "NarrowBand"
  language_code   = "en-US"

  input_data_config {
    data_access_role_arn = aws_iam_role.tgms_svc.arn
    s3_uri               = "s3://${aws_s3_bucket.tgms_datafeed.id}/training/"
  }
}

##############################################################################
# WorkSpaces connection alias.
##############################################################################

resource "aws_workspaces_connection_alias" "tgms" {
  connection_string = "tgms.workspaces.example.com"
}

##############################################################################
# Network Manager: global network + two devices + a connection between them.
##############################################################################

resource "aws_networkmanager_global_network" "tgms" {
  description = "tgms-global-network"
}

resource "aws_networkmanager_device" "tgms_a" {
  global_network_id = aws_networkmanager_global_network.tgms.id
  description       = "tgms-device-a"
}

resource "aws_networkmanager_device" "tgms_b" {
  global_network_id = aws_networkmanager_global_network.tgms.id
  description       = "tgms-device-b"
}

resource "aws_networkmanager_connection" "tgms" {
  global_network_id   = aws_networkmanager_global_network.tgms.id
  device_id           = aws_networkmanager_device.tgms_a.id
  connected_device_id = aws_networkmanager_device.tgms_b.id
}

##############################################################################
# CloudWatch Network Monitor: monitor + a TCP probe against our own subnet.
##############################################################################

resource "aws_networkmonitor_monitor" "tgms" {
  monitor_name = "tgms-monitor"
}

resource "aws_networkmonitor_probe" "tgms" {
  monitor_name     = aws_networkmonitor_monitor.tgms.monitor_name
  destination      = "10.213.1.50"
  destination_port = 443
  protocol         = "TCP"
  source_arn       = aws_subnet.tgms_ec2.arn
}

##############################################################################
# Elastic Beanstalk: application version (from an S3 object) and a
# configuration template.
##############################################################################

resource "aws_elastic_beanstalk_application" "tgms" {
  name = "tgms-eb-app"
}

resource "aws_elastic_beanstalk_application_version" "tgms" {
  name        = "tgms-eb-version"
  application = aws_elastic_beanstalk_application.tgms.name
  bucket      = aws_s3_bucket.tgms_datafeed.id
  key         = "eb/tgms.zip"
}

resource "aws_elastic_beanstalk_configuration_template" "tgms" {
  name                = "tgms-eb-template"
  application         = aws_elastic_beanstalk_application.tgms.name
  solution_stack_name = "64bit Amazon Linux 2023 v4.1.1 running Python 3.11"
}

##############################################################################
# RDS: Aurora cluster + a manual cluster snapshot + a same-account copy of
# that snapshot.
##############################################################################

resource "aws_rds_cluster" "tgms" {
  cluster_identifier  = "tgms-rds-cluster"
  engine              = "aurora-postgresql"
  master_username     = "admin"
  master_password     = "TransitGatewayMulticastAndRouteServerPass!"
  skip_final_snapshot = true
}

resource "aws_db_cluster_snapshot" "tgms" {
  db_cluster_identifier          = aws_rds_cluster.tgms.id
  db_cluster_snapshot_identifier = "tgms-rds-snap"
}

resource "aws_rds_cluster_snapshot_copy" "tgms" {
  source_db_cluster_snapshot_identifier = aws_db_cluster_snapshot.tgms.db_cluster_snapshot_arn
  target_db_cluster_snapshot_identifier = "tgms-rds-snap-copy"
}

##############################################################################
# EKS: cluster + an OIDC identity provider config + a pod identity
# association.
##############################################################################

resource "aws_iam_role" "tgms_eks" {
  name = "tgms-eks-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_subnet" "tgms_eks_b" {
  vpc_id            = aws_vpc.tgms_ec2.id
  cidr_block        = "10.213.3.0/24"
  availability_zone = "us-east-1b"
}

resource "aws_eks_cluster" "tgms" {
  name     = "tgms-eks"
  role_arn = aws_iam_role.tgms_eks.arn

  vpc_config {
    subnet_ids = [aws_subnet.tgms_ec2.id, aws_subnet.tgms_eks_b.id]
  }
}

resource "aws_eks_identity_provider_config" "tgms" {
  cluster_name = aws_eks_cluster.tgms.name

  oidc {
    client_id                     = "tgms-client"
    identity_provider_config_name = "tgms-idp"
    issuer_url                    = "https://oidc.tgms.example.com"
  }
}

resource "aws_iam_role" "tgms_pod" {
  name = "tgms-pod-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "pods.eks.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:TagSession"]
    }]
  })
}

resource "aws_eks_pod_identity_association" "tgms" {
  cluster_name    = aws_eks_cluster.tgms.name
  namespace       = "default"
  service_account = "tgms-sa"
  role_arn        = aws_iam_role.tgms_pod.arn
}
