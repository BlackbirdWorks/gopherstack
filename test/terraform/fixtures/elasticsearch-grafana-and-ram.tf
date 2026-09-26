##############################################################################
# RAM: a resource share, a principal association, a resource association, a
# cross-account share accepter, and org-wide sharing enablement.
##############################################################################

resource "aws_ram_resource_share" "egar" {
  name                      = "egar-share"
  allow_external_principals = true
}

resource "aws_subnet" "egar" {
  vpc_id     = aws_vpc.egar.id
  cidr_block = "10.49.1.0/24"

  tags = {
    Name = "egar-subnet"
  }
}

resource "aws_vpc" "egar" {
  cidr_block = "10.49.0.0/16"

  tags = {
    Name = "egar-vpc"
  }
}

resource "aws_ram_resource_association" "egar" {
  resource_share_arn = aws_ram_resource_share.egar.arn
  resource_arn       = aws_subnet.egar.arn
}

resource "aws_ram_principal_association" "egar" {
  resource_share_arn = aws_ram_resource_share.egar.arn
  principal          = "999999999999"
}

resource "aws_ram_resource_share_accepter" "egar" {
  share_arn = aws_ram_resource_share.egar.arn

  depends_on = [aws_ram_principal_association.egar]
}

# aws_ram_sharing_with_organization is left out. FIXED in code 2026-09-24:
# EnableSharingWithAwsOrganization (services/ram/cross_service.go) now creates
# the RAM service-linked role in IAM AND enables "ram.amazonaws.com" in
# Organizations, so both cross-service lookups this resource's Read performs
# (iam:GetRole, organizations:ListAWSServiceAccessForOrganization) would
# succeed. Still left out of THIS fixture: services/organizations' Organization
# is a per-backend singleton (CreateOrganization errors if one already exists),
# organizations-and-appstream.tf already creates one, and every terraform-fixture test runs
# t.Parallel() against the same shared emulator -- a second
# aws_organizations_organization here would race that fixture's, and depending on
# its org via a data source isn't safe either since parallel test apply
# order isn't guaranteed. See services/ram/PARITY.md items_still_open.

##############################################################################
# Grafana: a workspace, a license association, a SAML authentication
# configuration, a service account, and a service account token.
##############################################################################

resource "aws_grafana_workspace" "egar" {
  name                     = "egar-grafana"
  account_access_type      = "CURRENT_ACCOUNT"
  authentication_providers = ["AWS_SSO", "SAML"]
  permission_type          = "SERVICE_MANAGED"
}

resource "aws_grafana_license_association" "egar" {
  workspace_id = aws_grafana_workspace.egar.id
  license_type = "ENTERPRISE"
}

resource "aws_grafana_workspace_saml_configuration" "egar" {
  workspace_id = aws_grafana_workspace.egar.id

  editor_role_values = ["editor"]
  admin_role_values  = ["admin"]

  idp_metadata_xml = <<XML
<?xml version="1.0"?>
<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata" entityID="egar-idp">
  <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol"/>
</EntityDescriptor>
XML
}

resource "aws_grafana_workspace_service_account" "egar" {
  name         = "egar-sa"
  workspace_id = aws_grafana_workspace.egar.id
  grafana_role = "ADMIN"
}

resource "aws_grafana_workspace_service_account_token" "egar" {
  name               = "egar-token"
  workspace_id       = aws_grafana_workspace.egar.id
  service_account_id = aws_grafana_workspace_service_account.egar.service_account_id
  seconds_to_live    = 3600
}

##############################################################################
# Inspector2: a delegated admin account, a suppression filter, a member
# association, and an organization auto-enable configuration.
##############################################################################

resource "aws_inspector2_delegated_admin_account" "egar" {
  account_id = "555566667777"
}

resource "aws_inspector2_filter" "egar" {
  name        = "egar-filter"
  action      = "SUPPRESS"
  description = "egar suppression filter"

  filter_criteria {
    finding_type {
      comparison = "EQUALS"
      value      = "PACKAGE_VULNERABILITY"
    }
  }
}

resource "aws_inspector2_member_association" "egar" {
  account_id = "444455556666"
}

resource "aws_inspector2_organization_configuration" "egar" {
  auto_enable {
    ec2    = true
    ecr    = true
    lambda = true
  }

  depends_on = [aws_inspector2_delegated_admin_account.egar]
}

##############################################################################
# MediaLive: an input security group, a channel, a multiplex, and a
# multiplex program.
##############################################################################

resource "aws_medialive_input_security_group" "egar" {
  whitelist_rules {
    cidr = "10.49.0.0/16"
  }
}

resource "aws_medialive_input" "egar" {
  name = "egar-input"
  type = "RTMP_PUSH"

  destinations {
    stream_name = "egar/stream"
  }

  input_security_groups = [aws_medialive_input_security_group.egar.id]
}

resource "aws_medialive_channel" "egar" {
  name          = "egar-channel"
  channel_class = "SINGLE_PIPELINE"
  role_arn      = "arn:aws:iam::000000000000:role/egar-medialive-role"

  input_specification {
    codec            = "AVC"
    input_resolution = "HD"
    maximum_bitrate  = "MAX_10_MBPS"
  }

  destinations {
    id = "egardestination"

    settings {
      url = "s3://egar-bucket/test"
    }
  }

  input_attachments {
    input_attachment_name = "egar-input"
    input_id              = aws_medialive_input.egar.id
  }

  encoder_settings {
    timecode_config {
      source = "EMBEDDED"
    }

    audio_descriptions {
      audio_selector_name = "egar-audio-selector"
      name                = "egar-audio"
    }

    video_descriptions {
      name = "egar-video"
    }

    output_groups {
      output_group_settings {
        archive_group_settings {
          destination {
            destination_ref_id = "egardestination"
          }
        }
      }

      outputs {
        audio_description_names = ["egar-audio"]
        output_name             = "egar-output"
        video_description_name  = "egar-video"

        output_settings {
          archive_output_settings {
            name_modifier = "_1"
            extension     = "m2ts"

            container_settings {
              m2ts_settings {
                audio_buffer_model = "ATSC"
                buffer_model       = "MULTIPLEX"
                rate_mode          = "CBR"
              }
            }
          }
        }
      }
    }
  }
}

resource "aws_medialive_multiplex" "egar" {
  name               = "egar-multiplex"
  availability_zones = ["us-east-1a", "us-east-1b"]

  multiplex_settings {
    transport_stream_bitrate                = 1000000
    transport_stream_id                     = 1
    transport_stream_reserved_bitrate       = 1
    maximum_video_buffer_delay_milliseconds = 1000
  }
}

resource "aws_medialive_multiplex_program" "egar" {
  multiplex_id = aws_medialive_multiplex.egar.id
  program_name = "egar-program"

  multiplex_program_settings {
    program_number             = 1
    preferred_channel_pipeline = "CURRENTLY_ACTIVE"

    video_settings {
      constant_bitrate = 100000
    }
  }
}

##############################################################################
# Elasticsearch: a domain, a resource-based access policy, a SAML
# authentication configuration, and a VPC endpoint.
##############################################################################

resource "aws_elasticsearch_domain" "egar" {
  domain_name           = "egar-es"
  elasticsearch_version = "7.10"

  cluster_config {
    instance_type  = "t3.small.elasticsearch"
    instance_count = 1
  }

  ebs_options {
    ebs_enabled = true
    volume_size = 10
    volume_type = "gp2"
  }

  domain_endpoint_options {
    enforce_https       = true
    tls_security_policy = "Policy-Min-TLS-1-2-2019-07"
  }

  timeouts {
    create = "5s"
    delete = "5s"
    update = "5s"
  }
}

resource "aws_elasticsearch_domain_policy" "egar" {
  domain_name = aws_elasticsearch_domain.egar.domain_name

  access_policies = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "EgarESPolicy"
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "es:*"
      Resource  = "${aws_elasticsearch_domain.egar.arn}/*"
    }]
  })
}

resource "aws_elasticsearch_domain_saml_options" "egar" {
  domain_name = aws_elasticsearch_domain.egar.domain_name

  saml_options {
    enabled = true

    idp {
      entity_id        = "egar-idp"
      metadata_content = <<XML
<?xml version="1.0"?>
<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata" entityID="egar-idp">
  <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol"/>
</EntityDescriptor>
XML
    }

    roles_key   = "Role"
    subject_key = "NameID"
  }
}

resource "aws_vpc" "egar_es" {
  cidr_block = "10.50.0.0/16"

  tags = {
    Name = "egar-es-vpc"
  }
}

resource "aws_subnet" "egar_es" {
  vpc_id     = aws_vpc.egar_es.id
  cidr_block = "10.50.1.0/24"

  tags = {
    Name = "egar-es-subnet"
  }
}

resource "aws_security_group" "egar_es" {
  name   = "egar-es-sg"
  vpc_id = aws_vpc.egar_es.id
}

resource "aws_elasticsearch_vpc_endpoint" "egar" {
  domain_arn = aws_elasticsearch_domain.egar.arn

  vpc_options {
    subnet_ids         = [aws_subnet.egar_es.id]
    security_group_ids = [aws_security_group.egar_es.id]
  }
}
