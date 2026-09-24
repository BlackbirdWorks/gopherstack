##############################################################################
# RAM: a resource share, a principal association, a resource association, a
# cross-account share accepter, and org-wide sharing enablement.
##############################################################################

resource "aws_ram_resource_share" "mb49" {
  name                      = "mega-batch-49-share"
  allow_external_principals = true
}

resource "aws_subnet" "mb49" {
  vpc_id     = aws_vpc.mb49.id
  cidr_block = "10.49.1.0/24"

  tags = {
    Name = "mega-batch-49-subnet"
  }
}

resource "aws_vpc" "mb49" {
  cidr_block = "10.49.0.0/16"

  tags = {
    Name = "mega-batch-49-vpc"
  }
}

resource "aws_ram_resource_association" "mb49" {
  resource_share_arn = aws_ram_resource_share.mb49.arn
  resource_arn       = aws_subnet.mb49.arn
}

resource "aws_ram_principal_association" "mb49" {
  resource_share_arn = aws_ram_resource_share.mb49.arn
  principal          = "999999999999"
}

resource "aws_ram_resource_share_accepter" "mb49" {
  share_arn = aws_ram_resource_share.mb49.arn

  depends_on = [aws_ram_principal_association.mb49]
}

# aws_ram_sharing_with_organization is left out. FIXED in code 2026-09-24:
# EnableSharingWithAwsOrganization (services/ram/cross_service.go) now creates
# the RAM service-linked role in IAM AND enables "ram.amazonaws.com" in
# Organizations, so both cross-service lookups this resource's Read performs
# (iam:GetRole, organizations:ListAWSServiceAccessForOrganization) would
# succeed. Still left out of THIS fixture: services/organizations' Organization
# is a per-backend singleton (CreateOrganization errors if one already exists),
# mega-batch-48.tf already creates one, and every mega-batch test runs
# t.Parallel() against the same shared emulator -- a second
# aws_organizations_organization here would race batch 48's, and depending on
# batch 48's org via a data source isn't safe either since parallel test apply
# order isn't guaranteed. See services/ram/PARITY.md items_still_open.

##############################################################################
# Grafana: a workspace, a license association, a SAML authentication
# configuration, a service account, and a service account token.
##############################################################################

resource "aws_grafana_workspace" "mb49" {
  name                     = "mega-batch-49-grafana"
  account_access_type      = "CURRENT_ACCOUNT"
  authentication_providers = ["AWS_SSO", "SAML"]
  permission_type          = "SERVICE_MANAGED"
}

resource "aws_grafana_license_association" "mb49" {
  workspace_id = aws_grafana_workspace.mb49.id
  license_type = "ENTERPRISE"
}

resource "aws_grafana_workspace_saml_configuration" "mb49" {
  workspace_id = aws_grafana_workspace.mb49.id

  editor_role_values = ["editor"]
  admin_role_values  = ["admin"]

  idp_metadata_xml = <<XML
<?xml version="1.0"?>
<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata" entityID="mega-batch-49-idp">
  <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol"/>
</EntityDescriptor>
XML
}

resource "aws_grafana_workspace_service_account" "mb49" {
  name         = "mega-batch-49-sa"
  workspace_id = aws_grafana_workspace.mb49.id
  grafana_role = "ADMIN"
}

resource "aws_grafana_workspace_service_account_token" "mb49" {
  name               = "mega-batch-49-token"
  workspace_id       = aws_grafana_workspace.mb49.id
  service_account_id = aws_grafana_workspace_service_account.mb49.service_account_id
  seconds_to_live    = 3600
}

##############################################################################
# Inspector2: a delegated admin account, a suppression filter, a member
# association, and an organization auto-enable configuration.
##############################################################################

resource "aws_inspector2_delegated_admin_account" "mb49" {
  account_id = "555566667777"
}

resource "aws_inspector2_filter" "mb49" {
  name        = "mega-batch-49-filter"
  action      = "SUPPRESS"
  description = "mega-batch-49 suppression filter"

  filter_criteria {
    finding_type {
      comparison = "EQUALS"
      value      = "PACKAGE_VULNERABILITY"
    }
  }
}

resource "aws_inspector2_member_association" "mb49" {
  account_id = "444455556666"
}

resource "aws_inspector2_organization_configuration" "mb49" {
  auto_enable {
    ec2    = true
    ecr    = true
    lambda = true
  }

  depends_on = [aws_inspector2_delegated_admin_account.mb49]
}

##############################################################################
# MediaLive: an input security group, a channel, a multiplex, and a
# multiplex program.
##############################################################################

resource "aws_medialive_input_security_group" "mb49" {
  whitelist_rules {
    cidr = "10.49.0.0/16"
  }
}

resource "aws_medialive_input" "mb49" {
  name = "mega-batch-49-input"
  type = "RTMP_PUSH"

  destinations {
    stream_name = "mega-batch-49/stream"
  }

  input_security_groups = [aws_medialive_input_security_group.mb49.id]
}

resource "aws_medialive_channel" "mb49" {
  name          = "mega-batch-49-channel"
  channel_class = "SINGLE_PIPELINE"
  role_arn      = "arn:aws:iam::000000000000:role/mega-batch-49-medialive-role"

  input_specification {
    codec            = "AVC"
    input_resolution = "HD"
    maximum_bitrate  = "MAX_10_MBPS"
  }

  destinations {
    id = "mb49destination"

    settings {
      url = "s3://mega-batch-49-bucket/test"
    }
  }

  input_attachments {
    input_attachment_name = "mega-batch-49-input"
    input_id              = aws_medialive_input.mb49.id
  }

  encoder_settings {
    timecode_config {
      source = "EMBEDDED"
    }

    audio_descriptions {
      audio_selector_name = "mega-batch-49-audio-selector"
      name                = "mb49-audio"
    }

    video_descriptions {
      name = "mb49-video"
    }

    output_groups {
      output_group_settings {
        archive_group_settings {
          destination {
            destination_ref_id = "mb49destination"
          }
        }
      }

      outputs {
        audio_description_names = ["mb49-audio"]
        output_name             = "mb49-output"
        video_description_name  = "mb49-video"

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

resource "aws_medialive_multiplex" "mb49" {
  name               = "mega-batch-49-multiplex"
  availability_zones = ["us-east-1a", "us-east-1b"]

  multiplex_settings {
    transport_stream_bitrate                = 1000000
    transport_stream_id                     = 1
    transport_stream_reserved_bitrate       = 1
    maximum_video_buffer_delay_milliseconds = 1000
  }
}

resource "aws_medialive_multiplex_program" "mb49" {
  multiplex_id = aws_medialive_multiplex.mb49.id
  program_name = "mega-batch-49-program"

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

resource "aws_elasticsearch_domain" "mb49" {
  domain_name           = "mega-batch-49-es"
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

  timeouts {
    create = "5s"
    delete = "5s"
    update = "5s"
  }
}

resource "aws_elasticsearch_domain_policy" "mb49" {
  domain_name = aws_elasticsearch_domain.mb49.domain_name

  access_policies = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "MegaBatch49ESPolicy"
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "es:*"
      Resource  = "${aws_elasticsearch_domain.mb49.arn}/*"
    }]
  })
}

resource "aws_elasticsearch_domain_saml_options" "mb49" {
  domain_name = aws_elasticsearch_domain.mb49.domain_name

  saml_options {
    enabled = true

    idp {
      entity_id        = "mega-batch-49-idp"
      metadata_content = <<XML
<?xml version="1.0"?>
<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata" entityID="mega-batch-49-idp">
  <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol"/>
</EntityDescriptor>
XML
    }

    roles_key   = "Role"
    subject_key = "NameID"
  }
}

resource "aws_vpc" "mb49_es" {
  cidr_block = "10.50.0.0/16"

  tags = {
    Name = "mega-batch-49-es-vpc"
  }
}

resource "aws_subnet" "mb49_es" {
  vpc_id     = aws_vpc.mb49_es.id
  cidr_block = "10.50.1.0/24"

  tags = {
    Name = "mega-batch-49-es-subnet"
  }
}

resource "aws_security_group" "mb49_es" {
  name   = "mega-batch-49-es-sg"
  vpc_id = aws_vpc.mb49_es.id
}

resource "aws_elasticsearch_vpc_endpoint" "mb49" {
  domain_arn = aws_elasticsearch_domain.mb49.arn

  vpc_options {
    subnet_ids         = [aws_subnet.mb49_es.id]
    security_group_ids = [aws_security_group.mb49_es.id]
  }
}
