resource "aws_grafana_workspace" "example" {
  name                     = "gair-grafana"
  account_access_type      = "CURRENT_ACCOUNT"
  authentication_providers = ["SAML"]
  permission_type          = "SERVICE_MANAGED"
}

resource "aws_inspector2_enabler" "example" {
  account_ids    = ["000000000000"]
  resource_types = ["ECR"]
}

resource "aws_networkmanager_global_network" "example" {
  description = "gair global network"
}

resource "aws_resiliencehub_resiliency_policy" "example" {
  name        = "gairpolicy"
  description = "gair resiliency policy"
  tier        = "NonCritical"

  policy {
    az {
      rpo = "24h"
      rto = "24h"
    }
    hardware {
      rpo = "24h"
      rto = "24h"
    }
    software {
      rpo = "24h"
      rto = "24h"
    }
  }
}

resource "aws_cloudfront_key_value_store" "example" {
  name    = "gairkvs"
  comment = "gair kvs"
}
