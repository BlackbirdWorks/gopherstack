resource "aws_accessanalyzer_analyzer" "example" {
  analyzer_name = "anwl-analyzer"
  type          = "ACCOUNT"
}

resource "aws_accessanalyzer_archive_rule" "example" {
  analyzer_name = aws_accessanalyzer_analyzer.example.analyzer_name
  rule_name     = "anwl-archive-rule"

  filter {
    criteria = "resource"
    contains = ["*"]
  }
}

resource "aws_appmesh_mesh" "example" {
  name = "anwl-mesh"
}

resource "aws_appmesh_virtual_node" "example" {
  name      = "anwl-vnode"
  mesh_name = aws_appmesh_mesh.example.id

  spec {
    service_discovery {
      dns {
        hostname = "example.local"
      }
    }
  }
}

resource "aws_appmesh_virtual_router" "example" {
  name      = "anwl-vrouter"
  mesh_name = aws_appmesh_mesh.example.id

  spec {
    listener {
      port_mapping {
        port     = 8080
        protocol = "http"
      }
    }
  }
}

resource "aws_appmesh_route" "example" {
  name                = "anwl-route"
  mesh_name           = aws_appmesh_mesh.example.id
  virtual_router_name = aws_appmesh_virtual_router.example.name

  spec {
    http_route {
      match {
        prefix = "/"
      }

      action {
        weighted_target {
          virtual_node = aws_appmesh_virtual_node.example.name
          weight       = 100
        }
      }
    }
  }
}

resource "aws_appmesh_virtual_service" "example" {
  name      = "anwl-vservice.local"
  mesh_name = aws_appmesh_mesh.example.id

  spec {
    provider {
      virtual_node {
        virtual_node_name = aws_appmesh_virtual_node.example.name
      }
    }
  }
}

resource "aws_cleanrooms_collaboration" "example" {
  name                     = "anwl-collab"
  description              = "anwl collaboration"
  creator_display_name     = "anwl-creator"
  creator_member_abilities = ["CAN_QUERY", "CAN_RECEIVE_RESULTS"]
  query_log_status         = "DISABLED"
}

resource "aws_cleanrooms_configured_table" "example" {
  name            = "anwl-table"
  allowed_columns = ["col1", "col2"]
  analysis_method = "DIRECT_QUERY"

  table_reference {
    database_name = "anwl_db"
    table_name    = "anwl_tbl"
  }
}

resource "aws_cloudfront_key_value_store" "example" {
  name    = "anwl-kvs"
  comment = "anwl kvs"
}

resource "aws_cloudfrontkeyvaluestore_key" "example" {
  key_value_store_arn = aws_cloudfront_key_value_store.example.arn
  key                 = "anwl-key"
  value               = "anwl-value"
}

resource "aws_dlm_lifecycle_policy" "example" {
  description        = "anwl dlm policy"
  execution_role_arn = "arn:aws:iam::000000000000:role/anwl-dlm-role"
  state              = "ENABLED"

  policy_details {
    resource_types = ["VOLUME"]

    target_tags = {
      Snapshot = "appmesh-networkmanager-and-lightsail"
    }

    schedule {
      name = "anwl-schedule"

      create_rule {
        interval      = 24
        interval_unit = "HOURS"
        times         = ["03:00"]
      }

      retain_rule {
        count = 3
      }
    }
  }
}

resource "aws_grafana_workspace" "example" {
  name                     = "anwl-grafana"
  account_access_type      = "CURRENT_ACCOUNT"
  authentication_providers = ["AWS_SSO"]
  permission_type          = "SERVICE_MANAGED"
}

resource "aws_lightsail_static_ip" "example" {
  name = "anwl-staticip"
}

resource "aws_lightsail_bucket" "example" {
  name      = "anwl-bucket"
  bundle_id = "small_1_0"
}

resource "aws_lightsail_instance" "example" {
  name              = "anwl-instance"
  availability_zone = "us-east-1a"
  blueprint_id      = "amazon_linux_2023"
  bundle_id         = "nano_3_0"
}

resource "aws_networkmanager_global_network" "example" {
  description = "anwl global network"
}

resource "aws_networkmanager_site" "example" {
  global_network_id = aws_networkmanager_global_network.example.id
  description       = "anwl site"
}

resource "aws_networkmanager_device" "example" {
  global_network_id = aws_networkmanager_global_network.example.id
  site_id           = aws_networkmanager_site.example.id
  description       = "anwl device"
}

resource "aws_networkmanager_link" "example" {
  global_network_id = aws_networkmanager_global_network.example.id
  site_id           = aws_networkmanager_site.example.id
  description       = "anwl link"

  bandwidth {
    download_speed = 100
    upload_speed   = 10
  }
}

resource "aws_inspector2_enabler" "example" {
  account_ids    = ["000000000000"]
  resource_types = ["ECR"]
}
