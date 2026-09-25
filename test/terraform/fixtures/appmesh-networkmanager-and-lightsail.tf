resource "aws_accessanalyzer_analyzer" "example" {
  analyzer_name = "mega-batch-8-analyzer"
  type          = "ACCOUNT"
}

resource "aws_accessanalyzer_archive_rule" "example" {
  analyzer_name = aws_accessanalyzer_analyzer.example.analyzer_name
  rule_name     = "mega-batch-8-archive-rule"

  filter {
    criteria = "resource"
    contains = ["*"]
  }
}

resource "aws_appmesh_mesh" "example" {
  name = "mega-batch-8-mesh"
}

resource "aws_appmesh_virtual_node" "example" {
  name      = "mega-batch-8-vnode"
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
  name      = "mega-batch-8-vrouter"
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
  name                = "mega-batch-8-route"
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
  name      = "mega-batch-8-vservice.local"
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
  name                     = "mega-batch-8-collab"
  description              = "mega batch 8 collaboration"
  creator_display_name     = "mega-batch-8-creator"
  creator_member_abilities = ["CAN_QUERY", "CAN_RECEIVE_RESULTS"]
  query_log_status         = "DISABLED"
}

resource "aws_cleanrooms_configured_table" "example" {
  name            = "mega-batch-8-table"
  allowed_columns = ["col1", "col2"]
  analysis_method = "DIRECT_QUERY"

  table_reference {
    database_name = "mega_batch_8_db"
    table_name    = "mega_batch_8_tbl"
  }
}

resource "aws_cloudfront_key_value_store" "example" {
  name    = "mega-batch-8-kvs"
  comment = "mega batch 8 kvs"
}

resource "aws_cloudfrontkeyvaluestore_key" "example" {
  key_value_store_arn = aws_cloudfront_key_value_store.example.arn
  key                 = "mega-batch-8-key"
  value               = "mega-batch-8-value"
}

resource "aws_dlm_lifecycle_policy" "example" {
  description        = "mega batch 8 dlm policy"
  execution_role_arn = "arn:aws:iam::000000000000:role/mega-batch-8-dlm-role"
  state              = "ENABLED"

  policy_details {
    resource_types = ["VOLUME"]

    target_tags = {
      Snapshot = "mega-batch-8"
    }

    schedule {
      name = "mega-batch-8-schedule"

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
  name                     = "mega-batch-8-grafana"
  account_access_type      = "CURRENT_ACCOUNT"
  authentication_providers = ["AWS_SSO"]
  permission_type          = "SERVICE_MANAGED"
}

resource "aws_lightsail_static_ip" "example" {
  name = "mega-batch-8-staticip"
}

resource "aws_lightsail_bucket" "example" {
  name      = "mega-batch-8-bucket"
  bundle_id = "small_1_0"
}

resource "aws_lightsail_instance" "example" {
  name              = "mega-batch-8-instance"
  availability_zone = "us-east-1a"
  blueprint_id      = "amazon_linux_2023"
  bundle_id         = "nano_3_0"
}

resource "aws_networkmanager_global_network" "example" {
  description = "mega batch 8 global network"
}

resource "aws_networkmanager_site" "example" {
  global_network_id = aws_networkmanager_global_network.example.id
  description       = "mega batch 8 site"
}

resource "aws_networkmanager_device" "example" {
  global_network_id = aws_networkmanager_global_network.example.id
  site_id           = aws_networkmanager_site.example.id
  description       = "mega batch 8 device"
}

resource "aws_networkmanager_link" "example" {
  global_network_id = aws_networkmanager_global_network.example.id
  site_id           = aws_networkmanager_site.example.id
  description       = "mega batch 8 link"

  bandwidth {
    download_speed = 100
    upload_speed   = 10
  }
}

resource "aws_inspector2_enabler" "example" {
  account_ids    = ["000000000000"]
  resource_types = ["ECR"]
}
