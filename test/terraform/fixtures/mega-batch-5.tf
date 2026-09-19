resource "aws_accessanalyzer_analyzer" "example" {
  analyzer_name = "mega-batch-5-analyzer"
  type          = "ACCOUNT"
}

resource "aws_appmesh_mesh" "example" {
  name = "mega-batch-5-mesh"
}

resource "aws_cleanrooms_collaboration" "example" {
  name                     = "mega-batch-5-collab"
  description              = "mega-batch-5 clean rooms collaboration"
  creator_display_name     = "gopherstack"
  creator_member_abilities = ["CAN_QUERY", "CAN_RECEIVE_RESULTS"]
  query_log_status         = "DISABLED"

  data_encryption_metadata {
    allow_clear_text                            = true
    allow_duplicates                            = true
    allow_joins_on_columns_with_different_names = true
    preserve_nulls                              = false
  }
}

resource "aws_dax_cluster" "example" {
  cluster_name       = "mega-batch-5-dax"
  iam_role_arn       = "arn:aws:iam::000000000000:role/dax-role"
  node_type          = "dax.r4.large"
  replication_factor = 1
}

resource "aws_dx_connection" "example" {
  name      = "mega-batch-5-dx"
  bandwidth = "1Gbps"
  location  = "EqDC2"
}

resource "aws_dlm_lifecycle_policy" "example" {
  description        = "mega-batch-5 DLM lifecycle policy"
  execution_role_arn = "arn:aws:iam::000000000000:role/dlm-lifecycle-role"
  state              = "ENABLED"

  policy_details {
    resource_types = ["VOLUME"]

    schedule {
      name = "daily-snapshots"

      create_rule {
        interval      = 24
        interval_unit = "HOURS"
        times         = ["23:45"]
      }

      retain_rule {
        count = 14
      }
    }

    target_tags = {
      Snapshot = "true"
    }
  }
}
