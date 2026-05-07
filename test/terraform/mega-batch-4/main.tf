terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    redshiftserverless = var.gopherstack_endpoint
    sagemaker          = var.gopherstack_endpoint
    bedrockagent       = var.gopherstack_endpoint
    lakeformation      = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "The GopherStack local endpoint URL"
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Redshift Serverless
# ---------------------------------------------------------------------------

resource "aws_redshiftserverless_namespace" "example" {
  namespace_name = "example-namespace"
  db_name        = "exampledb"
  admin_username = "admin"

  log_exports = ["userlog", "connectionlog"]
}

resource "aws_redshiftserverless_workgroup" "example" {
  namespace_name = aws_redshiftserverless_namespace.example.namespace_name
  workgroup_name = "example-workgroup"
  base_capacity  = 64

  depends_on = [aws_redshiftserverless_namespace.example]
}

# ---------------------------------------------------------------------------
# SageMaker Pipeline
# ---------------------------------------------------------------------------

resource "aws_sagemaker_pipeline" "example" {
  pipeline_name         = "example-pipeline"
  pipeline_display_name = "Example Pipeline"

  pipeline_definition = jsonencode({
    Version = "2020-12-01"
    Steps   = []
  })

  role_arn = "arn:aws:iam::000000000000:role/sagemaker-role"
}

# ---------------------------------------------------------------------------
# Bedrock Agent
# ---------------------------------------------------------------------------

resource "aws_bedrockagent_agent" "example" {
  agent_name              = "example-agent"
  agent_resource_role_arn = "arn:aws:iam::000000000000:role/bedrock-agent-role"
  foundation_model        = "anthropic.claude-3-sonnet-20240229-v1:0"
  instruction             = "You are a helpful assistant."
}

resource "aws_bedrockagent_knowledge_base" "example" {
  name     = "example-knowledge-base"
  role_arn = "arn:aws:iam::000000000000:role/bedrock-kb-role"

  knowledge_base_configuration {
    type = "VECTOR"
    vector_knowledge_base_configuration {
      embedding_model_arn = "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1"
    }
  }

  storage_configuration {
    type = "OPENSEARCH_SERVERLESS"
    opensearch_serverless_configuration {
      collection_arn    = "arn:aws:aoss:us-east-1:000000000000:collection/example"
      vector_index_name = "example-index"
      field_mapping {
        vector_field   = "embedding"
        text_field     = "text"
        metadata_field = "metadata"
      }
    }
  }
}

# ---------------------------------------------------------------------------
# Lake Formation
# ---------------------------------------------------------------------------

resource "aws_lakeformation_resource" "example" {
  arn = "arn:aws:s3:::example-datalake-bucket"
}

resource "aws_lakeformation_data_lake_settings" "example" {
  admins = ["arn:aws:iam::000000000000:role/datalake-admin"]
}

resource "aws_lakeformation_lf_tag" "environment" {
  key    = "environment"
  values = ["dev", "staging", "prod"]
}

resource "aws_lakeformation_permissions" "example" {
  principal   = "arn:aws:iam::000000000000:role/analyst"
  permissions = ["SELECT"]

  table {
    database_name = "example_db"
    name          = "example_table"
  }

  depends_on = [aws_lakeformation_data_lake_settings.example]
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "redshift_namespace_arn" {
  value = aws_redshiftserverless_namespace.example.namespace_arn
}

output "redshift_workgroup_endpoint" {
  value = aws_redshiftserverless_workgroup.example.endpoint
}

output "sagemaker_pipeline_arn" {
  value = aws_sagemaker_pipeline.example.arn
}

output "bedrock_agent_id" {
  value = aws_bedrockagent_agent.example.agent_id
}

output "bedrock_knowledge_base_id" {
  value = aws_bedrockagent_knowledge_base.example.id
}
