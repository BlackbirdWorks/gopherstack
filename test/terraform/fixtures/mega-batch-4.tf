resource "aws_bedrockagent_agent" "example" {
  agent_name              = "example-agent"
  agent_resource_role_arn = "arn:aws:iam::000000000000:role/bedrock-agent-role"
  foundation_model        = "anthropic.claude-3-sonnet-20240229-v1:0"
  instruction             = "You are a helpful assistant that answers questions accurately and concisely."
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
