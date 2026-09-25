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

resource "aws_bedrock_guardrail" "example" {
  name                      = "mega-batch-4-guardrail"
  description               = "Test guardrail for mega-batch-4"
  blocked_input_messaging   = "Sorry, I cannot help with that."
  blocked_outputs_messaging = "I cannot provide that information."

  lifecycle {
    ignore_changes = [
      content_policy_config,
      sensitive_information_policy_config,
      word_policy_config,
    ]
  }
}

resource "aws_transcribe_vocabulary" "example" {
  vocabulary_name = "mega-batch-4-vocabulary"
  language_code   = "en-US"
  phrases         = ["gopherstack", "terraform"]
}

resource "aws_msk_cluster" "example" {
  cluster_name           = "mega-batch-4-kafka"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 1

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = ["subnet-00000000"]
    security_groups = ["sg-00000000"]
    storage_info {
      ebs_storage_info {
        volume_size = 20
      }
    }
  }
}

resource "aws_dynamodb_table" "example" {
  name         = "mega-batch-4-table"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }
}
