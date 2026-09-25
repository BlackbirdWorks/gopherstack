##############################################################################
# Bedrock Agents: a supervisor/collaborator agent pair, an agent alias, an
# agent collaborator association, a Lambda-backed action group, a knowledge
# base with an S3 data source, and a knowledge base association.
##############################################################################

resource "aws_iam_role" "mb42_agent" {
  name = "mega-batch-42-agent-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "bedrock.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_bedrockagent_agent" "mb42_collaborator" {
  agent_name                  = "mb42-collaborator-agent"
  agent_resource_role_arn     = aws_iam_role.mb42_agent.arn
  idle_session_ttl_in_seconds = 500
  foundation_model            = "anthropic.claude-3-5-sonnet-20241022-v2:0"
  instruction                 = "You are a collaborator agent that does what the supervisor tells you to do."
}

resource "aws_bedrockagent_agent_alias" "mb42_collaborator" {
  agent_alias_name = "mb42-collaborator-alias"
  agent_id         = aws_bedrockagent_agent.mb42_collaborator.agent_id
  description      = "mega-batch-42 collaborator alias"
}

resource "aws_bedrockagent_agent" "mb42_supervisor" {
  agent_name                  = "mb42-supervisor-agent"
  agent_resource_role_arn     = aws_iam_role.mb42_agent.arn
  agent_collaboration         = "SUPERVISOR"
  idle_session_ttl_in_seconds = 500
  foundation_model            = "anthropic.claude-3-5-sonnet-20241022-v2:0"
  instruction                 = "You are a supervisor agent that tells the collaborator agent what to do."
  prepare_agent                = false
}

resource "aws_bedrockagent_agent_collaborator" "mb42" {
  agent_id                    = aws_bedrockagent_agent.mb42_supervisor.agent_id
  collaboration_instruction   = "Tell the collaborator agent what to do."
  collaborator_name           = "mb42-collaborator"
  relay_conversation_history  = "TO_COLLABORATOR"
  prepare_agent                = false

  agent_descriptor {
    alias_arn = aws_bedrockagent_agent_alias.mb42_collaborator.agent_alias_arn
  }
}

resource "aws_lambda_function" "mb42_action_group" {
  function_name    = "mega-batch-42-action-group-fn"
  role             = aws_iam_role.mb42_agent.arn
  handler          = "index.handler"
  runtime          = "nodejs20.x"
  filename         = "{{.FunctionZip}}"
  source_code_hash = filebase64sha256("{{.FunctionZip}}")
}

resource "aws_bedrockagent_agent_action_group" "mb42" {
  action_group_name          = "mb42-action-group"
  agent_id                   = aws_bedrockagent_agent.mb42_supervisor.agent_id
  agent_version               = "DRAFT"
  skip_resource_in_use_check = true
  prepare_agent               = false

  action_group_executor {
    lambda = aws_lambda_function.mb42_action_group.arn
  }

  function_schema {
    member_functions {
      functions {
        name        = "mb42-example-function"
        description = "Example function for mega-batch-42"
        parameters {
          map_block_key = "query"
          type          = "string"
          description   = "The search query"
          required      = true
        }
      }
    }
  }
}

resource "aws_bedrockagent_knowledge_base" "mb42" {
  name     = "mb42-knowledge-base"
  role_arn = aws_iam_role.mb42_agent.arn

  knowledge_base_configuration {
    type = "VECTOR"
    vector_knowledge_base_configuration {
      embedding_model_arn = "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1"
    }
  }

  storage_configuration {
    type = "OPENSEARCH_SERVERLESS"
    opensearch_serverless_configuration {
      collection_arn    = "arn:aws:aoss:us-east-1:000000000000:collection/mb42"
      vector_index_name = "mb42-index"
      field_mapping {
        vector_field   = "embedding"
        text_field     = "text"
        metadata_field = "metadata"
      }
    }
  }
}

resource "aws_bedrockagent_agent_knowledge_base_association" "mb42" {
  agent_id             = aws_bedrockagent_agent.mb42_supervisor.agent_id
  description          = "mega-batch-42 knowledge base association"
  knowledge_base_id    = aws_bedrockagent_knowledge_base.mb42.id
  knowledge_base_state = "ENABLED"
}

resource "aws_s3_bucket" "mb42_ds" {
  bucket = "mega-batch-42-datasource-bucket"
}

resource "aws_bedrockagent_data_source" "mb42" {
  knowledge_base_id = aws_bedrockagent_knowledge_base.mb42.id
  name              = "mb42-data-source"

  data_source_configuration {
    type = "S3"
    s3_configuration {
      bucket_arn = aws_s3_bucket.mb42_ds.arn
    }
  }
}

##############################################################################
# Bedrock: a guardrail version, an inference profile, model invocation
# logging, a custom model customization job, and provisioned throughput.
##############################################################################

resource "aws_bedrock_guardrail" "mb42" {
  name                       = "mega-batch-42-guardrail"
  description                = "Test guardrail for mega-batch-42"
  blocked_input_messaging    = "Sorry, I cannot help with that."
  blocked_outputs_messaging  = "I cannot provide that information."

  lifecycle {
    ignore_changes = [
      content_policy_config,
      sensitive_information_policy_config,
      word_policy_config,
    ]
  }
}

resource "aws_bedrock_guardrail_version" "mb42" {
  description   = "mega-batch-42 guardrail version"
  guardrail_arn = aws_bedrock_guardrail.mb42.guardrail_arn
  skip_destroy  = true
}

resource "aws_bedrock_inference_profile" "mb42" {
  name        = "mega-batch-42-inference-profile"
  description = "mega-batch-42 inference profile"

  model_source {
    copy_from = "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-5-sonnet-20241022-v2:0"
  }
}

resource "aws_s3_bucket" "mb42_logging" {
  bucket = "mega-batch-42-logging-bucket"
}

resource "aws_s3_bucket_policy" "mb42_logging" {
  bucket = aws_s3_bucket.mb42_logging.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "MB42BedrockLogging"
      Effect    = "Allow"
      Principal = { Service = "bedrock.amazonaws.com" }
      Action    = ["s3:PutObject"]
      Resource  = ["${aws_s3_bucket.mb42_logging.arn}/*"]
    }]
  })
}

resource "aws_cloudwatch_log_group" "mb42_bedrock" {
  name = "/mega-batch-42/bedrock-logging"
}

resource "aws_iam_role" "mb42_logging" {
  name = "mega-batch-42-bedrock-logging-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "bedrock.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_bedrock_model_invocation_logging_configuration" "mb42" {
  depends_on = [aws_s3_bucket_policy.mb42_logging]

  logging_config {
    embedding_data_delivery_enabled = true
    image_data_delivery_enabled     = true
    text_data_delivery_enabled      = true

    s3_config {
      bucket_name = aws_s3_bucket.mb42_logging.id
      key_prefix  = "mega-batch-42"
    }

    cloudwatch_config {
      log_group_name = aws_cloudwatch_log_group.mb42_bedrock.name
      role_arn       = aws_iam_role.mb42_logging.arn
    }
  }
}

resource "aws_iam_role" "mb42_custom_model" {
  name = "mega-batch-42-custom-model-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "bedrock.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_s3_bucket" "mb42_training" {
  bucket = "mega-batch-42-training-bucket"
}

resource "aws_s3_bucket" "mb42_output" {
  bucket = "mega-batch-42-output-bucket"
}

resource "aws_bedrock_custom_model" "mb42" {
  custom_model_name      = "mega-batch-42-custom-model"
  job_name               = "mega-batch-42-customization-job"
  base_model_identifier  = "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1"
  role_arn               = aws_iam_role.mb42_custom_model.arn
  customization_type     = "FINE_TUNING"

  hyperparameters = {
    "epochCount"              = "1"
    "batchSize"               = "1"
    "learningRate"            = "0.005"
    "learningRateWarmupSteps" = "0"
  }

  output_data_config {
    s3_uri = "s3://${aws_s3_bucket.mb42_output.id}/data/"
  }

  training_data_config {
    s3_uri = "s3://${aws_s3_bucket.mb42_training.id}/data/train.jsonl"
  }
}

resource "aws_bedrock_provisioned_model_throughput" "mb42" {
  provisioned_model_name = "mega-batch-42-provisioned-throughput"
  model_arn              = "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1"
  model_units             = 1
}

##############################################################################
# Verified Permissions: a policy store, schema, policy template, a static
# and template-linked policy, plus a Cognito-backed identity source.
##############################################################################

resource "aws_verifiedpermissions_policy_store" "mb42" {
  description = "mega-batch-42 policy store"

  validation_settings {
    mode = "OFF"
  }
}

resource "aws_verifiedpermissions_schema" "mb42" {
  policy_store_id = aws_verifiedpermissions_policy_store.mb42.policy_store_id

  definition {
    value = jsonencode({
      "MB42" = {
        entityTypes = {
          User  = {}
          Photo = {}
        }
        actions = {
          view = {
            appliesTo = {
              principalTypes = ["User"]
              resourceTypes  = ["Photo"]
            }
          }
        }
      }
    })
  }
}

resource "aws_verifiedpermissions_policy_template" "mb42" {
  policy_store_id = aws_verifiedpermissions_policy_store.mb42.policy_store_id
  description      = "mega-batch-42 policy template"
  statement        = "permit (principal in ?principal, action, resource == ?resource);"
}

resource "aws_verifiedpermissions_policy" "mb42_static" {
  policy_store_id = aws_verifiedpermissions_policy_store.mb42.policy_store_id

  definition {
    static {
      description = "mega-batch-42 static policy"
      statement   = "permit (principal, action, resource);"
    }
  }
}

resource "aws_verifiedpermissions_policy" "mb42_template_linked" {
  policy_store_id = aws_verifiedpermissions_policy_store.mb42.policy_store_id

  definition {
    template_linked {
      policy_template_id = aws_verifiedpermissions_policy_template.mb42.policy_template_id

      principal {
        entity_id   = "mb42-user"
        entity_type = "MB42::User"
      }

      resource {
        entity_id   = "mb42-photo"
        entity_type = "MB42::Photo"
      }
    }
  }
}

resource "aws_cognito_user_pool" "mb42" {
  name = "mega-batch-42-user-pool"
}

resource "aws_cognito_user_pool_client" "mb42" {
  name                = "mega-batch-42-user-pool-client"
  user_pool_id        = aws_cognito_user_pool.mb42.id
  explicit_auth_flows = ["ADMIN_NO_SRP_AUTH"]
}

resource "aws_verifiedpermissions_identity_source" "mb42" {
  policy_store_id = aws_verifiedpermissions_policy_store.mb42.policy_store_id

  configuration {
    cognito_user_pool_configuration {
      user_pool_arn = aws_cognito_user_pool.mb42.arn
      client_ids    = [aws_cognito_user_pool_client.mb42.id]
    }
  }
}

##############################################################################
# ACM PCA: a root certificate authority, its self-signed root certificate,
# the CA-certificate association, a resource permission, and a resource
# policy.
##############################################################################

resource "aws_acmpca_certificate_authority" "mb42" {
  type = "ROOT"

  certificate_authority_configuration {
    key_algorithm     = "RSA_4096"
    signing_algorithm = "SHA512WITHRSA"

    subject {
      common_name = "mega-batch-42.example.com"
    }
  }

  permanent_deletion_time_in_days = 7
}

resource "aws_acmpca_certificate" "mb42" {
  certificate_authority_arn   = aws_acmpca_certificate_authority.mb42.arn
  certificate_signing_request = aws_acmpca_certificate_authority.mb42.certificate_signing_request
  signing_algorithm           = "SHA512WITHRSA"

  template_arn = "arn:aws:acm-pca:::template/RootCACertificate/V1"

  validity {
    type  = "YEARS"
    value = 1
  }
}

resource "aws_acmpca_certificate_authority_certificate" "mb42" {
  certificate_authority_arn = aws_acmpca_certificate_authority.mb42.arn

  certificate       = aws_acmpca_certificate.mb42.certificate
  certificate_chain = aws_acmpca_certificate.mb42.certificate_chain
}

resource "aws_acmpca_permission" "mb42" {
  certificate_authority_arn = aws_acmpca_certificate_authority.mb42.arn
  actions                   = ["IssueCertificate", "GetCertificate", "ListPermissions"]
  principal                 = "acm.amazonaws.com"

  depends_on = [aws_acmpca_certificate_authority_certificate.mb42]
}

resource "aws_acmpca_policy" "mb42" {
  resource_arn = aws_acmpca_certificate_authority.mb42.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "MB42AcmPcaPolicy"
      Effect = "Allow"
      Principal = {
        AWS = "000000000000"
      }
      Action   = ["acm-pca:DescribeCertificateAuthority", "acm-pca:GetCertificate"]
      Resource = aws_acmpca_certificate_authority.mb42.arn
    }]
  })

  depends_on = [aws_acmpca_certificate_authority_certificate.mb42]
}

##############################################################################
# AppConfig: an application, environment, hosted configuration profile and
# version, a deployment strategy and deployment, an extension and its
# association with the application.
##############################################################################

resource "aws_appconfig_application" "mb42" {
  name        = "mega-batch-42-app"
  description = "mega-batch-42 application"
}

resource "aws_appconfig_environment" "mb42" {
  name           = "mega-batch-42-env"
  application_id = aws_appconfig_application.mb42.id
}

resource "aws_appconfig_configuration_profile" "mb42" {
  application_id = aws_appconfig_application.mb42.id
  name           = "mega-batch-42-profile"
  location_uri   = "hosted"
}

resource "aws_appconfig_hosted_configuration_version" "mb42" {
  application_id           = aws_appconfig_application.mb42.id
  configuration_profile_id = aws_appconfig_configuration_profile.mb42.configuration_profile_id
  description              = "mega-batch-42 hosted configuration version"
  content_type             = "application/json"

  content = jsonencode({
    foo            = "bar"
    isThingEnabled = true
  })
}

resource "aws_appconfig_deployment_strategy" "mb42" {
  name                           = "mega-batch-42-deployment-strategy"
  description                    = "mega-batch-42 deployment strategy"
  deployment_duration_in_minutes = 0
  final_bake_time_in_minutes     = 0
  growth_factor                  = 100
  growth_type                    = "LINEAR"
  replicate_to                   = "NONE"
}

resource "aws_appconfig_deployment" "mb42" {
  application_id           = aws_appconfig_application.mb42.id
  configuration_profile_id = aws_appconfig_configuration_profile.mb42.configuration_profile_id
  configuration_version    = aws_appconfig_hosted_configuration_version.mb42.version_number
  deployment_strategy_id   = aws_appconfig_deployment_strategy.mb42.id
  description              = "mega-batch-42 deployment"
  environment_id           = aws_appconfig_environment.mb42.environment_id
}

resource "aws_sns_topic" "mb42_extension" {
  name = "mega-batch-42-extension-topic"
}

resource "aws_iam_role" "mb42_extension" {
  name = "mega-batch-42-extension-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "appconfig.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_appconfig_extension" "mb42" {
  name        = "mega-batch-42-extension"
  description = "mega-batch-42 extension"

  action_point {
    point = "ON_DEPLOYMENT_COMPLETE"
    action {
      name     = "mega-batch-42-action"
      role_arn = aws_iam_role.mb42_extension.arn
      uri      = aws_sns_topic.mb42_extension.arn
    }
  }
}

resource "aws_appconfig_extension_association" "mb42" {
  extension_arn = aws_appconfig_extension.mb42.arn
  resource_arn  = aws_appconfig_application.mb42.arn
}
