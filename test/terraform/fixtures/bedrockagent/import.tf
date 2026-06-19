resource "aws_bedrockagent_agent" "this" {
  agent_name              = "{{.AgentName}}"
  agent_resource_role_arn = "arn:aws:iam::000000000000:role/bedrock-agent-role"
  foundation_model        = "anthropic.claude-3-sonnet-20240229-v1:0"
  instruction             = "You are a helpful assistant."
}
