resource "aws_bedrock_guardrail" "this" {
  name                      = "tf-guardrail-{{.Suffix}}"
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
