resource "aws_secretsmanager_secret" "this" {
  name                    = "{{.SecretName}}"
  recovery_window_in_days = 0
}
