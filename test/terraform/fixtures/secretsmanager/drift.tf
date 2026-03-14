resource "aws_secretsmanager_secret" "this" {
  name                    = "{{.SecretName}}"
  description             = "original description"
  recovery_window_in_days = 0
}
