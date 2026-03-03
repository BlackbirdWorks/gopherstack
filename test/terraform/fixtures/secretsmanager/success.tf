resource "aws_secretsmanager_secret" "this" {
  name                    = "{{.SecretName}}"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "this" {
  secret_id     = aws_secretsmanager_secret.this.id
  secret_string = "my-test-secret-value"
}
