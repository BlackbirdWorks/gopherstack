resource "aws_kms_key" "this" {
  description             = "{{.KeyDesc}}"
  deletion_window_in_days = 7
}

resource "aws_kms_alias" "this" {
  name          = "{{.AliasName}}"
  target_key_id = aws_kms_key.this.key_id
}
