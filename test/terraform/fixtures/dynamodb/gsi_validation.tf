resource "aws_dynamodb_table" "this" {
  name         = "{{.TableName}}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "gsi_pk"
    type = "S"
  }

  global_secondary_index {
    name            = "by-gsi"
    hash_key        = "gsi_pk"
    projection_type = "ALL"
  }
}
