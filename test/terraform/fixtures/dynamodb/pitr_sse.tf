resource "aws_dynamodb_table" "this" {
  name         = "{{.TableName}}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  deletion_protection_enabled = true

  attribute {
    name = "pk"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  tags = {
    Environment = "test"
    Feature     = "pitr-sse"
  }
}
