resource "aws_dynamodb_table" "this" {
  name         = "{{.TableName}}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }
}
