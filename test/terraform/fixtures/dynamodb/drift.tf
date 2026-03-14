resource "aws_dynamodb_table" "this" {
  name           = "{{.TableName}}"
  billing_mode   = "PROVISIONED"
  read_capacity  = 5
  write_capacity = 5
  hash_key       = "pk"

  attribute {
    name = "pk"
    type = "S"
  }
}
