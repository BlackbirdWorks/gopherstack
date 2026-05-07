# ---------------------------------------------------------------------------
# DynamoDB comprehensive test: streams, global table, TTL, on-demand billing
# ---------------------------------------------------------------------------

# Table with DynamoDB Streams enabled
resource "aws_dynamodb_table" "streams" {
  name             = "{{.StreamsTable}}"
  billing_mode     = "PAY_PER_REQUEST"
  hash_key         = "pk"
  range_key        = "sk"
  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  tags = {
    Purpose = "streams-test"
  }
}

# Table for global table (must exist before creating global table)
resource "aws_dynamodb_table" "global_source" {
  name         = "{{.GlobalTable}}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  tags = {
    Purpose = "global-table-test"
  }
}

# Global table (v1 API wrapping the source table)
resource "aws_dynamodb_global_table" "example" {
  name = "{{.GlobalTable}}"

  replica {
    region_name = "us-east-1"
  }

  depends_on = [aws_dynamodb_table.global_source]
}

# On-demand billing table (PAY_PER_REQUEST from the start)
resource "aws_dynamodb_table" "on_demand" {
  name         = "{{.OnDemandTable}}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  tags = {
    Purpose = "on-demand-billing-test"
  }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "streams_table_name" {
  value = aws_dynamodb_table.streams.name
}

output "streams_table_arn" {
  value = aws_dynamodb_table.streams.arn
}

output "streams_table_stream_arn" {
  value = aws_dynamodb_table.streams.stream_arn
}

output "global_table_name" {
  value = aws_dynamodb_global_table.example.id
}

output "on_demand_table_name" {
  value = aws_dynamodb_table.on_demand.name
}
