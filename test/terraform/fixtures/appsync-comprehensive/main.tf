resource "aws_iam_role" "appsync" {
  name = "{{.Prefix}}-appsync-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "appsync.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_dynamodb_table" "items" {
  name         = "{{.Prefix}}-items"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }

  tags = {
    Environment = "test"
  }
}

resource "aws_appsync_graphql_api" "this" {
  name                = "{{.Prefix}}-api"
  authentication_type = "API_KEY"

  schema = <<-GRAPHQL
    type Item {
      id: ID!
      name: String
    }
    type Query {
      getItem(id: ID!): Item
      listItems: [Item]
    }
    type Mutation {
      putItem(id: ID!, name: String): Item
    }
    schema {
      query: Query
      mutation: Mutation
    }
  GRAPHQL

  tags = {
    Environment = "test"
  }
}

resource "aws_appsync_api_key" "this" {
  api_id      = aws_appsync_graphql_api.this.id
  description = "Test API key"
}

resource "aws_appsync_datasource" "dynamo" {
  api_id           = aws_appsync_graphql_api.this.id
  name             = "DynamoDBItems"
  type             = "AMAZON_DYNAMODB"
  service_role_arn = aws_iam_role.appsync.arn

  dynamodb_config {
    table_name = aws_dynamodb_table.items.name
    region     = "us-east-1"
  }
}

resource "aws_appsync_resolver" "get_item" {
  api_id      = aws_appsync_graphql_api.this.id
  type        = "Query"
  field       = "getItem"
  data_source = aws_appsync_datasource.dynamo.name
  kind        = "UNIT"

  request_template = jsonencode({
    version   = "2017-02-28"
    operation = "GetItem"
    key = {
      id = { S = "$ctx.args.id" }
    }
  })

  response_template = "$util.toJson($ctx.result)"
}

resource "aws_appsync_resolver" "put_item" {
  api_id      = aws_appsync_graphql_api.this.id
  type        = "Mutation"
  field       = "putItem"
  data_source = aws_appsync_datasource.dynamo.name
  kind        = "UNIT"

  request_template = jsonencode({
    version   = "2017-02-28"
    operation = "PutItem"
    key = {
      id = { S = "$ctx.args.id" }
    }
    attributeValues = {
      name = { S = "$ctx.args.name" }
    }
  })

  response_template = "$util.toJson($ctx.result)"
}
