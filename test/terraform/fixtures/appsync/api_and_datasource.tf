resource "aws_appsync_graphql_api" "this" {
  name                = "{{.APIName}}"
  authentication_type = "API_KEY"
}

resource "aws_appsync_datasource" "none_ds" {
  api_id = aws_appsync_graphql_api.this.id
  name   = "NoneDS"
  type   = "NONE"
}
