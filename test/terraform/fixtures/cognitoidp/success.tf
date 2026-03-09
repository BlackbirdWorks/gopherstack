resource "aws_cognito_user_pool" "this" {
  name = "{{.PoolName}}"
}

resource "aws_cognito_user_pool_client" "this" {
  name         = "{{.ClientName}}"
  user_pool_id = aws_cognito_user_pool.this.id
}
