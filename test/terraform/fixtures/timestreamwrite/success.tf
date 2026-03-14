resource "terraform_data" "timestreamwrite_create_db" {
  triggers_replace = {
    endpoint      = "{{.Endpoint}}"
    database_name = "{{.DatabaseName}}"
    table_name    = "{{.TableName}}"
  }

  provisioner "local-exec" {
    environment = {
      AWS_ACCESS_KEY_ID     = "test"
      AWS_SECRET_ACCESS_KEY = "test"
      AWS_DEFAULT_REGION    = "us-east-1"
    }
    command = "aws --endpoint-url '{{.Endpoint}}' timestream-write create-database --database-name '{{.DatabaseName}}' && aws --endpoint-url '{{.Endpoint}}' timestream-write create-table --database-name '{{.DatabaseName}}' --table-name '{{.TableName}}'"
  }
}
