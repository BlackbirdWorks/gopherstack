resource "aws_amplify_app" "this" {
  name = "{{.AppName}}"
}

resource "aws_amplify_branch" "main" {
  app_id      = aws_amplify_app.this.id
  branch_name = "main"
}
