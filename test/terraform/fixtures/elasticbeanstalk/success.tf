resource "aws_elastic_beanstalk_application" "this" {
  name        = "tf-app-{{.Suffix}}"
  description = "Terraform test application"
}

resource "aws_elastic_beanstalk_environment" "this" {
  name                = "tf-env-{{.Suffix}}"
  application         = aws_elastic_beanstalk_application.this.name
  solution_stack_name = "64bit Amazon Linux 2023 v4.0.0 running Python 3.11"
}
