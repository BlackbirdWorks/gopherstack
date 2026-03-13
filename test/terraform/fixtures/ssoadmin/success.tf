data "aws_ssoadmin_instances" "this" {}

resource "aws_ssoadmin_permission_set" "this" {
  name         = "{{.PermissionSetName}}"
  description  = "Terraform test permission set"
  instance_arn = tolist(data.aws_ssoadmin_instances.this.arns)[0]

  session_duration = "PT1H"
}
