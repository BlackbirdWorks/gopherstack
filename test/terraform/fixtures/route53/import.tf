resource "aws_route53_zone" "this" {
  name          = "{{.ZoneName}}"
  comment       = "Managed by Terraform"
  force_destroy = false

  lifecycle {
    ignore_changes = [force_destroy]
  }
}
