resource "aws_route53_zone" "this" {
  name = "{{.ZoneName}}"
}
