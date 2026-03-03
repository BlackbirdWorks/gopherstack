resource "aws_route53_zone" "this" {
  name = "{{.ZoneName}}"
}

resource "aws_route53_record" "this" {
  zone_id = aws_route53_zone.this.zone_id
  name    = "www.{{.ZoneName}}"
  type    = "A"
  ttl     = 300
  records = ["1.2.3.4"]
}
