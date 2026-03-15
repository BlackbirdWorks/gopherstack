resource "aws_route53_record" "this" {
  zone_id = "{{.ZoneID}}"
  name    = "{{.RecordName}}"
  type    = "A"
  ttl     = 300
  records = ["1.2.3.4"]
}
