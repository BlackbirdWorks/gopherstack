resource "aws_opensearch_domain" "this" {
  domain_name    = "{{.DomainName}}"
  engine_version = "OpenSearch_2.3"

  timeouts {
    create = "5s"
    delete = "5s"
    update = "5s"
  }
}
