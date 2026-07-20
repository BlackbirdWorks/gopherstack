resource "aws_opensearch_domain" "this" {
  domain_name    = "{{.DomainName}}"
  engine_version = "OpenSearch_2.3"

  domain_endpoint_options {
    enforce_https       = true
    tls_security_policy = "Policy-Min-TLS-1-2-2019-07"
  }
  timeouts {
    create = "5s"
    delete = "5s"
    update = "5s"
  }
}
