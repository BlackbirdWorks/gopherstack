resource "aws_service_discovery_http_namespace" "this" {
  name = "{{.NamespaceName}}"
}

resource "aws_service_discovery_service" "this" {
  name         = "{{.ServiceName}}"
  namespace_id = aws_service_discovery_http_namespace.this.id
}
