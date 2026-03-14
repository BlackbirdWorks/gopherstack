resource "aws_elasticsearch_domain" "this" {
  domain_name           = "{{.DomainName}}"
  elasticsearch_version = "7.10"

  cluster_config {
    instance_type  = "t3.small.elasticsearch"
    instance_count = 1
  }

  ebs_options {
    ebs_enabled = true
    volume_size = 10
    volume_type = "gp2"
  }

  tags = {
    Name        = "{{.DomainName}}"
    Environment = "test"
  }

  timeouts {
    create = "5s"
    delete = "5s"
    update = "5s"
  }
}
