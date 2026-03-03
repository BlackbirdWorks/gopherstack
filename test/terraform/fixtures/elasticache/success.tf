resource "aws_elasticache_cluster" "this" {
  cluster_id           = "{{.ClusterID}}"
  engine               = "memcached"
  node_type            = "cache.t3.micro"
  num_cache_nodes      = 1
  parameter_group_name = "default.memcached1.6"

  timeouts {
    create = "2m"
    delete = "2m"
  }
}
