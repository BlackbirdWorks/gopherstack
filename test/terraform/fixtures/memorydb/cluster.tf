resource "aws_memorydb_cluster" "this" {
  name       = "{{.ClusterName}}"
  node_type  = "db.r6g.large"
  acl_name   = "open-access"
  num_shards = 1

  tags = {
    Environment = "test"
  }
}
