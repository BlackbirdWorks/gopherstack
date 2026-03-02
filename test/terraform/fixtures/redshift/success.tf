resource "aws_redshift_cluster" "this" {
  cluster_identifier  = "{{.ClusterID}}"
  database_name       = "testdb"
  master_username     = "admin"
  master_password     = "Test1234!"
  node_type           = "dc2.large"
  cluster_type        = "single-node"
  skip_final_snapshot = true

  timeouts {
    create = "2m"
    delete = "2m"
    update = "2m"
  }
}
