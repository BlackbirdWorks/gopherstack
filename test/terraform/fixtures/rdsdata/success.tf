resource "aws_rds_cluster" "this" {
  cluster_identifier  = "{{.ClusterIdentifier}}"
  engine              = "aurora-mysql"
  master_username     = "admin"
  master_password     = "password123"
  skip_final_snapshot = true
}
