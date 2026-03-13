resource "aws_redshiftdata_statement" "this" {
  cluster_identifier = "{{.ClusterID}}"
  database           = "dev"
  sql                = "SELECT 1"
}
