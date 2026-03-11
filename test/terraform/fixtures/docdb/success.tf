resource "aws_docdb_subnet_group" "this" {
  name       = "tf-docdb-sg-{{.Suffix}}"
  subnet_ids = ["subnet-00000000", "subnet-11111111"]
}

resource "aws_docdb_cluster" "this" {
  cluster_identifier      = "tf-docdb-{{.Suffix}}"
  engine                  = "docdb"
  master_username         = "admin"
  master_password         = "password123"
  db_subnet_group_name    = aws_docdb_subnet_group.this.name
  skip_final_snapshot     = true
}

resource "aws_docdb_cluster_instance" "this" {
  identifier         = "tf-docdb-inst-{{.Suffix}}"
  cluster_identifier = aws_docdb_cluster.this.id
  instance_class     = "db.t3.medium"
}
