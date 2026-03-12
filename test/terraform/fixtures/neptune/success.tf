resource "aws_neptune_subnet_group" "this" {
  name       = "tf-neptune-sg-{{.Suffix}}"
  subnet_ids = ["subnet-00000000", "subnet-11111111"]
}

resource "aws_neptune_cluster" "this" {
  cluster_identifier                  = "tf-neptune-{{.Suffix}}"
  engine                              = "neptune"
  skip_final_snapshot                 = true
  neptune_subnet_group_name           = aws_neptune_subnet_group.this.name
  apply_immediately                   = true
}

resource "aws_neptune_cluster_instance" "this" {
  identifier         = "tf-neptune-inst-{{.Suffix}}"
  cluster_identifier = aws_neptune_cluster.this.id
  instance_class     = "db.r5.large"
  engine             = "neptune"
}
