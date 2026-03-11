resource "aws_eks_cluster" "this" {
  name     = "{{.ClusterName}}"
  version  = "1.32"
  role_arn = "arn:aws:iam::000000000000:role/eks-role"

  vpc_config {
    subnet_ids = ["subnet-00000000", "subnet-11111111"]
  }

  tags = {
    Environment = "test"
  }
}
