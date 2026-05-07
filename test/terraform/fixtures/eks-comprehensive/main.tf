resource "aws_eks_cluster" "main" {
  name     = "{{.ClusterName}}"
  version  = "1.32"
  role_arn = "arn:aws:iam::000000000000:role/eks-cluster-role"

  vpc_config {
    subnet_ids = ["subnet-00000000", "subnet-00000001"]
  }

  tags = {
    Environment = "test"
    Suite       = "comprehensive"
  }
}

resource "aws_eks_node_group" "workers" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "workers"
  node_role_arn   = "arn:aws:iam::000000000000:role/eks-nodegroup-role"
  subnet_ids      = ["subnet-00000000", "subnet-00000001"]

  ami_type       = "AL2_x86_64"
  capacity_type  = "ON_DEMAND"
  instance_types = ["t3.medium"]

  scaling_config {
    desired_size = 2
    min_size     = 1
    max_size     = 5
  }

  tags = {
    Environment = "test"
  }

  depends_on = [aws_eks_cluster.main]
}

resource "aws_eks_fargate_profile" "serverless" {
  cluster_name           = aws_eks_cluster.main.name
  fargate_profile_name   = "serverless"
  pod_execution_role_arn = "arn:aws:iam::000000000000:role/eks-fargate-role"
  subnet_ids             = ["subnet-00000000", "subnet-00000001"]

  selector {
    namespace = "serverless"
  }

  tags = {
    Environment = "test"
  }

  depends_on = [aws_eks_cluster.main]
}

resource "aws_eks_addon" "coredns" {
  cluster_name  = aws_eks_cluster.main.name
  addon_name    = "coredns"
  addon_version = "v1.11.4-eksbuild.2"

  tags = {
    Component = "dns"
  }

  depends_on = [aws_eks_node_group.workers]
}

resource "aws_eks_access_entry" "admin" {
  cluster_name  = aws_eks_cluster.main.name
  principal_arn = "arn:aws:iam::000000000000:role/eks-admin-role"
  type          = "STANDARD"

  tags = {
    Role = "admin"
  }

  depends_on = [aws_eks_cluster.main]
}

resource "aws_eks_access_policy_association" "admin_policy" {
  cluster_name  = aws_eks_cluster.main.name
  principal_arn = aws_eks_access_entry.admin.principal_arn
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"

  access_scope {
    type = "cluster"
  }

  depends_on = [aws_eks_access_entry.admin]
}
