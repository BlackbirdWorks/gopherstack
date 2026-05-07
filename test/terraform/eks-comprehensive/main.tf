terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    eks = "http://localhost:4566"
    iam = "http://localhost:4566"
  }
}

# --------------------------------------------------------------------------
# EKS Cluster with OIDC issuer
# --------------------------------------------------------------------------

resource "aws_eks_cluster" "main" {
  name     = "gopherstack-comprehensive-cluster"
  role_arn = "arn:aws:iam::000000000000:role/eks-cluster-role"
  version  = "1.32"

  vpc_config {
    subnet_ids = ["subnet-00000000", "subnet-00000001"]
  }

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
    Suite       = "comprehensive"
  }
}

# --------------------------------------------------------------------------
# Managed node group with scaling config
# --------------------------------------------------------------------------

resource "aws_eks_node_group" "workers" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "workers"
  node_role_arn   = "arn:aws:iam::000000000000:role/eks-nodegroup-role"
  subnet_ids      = ["subnet-00000000", "subnet-00000001"]

  ami_type       = "AL2_x86_64"
  capacity_type  = "ON_DEMAND"
  instance_types = ["t3.medium"]
  version        = "1.32"

  scaling_config {
    desired_size = 2
    min_size     = 1
    max_size     = 5
  }

  tags = {
    Environment = "test"
    NodeType    = "workers"
  }

  depends_on = [aws_eks_cluster.main]
}

# --------------------------------------------------------------------------
# Fargate profile with namespace selectors
# --------------------------------------------------------------------------

resource "aws_eks_fargate_profile" "serverless" {
  cluster_name           = aws_eks_cluster.main.name
  fargate_profile_name   = "serverless"
  pod_execution_role_arn = "arn:aws:iam::000000000000:role/eks-fargate-role"
  subnet_ids             = ["subnet-00000000", "subnet-00000001"]

  selector {
    namespace = "serverless"
    labels = {
      "app.kubernetes.io/managed-by" = "fargate"
    }
  }

  selector {
    namespace = "kube-system"
  }

  tags = {
    Environment = "test"
  }

  depends_on = [aws_eks_cluster.main]
}

# --------------------------------------------------------------------------
# EKS Add-on (coredns) — realistic version
# --------------------------------------------------------------------------

resource "aws_eks_addon" "coredns" {
  cluster_name  = aws_eks_cluster.main.name
  addon_name    = "coredns"
  addon_version = "v1.11.4-eksbuild.2"

  tags = {
    Component = "dns"
  }

  depends_on = [aws_eks_node_group.workers]
}

# --------------------------------------------------------------------------
# Access entry linking an IAM role to the cluster
# --------------------------------------------------------------------------

resource "aws_eks_access_entry" "admin" {
  cluster_name  = aws_eks_cluster.main.name
  principal_arn = "arn:aws:iam::000000000000:role/eks-admin-role"
  type          = "STANDARD"

  tags = {
    Environment = "test"
    Role        = "admin"
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

# --------------------------------------------------------------------------
# Outputs
# --------------------------------------------------------------------------

output "cluster_name" {
  value = aws_eks_cluster.main.name
}

output "cluster_endpoint" {
  value = aws_eks_cluster.main.endpoint
}

output "cluster_oidc_issuer" {
  description = "OIDC issuer URL for the cluster — used by aws_iam_openid_connect_provider"
  value       = try(aws_eks_cluster.main.identity[0].oidc[0].issuer, "")
}

output "nodegroup_status" {
  value = aws_eks_node_group.workers.status
}

output "nodegroup_desired_size" {
  value = aws_eks_node_group.workers.scaling_config[0].desired_size
}

output "nodegroup_min_size" {
  value = aws_eks_node_group.workers.scaling_config[0].min_size
}

output "nodegroup_max_size" {
  value = aws_eks_node_group.workers.scaling_config[0].max_size
}

output "fargate_profile_status" {
  value = aws_eks_fargate_profile.serverless.status
}

output "addon_status" {
  value = aws_eks_addon.coredns.status
}

output "addon_version" {
  value = aws_eks_addon.coredns.addon_version
}

output "access_entry_arn" {
  value = aws_eks_access_entry.admin.access_entry_arn
}
