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
# EKS Cluster
# --------------------------------------------------------------------------

resource "aws_eks_cluster" "main" {
  name     = "gopherstack-test-cluster"
  role_arn = "arn:aws:iam::000000000000:role/eks-cluster-role"
  version  = "1.32"

  vpc_config {
    subnet_ids = ["subnet-00000000", "subnet-00000001"]
  }

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

# --------------------------------------------------------------------------
# Managed node group
# --------------------------------------------------------------------------

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
    max_size     = 4
  }

  tags = {
    Environment = "test"
  }

  depends_on = [aws_eks_cluster.main]
}

# --------------------------------------------------------------------------
# EKS Add-on
# --------------------------------------------------------------------------

resource "aws_eks_addon" "coredns" {
  cluster_name = aws_eks_cluster.main.name
  addon_name   = "coredns"

  tags = {
    Component = "dns"
  }

  depends_on = [aws_eks_node_group.workers]
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

output "addon_status" {
  value = aws_eks_addon.coredns.status
}
