package ec2

const (
	rootDeviceSda1      = "/dev/sda1"
	platformWindows     = "windows"
	imdsSupportV2       = "v2.0"
	ownerBottlerocket   = "092701018921"
	ownerCanonical      = "099720109477"
	descECSOptimizedAL2 = "Amazon ECS-optimized Amazon Linux 2 AMI"
)

// publicCatalogAMIs are the images the SSM public parameter catalog points at,
// so those ids resolve through DescribeImages.
//
//nolint:gochecknoglobals // static catalog data
var publicCatalogAMIs = []AMIStub{
	{
		ImageID: "ami-0d761479256225605", Name: "al2023-ami-kernel-default-arm64",
		Description:  "Amazon Linux 2023 AMI (arm64)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-0f713cec7ef8c2ed1", Name: "al2023-ami-kernel-default-x86_64",
		Description:  "Amazon Linux 2023 AMI (x86_64)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-012ac7cd906ff79e4", Name: "al2023-ami-minimal-kernel-default-arm64",
		Description:  "Amazon Linux 2023 AMI (arm64)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-0e66287a6f64d09e5", Name: "al2023-ami-minimal-kernel-default-x86_64",
		Description:  "Amazon Linux 2023 AMI (x86_64)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-0aa0d966352c076f2", Name: "amzn2-ami-hvm-arm64-gp2",
		Description:  "Amazon Linux 2 AMI (arm64)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-02fa41501ee2dbe98", Name: "amzn2-ami-kernel-5.10-hvm-arm64-gp2",
		Description:  "Amazon Linux 2 AMI (arm64)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-0ad0780a5c6bae931", Name: "amzn2-ami-kernel-5.10-hvm-x86_64-gp2",
		Description:  "Amazon Linux 2 AMI (x86_64)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-07fa6e20713d5bbf9", Name: "Windows_Server-2016-English-Full-Base",
		Description:  "Windows Server 2016 English Full Base",
		Architecture: archX8664, RootDeviceName: rootDeviceSda1, OwnerID: imageOwnerAliasAmazon,
		Platform: platformWindows,
	},
	{
		ImageID: "ami-029f9997141abe243", Name: "Windows_Server-2019-English-Full-Base",
		Description:  "Windows Server 2019 English Full Base",
		Architecture: archX8664, RootDeviceName: rootDeviceSda1, OwnerID: imageOwnerAliasAmazon,
		Platform: platformWindows,
	},
	{
		ImageID: "ami-006d4b36f4a60ca5f", Name: "Windows_Server-2022-English-Core-Base",
		Description:  "Windows Server 2022 English Core Base",
		Architecture: archX8664, RootDeviceName: rootDeviceSda1, OwnerID: imageOwnerAliasAmazon,
		Platform: platformWindows,
	},
	{
		ImageID: "ami-03dfa776c79e64915", Name: "Windows_Server-2025-English-Full-Base",
		Description:  "Windows Server 2025 English Full Base",
		Architecture: archX8664, RootDeviceName: rootDeviceSda1, OwnerID: imageOwnerAliasAmazon,
		Platform: platformWindows,
	},
	{
		ImageID: "ami-0ef740f4805e7b007", Name: "bottlerocket-aws-ecs-1-arm64",
		Description:  "Bottlerocket OS for aws-ecs-1 (arm64)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: ownerBottlerocket,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-0a66ee52e0b6c6cdb", Name: "bottlerocket-aws-ecs-1-x86_64",
		Description:  "Bottlerocket OS for aws-ecs-1 (x86_64)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: ownerBottlerocket,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-0c4062fb18eafe99a", Name: "bottlerocket-aws-k8s-1.30-arm64",
		Description:  "Bottlerocket OS for aws-k8s-1.30 (arm64)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: ownerBottlerocket,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-01625ad2a5be54233", Name: "bottlerocket-aws-k8s-1.30-x86_64",
		Description:  "Bottlerocket OS for aws-k8s-1.30 (x86_64)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: ownerBottlerocket,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-038846ab014f5a783", Name: "bottlerocket-aws-k8s-1.31-arm64",
		Description:  "Bottlerocket OS for aws-k8s-1.31 (arm64)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: ownerBottlerocket,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-0e1bc505b96239262", Name: "bottlerocket-aws-k8s-1.31-x86_64",
		Description:  "Bottlerocket OS for aws-k8s-1.31 (x86_64)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: ownerBottlerocket,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-01d57e90f6d42b1e0", Name: "bottlerocket-aws-k8s-1.32-arm64",
		Description:  "Bottlerocket OS for aws-k8s-1.32 (arm64)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: ownerBottlerocket,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-0ecf6eac7f2835b1d", Name: "bottlerocket-aws-k8s-1.32-x86_64",
		Description:  "Bottlerocket OS for aws-k8s-1.32 (x86_64)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: ownerBottlerocket,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-057f486f0e508831b", Name: "ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server",
		Description:  "Canonical, Ubuntu, 20.04 LTS, amd64",
		Architecture: archX8664, RootDeviceName: rootDeviceSda1, OwnerID: ownerCanonical,
	},
	{
		ImageID: "ami-0772212b6c6aa24c4", Name: "ubuntu/images/hvm-ssd/ubuntu-focal-20.04-arm64-server",
		Description:  "Canonical, Ubuntu, 20.04 LTS, arm64",
		Architecture: archArm64, RootDeviceName: rootDeviceSda1, OwnerID: ownerCanonical,
	},
	{
		ImageID: "ami-06f4ad9548c59db32", Name: "ubuntu/images/hvm-ssd-gp3/ubuntu-jammy-22.04-amd64-server",
		Description:  "Canonical, Ubuntu, 22.04 LTS, amd64",
		Architecture: archX8664, RootDeviceName: rootDeviceSda1, OwnerID: ownerCanonical,
	},
	{
		ImageID: "ami-02ad62eb38bf6f140", Name: "ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-arm64-server",
		Description:  "Canonical, Ubuntu, 22.04 LTS, arm64",
		Architecture: archArm64, RootDeviceName: rootDeviceSda1, OwnerID: ownerCanonical,
	},
	{
		ImageID: "ami-0663e6754638bbff3", Name: "ubuntu/images/hvm-ssd/ubuntu-noble-24.04-amd64-server",
		Description:  "Canonical, Ubuntu, 24.04 LTS, amd64",
		Architecture: archX8664, RootDeviceName: rootDeviceSda1, OwnerID: ownerCanonical,
	},
	{
		ImageID: "ami-03dad1982e716eefd", Name: "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server",
		Description:  "Canonical, Ubuntu, 24.04 LTS, amd64",
		Architecture: archX8664, RootDeviceName: rootDeviceSda1, OwnerID: ownerCanonical,
	},
	{
		ImageID: "ami-0f7d407cafed29db8", Name: "ubuntu/images/hvm-ssd/ubuntu-noble-24.04-arm64-server",
		Description:  "Canonical, Ubuntu, 24.04 LTS, arm64",
		Architecture: archArm64, RootDeviceName: rootDeviceSda1, OwnerID: ownerCanonical,
	},
	{
		ImageID: "ami-030fa048780deae84", Name: "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-arm64-server",
		Description:  "Canonical, Ubuntu, 24.04 LTS, arm64",
		Architecture: archArm64, RootDeviceName: rootDeviceSda1, OwnerID: ownerCanonical,
	},
	{
		ImageID: "ami-05a45e02b22d3dc17", Name: "amzn2-ami-ecs-hvm-arm64-ebs",
		Description:  descECSOptimizedAL2,
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-0bf441d4e14a6f335", Name: "amzn2-ami-ecs-hvm-kernel-5.10-x86_64-ebs",
		Description:  descECSOptimizedAL2,
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-044b24a5e624f890d", Name: "amzn2-ami-ecs-hvm-x86_64-ebs",
		Description:  descECSOptimizedAL2,
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-04e934871fef181a3", Name: "al2023-ami-ecs-hvm-arm64",
		Description:  "Amazon ECS-optimized Amazon Linux 2023 AMI",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-0ee26b1d2c4433d12", Name: "al2023-ami-ecs-hvm-x86_64",
		Description:  "Amazon ECS-optimized Amazon Linux 2023 AMI",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-044904bd2a79763a7", Name: "amazon-eks-arm64-node-1.28",
		Description:  "Amazon EKS-optimized Amazon Linux 2 arm64 node AMI (Kubernetes 1.28)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-0756b4eb9507938c2", Name: "amazon-eks-gpu-node-1.28",
		Description:  "Amazon EKS-optimized Amazon Linux 2 GPU node AMI (Kubernetes 1.28)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-0fd95386f860283d7", Name: "amazon-eks-node-1.28",
		Description:  "Amazon EKS-optimized Amazon Linux 2 node AMI (Kubernetes 1.28)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-07cee3acd5a0b8ce6", Name: "amazon-eks-node-al2023-arm64-standard-1.28",
		Description:  "Amazon EKS-optimized Amazon Linux 2023 node AMI (Kubernetes 1.28)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-0526dbb21ac324235", Name: "amazon-eks-node-al2023-x86_64-standard-1.28",
		Description:  "Amazon EKS-optimized Amazon Linux 2023 node AMI (Kubernetes 1.28)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-0c10e336ffcf152fc", Name: "amazon-eks-arm64-node-1.29",
		Description:  "Amazon EKS-optimized Amazon Linux 2 arm64 node AMI (Kubernetes 1.29)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-0a5f5b9f36bd95142", Name: "amazon-eks-gpu-node-1.29",
		Description:  "Amazon EKS-optimized Amazon Linux 2 GPU node AMI (Kubernetes 1.29)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-0536323db358bf9a2", Name: "amazon-eks-node-1.29",
		Description:  "Amazon EKS-optimized Amazon Linux 2 node AMI (Kubernetes 1.29)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-036b4d4add3dd7c8d", Name: "amazon-eks-node-al2023-arm64-standard-1.29",
		Description:  "Amazon EKS-optimized Amazon Linux 2023 node AMI (Kubernetes 1.29)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-00472713bc478611e", Name: "amazon-eks-node-al2023-x86_64-standard-1.29",
		Description:  "Amazon EKS-optimized Amazon Linux 2023 node AMI (Kubernetes 1.29)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-0e9609af5b5871240", Name: "amazon-eks-arm64-node-1.30",
		Description:  "Amazon EKS-optimized Amazon Linux 2 arm64 node AMI (Kubernetes 1.30)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-0b662af5df45d366b", Name: "amazon-eks-gpu-node-1.30",
		Description:  "Amazon EKS-optimized Amazon Linux 2 GPU node AMI (Kubernetes 1.30)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-02d32a8013dc10914", Name: "amazon-eks-node-1.30",
		Description:  "Amazon EKS-optimized Amazon Linux 2 node AMI (Kubernetes 1.30)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-00899968de2db7bc9", Name: "amazon-eks-node-al2023-arm64-standard-1.30",
		Description:  "Amazon EKS-optimized Amazon Linux 2023 node AMI (Kubernetes 1.30)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-045158f5b6df2762f", Name: "amazon-eks-node-al2023-x86_64-standard-1.30",
		Description:  "Amazon EKS-optimized Amazon Linux 2023 node AMI (Kubernetes 1.30)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-0958db9f209496d65", Name: "amazon-eks-arm64-node-1.31",
		Description:  "Amazon EKS-optimized Amazon Linux 2 arm64 node AMI (Kubernetes 1.31)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-04268b63724a88e0b", Name: "amazon-eks-gpu-node-1.31",
		Description:  "Amazon EKS-optimized Amazon Linux 2 GPU node AMI (Kubernetes 1.31)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-03308f0cdc5696d64", Name: "amazon-eks-node-1.31",
		Description:  "Amazon EKS-optimized Amazon Linux 2 node AMI (Kubernetes 1.31)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-0fffa2535c71b5823", Name: "amazon-eks-node-al2023-arm64-standard-1.31",
		Description:  "Amazon EKS-optimized Amazon Linux 2023 node AMI (Kubernetes 1.31)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-06a917325c1a1716e", Name: "amazon-eks-node-al2023-x86_64-standard-1.31",
		Description:  "Amazon EKS-optimized Amazon Linux 2023 node AMI (Kubernetes 1.31)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-0a66d2cf88f4bdea8", Name: "amazon-eks-arm64-node-1.32",
		Description:  "Amazon EKS-optimized Amazon Linux 2 arm64 node AMI (Kubernetes 1.32)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-0e31d9c87b2285748", Name: "amazon-eks-gpu-node-1.32",
		Description:  "Amazon EKS-optimized Amazon Linux 2 GPU node AMI (Kubernetes 1.32)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-0b8442e34b53286aa", Name: "amazon-eks-node-1.32",
		Description:  "Amazon EKS-optimized Amazon Linux 2 node AMI (Kubernetes 1.32)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-06fb74d1948e572ad", Name: "amazon-eks-node-al2023-arm64-standard-1.32",
		Description:  "Amazon EKS-optimized Amazon Linux 2023 node AMI (Kubernetes 1.32)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-080d4e40a1c886316", Name: "amazon-eks-node-al2023-x86_64-standard-1.32",
		Description:  "Amazon EKS-optimized Amazon Linux 2023 node AMI (Kubernetes 1.32)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-07e87f60e82f2d020", Name: "amazon-eks-arm64-node-1.33",
		Description:  "Amazon EKS-optimized Amazon Linux 2 arm64 node AMI (Kubernetes 1.33)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-05cf2e661e76f6a85", Name: "amazon-eks-gpu-node-1.33",
		Description:  "Amazon EKS-optimized Amazon Linux 2 GPU node AMI (Kubernetes 1.33)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-0bb46607b6772395c", Name: "amazon-eks-node-1.33",
		Description:  "Amazon EKS-optimized Amazon Linux 2 node AMI (Kubernetes 1.33)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
	},
	{
		ImageID: "ami-0fd79a6c2178dbb24", Name: "amazon-eks-node-al2023-arm64-standard-1.33",
		Description:  "Amazon EKS-optimized Amazon Linux 2023 node AMI (Kubernetes 1.33)",
		Architecture: archArm64, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
	{
		ImageID: "ami-0277f73ec6cb10c9a", Name: "amazon-eks-node-al2023-x86_64-standard-1.33",
		Description:  "Amazon EKS-optimized Amazon Linux 2023 node AMI (Kubernetes 1.33)",
		Architecture: archX8664, RootDeviceName: defaultRootDeviceName, OwnerID: imageOwnerAliasAmazon,
		ImdsSupport: imdsSupportV2,
	},
}
