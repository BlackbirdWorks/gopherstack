package gcpcontainer

import (
	"context"

	"cloud.google.com/go/container/apiv1/containerpb"
)

type clusterManager struct {
	containerpb.UnimplementedClusterManagerServer
	b *InMemoryBackend
}

func (s *clusterManager) CreateCluster(
	_ context.Context,
	in *containerpb.CreateClusterRequest,
) (*containerpb.Operation, error) {
	return s.b.CreateCluster(in)
}

func (s *clusterManager) GetCluster(
	_ context.Context,
	in *containerpb.GetClusterRequest,
) (*containerpb.Cluster, error) {
	return s.b.GetCluster(in)
}

func (s *clusterManager) ListClusters(
	_ context.Context,
	in *containerpb.ListClustersRequest,
) (*containerpb.ListClustersResponse, error) {
	return s.b.ListClusters(in)
}

func (s *clusterManager) UpdateCluster(
	_ context.Context,
	in *containerpb.UpdateClusterRequest,
) (*containerpb.Operation, error) {
	return s.b.UpdateCluster(in)
}

func (s *clusterManager) DeleteCluster(
	_ context.Context,
	in *containerpb.DeleteClusterRequest,
) (*containerpb.Operation, error) {
	return s.b.DeleteCluster(in)
}

func (s *clusterManager) SetLabels(
	_ context.Context,
	in *containerpb.SetLabelsRequest,
) (*containerpb.Operation, error) {
	return s.b.SetLabels(in)
}

func (s *clusterManager) SetMasterAuth(
	_ context.Context,
	in *containerpb.SetMasterAuthRequest,
) (*containerpb.Operation, error) {
	return s.b.SetMasterAuth(in)
}

func (s *clusterManager) SetLegacyAbac(
	_ context.Context,
	in *containerpb.SetLegacyAbacRequest,
) (*containerpb.Operation, error) {
	return s.b.SetLegacyAbac(in)
}

func (s *clusterManager) SetNetworkPolicy(
	_ context.Context,
	in *containerpb.SetNetworkPolicyRequest,
) (*containerpb.Operation, error) {
	return s.b.SetNetworkPolicy(in)
}

func (s *clusterManager) SetMaintenancePolicy(
	_ context.Context,
	in *containerpb.SetMaintenancePolicyRequest,
) (*containerpb.Operation, error) {
	return s.b.SetMaintenancePolicy(in)
}

func (s *clusterManager) StartIPRotation(
	_ context.Context,
	in *containerpb.StartIPRotationRequest,
) (*containerpb.Operation, error) {
	return s.b.StartIPRotation(in)
}

func (s *clusterManager) CompleteIPRotation(
	_ context.Context,
	in *containerpb.CompleteIPRotationRequest,
) (*containerpb.Operation, error) {
	return s.b.CompleteIPRotation(in)
}

func (s *clusterManager) SetLoggingService(
	_ context.Context,
	in *containerpb.SetLoggingServiceRequest,
) (*containerpb.Operation, error) {
	return s.b.SetLoggingService(in)
}

func (s *clusterManager) SetMonitoringService(
	_ context.Context,
	in *containerpb.SetMonitoringServiceRequest,
) (*containerpb.Operation, error) {
	return s.b.SetMonitoringService(in)
}

func (s *clusterManager) SetAddonsConfig(
	_ context.Context,
	in *containerpb.SetAddonsConfigRequest,
) (*containerpb.Operation, error) {
	return s.b.SetAddonsConfig(in)
}

func (s *clusterManager) SetLocations(
	_ context.Context,
	in *containerpb.SetLocationsRequest,
) (*containerpb.Operation, error) {
	return s.b.SetLocations(in)
}

func (s *clusterManager) UpdateMaster(
	_ context.Context,
	in *containerpb.UpdateMasterRequest,
) (*containerpb.Operation, error) {
	return s.b.UpdateMaster(in)
}

func (s *clusterManager) CreateNodePool(
	_ context.Context,
	in *containerpb.CreateNodePoolRequest,
) (*containerpb.Operation, error) {
	return s.b.CreateNodePool(in)
}

func (s *clusterManager) GetNodePool(
	_ context.Context,
	in *containerpb.GetNodePoolRequest,
) (*containerpb.NodePool, error) {
	return s.b.GetNodePool(in)
}

func (s *clusterManager) ListNodePools(
	_ context.Context,
	in *containerpb.ListNodePoolsRequest,
) (*containerpb.ListNodePoolsResponse, error) {
	return s.b.ListNodePools(in)
}

func (s *clusterManager) UpdateNodePool(
	_ context.Context,
	in *containerpb.UpdateNodePoolRequest,
) (*containerpb.Operation, error) {
	return s.b.UpdateNodePool(in)
}

func (s *clusterManager) DeleteNodePool(
	_ context.Context,
	in *containerpb.DeleteNodePoolRequest,
) (*containerpb.Operation, error) {
	return s.b.DeleteNodePool(in)
}

func (s *clusterManager) SetNodePoolSize(
	_ context.Context,
	in *containerpb.SetNodePoolSizeRequest,
) (*containerpb.Operation, error) {
	return s.b.SetNodePoolSize(in)
}

func (s *clusterManager) SetNodePoolAutoscaling(
	_ context.Context,
	in *containerpb.SetNodePoolAutoscalingRequest,
) (*containerpb.Operation, error) {
	return s.b.SetNodePoolAutoscaling(in)
}

func (s *clusterManager) SetNodePoolManagement(
	_ context.Context,
	in *containerpb.SetNodePoolManagementRequest,
) (*containerpb.Operation, error) {
	return s.b.SetNodePoolManagement(in)
}

func (s *clusterManager) RollbackNodePoolUpgrade(
	_ context.Context,
	in *containerpb.RollbackNodePoolUpgradeRequest,
) (*containerpb.Operation, error) {
	return s.b.RollbackNodePoolUpgrade(in)
}

func (s *clusterManager) GetOperation(
	_ context.Context,
	in *containerpb.GetOperationRequest,
) (*containerpb.Operation, error) {
	return s.b.GetOperation(in)
}

func (s *clusterManager) ListOperations(
	_ context.Context,
	in *containerpb.ListOperationsRequest,
) (*containerpb.ListOperationsResponse, error) {
	return s.b.ListOperations(in)
}

func (s *clusterManager) GetServerConfig(
	_ context.Context,
	in *containerpb.GetServerConfigRequest,
) (*containerpb.ServerConfig, error) {
	return s.b.GetServerConfig(in)
}
