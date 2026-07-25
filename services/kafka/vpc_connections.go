package kafka

import (
	"context"
	"slices"
	"time"
)

// CreateVpcConnection creates a new VPC connection to an MSK cluster.
// clientSubnets/securityGroups are required fields on the real
// CreateVpcConnectionInput (see aws-sdk-go-v2/service/kafka's
// api_op_CreateVpcConnection.go); they are stored and echoed back by
// DescribeVpcConnection.
func (b *InMemoryBackend) CreateVpcConnection(
	ctx context.Context,
	targetClusterArn, vpcID, authentication string,
	clientSubnets, securityGroups []string,
	tags map[string]string,
) (*VpcConnection, error) {
	region := regionFromARN(targetClusterArn, getRegion(ctx, b.region))

	b.mu.Lock("CreateVpcConnection")
	defer b.mu.Unlock()

	if !b.clusters.Has(targetClusterArn) {
		return nil, ErrNotFound
	}

	vpcConnectionArn := b.vpcConnectionARN(region, targetClusterArn, vpcID)
	conn := &VpcConnection{
		VpcConnectionArn: vpcConnectionArn,
		TargetClusterArn: targetClusterArn,
		VpcID:            vpcID,
		Authentication:   authentication,
		State:            VpcConnectionStateAvailable,
		CreationTime:     time.Now().UTC().Format(time.RFC3339),
		SubnetIDs:        append([]string(nil), clientSubnets...),
		SecurityGroupIDs: append([]string(nil), securityGroups...),
		Tags:             nonNilTagsCopy(tags),
	}
	b.vpcConnections.Put(conn)

	return cloneVpcConnection(conn), nil
}

// DeleteVpcConnection deletes a VPC connection by ARN.
func (b *InMemoryBackend) DeleteVpcConnection(_ context.Context, vpcConnectionArn string) error {
	b.mu.Lock("DeleteVpcConnection")
	defer b.mu.Unlock()

	if !b.vpcConnections.Delete(vpcConnectionArn) {
		return ErrNotFound
	}

	return nil
}

// DescribeVpcConnection retrieves a VPC connection by ARN.
func (b *InMemoryBackend) DescribeVpcConnection(_ context.Context, vpcConnectionArn string) (*VpcConnection, error) {
	b.mu.RLock("DescribeVpcConnection")
	defer b.mu.RUnlock()

	v, ok := b.vpcConnections.Get(vpcConnectionArn)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneVpcConnection(v), nil
}

// ListVpcConnections returns all VPC connections in the request's region sorted by ARN.
func (b *InMemoryBackend) ListVpcConnections(ctx context.Context) []*VpcConnection {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListVpcConnections")
	defer b.mu.RUnlock()

	conns := b.vpcConnectionsByRegion.Get(region)
	out := make([]*VpcConnection, 0, len(conns))
	for _, v := range conns {
		out = append(out, cloneVpcConnection(v))
	}

	slices.SortFunc(out, func(a, b *VpcConnection) int {
		if a.VpcConnectionArn < b.VpcConnectionArn {
			return -1
		}
		if a.VpcConnectionArn > b.VpcConnectionArn {
			return 1
		}

		return 0
	})

	return out
}

// ListClientVpcConnections returns all VPC connections for a given cluster.
func (b *InMemoryBackend) ListClientVpcConnections(_ context.Context, clusterArn string) ([]*VpcConnection, error) {
	b.mu.RLock("ListClientVpcConnections")
	defer b.mu.RUnlock()

	return collectClusterChildrenLocked(
		b.clusters,
		b.vpcConnectionsByCluster,
		clusterArn,
		cloneVpcConnection,
		func(v *VpcConnection) string { return v.VpcConnectionArn },
	)
}

// RejectClientVpcConnection rejects (deletes) a VPC connection.
func (b *InMemoryBackend) RejectClientVpcConnection(ctx context.Context, vpcConnectionArn string) error {
	return b.DeleteVpcConnection(ctx, vpcConnectionArn)
}

// AddVpcConnectionInternal creates a VPC connection directly for testing purposes.
func (b *InMemoryBackend) AddVpcConnectionInternal(clusterArn, vpcID string) *VpcConnection {
	b.mu.Lock("AddVpcConnectionInternal")
	defer b.mu.Unlock()

	region := regionFromARN(clusterArn, b.region)
	vpcConnectionArn := b.vpcConnectionARN(region, clusterArn, vpcID)
	conn := &VpcConnection{
		VpcConnectionArn: vpcConnectionArn,
		TargetClusterArn: clusterArn,
		VpcID:            vpcID,
		State:            VpcConnectionStateAvailable,
		CreationTime:     time.Now().UTC().Format(time.RFC3339),
		Tags:             make(map[string]string),
	}
	b.vpcConnections.Put(conn)

	return cloneVpcConnection(conn)
}

// cloneVpcConnection creates a deep copy of a VpcConnection.
func cloneVpcConnection(v *VpcConnection) *VpcConnection {
	return &VpcConnection{
		VpcConnectionArn: v.VpcConnectionArn,
		TargetClusterArn: v.TargetClusterArn,
		VpcID:            v.VpcID,
		Authentication:   v.Authentication,
		State:            v.State,
		CreationTime:     v.CreationTime,
		SubnetIDs:        append([]string(nil), v.SubnetIDs...),
		SecurityGroupIDs: append([]string(nil), v.SecurityGroupIDs...),
		Tags:             nonNilTagsCopy(v.Tags),
	}
}
