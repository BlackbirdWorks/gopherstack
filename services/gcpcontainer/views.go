//nolint:staticcheck // legacy cluster fields are deprecated in the proto but still served
package gcpcontainer

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/protobuf/proto"
)

const (
	etagBytes = 8
	maxZones  = 16
)

func etagOf(m proto.Message) string {
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		return ""
	}

	sum := sha256.Sum256(raw)

	return hex.EncodeToString(sum[:etagBytes])
}

func (b *InMemoryBackend) poolStatusLocked(c *clusterRec, p *poolRec, active []*opRec) containerpb.NodePool_Status {
	if p.DeleteOp != "" || c.DeleteOp != "" {
		return containerpb.NodePool_STOPPING
	}

	if c.Failure != "" {
		return containerpb.NodePool_ERROR
	}

	st := containerpb.NodePool_RUNNING

	for _, o := range active {
		switch {
		case o.Kind == kindCreate, o.Kind == kindPoolOp && o.Pool == p.Pool.GetName() &&
			o.Op.GetOperationType() == containerpb.Operation_CREATE_NODE_POOL:
			return containerpb.NodePool_PROVISIONING
		case o.Kind == kindClusterOp && o.Op.GetOperationType() == containerpb.Operation_UPGRADE_NODES,
			o.Kind == kindPoolOp && o.Pool == p.Pool.GetName():
			st = containerpb.NodePool_RECONCILING
		}
	}

	return st
}

func (b *InMemoryBackend) clusterStatusLocked(c *clusterRec, active []*opRec) containerpb.Cluster_Status {
	switch {
	case c.DeleteOp != "":
		return containerpb.Cluster_STOPPING
	case c.Failure != "":
		return containerpb.Cluster_ERROR
	}

	for _, o := range active {
		if o.Kind == kindCreate {
			return containerpb.Cluster_PROVISIONING
		}
	}

	if len(active) > 0 {
		return containerpb.Cluster_RECONCILING
	}

	return containerpb.Cluster_RUNNING
}

func poolView(p *poolRec, st containerpb.NodePool_Status) *containerpb.NodePool {
	v := clone(p.Pool)
	v.Status = st
	v.Etag = etagOf(p.Pool)

	return v
}

func (b *InMemoryBackend) poolViewLocked(c *clusterRec, p *poolRec) *containerpb.NodePool {
	active := b.activeOpsLocked(c.Project, c.Cluster.GetLocation(), c.Cluster.GetName())

	return poolView(p, b.poolStatusLocked(c, p, active))
}

func (b *InMemoryBackend) clusterViewLocked(c *clusterRec) *containerpb.Cluster {
	active := b.activeOpsLocked(c.Project, c.Cluster.GetLocation(), c.Cluster.GetName())
	v := clone(c.Cluster)
	v.Status = b.clusterStatusLocked(c, active)
	v.StatusMessage = c.Failure
	v.Etag = etagOf(c.Cluster)
	v.NodePools = nil
	v.InstanceGroupUrls = nil
	v.CurrentNodeCount = 0

	oldest := ""

	for _, p := range c.Pools {
		v.NodePools = append(v.NodePools, poolView(p, b.poolStatusLocked(c, p, active)))
		v.InstanceGroupUrls = append(v.InstanceGroupUrls, p.Pool.GetInstanceGroupUrls()...)
		zones := min(max(len(p.Pool.GetLocations()), 1), maxZones)
		v.CurrentNodeCount += p.Size * int32(zones)

		if oldest == "" || compareVersions(p.Pool.GetVersion(), oldest) < 0 {
			oldest = p.Pool.GetVersion()
		}
	}

	if oldest != "" {
		v.CurrentNodeVersion = oldest
	}

	slices.Sort(v.GetInstanceGroupUrls())

	return v
}
