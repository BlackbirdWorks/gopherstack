//nolint:staticcheck // legacy request and operation fields are deprecated in the proto but still served
package gcpcontainer

import (
	"slices"
	"strings"

	"cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type poolTarget struct {
	name    string
	project string
	zone    string
	cluster string
	pool    string
}

// mutatePool runs apply on a node pool and records an operation of type typ for it.
func (b *InMemoryBackend) mutatePool(
	t poolTarget, typ containerpb.Operation_Type, apply func(c *clusterRec, cl *containerpb.Cluster, pr *poolRec) error,
) (*containerpb.Operation, error) {
	r, err := resolvePool(t.name, t.project, t.zone, t.cluster, t.pool)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("mutatePool")
	defer b.mu.Unlock()

	b.sweepLocked()

	c, err := b.liveClusterLocked(r)
	if err != nil {
		return nil, err
	}

	if c.Cluster.GetAutopilot().GetEnabled() {
		return nil, errPrecondition(c.Cluster.GetSelfLink(), "Node pools of Autopilot clusters are managed by Google.")
	}

	pr, ok := c.pool(r.pool)
	if !ok {
		return nil, errNotFound(r.poolName())
	}

	if err = b.conflictLocked(r, r.pool); err != nil {
		return nil, err
	}

	saved := clonePools(c.Pools)
	cl := clone(c.Cluster)

	if err = apply(c, cl, pr); err != nil {
		c.Pools = saved

		return nil, err
	}

	c.Cluster = cl

	dur := b.timing.Mutate
	if typ == containerpb.Operation_DELETE_NODE_POOL {
		dur = b.timing.Delete
	}

	o := b.newOpLocked(r, typ, kindPoolOp, poolSelfLink(r), dur, false)
	if typ == containerpb.Operation_DELETE_NODE_POOL {
		pr.DeleteOp = o.Op.GetName()
	}

	return b.opViewLocked(o), nil
}

func resolveClusterParent(parent, project, zone, cluster string) (ref, error) {
	if parent == "" {
		return resolveCluster("", project, zone, cluster)
	}

	ids, ok := parseRes(parent, kindProjects, kindLocations, kindClusters)
	if !ok {
		return ref{}, errInvalid(
			"parent",
			"Invalid parent %q: want projects/{project}/locations/{location}/clusters/{cluster}.",
			parent,
		)
	}

	return ref{project: ids[0], location: ids[1], cluster: ids[2]}, nil
}

// CreateNodePool adds a node pool to a cluster.
func (b *InMemoryBackend) CreateNodePool(req *containerpb.CreateNodePoolRequest) (*containerpb.Operation, error) {
	r, err := resolveClusterParent(
		req.GetParent(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
	)
	if err != nil {
		return nil, err
	}

	if req.GetNodePool() == nil {
		return nil, errInvalid("node_pool", "Node pool must be specified.")
	}

	b.mu.Lock("CreateNodePool")
	defer b.mu.Unlock()

	b.sweepLocked()

	c, err := b.liveClusterLocked(r)
	if err != nil {
		return nil, err
	}

	if c.Cluster.GetAutopilot().GetEnabled() {
		return nil, errPrecondition(c.Cluster.GetSelfLink(), "Autopilot clusters do not support creating node pools.")
	}

	pc := poolCtx{
		ref:     r,
		version: c.Cluster.GetCurrentMasterVersion(),
		zones:   c.Cluster.GetLocations(),
		autoUpdate: c.Cluster.GetReleaseChannel().
			GetChannel() !=
			containerpb.ReleaseChannel_UNSPECIFIED,
	}

	pr, err := b.buildPoolLocked(pc, req.GetNodePool())
	if err != nil {
		return nil, err
	}

	r.pool = pr.Pool.GetName()
	if _, exists := c.pool(r.pool); exists {
		return nil, errExists(r.poolName())
	}

	if err = b.conflictLocked(r, r.pool); err != nil {
		return nil, err
	}

	c.Pools = append(c.Pools, pr)
	o := b.newOpLocked(r, containerpb.Operation_CREATE_NODE_POOL, kindPoolOp, poolSelfLink(r), b.timing.Create, false)

	return b.opViewLocked(o), nil
}

// GetNodePool returns one node pool.
func (b *InMemoryBackend) GetNodePool(req *containerpb.GetNodePoolRequest) (*containerpb.NodePool, error) {
	r, err := resolvePool(
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
		req.GetNodePoolId(),
	)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetNodePool")
	defer b.mu.RUnlock()

	c, err := b.liveClusterLocked(r)
	if err != nil {
		return nil, err
	}

	pr, ok := c.pool(r.pool)
	if !ok || (pr.DeleteOp != "" && b.deleteDoneLocked(r.clusterKey(), pr.DeleteOp)) {
		return nil, errNotFound(r.poolName())
	}

	return b.poolViewLocked(c, pr), nil
}

// ListNodePools lists a cluster's node pools.
func (b *InMemoryBackend) ListNodePools(
	req *containerpb.ListNodePoolsRequest,
) (*containerpb.ListNodePoolsResponse, error) {
	r, err := resolveClusterParent(
		req.GetParent(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
	)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("ListNodePools")
	defer b.mu.RUnlock()

	c, err := b.liveClusterLocked(r)
	if err != nil {
		return nil, err
	}

	resp := &containerpb.ListNodePoolsResponse{}

	for _, pr := range c.Pools {
		if pr.DeleteOp == "" || !b.deleteDoneLocked(r.clusterKey(), pr.DeleteOp) {
			resp.NodePools = append(resp.NodePools, b.poolViewLocked(c, pr))
		}
	}

	return resp, nil
}

// DeleteNodePool starts deletion of a node pool.
func (b *InMemoryBackend) DeleteNodePool(req *containerpb.DeleteNodePoolRequest) (*containerpb.Operation, error) {
	t := poolTarget{
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
		req.GetNodePoolId(),
	}

	return b.mutatePool(
		t,
		containerpb.Operation_DELETE_NODE_POOL,
		func(_ *clusterRec, _ *containerpb.Cluster, _ *poolRec) error {
			return nil
		},
	)
}

// SetNodePoolSize resizes a node pool.
func (b *InMemoryBackend) SetNodePoolSize(req *containerpb.SetNodePoolSizeRequest) (*containerpb.Operation, error) {
	t := poolTarget{
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
		req.GetNodePoolId(),
	}

	return b.mutatePool(
		t,
		containerpb.Operation_SET_NODE_POOL_SIZE,
		func(_ *clusterRec, _ *containerpb.Cluster, pr *poolRec) error {
			if n := req.GetNodeCount(); n < 0 || n > maxNodeCount {
				return errInvalid("node_count", "Node count must be between 0 and %d.", maxNodeCount)
			}

			if pr.Pool.GetAutoscaling().GetEnabled() {
				return errPrecondition(
					pr.Pool.GetSelfLink(),
					"Node pool %s has autoscaling enabled; disable it before resizing.",
					pr.Pool.GetName(),
				)
			}

			pr.Size = req.GetNodeCount()

			return nil
		},
	)
}

func setPoolAutoscaling(pr *poolRec, a *containerpb.NodePoolAutoscaling) error {
	if a == nil {
		return errInvalid("autoscaling", "Autoscaling must be specified.")
	}

	if err := validAutoscaling(a); err != nil {
		return err
	}

	pr.Pool.Autoscaling = clone(a)

	if a.GetEnabled() {
		zones := int32(max(len(pr.Pool.GetLocations()), 1)) //nolint:gosec // at most a handful of zones
		lo, hi := a.GetMinNodeCount(), a.GetMaxNodeCount()

		if a.GetTotalMaxNodeCount() > 0 {
			lo, hi = (a.GetTotalMinNodeCount()+zones-1)/zones, a.GetTotalMaxNodeCount()/zones
		}

		pr.Size = min(max(pr.Size, lo), max(hi, lo))
	}

	return nil
}

// SetNodePoolAutoscaling configures a node pool's autoscaler.
func (b *InMemoryBackend) SetNodePoolAutoscaling(
	req *containerpb.SetNodePoolAutoscalingRequest,
) (*containerpb.Operation, error) {
	t := poolTarget{
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
		req.GetNodePoolId(),
	}

	return b.mutatePool(
		t,
		containerpb.Operation_UPDATE_CLUSTER,
		func(_ *clusterRec, _ *containerpb.Cluster, pr *poolRec) error {
			return setPoolAutoscaling(pr, req.GetAutoscaling())
		},
	)
}

// SetNodePoolManagement sets auto-upgrade and auto-repair.
func (b *InMemoryBackend) SetNodePoolManagement(
	req *containerpb.SetNodePoolManagementRequest,
) (*containerpb.Operation, error) {
	t := poolTarget{
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
		req.GetNodePoolId(),
	}

	return b.mutatePool(
		t,
		containerpb.Operation_SET_NODE_POOL_MANAGEMENT,
		func(_ *clusterRec, cl *containerpb.Cluster, pr *poolRec) error {
			m := req.GetManagement()
			if m == nil {
				return errInvalid("management", "Management must be specified.")
			}

			if !m.GetAutoUpgrade() &&
				cl.GetReleaseChannel().
					GetChannel() !=
					containerpb.ReleaseChannel_UNSPECIFIED {
				return errInvalid(
					"management.auto_upgrade",
					"Auto-upgrade cannot be disabled for node pools of a cluster on a release channel.",
				)
			}

			pr.Pool.Management = clone(m)

			return nil
		},
	)
}

// RollbackNodePoolUpgrade restores the version a node pool had before its last upgrade.
func (b *InMemoryBackend) RollbackNodePoolUpgrade(
	req *containerpb.RollbackNodePoolUpgradeRequest,
) (*containerpb.Operation, error) {
	t := poolTarget{
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
		req.GetNodePoolId(),
	}

	return b.mutatePool(
		t,
		containerpb.Operation_UPGRADE_NODES,
		func(_ *clusterRec, _ *containerpb.Cluster, pr *poolRec) error {
			if pr.PrevVersion == "" {
				return errPrecondition(
					pr.Pool.GetSelfLink(),
					"Node pool %s has no upgrade to roll back.",
					pr.Pool.GetName(),
				)
			}

			pr.Pool.Version, pr.PrevVersion = pr.PrevVersion, ""

			return nil
		},
	)
}

// poolFields are the UpdateNodePoolRequest fields handled explicitly; every other populated field must map onto a
// NodeConfig field of the same name and type.
var poolFields = map[string]bool{ //nolint:gochecknoglobals // immutable catalog
	"project_id": true, "zone": true, "cluster_id": true, "node_pool_id": true, "name": true, fieldEtag: true,
	"node_version": true, "image_type": true, "locations": true, "tags": true, "taints": true, "labels": true,
	"resource_labels": true, "upgrade_settings": true, "machine_type": true, "disk_type": true, "disk_size_gb": true,
	"node_network_config": true,
}

// targetFields identify the node pool or guard the request; they change nothing.
var targetFields = map[string]bool{ //nolint:gochecknoglobals // immutable catalog
	"project_id": true, "zone": true, "cluster_id": true, "node_pool_id": true, "name": true, fieldEtag: true,
}

// UpdateNodePool changes node pool settings.
func (b *InMemoryBackend) UpdateNodePool(req *containerpb.UpdateNodePoolRequest) (*containerpb.Operation, error) {
	t := poolTarget{req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId(), req.GetNodePoolId()}

	typ := containerpb.Operation_UPDATE_CLUSTER
	if req.GetNodeVersion() != "" {
		typ = containerpb.Operation_UPGRADE_NODES
	}

	return b.mutatePool(t, typ, func(c *clusterRec, cl *containerpb.Cluster, pr *poolRec) error {
		if e := req.GetEtag(); e != "" && e != etagOf(pr.Pool) {
			return errPrecondition(pr.Pool.GetSelfLink(), "The etag %q does not match the node pool's current etag.", e)
		}

		if err := b.applyPoolRequest(c, cl, pr, req); err != nil {
			return err
		}

		return validNodeConfig(b.compute, pr.Pool.GetConfig())
	})
}

func (b *InMemoryBackend) applyPoolRequest(
	c *clusterRec,
	cl *containerpb.Cluster,
	pr *poolRec,
	req *containerpb.UpdateNodePoolRequest,
) error {
	changes := 0

	req.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		if !targetFields[string(fd.Name())] {
			changes++
		}

		return true
	})

	if changes == 0 {
		return errInvalid("update", "No update was specified.")
	}

	if err := applyPoolConfig(pr.Pool.GetConfig(), req); err != nil {
		return err
	}

	if v := req.GetNodeVersion(); v != "" {
		if err := upgradeNodes(pr, v, cl.GetCurrentMasterVersion()); err != nil {
			return err
		}
	}

	if len(req.GetLocations()) > 0 {
		zones, err := poolZones(req.GetLocations(), cl.GetLocations())
		if err != nil {
			return err
		}

		pr.Pool.Locations = zones
		pr.Pool.InstanceGroupUrls = groupURLs(c.Project, zones, cl.GetName(), pr.Pool.GetName())
	}

	if us := req.GetUpgradeSettings(); us != nil {
		pr.Pool.UpgradeSettings = clone(us)
	}

	if nn := req.GetNodeNetworkConfig(); nn != nil {
		pr.Pool.NetworkConfig = clone(nn)
	}

	return applyGenericPool(pr.Pool.GetConfig(), req)
}

func applyPoolConfig(nc *containerpb.NodeConfig, req *containerpb.UpdateNodePoolRequest) error {
	if t := req.GetImageType(); t != "" {
		if !validImageType(t) {
			return errInvalid("image_type", "Image type %q is not supported.", t)
		}

		nc.ImageType = t
	}

	nc.MachineType = firstNonEmpty(req.GetMachineType(), nc.GetMachineType())
	nc.DiskType = firstNonEmpty(req.GetDiskType(), nc.GetDiskType())

	if d := req.GetDiskSizeGb(); d != 0 {
		nc.DiskSizeGb = int32(d) //nolint:gosec // range-checked by validNodeConfig afterwards
	}

	if req.GetTags() != nil {
		nc.Tags = slices.Clone(req.GetTags().GetTags())
	}

	if req.GetTaints() != nil {
		nc.Taints = cloneTaints(req.GetTaints().GetTaints())
	}

	if req.GetLabels() != nil {
		nc.Labels = req.GetLabels().GetLabels()
	}

	if req.GetResourceLabels() != nil {
		nc.ResourceLabels = req.GetResourceLabels().GetLabels()
	}

	return nil
}

func cloneTaints(in []*containerpb.NodeTaint) []*containerpb.NodeTaint {
	out := make([]*containerpb.NodeTaint, len(in))
	for i, t := range in {
		out[i] = clone(t)
	}

	return out
}

func applyGenericPool(nc *containerpb.NodeConfig, req *containerpb.UpdateNodePoolRequest) error {
	var err error

	req.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		name := string(fd.Name())
		if poolFields[name] {
			return true
		}

		if !fieldAccepts(nc.ProtoReflect(), name, fd) {
			err = errUnsupported(strings.TrimSpace(name))

			return false
		}

		set(nc.ProtoReflect(), name, fd, v)

		return true
	})

	return err
}
