//nolint:staticcheck // legacy request fields are deprecated in the proto but still served
package gcpcontainer

import (
	"cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

const cancelledFailure = "Operation was cancelled."

func upgradeOp(o *opRec, typ containerpb.Operation_Type) bool {
	return o.Op.GetOperationType() == typ
}

// CancelOperation cancels a running upgrade operation.
func (b *InMemoryBackend) CancelOperation(req *containerpb.CancelOperationRequest) (*emptypb.Empty, error) {
	r, name, err := resolveOperation(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetOperationId())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CancelOperation")
	defer b.mu.Unlock()

	o, ok := b.ops[opKey(r.project, r.location, name)]
	if !ok {
		return nil, errNotFound(locationPath(r.project, r.location) + "/" + kindOps + "/" + name)
	}

	if b.opDoneLocked(o) {
		return nil, errPrecondition(o.Op.GetSelfLink(), "Operation %s is already done.", name)
	}

	if !upgradeOp(o, containerpb.Operation_UPGRADE_MASTER) && !upgradeOp(o, containerpb.Operation_UPGRADE_NODES) {
		return nil, errPrecondition(
			o.Op.GetSelfLink(), "Operation %s of type %s cannot be cancelled.", name, o.Op.GetOperationType())
	}

	b.finishOpLocked(o, cancelledFailure)

	return &emptypb.Empty{}, nil
}

func (b *InMemoryBackend) runningUpgradeLocked(r ref, typ containerpb.Operation_Type, pool string) *opRec {
	for _, o := range b.activeOpsLocked(r.project, r.location, r.cluster) {
		if upgradeOp(o, typ) && (pool == "" || o.Pool == "" || o.Pool == pool) {
			return o
		}
	}

	return nil
}

// CompleteNodePoolUpgrade finishes the running upgrade of a node pool right away.
func (b *InMemoryBackend) CompleteNodePoolUpgrade(
	req *containerpb.CompleteNodePoolUpgradeRequest,
) (*emptypb.Empty, error) {
	r, err := resolvePool(req.GetName(), "", "", "", "")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CompleteNodePoolUpgrade")
	defer b.mu.Unlock()

	c, err := b.liveClusterLocked(r)
	if err != nil {
		return nil, err
	}

	pr, ok := c.pool(r.pool)
	if !ok || pr.DeleteOp != "" {
		return nil, errNotFound(r.clusterName() + "/" + kindNodePools + "/" + r.pool)
	}

	o := b.runningUpgradeLocked(r, containerpb.Operation_UPGRADE_NODES, r.pool)
	if o == nil {
		return nil, errPrecondition(pr.Pool.GetSelfLink(), "Node pool %s has no upgrade in progress.", r.pool)
	}

	b.finishOpLocked(o, "")

	return &emptypb.Empty{}, nil
}

// CompleteControlPlaneUpgrade finishes the running control plane upgrade of a cluster right away.
func (b *InMemoryBackend) CompleteControlPlaneUpgrade(
	req *containerpb.CompleteControlPlaneUpgradeRequest,
) (*containerpb.Operation, error) {
	r, err := resolveCluster(req.GetName(), "", "", "")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CompleteControlPlaneUpgrade")
	defer b.mu.Unlock()

	c, err := b.liveClusterLocked(r)
	if err != nil {
		return nil, err
	}

	if v := req.GetVersion(); v != "" && v != c.Cluster.GetCurrentMasterVersion() {
		return nil, errInvalid("version", "Version %q is not the version being upgraded to.", v)
	}

	o := b.runningUpgradeLocked(r, containerpb.Operation_UPGRADE_MASTER, "")
	if o == nil {
		return nil, errPrecondition(
			c.Cluster.GetSelfLink(),
			"Cluster %s has no control plane upgrade in progress.",
			r.cluster,
		)
	}

	b.finishOpLocked(o, "")

	return b.opViewLocked(o), nil
}

// upgradeTargets returns the newest minor and patch versions the cluster could move to from current.
func upgradeTargets(channel containerpb.ReleaseChannel_Channel, current string) (string, string) {
	cands := validMasterVersions()
	if spec, ok := channelByEnum(channel); ok {
		cands = sortedDesc(spec.versions)
	}

	var minorTarget, patchTarget string

	for _, v := range cands {
		if compareVersions(v, current) <= 0 {
			continue
		}

		if minor(v) == minor(current) && patchTarget == "" {
			patchTarget = v
		}

		if minor(v) > minor(current) && minorTarget == "" {
			minorTarget = v
		}
	}

	return minorTarget, patchTarget
}

// exclusionScope returns the scope of the strongest maintenance exclusion in effect now.
func (b *InMemoryBackend) exclusionScope(
	cl *containerpb.Cluster,
) (containerpb.MaintenanceExclusionOptions_Scope, bool) {
	now := b.now()
	found := false
	scope := containerpb.MaintenanceExclusionOptions_NO_MINOR_UPGRADES

	for _, w := range cl.GetMaintenancePolicy().GetWindow().GetMaintenanceExclusions() {
		if now.Before(w.GetStartTime().AsTime()) || !now.Before(w.GetEndTime().AsTime()) {
			continue
		}

		found = true

		if w.GetMaintenanceExclusionOptions().GetScope() == containerpb.MaintenanceExclusionOptions_NO_UPGRADES {
			scope = containerpb.MaintenanceExclusionOptions_NO_UPGRADES
		}
	}

	return scope, found
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}

	return &s
}

func clusterAutoUpgrade(
	scope containerpb.MaintenanceExclusionOptions_Scope, paused bool,
) ([]containerpb.ClusterUpgradeInfo_AutoUpgradeStatus, []containerpb.ClusterUpgradeInfo_AutoUpgradePausedReason) {
	switch {
	case !paused:
		return []containerpb.ClusterUpgradeInfo_AutoUpgradeStatus{containerpb.ClusterUpgradeInfo_ACTIVE}, nil
	case scope == containerpb.MaintenanceExclusionOptions_NO_UPGRADES:
		return []containerpb.ClusterUpgradeInfo_AutoUpgradeStatus{containerpb.ClusterUpgradeInfo_UPGRADE_PAUSED},
			[]containerpb.ClusterUpgradeInfo_AutoUpgradePausedReason{
				containerpb.ClusterUpgradeInfo_MAINTENANCE_EXCLUSION_NO_UPGRADES,
			}
	}

	return []containerpb.ClusterUpgradeInfo_AutoUpgradeStatus{containerpb.ClusterUpgradeInfo_MINOR_UPGRADE_PAUSED},
		[]containerpb.ClusterUpgradeInfo_AutoUpgradePausedReason{
			containerpb.ClusterUpgradeInfo_MAINTENANCE_EXCLUSION_NO_MINOR_UPGRADES,
		}
}

func poolAutoUpgrade(
	scope containerpb.MaintenanceExclusionOptions_Scope, paused bool,
) ([]containerpb.NodePoolUpgradeInfo_AutoUpgradeStatus, []containerpb.NodePoolUpgradeInfo_AutoUpgradePausedReason) {
	switch {
	case !paused:
		return []containerpb.NodePoolUpgradeInfo_AutoUpgradeStatus{containerpb.NodePoolUpgradeInfo_ACTIVE}, nil
	case scope == containerpb.MaintenanceExclusionOptions_NO_UPGRADES:
		return []containerpb.NodePoolUpgradeInfo_AutoUpgradeStatus{containerpb.NodePoolUpgradeInfo_UPGRADE_PAUSED},
			[]containerpb.NodePoolUpgradeInfo_AutoUpgradePausedReason{
				containerpb.NodePoolUpgradeInfo_MAINTENANCE_EXCLUSION_NO_UPGRADES,
			}
	}

	return []containerpb.NodePoolUpgradeInfo_AutoUpgradeStatus{containerpb.NodePoolUpgradeInfo_MINOR_UPGRADE_PAUSED},
		[]containerpb.NodePoolUpgradeInfo_AutoUpgradePausedReason{
			containerpb.NodePoolUpgradeInfo_MAINTENANCE_EXCLUSION_NO_MINOR_UPGRADES,
		}
}

// FetchClusterUpgradeInfo reports the upgrade targets and auto-upgrade state of a cluster.
func (b *InMemoryBackend) FetchClusterUpgradeInfo(
	req *containerpb.FetchClusterUpgradeInfoRequest,
) (*containerpb.ClusterUpgradeInfo, error) {
	r, err := resolveCluster(req.GetName(), "", "", "")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("FetchClusterUpgradeInfo")
	defer b.mu.RUnlock()

	c, err := b.liveClusterLocked(r)
	if err != nil {
		return nil, err
	}

	cl := c.Cluster
	minorV, patchV := upgradeTargets(cl.GetReleaseChannel().GetChannel(), cl.GetCurrentMasterVersion())
	scope, paused := b.exclusionScope(cl)
	status, reason := clusterAutoUpgrade(scope, paused)

	return &containerpb.ClusterUpgradeInfo{
		MinorTargetVersion: strPtr(minorV), PatchTargetVersion: strPtr(patchV),
		AutoUpgradeStatus: status, PausedReason: reason,
	}, nil
}

// FetchNodePoolUpgradeInfo reports the upgrade targets and auto-upgrade state of a node pool.
func (b *InMemoryBackend) FetchNodePoolUpgradeInfo(
	req *containerpb.FetchNodePoolUpgradeInfoRequest,
) (*containerpb.NodePoolUpgradeInfo, error) {
	r, err := resolvePool(req.GetName(), "", "", "", "")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("FetchNodePoolUpgradeInfo")
	defer b.mu.RUnlock()

	c, err := b.liveClusterLocked(r)
	if err != nil {
		return nil, err
	}

	pr, ok := c.pool(r.pool)
	if !ok || pr.DeleteOp != "" {
		return nil, errNotFound(r.clusterName() + "/" + kindNodePools + "/" + r.pool)
	}

	minorV, patchV := upgradeTargets(c.Cluster.GetReleaseChannel().GetChannel(), pr.Pool.GetVersion())
	scope, paused := b.exclusionScope(c.Cluster)
	status, reason := poolAutoUpgrade(scope, paused)

	return &containerpb.NodePoolUpgradeInfo{
		MinorTargetVersion: strPtr(minorV), PatchTargetVersion: strPtr(patchV),
		AutoUpgradeStatus: status, PausedReason: reason,
	}, nil
}
