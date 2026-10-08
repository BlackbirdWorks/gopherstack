package redshift

import (
	"fmt"
	"strings"
)

// validateNewClusterIDLocked checks a ModifyCluster NewClusterIdentifier.
func (b *InMemoryBackend) validateNewClusterIDLocked(oldID, newID string) error {
	if newID == "" || newID == oldID {
		return nil
	}

	if err := validateClusterID(newID); err != nil {
		return err
	}

	if b.clusters.Has(newID) {
		return fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, newID)
	}

	return nil
}

// renameClusterLocked re-keys a cluster and every store that references it by identifier.
// Manual snapshots keep their source ClusterIdentifier.
func (b *InMemoryBackend) renameClusterLocked(oldID, newID string) {
	cluster, ok := b.clusters.Get(oldID)
	if !ok || newID == "" || newID == oldID {
		return
	}

	b.clusters.Delete(oldID)

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Deregister(cluster.Endpoint)
	}

	cluster.ClusterIdentifier = newID
	cluster.Endpoint = strings.Replace(cluster.Endpoint, oldID+".", newID+".", 1)
	b.clusters.Put(cluster)

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(cluster.Endpoint)
	}

	rekeyMapLocked(b.activeResizes, oldID, newID)
	rekeyMapLocked(b.loggingStatuses, oldID, newID)
	rekeyMapLocked(b.snapshotCopyConfigs, oldID, newID)
	rekeyMapLocked(b.clusterTransitions, oldID, newID)

	b.renameClusterRefsLocked(oldID, newID)
}

func rekeyMapLocked[V any](m map[string]V, oldID, newID string) {
	if v, ok := m[oldID]; ok {
		delete(m, oldID)
		m[newID] = v
	}
}

func (b *InMemoryBackend) renameClusterRefsLocked(oldID, newID string) {
	for _, p := range b.partners.All() {
		if p.ClusterIdentifier == oldID {
			b.partners.Delete(partnersKeyFn(p))
			p.ClusterIdentifier = newID
			b.partners.Put(p)
		}
	}

	for _, a := range b.endpointAuths.All() {
		if a.ClusterIdentifier == oldID {
			b.endpointAuths.Delete(endpointAuthsKeyFn(a))
			a.ClusterIdentifier = newID
			b.endpointAuths.Put(a)
		}
	}

	for _, d := range b.customDomains.All() {
		if d.ClusterIdentifier == oldID {
			b.customDomains.Delete(customDomainsKeyFn(d))
			d.ClusterIdentifier = newID
			b.customDomains.Put(d)
		}
	}

	if cfg, ok := b.clusterLakehouseConfig.Get(oldID); ok {
		b.clusterLakehouseConfig.Delete(oldID)
		cfg.ClusterIdentifier = newID
		b.clusterLakehouseConfig.Put(cfg)
	}

	for _, ul := range b.usageLimits.All() {
		if ul.ClusterIdentifier == oldID {
			ul.ClusterIdentifier = newID
		}
	}

	for _, ep := range b.endpointAccesses.All() {
		if ep.ClusterIdentifier == oldID {
			ep.ClusterIdentifier = newID
		}
	}

	for _, tr := range b.tableRestores.All() {
		if tr.ClusterIdentifier == oldID {
			tr.ClusterIdentifier = newID
		}
	}

	for _, sa := range b.scheduledActions.All() {
		retargetScheduledAction(sa.TargetAction, oldID, newID)
	}

	b.renameClusterPolicyLocked(oldID, newID)
}

func retargetScheduledAction(t *ScheduledActionTarget, oldID, newID string) {
	if t == nil {
		return
	}

	if t.PauseCluster != nil && t.PauseCluster.ClusterIdentifier == oldID {
		t.PauseCluster.ClusterIdentifier = newID
	}

	if t.ResumeCluster != nil && t.ResumeCluster.ClusterIdentifier == oldID {
		t.ResumeCluster.ClusterIdentifier = newID
	}

	if t.ResizeCluster != nil && t.ResizeCluster.ClusterIdentifier == oldID {
		t.ResizeCluster.ClusterIdentifier = newID
	}
}

func (b *InMemoryBackend) renameClusterPolicyLocked(oldID, newID string) {
	oldArn := b.arnLocked(tagTypeCluster, oldID)

	if pol, ok := b.resourcePolicies.Get(oldArn); ok {
		b.resourcePolicies.Delete(oldArn)
		pol.ResourceArn = b.arnLocked(tagTypeCluster, newID)
		b.resourcePolicies.Put(pol)
	}
}
