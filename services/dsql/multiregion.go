package dsql

import (
	"slices"
	"time"
)

const statusPendingSetup = "PENDING_SETUP"

// validatePeersLocked checks every peer ARN in m resolves to a live cluster
// in a different Region that shares m's witness Region. Caller holds b.mu.
func (b *InMemoryBackend) validatePeersLocked(m *MultiRegionProperties, self *Cluster, region string) error {
	if m == nil {
		return nil
	}

	if m.WitnessRegion == "" && len(m.Clusters) > 0 {
		return ErrValidation
	}

	for _, peerARN := range m.Clusters {
		id, ok := clusterIdentifierFromResourceARN(peerARN)
		if !ok {
			return ErrValidation
		}

		peer, ok := b.clusters.Get(id)
		if !ok {
			return ErrClusterNotFound
		}

		if (self != nil && peer.Identifier == self.Identifier) || peer.Region == region {
			return ErrValidation
		}

		if peer.MultiRegion == nil || peer.MultiRegion.WitnessRegion != m.WitnessRegion {
			return ErrValidation
		}
	}

	return nil
}

// mutuallyPeeredLocked reports whether c lists a peer that lists c back.
func (b *InMemoryBackend) mutuallyPeeredLocked(c *Cluster) bool {
	if c.MultiRegion == nil {
		return false
	}

	for _, peerARN := range c.MultiRegion.Clusters {
		id, ok := clusterIdentifierFromResourceARN(peerARN)
		if !ok {
			continue
		}

		if peer, found := b.clusters.Get(id); found && peer.MultiRegion != nil &&
			slices.Contains(peer.MultiRegion.Clusters, c.ARN) {
			return true
		}
	}

	return false
}

// activatePeersLocked moves PENDING_SETUP peers of c that are now mutually
// peered with c into UPDATING, from which they settle to ACTIVE.
func (b *InMemoryBackend) activatePeersLocked(c *Cluster, until time.Time) {
	if c.MultiRegion == nil {
		return
	}

	for _, peerARN := range c.MultiRegion.Clusters {
		id, ok := clusterIdentifierFromResourceARN(peerARN)
		if !ok {
			continue
		}

		peer, found := b.clusters.Get(id)
		if !found || peer.Status != statusPendingSetup || !b.mutuallyPeeredLocked(peer) {
			continue
		}

		peer.Status = statusUpdating
		peer.PendingUntil = until
	}
}

// unlinkPeersLocked removes c's ARN from every peer's peer list.
func (b *InMemoryBackend) unlinkPeersLocked(c *Cluster) {
	for _, other := range b.clusters.All() {
		if other.MultiRegion != nil {
			other.MultiRegion.Clusters = slices.DeleteFunc(other.MultiRegion.Clusters, func(a string) bool {
				return a == c.ARN
			})
		}
	}
}
