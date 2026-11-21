package rds

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

const (
	quotaServiceCode = "rds"
	quotaDBInstances = "L-7B6409FD"
	quotaDBClusters  = "L-952B80B8"
)

// SetQuotaLimiter makes CreateDBInstance and CreateDBCluster honour Service Quotas in every Region; nil disables it.
func (h *Handler) SetQuotaLimiter(f quotalimit.Func) { h.Backend.quotaGate.Set(f) }

// InstanceCount returns the DB instances held in region; a Region never used holds none.
func (h *Handler) InstanceCount(region string) int {
	if b := h.existingBackend(region); b != nil {
		return b.InstanceCount()
	}

	return 0
}

// ClusterCount returns the DB clusters held in region; a Region never used holds none.
func (h *Handler) ClusterCount(region string) int {
	if b := h.existingBackend(region); b != nil {
		return b.ClusterCount()
	}

	return 0
}

func (h *Handler) existingBackend(region string) *InMemoryBackend {
	h.mu.Lock()
	defer h.mu.Unlock()

	if region == "" {
		return h.Backend
	}

	return h.backends[region]
}

// InstanceCount returns the DB instances in the backend's Region.
func (b *InMemoryBackend) InstanceCount() int {
	b.mu.RLock("InstanceCount")
	defer b.mu.RUnlock()

	return len(b.instances.All())
}

// ClusterCount returns the DB clusters in the backend's Region.
func (b *InMemoryBackend) ClusterCount() int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return len(b.clusters.All())
}

func (b *InMemoryBackend) quotaLimit(code string) quotalimit.Limit {
	return b.quotaGate.Lookup(b.accountID, b.region, quotaServiceCode, code)
}

func quotaExceededErr(sentinel error, kind string, limit quotalimit.Limit) error {
	return fmt.Errorf("%w: the account already has the maximum number of %s (%.0f)", sentinel, kind, limit.Value)
}

func (b *InMemoryBackend) checkNewClusterLocked(id string, quota quotalimit.Limit) error {
	if _, exists := b.clusters.Get(normalizeID(id)); exists {
		return fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, id)
	}

	if quota.Reached(len(b.clusters.All())) {
		return quotaExceededErr(ErrClusterQuotaExceeded, "DB clusters", quota)
	}

	return nil
}
