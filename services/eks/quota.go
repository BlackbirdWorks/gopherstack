package eks

import "github.com/blackbirdworks/gopherstack/pkgs/quotalimit"

const (
	quotaServiceCode = "eks"
	quotaClusters    = "L-1194D53C"
)

// SetQuotaLimiter makes CreateCluster honour the Service Quotas cluster limit; nil restores the built-in cap.
func (b *InMemoryBackend) SetQuotaLimiter(f quotalimit.Func) { b.quotaGate.Set(f) }

// ClusterTotal returns the number of EKS-provisioned (not registered) clusters.
func (b *InMemoryBackend) ClusterTotal() int {
	b.mu.RLock("ClusterTotal")
	defer b.mu.RUnlock()

	return b.countClusters(false)
}
