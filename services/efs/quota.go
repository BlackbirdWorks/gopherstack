package efs

import "github.com/blackbirdworks/gopherstack/pkgs/quotalimit"

const (
	quotaServiceCode = "elasticfilesystem"
	quotaFileSystems = "L-848C634D"
)

// SetQuotaLimiter makes CreateFileSystem honour the Service Quotas file system limit; nil restores the built-in cap.
func (h *Handler) SetQuotaLimiter(f quotalimit.Func) { h.Backend.quotaGate.Set(f) }

// FileSystemCount returns the file systems held in region; empty region means the backend's.
func (h *Handler) FileSystemCount(region string) int {
	b := h.Backend
	if region == "" {
		region = b.region
	}

	b.mu.RLock("FileSystemCount")
	defer b.mu.RUnlock()

	return len(b.fileSystemsByRegion.Get(region))
}

func (b *InMemoryBackend) fileSystemLimit(region string) quotalimit.Limit {
	return b.quotaGate.Lookup(b.accountID, region, quotaServiceCode, quotaFileSystems)
}

func (b *InMemoryBackend) fileSystemCap(limit quotalimit.Limit) int {
	if limit.Set {
		return int(limit.Value)
	}

	return b.limits.fileSystemsPerAccount
}
