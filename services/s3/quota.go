package s3

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

const (
	quotaServiceCode = "s3"
	quotaBuckets     = "L-DC2B2D3D"
)

// SetQuotaLimiter makes CreateBucket honour the Service Quotas bucket limit; nil disables it.
func (b *InMemoryBackend) SetQuotaLimiter(f quotalimit.Func) { b.quotaGate.Set(f) }

// BucketCount returns the general-purpose buckets an account owns.
func (b *InMemoryBackend) BucketCount(account string) int {
	b.mu.RLock("BucketCount")
	defer b.mu.RUnlock()

	return b.bucketCountLocked(account)
}

func (b *InMemoryBackend) bucketCountLocked(account string) int {
	n := 0

	for _, bk := range b.buckets.All() {
		if !bk.IsDirectoryBucket && !bk.DeletePending && bk.OwnerAccountID == account {
			n++
		}
	}

	return n
}

func (b *InMemoryBackend) bucketLimit(ctx context.Context, region string) quotalimit.Limit {
	return b.quotaGate.Lookup(awsmeta.Account(ctx), region, quotaServiceCode, quotaBuckets)
}
