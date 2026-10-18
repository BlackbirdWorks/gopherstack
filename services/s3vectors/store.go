// Package s3vectors emulates Amazon S3 Vectors: vector buckets, indexes and exact nearest-neighbour search.
package s3vectors

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName   = "s3vectors"
	defaultRegion = "us-east-1"
	modeClassic   = "CLASSIC"
	modeEnhanced  = "ENHANCED"
)

// classicCutoff is the date from which new vector buckets can no longer use CLASSIC indexes.
func classicCutoff() time.Time { return time.Date(2026, time.September, 30, 0, 0, 0, 0, time.UTC) }

// InMemoryBackend is the S3 Vectors state; one coarse lock guards every table.
type InMemoryBackend struct {
	buckets   *store.Table[Bucket]
	indexes   *store.Table[Index]
	vectors   *store.Table[VectorSet]
	registry  *store.Registry
	mu        *lockmetrics.RWMutex
	now       func() time.Time
	accountID string
	region    string
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	if region == "" {
		region = defaultRegion
	}

	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New(serviceName),
		now:       func() time.Time { return time.Now().UTC() },
		accountID: accountID,
		region:    region,
	}

	b.buckets = store.Register(b.registry, "vectorBuckets", store.New(bucketKey))
	b.indexes = store.Register(b.registry, "vectorIndexes", store.New(indexKey))
	b.vectors = store.Register(b.registry, "vectorSets", store.New(setKey))

	return b
}

// Region returns the default Region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the default account.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears every table.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}

// scope identifies the account and Region a request operates in.
type scope struct{ account, region string }

func (b *InMemoryBackend) scopeOf(ctx context.Context) scope {
	sc := scope{account: awsmeta.Account(ctx), region: awsmeta.Region(ctx)}
	if sc.account == "" {
		sc.account = b.accountID
	}

	if sc.region == "" {
		sc.region = b.region
	}

	return sc
}
