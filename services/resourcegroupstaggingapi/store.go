// Package resourcegroupstaggingapi provides a mock implementation of the AWS Resource Groups
// Tagging API service. It provides a cross-service tag-based resource lookup layer on top
// of the existing per-service backends.
package resourcegroupstaggingapi

import (
	"context"
	"errors"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// errCodeInvalidParameter is the wire error code real AWS resourcegroupstaggingapi uses
// for parameter validation failures. The service's error model (see
// aws-sdk-go-v2/service/resourcegroupstaggingapi/types/errors.go) has no
// "ValidationException" shape at all, so this -- not ValidationException -- is the code
// every parameter-validation failure in this package must carry.
const errCodeInvalidParameter = "InvalidParameterException"

// ErrValidation is returned when a request fails parameter validation; its wire error
// code is errCodeInvalidParameter.
var ErrValidation = errors.New(errCodeInvalidParameter)

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// InMemoryBackend is the in-memory store for the Resource Groups Tagging API.
// It maintains a registry of service-specific resource providers and tagging adapters.
// Report state and the resource cache are nested by region so that same-named resources
// created in different regions are fully isolated.
type InMemoryBackend struct {
	mu                *lockmetrics.RWMutex
	registry          *store.Registry
	reportStates      *store.Table[reportCreationState] // region → report state
	caches            map[string]*resourceCache         // region → resource cache
	nowFunc           func() string
	clockFunc         func() time.Time
	accountID         string
	defaultRegion     string
	providers         []ResourceProvider
	filteredProviders []FilteredResourceProvider
	taggers           []ARNTagger
	untaggers         []ARNUntagger
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID:     accountID,
		defaultRegion: region,
		mu:            lockmetrics.New("resourcegroupstaggingapi"),
		registry:      store.NewRegistry(),
		caches:        make(map[string]*resourceCache),
	}

	registerAllTables(b)

	b.nowFunc = b.defaultNow
	b.clockFunc = time.Now

	return b
}

// defaultNow returns the current UTC time in RFC3339 format.
func (b *InMemoryBackend) defaultNow() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// Region returns the default AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears dynamic per-test state (all region report states and caches) but
// intentionally preserves the registered providers, taggers, and untaggers. These
// are wired at server startup by wireResourceGroupsTagging and must persist across
// service resets, otherwise the cross-service tagging integration breaks.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.reportStates.Reset()
	clear(b.caches)
}

// now returns the current time string using nowFunc.
func (b *InMemoryBackend) now() string {
	return b.nowFunc()
}
