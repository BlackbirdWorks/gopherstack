package secretsmanager

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	errResourceNotFoundException = "ResourceNotFoundException"

	// Operation name constants: shared between the handler's dispatch/support tables
	// and the backend's lockmetrics labels so the literal appears once per operation
	// (avoids goconst duplication across handler.go and backend.go).
	opDescribeSecret         = "DescribeSecret"
	opGetResourcePolicy      = "GetResourcePolicy"
	opListSecrets            = "ListSecrets"
	opValidateResourcePolicy = "ValidateResourcePolicy"
)

// defaultMaxResults is the default maximum number of secrets to list.
const defaultMaxResults = 100

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// InMemoryBackend is a concurrency-safe in-memory Secrets Manager backend.
// InMemoryBackend stores Secrets Manager state. secrets is isolated per region
// via a region-qualified composite key ("region|name") on a single flat
// store.Table, with a secondary index (secretsByRegion) grouping entries by
// region for per-region scans (ListSecrets, BatchGetSecretValue's filter
// path); see store_setup.go. resourcePolicies and replicationConfigs remain
// nested map[string]map[string]... (outer key = region) since their values
// (a bare string, a bare slice) carry no identity of their own to key a
// store.Table by -- see store_setup.go's doc comment for the full rationale.
type InMemoryBackend struct {
	lambdaInvoker LambdaInvoker
	// kms is the optional KMS encryptor wired via SetKMSEncryptor (see kms.go).
	// When nil, secret values are stored/returned as plaintext exactly as
	// before KMS integration (backward compatible default).
	kms                KMSEncryptor
	registry           *store.Registry
	secrets            *store.Table[Secret]
	secretsByRegion    *store.Index[Secret]
	resourcePolicies   map[string]map[string]string
	replicationConfigs map[string]map[string][]ReplicationStatusType
	mu                 *lockmetrics.RWMutex
	now                func() time.Time
	schedulerStop      chan struct{}
	svcCtx             context.Context
	accountID          string
	region             string
	schedulerOnce      sync.Once
	schedulerStopOnce  sync.Once
	schedulerWG        sync.WaitGroup
}

// SetLambdaInvoker stores the Lambda invoker on the backend. The rotation
// scheduler uses it to invoke Lambda steps for scheduled rotations.
func (b *InMemoryBackend) SetLambdaInvoker(invoker LambdaInvoker) {
	b.lambdaInvoker = invoker
}

// NewInMemoryBackend creates and returns a new empty Secrets Manager backend with default account/region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(MockAccountID, MockRegion)
}

// NewInMemoryBackendWithConfig creates a new Secrets Manager backend with the given account ID and region
// and a background service context.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new Secrets Manager backend whose background
// goroutines are bounded by svcCtx. If svcCtx is nil, [context.Background] is used.
func NewInMemoryBackendWithContext(svcCtx context.Context, accountID, region string) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	b := &InMemoryBackend{
		registry:           store.NewRegistry(),
		resourcePolicies:   make(map[string]map[string]string),
		replicationConfigs: make(map[string]map[string][]ReplicationStatusType),
		accountID:          accountID,
		region:             region,
		mu:                 lockmetrics.New("secretsmanager"),
		now:                time.Now,
		schedulerStop:      make(chan struct{}),
		svcCtx:             svcCtx,
	}

	registerAllTables(b)

	return b
}

// secretGet returns the secret stored under region+name, performing no
// mutation -- safe to call while holding either b.mu.RLock or b.mu.Lock.
// Replaces the pre-Phase-3.3 secretsStoreRO(region)[name] (and, for
// write-path callers, secretsStore(region)[name]) map lookup: store.Table.Get
// never lazily creates anything, so the "safe to call under RLock" guarantee
// the old *StoreRO helpers were carefully split out for is now structural
// rather than a documented caveat callers had to remember to honour.
func (b *InMemoryBackend) secretGet(region, name string) (*Secret, bool) {
	return b.secrets.Get(secretTableKey(region, name))
}

// secretHas reports whether region+name exists, without mutation.
func (b *InMemoryBackend) secretHas(region, name string) bool {
	return b.secrets.Has(secretTableKey(region, name))
}

// secretPut inserts or replaces s, which must already have its unexported
// region field set (see CreateSecret's literal and AddSecretInternal) so the
// key it is stored under matches secretKeyFn.
func (b *InMemoryBackend) secretPut(s *Secret) {
	b.secrets.Put(s)
}

// secretDelete removes the secret stored under region+name. Every call site
// already knows the secret exists (it was just looked up via secretGet), so
// [store.Table.Delete]'s existed-bool is intentionally discarded here.
func (b *InMemoryBackend) secretDelete(region, name string) {
	b.secrets.Delete(secretTableKey(region, name))
}

// secretsInRegion returns every secret registered under region, in
// unspecified order (mirrors the old secrets[region] map's iteration). The
// returned slice is owned by the underlying index -- see [store.Index.Get] --
// so callers that need to mutate the table while iterating must copy it first.
func (b *InMemoryBackend) secretsInRegion(region string) []*Secret {
	return b.secretsByRegion.Get(region)
}

// The *Store helpers return the per-region inner map, lazily creating it.
// Callers must hold b.mu.

func (b *InMemoryBackend) resourcePoliciesStore(region string) map[string]string {
	if b.resourcePolicies[region] == nil {
		b.resourcePolicies[region] = make(map[string]string)
	}

	return b.resourcePolicies[region]
}

func (b *InMemoryBackend) replicationConfigsStore(region string) map[string][]ReplicationStatusType {
	if b.replicationConfigs[region] == nil {
		b.replicationConfigs[region] = make(map[string][]ReplicationStatusType)
	}

	return b.replicationConfigs[region]
}

// The *StoreRO helpers return the per-region inner map WITHOUT lazily creating it,
// so they are safe to call while only holding a read lock (b.mu.RLock). A plain Go
// map read/range/lookup on a nil map is well-defined (returns the zero value / no
// iterations), so callers get correct "no entries for this region" behaviour without
// mutating the outer map. Never use these under RLock if the result will be written
// through by the caller — that still requires the write-locked *Store variant above.
// (secrets itself no longer needs a StoreRO variant: store.Table.Get never lazily
// creates anything, so secretGet above is unconditionally RLock-safe.)

func (b *InMemoryBackend) resourcePoliciesStoreRO(region string) map[string]string {
	return b.resourcePolicies[region]
}

func (b *InMemoryBackend) replicationConfigsStoreRO(region string) map[string][]ReplicationStatusType {
	return b.replicationConfigs[region]
}

// resolveSecretID resolves a name or ARN to the internal key (name).
func resolveSecretID(secretID string) string {
	if strings.HasPrefix(secretID, "arn:aws:secretsmanager:") {
		// Extract name from ARN: arn:aws:secretsmanager:region:account:secret:name-suffix
		parts := strings.Split(secretID, ":")
		if len(parts) >= arnMinParts {
			nameWithSuffix := parts[arnNameIndex]
			// Remove the trailing -XXXXXX suffix
			if len(nameWithSuffix) > arnSuffixLen {
				return nameWithSuffix[:len(nameWithSuffix)-arnSuffixLen]
			}

			return nameWithSuffix
		}
	}

	return secretID
}

// validateMaxResults returns an error when MaxResults is outside [1, limit].
func validateMaxResults(n *int64, limit int64) error {
	if n == nil {
		return nil
	}

	if *n < 1 || *n > limit {
		return fmt.Errorf(
			"%w: MaxResults must be between 1 and %d",
			ErrInvalidParameter,
			limit,
		)
	}

	return nil
}

// parseToken converts a pagination token string to an integer start index.
func parseToken(token string) int {
	if token == "" {
		return 0
	}

	idx, err := strconv.Atoi(token)
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}

// regionFromARN extracts the region component (index 3) from an AWS ARN
// (arn:partition:service:region:account:resource), falling back to defaultRegion.
func regionFromARN(resourceARN, defaultRegion string) string {
	parts := strings.Split(resourceARN, ":")
	const regionIndex = 3
	if len(parts) > regionIndex && parts[regionIndex] != "" {
		return parts[regionIndex]
	}

	return defaultRegion
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, secret := range b.secrets.All() {
		if secret.Tags != nil {
			secret.Tags.Close()
		}
	}

	b.registry.ResetAll()
	b.resourcePolicies = make(map[string]map[string]string)
	b.replicationConfigs = make(map[string]map[string][]ReplicationStatusType)
}

// AccountID returns the AWS account ID configured for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the AWS region configured for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

// AddSecretInternal seeds the backend with a pre-built Secret for testing.
// The secret is placed in the region encoded in its ARN (falling back to the
// backend's default region). Must not be called concurrently with other operations.
func (b *InMemoryBackend) AddSecretInternal(s *Secret) {
	s.region = regionFromARN(s.ARN, b.region)
	b.secretPut(s)
}
