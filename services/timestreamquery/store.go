package timestreamquery

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// Every backend operation resolves the caller's region from the request context and
// operates only on that region's nested store.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
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

// InMemoryBackend is the in-memory backend for the Timestream Query service.
type InMemoryBackend struct {
	mu                       *lockmetrics.RWMutex
	registry                 *store.Registry
	scheduledQueries         *store.Table[ScheduledQuery] // keyed by Arn (globally unique; embeds region)
	scheduledQueriesByRegion *store.Index[ScheduledQuery] // region → *ScheduledQuery
	queries                  *store.Table[QueryResult]    // keyed by QueryID; not region-isolated
	accountSettings          map[string]AccountSettings   // region → settings
	clientTokens             *clientTokenCache            // Query ClientToken -> QueryID
	scheduledQueryTokens     *clientTokenCache            // CreateScheduledQuery ClientToken -> Arn
	pageStore                *nextTokenStore
	accountID                string
	defaultRegion            string
}

// NewInMemoryBackend creates a new in-memory Timestream Query backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID:            accountID,
		defaultRegion:        region,
		mu:                   lockmetrics.New("timestreamquery"),
		registry:             store.NewRegistry(),
		accountSettings:      make(map[string]AccountSettings),
		clientTokens:         newClientTokenCache(queryClientTokenTTL),
		scheduledQueryTokens: newClientTokenCache(createScheduledQueryClientTokenTTL),
		pageStore:            newNextTokenStore(),
	}
	registerAllTables(b)

	return b
}

// Reset clears all backend state, returning it to a freshly initialised condition.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.accountSettings = make(map[string]AccountSettings)
	b.clientTokens = newClientTokenCache(queryClientTokenTTL)
	b.scheduledQueryTokens = newClientTokenCache(createScheduledQueryClientTokenTTL)
	b.pageStore = newNextTokenStore()
}

// AccountID returns the account ID for the backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the default region for the backend.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }
