package resourcegroupstaggingapi

import (
	"context"
	"slices"
	"time"
)

// TaggedResource represents a resource with its ARN, type, and tag set.
type TaggedResource struct {
	Tags         map[string]string
	ResourceARN  string
	ResourceType string
}

// TagFilter represents a single tag filter: resources must have the given key
// and (if Values is non-empty) one of the given values.
type TagFilter struct {
	// Key is the tag key to filter by.
	Key string `json:"Key"`
	// Values are the acceptable tag values; empty means any value.
	Values []string `json:"Values,omitempty"`
}

// ResourceProvider is a function that enumerates tagged resources for a service.
// Registered providers are called on every GetResources request.
// The context carries the per-request AWS region so providers can filter accordingly.
type ResourceProvider func(ctx context.Context) []TaggedResource

// FilteredResourceProvider is a resource provider that accepts tag and resource-type
// filters so that it can perform provider-side filter pushdown. When filters are
// non-empty the provider is expected to return only resources that satisfy them;
// when both slices are empty the provider must return all resources.
// The context carries the per-request AWS region.
type FilteredResourceProvider func(ctx context.Context, tagFilters []TagFilter, typeFilters []string) []TaggedResource

// ARNTagger applies a set of tags to the resource identified by the given ARN.
// It returns true when it handled the ARN (even on error) and false when the ARN
// belongs to a different service and should be tried by the next registered tagger.
// The context carries the per-request AWS region.
type ARNTagger func(ctx context.Context, arn string, tags map[string]string) (bool, error)

// ARNUntagger removes the specified tag keys from the resource identified by the
// given ARN. Same handled/not-handled semantics as ARNTagger.
// The context carries the per-request AWS region.
type ARNUntagger func(ctx context.Context, arn string, keys []string) (bool, error)

// TagPolicyProvider returns this account's effective TAG_POLICY document content --
// the same JSON a real DescribeEffectivePolicy(PolicyType=TAG_POLICY) call against
// AWS Organizations would return (see services/organizations/effective_policy.go) --
// and whether one is configured at all. ListRequiredTags uses this to derive real
// required-tag data instead of always returning an empty list. Central wiring (cli.go)
// is expected to register the organizations backend's effective policy for this
// account; with no provider registered, ListRequiredTags accurately reports the real
// AWS behavior for an account with no tag policy attached: an empty list.
type TagPolicyProvider func() (content string, ok bool)

// resourceCache holds a cached snapshot of GetResources results.
type resourceCache struct {
	expiresAt time.Time
	resources []TaggedResource
}

// resourceCacheTTL is the time-to-live for the GetResources result cache.
const resourceCacheTTL = 30 * time.Second

// RegisterProvider adds a tagged-resource provider to the registry.
// Providers are called in registration order on every GetResources request.
func (b *InMemoryBackend) RegisterProvider(p ResourceProvider) {
	b.mu.Lock("RegisterProvider")
	defer b.mu.Unlock()

	b.providers = append(b.providers, p)
	clear(b.caches)
}

// RegisterFilteredProvider adds a filter-aware resource provider to the registry.
// The provider receives the tag and resource-type filters from GetResources so that
// it can perform provider-side filter pushdown instead of returning all resources.
func (b *InMemoryBackend) RegisterFilteredProvider(p FilteredResourceProvider) {
	b.mu.Lock("RegisterFilteredProvider")
	defer b.mu.Unlock()

	b.filteredProviders = append(b.filteredProviders, p)
	clear(b.caches)
}

// RegisterARNTagger adds an ARN-based tagger to the registry.
// Taggers are tried in registration order; the first one that returns
// handled=true is used and the rest are skipped.
func (b *InMemoryBackend) RegisterARNTagger(t ARNTagger) {
	b.mu.Lock("RegisterARNTagger")
	defer b.mu.Unlock()

	b.taggers = append(b.taggers, t)
}

// RegisterARNUntagger adds an ARN-based untagger to the registry.
// Same semantics as RegisterARNTagger.
func (b *InMemoryBackend) RegisterARNUntagger(u ARNUntagger) {
	b.mu.Lock("RegisterARNUntagger")
	defer b.mu.Unlock()

	b.untaggers = append(b.untaggers, u)
}

// RegisterTagPolicyProvider sets the single provider ListRequiredTags consults for
// this account's effective TAG_POLICY document. A second call replaces the first --
// there is exactly one effective tag policy per account, unlike the many-provider
// registries above.
func (b *InMemoryBackend) RegisterTagPolicyProvider(p TagPolicyProvider) {
	b.mu.Lock("RegisterTagPolicyProvider")
	defer b.mu.Unlock()

	b.tagPolicyProvider = p
}

// getResources collects all resources from registered providers.
// Plain providers are called with ctx; filtered providers receive ctx and the
// supplied filters so they can perform provider-side pushdown.
// When tagFilters and typeFilters are both empty the per-region cache is consulted first.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) getResources(
	ctx context.Context,
	tagFilters []TagFilter,
	typeFilters []string,
) []TaggedResource {
	region := getRegion(ctx, b.defaultRegion)
	useCache := len(tagFilters) == 0 && len(typeFilters) == 0

	if useCache {
		if c := b.caches[region]; c != nil && time.Now().Before(c.expiresAt) {
			return c.resources
		}
	}

	perProvider := make([][]TaggedResource, 0, len(b.providers)+len(b.filteredProviders))
	for _, p := range b.providers {
		perProvider = append(perProvider, p(ctx))
	}

	for _, p := range b.filteredProviders {
		perProvider = append(perProvider, p(ctx, tagFilters, typeFilters))
	}

	all := deduplicateResources(slices.Concat(perProvider...))

	if useCache {
		b.caches[region] = &resourceCache{
			resources: all,
			expiresAt: time.Now().Add(resourceCacheTTL),
		}
	}

	return all
}

// invalidateCache clears all per-region resource caches. Caller must hold a write lock.
func (b *InMemoryBackend) invalidateCache() {
	clear(b.caches)
}

// deduplicateResources de-duplicates resources by ARN; the last occurrence wins.
func deduplicateResources(all []TaggedResource) []TaggedResource {
	if len(all) == 0 {
		return all
	}

	order := make([]string, 0, len(all))
	index := make(map[string]TaggedResource, len(all))

	for _, r := range all {
		if _, exists := index[r.ResourceARN]; !exists {
			order = append(order, r.ResourceARN)
		}

		index[r.ResourceARN] = r
	}

	result := make([]TaggedResource, 0, len(order))
	for _, arn := range order {
		result = append(result, index[arn])
	}

	return result
}
