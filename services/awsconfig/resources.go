package awsconfig

import (
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// BatchGetAggregateResourceConfig returns configuration items for aggregate
// resources. This emulator does not model multi-account aggregation
// separately from the account's own resource-config state (mirroring
// SelectAggregateResourceConfig), so each identifier is resolved against
// b.resourceConfigs (populated by PutResourceConfig) instead of being
// blanket-reported unprocessed; only identifiers with no matching discovered
// resource are unprocessed.
func (b *InMemoryBackend) BatchGetAggregateResourceConfig(
	_ string,
	identifiers []AggregateResourceIdentifier,
) ([]BaseConfigurationItem, []AggregateResourceIdentifier) {
	b.mu.RLock("BatchGetAggregateResourceConfig")
	defer b.mu.RUnlock()

	items := make([]BaseConfigurationItem, 0, len(identifiers))
	unprocessed := make([]AggregateResourceIdentifier, 0, len(identifiers))

	for _, id := range identifiers {
		item, ok := b.resourceConfigs.Get(resourceConfigItemKey(id.ResourceType, id.ResourceID))
		if !ok {
			unprocessed = append(unprocessed, id)

			continue
		}

		items = append(items, BaseConfigurationItem{ResourceType: item.ResourceType, ResourceID: item.ResourceID})
	}

	return items, unprocessed
}

// BatchGetResourceConfig returns configuration items for the requested resource
// keys, resolving each against b.resourceConfigs (populated by PutResourceConfig)
// instead of blanket-reporting every key unprocessed; only keys with no matching
// discovered resource are unprocessed.
func (b *InMemoryBackend) BatchGetResourceConfig(
	keys []ResourceKey,
) ([]BaseConfigurationItem, []ResourceKey) {
	b.mu.RLock("BatchGetResourceConfig")
	defer b.mu.RUnlock()

	items := make([]BaseConfigurationItem, 0, len(keys))
	unprocessed := make([]ResourceKey, 0, len(keys))

	for _, k := range keys {
		item, ok := b.resourceConfigs.Get(resourceConfigItemKey(k.ResourceType, k.ResourceID))
		if !ok {
			unprocessed = append(unprocessed, k)

			continue
		}

		items = append(items, BaseConfigurationItem{ResourceType: item.ResourceType, ResourceID: item.ResourceID})
	}

	return items, unprocessed
}

// DeleteResourceConfig removes the discovered configuration item for a
// resource from b.resourceConfigs. Deletion is idempotent (no error for an
// already-absent resource), matching real AWS Config's DeleteResourceConfig
// error model (verified against aws-sdk-go-v2/service/configservice's
// deserializer: only NoRunningConfigurationRecorderException/
// ValidationException, never a not-found exception). The resource's history
// (b.resourceHistory) is intentionally preserved, mirroring AWS which keeps
// prior configuration history entries after a resource is deleted.
func (b *InMemoryBackend) DeleteResourceConfig(resourceType, resourceID string) error {
	b.mu.Lock("DeleteResourceConfig")
	defer b.mu.Unlock()

	b.resourceConfigs.Delete(resourceConfigItemKey(resourceType, resourceID))

	return nil
}

// GetDiscoveredResourceCounts returns zero counts.
func (b *InMemoryBackend) GetDiscoveredResourceCounts() int64 { return 0 }

// ListAggregateDiscoveredResources returns discovered resources of resourceType
// as seen through aggregatorName, tagged with the local account/region as the
// source (mirroring SelectAggregateResourceConfig/GetAggregateResourceConfig,
// already-established for the same single-account-emulator reason). Only the
// aggregator's existence is genuinely validated
// (NoSuchConfigurationAggregatorException); accountFilter/regionFilter narrow
// against the local account/region, resourceIDFilter against the resource ID.
func (b *InMemoryBackend) ListAggregateDiscoveredResources(
	aggregatorName, resourceType, accountFilter, regionFilter, resourceIDFilter string,
) ([]AggregateResourceIdentifier, error) {
	b.mu.RLock("ListAggregateDiscoveredResources")
	defer b.mu.RUnlock()

	if err := b.requireAggregatorLocked(aggregatorName); err != nil {
		return nil, err
	}

	if accountFilter != "" && accountFilter != b.accountID {
		return []AggregateResourceIdentifier{}, nil
	}

	if regionFilter != "" && regionFilter != b.region {
		return []AggregateResourceIdentifier{}, nil
	}

	byType := b.resourceConfigsByType.Get(resourceType)
	out := make([]AggregateResourceIdentifier, 0, len(byType))

	for _, item := range byType {
		if resourceIDFilter != "" && item.ResourceID != resourceIDFilter {
			continue
		}

		out = append(out, AggregateResourceIdentifier{
			SourceAccountID: b.accountID,
			SourceRegion:    b.region,
			ResourceID:      item.ResourceID,
			ResourceType:    item.ResourceType,
		})
	}

	slices.SortFunc(out, func(a, c AggregateResourceIdentifier) int {
		return strings.Compare(a.ResourceID, c.ResourceID)
	})

	return out, nil
}

// PutResourceConfig stores configuration for a resource. The latest state is kept
// for discovery, and a configuration-history entry is appended whenever the
// configuration actually changes (mirroring AWS Config which records on change).
func (b *InMemoryBackend) PutResourceConfig(resourceType, resourceID, configuration string) error {
	b.mu.Lock("PutResourceConfig")
	defer b.mu.Unlock()

	b.captureCounter++

	item := ResourceConfigItem{
		ResourceType:                 resourceType,
		ResourceID:                   resourceID,
		Configuration:                configuration,
		ConfigurationItemCaptureTime: float64(time.Now().Unix()),
	}

	b.resourceConfigs.Put(&item)

	key := resourceEvalKey(resourceType, resourceID)

	hist := b.resourceHistory[key]
	if len(hist) == 0 || hist[len(hist)-1].Configuration != configuration {
		b.resourceHistory[key] = append(hist, item)
	}

	return nil
}

// resourceHistoryLocked returns the configuration history for a resource ordered
// most-recent first. The caller must hold at least a read lock.
func (b *InMemoryBackend) resourceHistoryLocked(resourceType, resourceID string) []ResourceConfigItem {
	hist := b.resourceHistory[resourceEvalKey(resourceType, resourceID)]
	if len(hist) == 0 {
		return []ResourceConfigItem{}
	}

	out := make([]ResourceConfigItem, len(hist))
	for i, item := range hist {
		out[len(hist)-1-i] = item
	}

	return out
}

// GetResourceConfigHistory returns the full configuration history for a resource,
// most-recent first.
func (b *InMemoryBackend) GetResourceConfigHistory(resourceType, resourceID string) []ResourceConfigItem {
	b.mu.RLock("GetResourceConfigHistory")
	defer b.mu.RUnlock()

	return b.resourceHistoryLocked(resourceType, resourceID)
}

// GetResourceConfigHistoryPage returns a page of a resource's configuration
// history (most-recent first) along with an opaque continuation token.
func (b *InMemoryBackend) GetResourceConfigHistoryPage(
	resourceType, resourceID string,
	limit int,
	token string,
) ([]ResourceConfigItem, string) {
	b.mu.RLock("GetResourceConfigHistoryPage")
	defer b.mu.RUnlock()

	const defaultLimit = 100

	p := page.New(b.resourceHistoryLocked(resourceType, resourceID), token, limit, defaultLimit)

	return p.Data, p.Next
}

// ListDiscoveredResources returns all discovered resources of the given type.
func (b *InMemoryBackend) ListDiscoveredResources(resourceType string) []ResourceConfigItem {
	b.mu.RLock("ListDiscoveredResources")
	defer b.mu.RUnlock()

	byType := b.resourceConfigsByType.Get(resourceType)
	if len(byType) == 0 {
		return []ResourceConfigItem{}
	}

	out := make([]ResourceConfigItem, 0, len(byType))
	for _, item := range byType {
		out = append(out, *item)
	}

	return out
}

// GetAggregateDiscoveredResourceCounts returns the total count of discovered resources.
func (b *InMemoryBackend) GetAggregateDiscoveredResourceCounts() int32 {
	b.mu.RLock("GetAggregateDiscoveredResourceCounts")
	defer b.mu.RUnlock()

	return int32(b.resourceConfigs.Len()) //nolint:gosec // Len is non-negative and bounded
}

// GetAggregateResourceConfig returns the first resource config found, or an empty item.
func (b *InMemoryBackend) GetAggregateResourceConfig() *BaseConfigurationItem {
	b.mu.RLock("GetAggregateResourceConfig")
	defer b.mu.RUnlock()

	for _, item := range b.resourceConfigs.All() {
		return &BaseConfigurationItem{
			ResourceType: item.ResourceType,
			ResourceID:   item.ResourceID,
		}
	}

	return &BaseConfigurationItem{}
}

// resourceConfigItemsLocked returns every discovered resource configuration
// item across all resource types. Caller must hold at least a read lock.
func (b *InMemoryBackend) resourceConfigItemsLocked() []*ResourceConfigItem {
	return b.resourceConfigs.All()
}

// SelectResourceConfig evaluates a minimal SQL-like "SELECT fields WHERE
// key = value / LIKE pattern" query (see select_query.go) against the
// account's discovered resource configurations, instead of ignoring the
// query entirely.
func (b *InMemoryBackend) SelectResourceConfig(expression string) []string {
	b.mu.RLock("SelectResourceConfig")
	defer b.mu.RUnlock()

	return evaluateSelectQuery(b.resourceConfigItemsLocked(), expression)
}

// SelectAggregateResourceConfig evaluates the same query language as
// SelectResourceConfig. This emulator does not model multi-account
// aggregation separately from the account's own resource-config state, so
// (mirroring DescribeAggregateComplianceByConfigRules, which reuses the
// account's rule evaluations for its aggregate view) it reuses
// resourceConfigItemsLocked rather than returning an empty result.
func (b *InMemoryBackend) SelectAggregateResourceConfig(expression string) []string {
	b.mu.RLock("SelectAggregateResourceConfig")
	defer b.mu.RUnlock()

	return evaluateSelectQuery(b.resourceConfigItemsLocked(), expression)
}
