package eventbridge

import (
	"encoding/base64"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func (b *InMemoryBackend) busARN(region, name string) string {
	return arn.Build("events", region, b.accountID, "event-bus/"+name)
}

func (b *InMemoryBackend) ruleARN(region, busName, ruleName string) string {
	return arn.Build("events", region, b.accountID, "rule/"+busName+"/"+ruleName)
}

func (b *InMemoryBackend) apiDestinationARN(name string) string {
	return arn.Build("events", b.region, b.accountID, "api-destination/"+name)
}

func (b *InMemoryBackend) archiveARN(name string) string {
	return arn.Build("events", b.region, b.accountID, "archive/"+name)
}

func (b *InMemoryBackend) connectionARN(name string) string {
	return arn.Build("events", b.region, b.accountID, "connection/"+name)
}

func (b *InMemoryBackend) endpointARN(name string) string {
	return arn.Build("events", b.region, b.accountID, "endpoint/"+name)
}

func (b *InMemoryBackend) partnerSourceARN(name string) string {
	return arn.Build("events", b.region, b.accountID, "event-source/aws.partner/"+name)
}

func (b *InMemoryBackend) replayARN(name string) string {
	return arn.Build("events", b.region, b.accountID, "replay/"+name)
}

// targetKey returns the region-free inner key used to store a rule's targets
// within a region-scoped target map.
func (b *InMemoryBackend) targetKey(busName, ruleName string) string {
	return ebBusKey(busName) + "/" + ruleName
}

// busesTable returns the *store.Table[EventBus] for the given region, lazily
// creating and registering it. Callers must hold b.mu.
func (b *InMemoryBackend) busesTable(region string) *store.Table[EventBus] {
	return getOrCreateTable(b.registry, &b.tableMu, b.buses, "buses", region, eventBusKeyFn)
}

// rulesStore returns the region's bus->Table[Rule] map, lazily creating the
// per-region entry (but NOT any per-bus Table -- use ruleTableFor for that).
// Callers must hold b.mu.
func (b *InMemoryBackend) rulesStore(region string) map[string]*store.Table[Rule] {
	b.tableMu.Lock()
	defer b.tableMu.Unlock()
	if b.rules[region] == nil {
		b.rules[region] = make(map[string]*store.Table[Rule])
	}

	return b.rules[region]
}

// ruleTableFor returns the *store.Table[Rule] for the given region and bus
// key, lazily creating and registering it. Callers must hold b.mu.
func (b *InMemoryBackend) ruleTableFor(region, busKey string) *store.Table[Rule] {
	return getOrCreateNestedTable(b.registry, &b.tableMu, b.rules, "rules", region, busKey, ruleKeyFn)
}

// targetsStore returns the region's parentKey->Table[Target] map, lazily
// creating the per-region entry (but NOT any per-parent Table -- use
// targetTableFor for that). Callers must hold b.mu.
func (b *InMemoryBackend) targetsStore(region string) map[string]*store.Table[Target] {
	b.tableMu.Lock()
	defer b.tableMu.Unlock()
	if b.targets[region] == nil {
		b.targets[region] = make(map[string]*store.Table[Target])
	}

	return b.targets[region]
}

// targetTableFor returns the *store.Table[Target] for the given region and
// target parent key (bus/rule, see targetKey), lazily creating and
// registering it. Callers must hold b.mu.
func (b *InMemoryBackend) targetTableFor(region, parentKey string) *store.Table[Target] {
	return getOrCreateNestedTable(b.registry, &b.tableMu, b.targets, "targets", region, parentKey, targetKeyFnV)
}

// targetsByARNStore returns (lazily creating) the per-region ARN→targetKeys index.
// Callers must hold b.mu.
func (b *InMemoryBackend) targetsByARNStore(region string) map[string]map[string]struct{} {
	b.tableMu.Lock()
	defer b.tableMu.Unlock()
	if b.targetsByARN[region] == nil {
		b.targetsByARN[region] = make(map[string]map[string]struct{})
	}

	return b.targetsByARN[region]
}

// arnIndexAdd adds targetKey to the ARN index for the given region.
// Callers must hold b.mu.
func (b *InMemoryBackend) arnIndexAdd(region, arn, targetKey string) {
	idx := b.targetsByARNStore(region)
	if idx[arn] == nil {
		idx[arn] = make(map[string]struct{})
	}

	idx[arn][targetKey] = struct{}{}
}

// arnIndexRemoveTarget removes targetKey from the ARN index entry for the given arn.
// Callers must hold b.mu.
func (b *InMemoryBackend) arnIndexRemoveTarget(region, arn, targetKey string) {
	idx := b.targetsByARN[region]
	if idx == nil {
		return
	}

	delete(idx[arn], targetKey)

	if len(idx[arn]) == 0 {
		delete(idx, arn)
	}
}

// arnIndexRemoveRule removes all ARN entries for the given targetKey (i.e. a rule was deleted).
// Callers must hold b.mu.
func (b *InMemoryBackend) arnIndexRemoveRule(region, targetKey string, tTable *store.Table[Target]) {
	if tTable == nil {
		return
	}

	idx := b.targetsByARN[region]
	if idx == nil {
		return
	}

	for _, t := range tTable.All() {
		delete(idx[t.Arn], targetKey)
		if len(idx[t.Arn]) == 0 {
			delete(idx, t.Arn)
		}
	}
}

// ruleIndexStore returns the rule index map for the given region, lazily creating it.
// Callers must hold b.mu.
func (b *InMemoryBackend) ruleIndexStore(region string) map[string]map[ruleIndexKey]map[string]*Rule {
	if b.ruleIndex[region] == nil {
		b.ruleIndex[region] = make(map[string]map[ruleIndexKey]map[string]*Rule)
	}

	return b.ruleIndex[region]
}

// eventSourcesTable returns the *store.Table[EventSource] for the given
// region, lazily creating and registering it. Callers must hold b.mu.
func (b *InMemoryBackend) eventSourcesTable(region string) *store.Table[EventSource] {
	return getOrCreateTable(b.registry, &b.tableMu, b.eventSources, "eventSources", region, eventSourceKeyFn)
}

// replaysTable returns the *store.Table[Replay] for the given region, lazily
// creating and registering it. Callers must hold b.mu.
func (b *InMemoryBackend) replaysTable(region string) *store.Table[Replay] {
	return getOrCreateTable(b.registry, &b.tableMu, b.replays, "replays", region, replayKeyFn)
}

// apiDestinationsTable returns the *store.Table[APIDestination] for the given
// region, lazily creating and registering it. Callers must hold b.mu.
func (b *InMemoryBackend) apiDestinationsTable(region string) *store.Table[APIDestination] {
	return getOrCreateTable(b.registry, &b.tableMu, b.apiDestinations, "apiDestinations", region, apiDestinationKeyFn)
}

// archivesTable returns the *store.Table[Archive] for the given region,
// lazily creating and registering it. Callers must hold b.mu.
func (b *InMemoryBackend) archivesTable(region string) *store.Table[Archive] {
	return getOrCreateTable(b.registry, &b.tableMu, b.archives, "archives", region, archiveKeyFn)
}

// archivedEventsStore returns the archived-events map for the given region.
// Callers must hold b.mu.
func (b *InMemoryBackend) archivedEventsStore(region string) map[string][]EventEntry {
	if b.archivedEvents[region] == nil {
		b.archivedEvents[region] = make(map[string][]EventEntry)
	}

	return b.archivedEvents[region]
}

// archivedEventsStoreRO is the non-mutating twin of archivedEventsStore, for
// callers holding only a read lock. Creating the region map on a pure read is
// pointless anyway, and doing it under RLock is a concurrent map write plus a
// race -- the same class fixed across 17 services in c381f62b3.
func (b *InMemoryBackend) archivedEventsStoreRO(region string) map[string][]EventEntry {
	return b.archivedEvents[region]
}

// connectionsTable returns the *store.Table[Connection] for the given region,
// lazily creating and registering it. Callers must hold b.mu.
func (b *InMemoryBackend) connectionsTable(region string) *store.Table[Connection] {
	return getOrCreateTable(b.registry, &b.tableMu, b.connections, "connections", region, connectionKeyFn)
}

// endpointsTable returns the *store.Table[Endpoint] for the given region,
// lazily creating and registering it. Callers must hold b.mu.
func (b *InMemoryBackend) endpointsTable(region string) *store.Table[Endpoint] {
	return getOrCreateTable(b.registry, &b.tableMu, b.endpoints, "endpoints", region, endpointKeyFn)
}

// partnerSourcesTable returns the *store.Table[PartnerEventSource] for the
// given region, lazily creating and registering it. Callers must hold b.mu.
func (b *InMemoryBackend) partnerSourcesTable(region string) *store.Table[PartnerEventSource] {
	return getOrCreateTable(b.registry, &b.tableMu, b.partnerSources, "partnerSources", region, partnerEventSourceKeyFn)
}

// pipesTable returns the backend's single, global *store.Table[Pipe], lazily
// creating and registering it. Callers must hold b.mu.
func (b *InMemoryBackend) pipesTable() *store.Table[Pipe] {
	return getOrCreateGlobalTable(b.auxRegistry, &b.tableMu, &b.pipes, "pipes", pipeKeyFn)
}

// registriesTable returns the backend's single, global
// *store.Table[SchemaRegistry], lazily creating and registering it. Callers
// must hold b.mu.
func (b *InMemoryBackend) registriesTable() *store.Table[SchemaRegistry] {
	return getOrCreateGlobalTable(b.auxRegistry, &b.tableMu, &b.registries, "registries", schemaRegistryKeyFn)
}

// schemasTableFor returns the *store.Table[Schema] for the given registry,
// lazily creating and registering it. Callers must hold b.mu.
func (b *InMemoryBackend) schemasTableFor(registryName string) *store.Table[Schema] {
	return getOrCreateTable(b.auxRegistry, &b.tableMu, b.schemas, "schemas", registryName, schemaKeyFn)
}

// getSchema is a nil-safe read of a registry's schema table: it returns
// (nil, false) rather than panicking when the registry has no schemas table
// yet (a plain nil-map read would have been safe pre-conversion; a nil
// *store.Table[Schema] method call is not). Callers must hold b.mu (for
// reading or writing).
func (b *InMemoryBackend) getSchema(registryName, schemaName string) (*Schema, bool) {
	t := b.schemas[registryName]
	if t == nil {
		return nil, false
	}

	return t.Get(schemaName)
}

// busePoliciesStore returns the event bus policy map for the given region.
// Callers must hold b.mu.
func (b *InMemoryBackend) busePoliciesStore(region string) map[string]*EventBusPolicy {
	if b.busePolicies[region] == nil {
		b.busePolicies[region] = make(map[string]*EventBusPolicy)
	}

	return b.busePolicies[region]
}

// busePoliciesStoreRO returns the region-scoped busePolicies map for region
// without mutating the outer map. Safe to call while holding only
// b.mu.RLock(): if the region has not been observed yet, it returns a fresh,
// unregistered, empty map instead of lazily creating (and persisting) an
// entry.
func (b *InMemoryBackend) busePoliciesStoreRO(region string) map[string]*EventBusPolicy {
	if v := b.busePolicies[region]; v != nil {
		return v
	}

	return map[string]*EventBusPolicy{}
}

func (b *InMemoryBackend) getOrCompilePattern(patternJSON string) (*compiledPattern, error) {
	if cached, ok := b.patternCache.Load(patternJSON); ok {
		compiled, castOK := cached.(*compiledPattern)
		if castOK {
			return compiled, nil
		}
	}

	compiled, err := compilePattern(patternJSON)
	if err != nil {
		return nil, err
	}

	actual, _ := b.patternCache.LoadOrStore(patternJSON, compiled)
	cachedCompiled, castOK := actual.(*compiledPattern)
	if castOK {
		return cachedCompiled, nil
	}

	return compiled, nil
}

func (b *InMemoryBackend) addRuleToIndex(region, busKey string, rule *Rule) {
	if rule.compiledPattern == nil && rule.EventPattern == "" {
		return
	}
	regionIndex := b.ruleIndexStore(region)
	indexes := regionIndex[busKey]
	if indexes == nil {
		indexes = make(map[ruleIndexKey]map[string]*Rule)
		regionIndex[busKey] = indexes
	}

	keys := indexKeysFromRule(rule)
	for _, key := range keys {
		bucket := indexes[key]
		if bucket == nil {
			bucket = make(map[string]*Rule)
			indexes[key] = bucket
		}
		bucket[rule.Name] = rule
	}
	rule.indexKeys = keys
}

func (b *InMemoryBackend) removeRuleFromIndex(region, busKey string, rule *Rule) {
	regionIndex := b.ruleIndex[region]
	if regionIndex == nil {
		return
	}
	indexes := regionIndex[busKey]
	if indexes == nil {
		return
	}

	for _, key := range rule.indexKeys {
		bucket := indexes[key]
		if bucket == nil {
			continue
		}
		delete(bucket, rule.Name)
		if len(bucket) == 0 {
			delete(indexes, key)
		}
	}

	if len(indexes) == 0 {
		delete(regionIndex, busKey)
	}
}

func indexKeysFromRule(rule *Rule) []ruleIndexKey {
	sources := []string{ruleIndexAny}
	detailTypes := []string{ruleIndexAny}

	if rule.compiledPattern != nil && len(rule.compiledPattern.sourceExactValues) > 0 {
		sources = rule.compiledPattern.sourceExactValues
	}
	if rule.compiledPattern != nil && len(rule.compiledPattern.detailTypeExactValues) > 0 {
		detailTypes = rule.compiledPattern.detailTypeExactValues
	}

	const maxSize = 10000
	size := 0
	if len(sources) <= maxSize && len(detailTypes) <= maxSize {
		size = len(sources) * len(detailTypes)
		if size > maxSize {
			size = 0
		}
	}

	keys := make([]ruleIndexKey, 0, size)
	for _, source := range sources {
		for _, detailType := range detailTypes {
			keys = append(keys, ruleIndexKey{
				source:     source,
				detailType: detailType,
			})
		}
	}

	return keys
}

// encodeNextToken encodes a pagination offset as an opaque base64 token.
// Real AWS EventBridge tokens are opaque; encoding prevents callers from
// treating the token as a stable integer offset.
func encodeNextToken(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

func parseNextToken(token string) int {
	if token == "" {
		return 0
	}
	// Try base64-encoded offset first (current format).
	if decoded, decErr := base64.StdEncoding.DecodeString(token); decErr == nil {
		if idx, atoiErr := strconv.Atoi(string(decoded)); atoiErr == nil && idx >= 0 {
			return idx
		}
	}
	// Fallback: accept plain decimal strings from tokens produced before this change.
	idx, err := strconv.Atoi(token)
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}

// paginate applies offset-based pagination to a pre-sorted slice with the default
// page size. It returns the page slice and an opaque next-page token (or "").
func paginate[T any](all []T, nextToken string) ([]T, string) {
	return paginateN(all, nextToken, 0)
}

// paginateN is like paginate but respects a caller-supplied page size.
// A limit of 0 uses the default page size (100).
func paginateN[T any](all []T, nextToken string, limit int) ([]T, string) {
	const defaultLimit = 100
	if limit <= 0 {
		limit = defaultLimit
	}

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []T{}, ""
	}

	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken
}

// pipeARN builds an ARN for an EventBridge Pipe.
func (b *InMemoryBackend) pipeARN(name string) string {
	return arn.Build("events", b.region, b.accountID, "pipe/"+name)
}

func (b *InMemoryBackend) registryARN(name string) string {
	return arn.Build("schemas", b.region, b.accountID, "registry/"+name)
}

func (b *InMemoryBackend) schemaARN(registryName, schemaName string) string {
	return arn.Build("schemas", b.region, b.accountID, "schema/"+registryName+"/"+schemaName)
}

func (b *InMemoryBackend) schemaVersionKey(registryName, schemaName string) string {
	return registryName + "/" + schemaName
}

func (b *InMemoryBackend) codeBindingKey(registryName, schemaName, language string) string {
	return registryName + "/" + schemaName + "/" + language
}
