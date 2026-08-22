package cloudwatchlogs

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T (and map[string]T / nested map[string]map[string]*T) resource
// field on InMemoryBackend is replaced by a *store.Table[T], registered exactly
// once here. See pkgs/store's package doc and the services/ec2 (commit
// 12e611a4, data-driven registration slice) and services/sqs (commit
// 0f09d77c, DTO-registry) conversions this follows.
//
// Two maps are deliberately NOT converted and remain plain maps -- see the
// comment above registerAllTables for why.
//
// # Region-qualified tables
//
// groups, streams, subscriptionFilters, and metricFilters were nested
// map[string]map[string]... keyed by region as the outer key. store.Table has
// a single flat string key, so each value type gains an unexported `region`
// field (and LogStream additionally gains `logGroupName` + inline `events`)
// used to build a region-qualified composite key -- see groupTableKey et al.
// below. Because these fields are unexported they never appear in the wire
// JSON these types double as (e.g. DescribeLogGroups' `logGroups` response),
// but that also means they cannot survive Table.Snapshot's json.Marshal on
// their own; persistence.go round-trips them through DTOs
// (logGroupSnapshot, logStreamSnapshot, ...) that carry the same data as
// ordinary exported fields instead.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// --- composite-key helpers for region-qualified tables ---

// groupTableKey returns the store.Table primary key for a log group.
func groupTableKey(region, groupName string) string {
	return region + "|" + groupName
}

// streamTableKey returns the store.Table primary key for a log stream.
func streamTableKey(region, groupName, streamName string) string {
	return streamGroupKey(region, groupName) + "|" + streamName
}

// streamGroupKey returns the store.Index key grouping every stream that
// belongs to one region+log-group pair (mirrors the old streams[region][group] map).
func streamGroupKey(region, groupName string) string {
	return region + "|" + groupName
}

// subFilterTableKey returns the store.Table primary key for a subscription filter.
func subFilterTableKey(region, groupName, filterName string) string {
	return region + "|" + groupName + "|" + filterName
}

// metricFilterTableKey returns the store.Table primary key for a metric filter.
func metricFilterTableKey(region, groupName, filterName string) string {
	return region + "|" + groupName + "|" + filterName
}

// --- store.Table key functions ---

func logGroupKeyFn(g *LogGroup) string { return groupTableKey(g.region, g.LogGroupName) }

func logStreamKeyFn(s *LogStream) string {
	return streamTableKey(s.region, s.logGroupName, s.LogStreamName)
}

func logStreamGroupIndexKeyFn(s *LogStream) string {
	return streamGroupKey(s.region, s.logGroupName)
}

func subscriptionFilterKeyFn(f *SubscriptionFilter) string {
	return subFilterTableKey(f.region, f.LogGroupName, f.FilterName)
}

func subscriptionFilterGroupIndexKeyFn(f *SubscriptionFilter) string {
	return streamGroupKey(f.region, f.LogGroupName)
}

func metricFilterKeyFn(f *MetricFilter) string {
	return metricFilterTableKey(f.region, f.LogGroupName, f.FilterName)
}

func metricFilterGroupIndexKeyFn(f *MetricFilter) string {
	return streamGroupKey(f.region, f.LogGroupName)
}

func accountPolicyKeyFn(p *AccountPolicy) string           { return p.PolicyName + ":" + p.PolicyType }
func exportTaskKeyFn(t *ExportTask) string                 { return t.TaskID }
func importTaskKeyFn(t *ImportTask) string                 { return t.ImportID }
func deliveryKeyFn(d *Delivery) string                     { return d.ID }
func logAnomalyDetectorKeyFn(d *LogAnomalyDetector) string { return d.AnomalyDetectorArn }
func scheduledQueryKeyFn(q *ScheduledQuery) string         { return q.ScheduledQueryArn }
func queryDefinitionKeyFn(q *QueryDefinition) string       { return q.QueryDefinitionID }
func resourcePolicyKeyFn(p *ResourcePolicy) string {
	return resourcePolicyStoreKey(p.PolicyName, p.ResourceArn)
}
func deliveryDestinationKeyFn(d *DeliveryDestination) string { return d.Name }
func deliverySourceKeyFn(s *DeliverySource) string           { return s.Name }
func cwlDestinationKeyFn(d *CWLDestination) string           { return d.DestinationName }
func indexPolicyKeyFn(p *IndexPolicy) string                 { return p.LogGroupIdentifier }
func transformerKeyFn(t *Transformer) string                 { return t.LogGroupIdentifier }
func cwlIntegrationKeyFn(i *CWLIntegration) string           { return i.Name }
func lookupTableKeyFn(t *LookupTable) string                 { return t.LookupTableArn }
func syslogConfigurationKeyFn(c *SyslogConfiguration) string { return c.LogGroupIdentifier }

// storageTierPolicySingletonKey is the fixed store.Table key for the single
// account-level StorageTierPolicy row (see the doc comment in models.go for
// why this is a singleton).
const storageTierPolicySingletonKey = "account"

func storageTierPolicyKeyFn(_ *StorageTierPolicy) string { return storageTierPolicySingletonKey }

// anomalyTableKey returns the store.Table primary key for an Anomaly.
func anomalyTableKey(detectorArn, anomalyID string) string { return detectorArn + "|" + anomalyID }

// anomalyKeyFn keys an Anomaly by its (detector, id) pair; anomalyDetectorIndexKeyFn
// groups every anomaly belonging to one detector (mirrors the old
// anomalies[detectorArn] map) for ListAnomalies(anomalyDetectorArn="").
func anomalyKeyFn(a *Anomaly) string              { return anomalyTableKey(a.AnomalyDetectorArn, a.AnomalyID) }
func anomalyDetectorIndexKeyFn(a *Anomaly) string { return a.AnomalyDetectorArn }

// scheduledQueryRunHistory wraps the run-history slice AWS keeps per scheduled
// query. It exists because store.Table requires a value with its own identity
// field; a bare []*ScheduledQueryRunSummary has none (multiple runs share the
// same Arn), so -- like inlining log events on LogStream -- the slice is
// nested one level under a keyed parent entity instead.
type scheduledQueryRunHistory struct {
	Arn  string                      `json:"arn"`
	Runs []*ScheduledQueryRunSummary `json:"runs"`
}

func scheduledQueryRunHistoryKeyFn(h *scheduledQueryRunHistory) string { return h.Arn }

// kmsKeyEntry, s3TableIntegrationEntry, dataProtectionPolicyEntry, and
// deletionProtectionEntry each wrap a bare map[string]string / map[string]bool
// value in a minimal identified struct, since store.Table requires values to
// carry their own primary key.
type kmsKeyEntry struct {
	Key      string `json:"key"`
	KmsKeyID string `json:"kmsKeyId"`
}

func kmsKeyEntryKeyFn(e *kmsKeyEntry) string { return e.Key }

type s3TableIntegrationEntry struct {
	ID               string `json:"id"`
	IntegrationArn   string `json:"integrationArn"`
	DataSourceName   string `json:"dataSourceName"`
	DataSourceType   string `json:"dataSourceType"`
	CreatedTimeStamp int64  `json:"createdTimeStamp"`
}

func s3TableIntegrationEntryKeyFn(e *s3TableIntegrationEntry) string { return e.ID }

type dataProtectionPolicyEntry struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
	PolicyDocument     string `json:"policyDocument"`
	LastUpdatedTime    int64  `json:"lastUpdatedTime"`
}

func dataProtectionPolicyEntryKeyFn(e *dataProtectionPolicyEntry) string {
	return e.LogGroupIdentifier
}

type deletionProtectionEntry struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
	Protected          bool   `json:"protected"`
}

func deletionProtectionEntryKeyFn(e *deletionProtectionEntry) string { return e.LogGroupIdentifier }

// storedQueryKeyFn keys a storedQuery by the QueryID assigned when StartQuery
// stored it.
func storedQueryKeyFn(q *storedQuery) string { return q.info.QueryID }

// registerAllTables registers every persisted resource table on b.registry
// exactly once. It must be called during construction only (immediately after
// b.registry is created), never on every Reset() -- store.Register panics on a
// duplicate name, so runtime resets go through b.registry.ResetAll() instead
// (see InMemoryBackend.Reset in backend.go).
//
// The following resource fields are deliberately left as plain maps (not
// registered anywhere), because their value carries no identity field of its
// own to serve as a store.Table key and both are ephemeral, never-persisted
// caches (not part of backendSnapshot before or after this refactor):
//   - compiledPatterns: value type compiledFilterPattern carries no identity
//     field of its own (it holds only the compiled matcher, not the source
//     pattern string it was compiled from); keyed externally by the raw
//     pattern string. Guarded by its own compiledPatternsMu, independent of
//     b.mu, matching the safemap "isolated cache" exception.
//   - parsedQueries: value type insightsQuery carries no identity field of its
//     own (keyed externally by the raw query string) and holds a []queryStage
//     of unexported interface implementations that cannot round-trip through
//     JSON in any case.
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per persisted resource table. These are the "clean" tables (see
// persistence.go) whose value type's identity field is already a normal
// exported struct field, so store.Table's own JSON marshal round-trips them
// without a separate DTO.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.accountPolicies = store.Register(b.registry, "accountPolicies", store.New(accountPolicyKeyFn))
	},
	func(b *InMemoryBackend) {
		b.exportTasks = store.Register(b.registry, "exportTasks", store.New(exportTaskKeyFn))
	},
	func(b *InMemoryBackend) {
		b.importTasks = store.Register(b.registry, "importTasks", store.New(importTaskKeyFn))
	},
	func(b *InMemoryBackend) {
		b.deliveries = store.Register(b.registry, "deliveries", store.New(deliveryKeyFn))
	},
	func(b *InMemoryBackend) {
		b.logAnomalyDetectors = store.Register(
			b.registry, "logAnomalyDetectors", store.New(logAnomalyDetectorKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.scheduledQueries = store.Register(b.registry, "scheduledQueries", store.New(scheduledQueryKeyFn))
	},
	func(b *InMemoryBackend) {
		b.s3TableIntegrations = store.Register(
			b.registry, "s3TableIntegrations", store.New(s3TableIntegrationEntryKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.kmsKeys = store.Register(b.registry, "kmsKeys", store.New(kmsKeyEntryKeyFn))
	},
	func(b *InMemoryBackend) {
		b.queryDefinitions = store.Register(b.registry, "queryDefinitions", store.New(queryDefinitionKeyFn))
	},
	func(b *InMemoryBackend) {
		b.dataProtectionPolicies = store.Register(
			b.registry, "dataProtectionPolicies", store.New(dataProtectionPolicyEntryKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.resourcePolicies = store.Register(b.registry, "resourcePolicies", store.New(resourcePolicyKeyFn))
	},
	func(b *InMemoryBackend) {
		b.deliveryDestinations = store.Register(
			b.registry, "deliveryDestinations", store.New(deliveryDestinationKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.deliverySources = store.Register(b.registry, "deliverySources", store.New(deliverySourceKeyFn))
	},
	func(b *InMemoryBackend) {
		b.destinations = store.Register(b.registry, "destinations", store.New(cwlDestinationKeyFn))
	},
	func(b *InMemoryBackend) {
		b.indexPolicies = store.Register(b.registry, "indexPolicies", store.New(indexPolicyKeyFn))
	},
	func(b *InMemoryBackend) {
		b.transformers = store.Register(b.registry, "transformers", store.New(transformerKeyFn))
	},
	func(b *InMemoryBackend) {
		b.integrations = store.Register(b.registry, "integrations", store.New(cwlIntegrationKeyFn))
	},
	func(b *InMemoryBackend) {
		b.deletionProtected = store.Register(
			b.registry, "deletionProtected", store.New(deletionProtectionEntryKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.lookupTables = store.Register(b.registry, "lookupTables", store.New(lookupTableKeyFn))
	},
	func(b *InMemoryBackend) {
		b.syslogConfigurations = store.Register(
			b.registry, "syslogConfigurations", store.New(syslogConfigurationKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.storageTierPolicy = store.Register(b.registry, "storageTierPolicy", store.New(storageTierPolicyKeyFn))
	},
}

// registerEphemeralTables registers every never-persisted-but-still-a-resource
// table on b.ephemeralRegistry, so InMemoryBackend.Reset can clear them via one
// ResetAll() call same as the persisted tables, without Snapshot/Restore ever
// touching them (b.ephemeralRegistry is never passed to persistence.go).
func registerEphemeralTables(b *InMemoryBackend) {
	b.queries = store.Register(b.ephemeralRegistry, "queries", store.New(storedQueryKeyFn))
	b.anomalies = store.Register(b.ephemeralRegistry, "anomalies", store.New(anomalyKeyFn))
	b.anomalyByDetector = b.anomalies.AddIndex("byDetector", anomalyDetectorIndexKeyFn)
	b.scheduledQueryRuns = store.Register(
		b.ephemeralRegistry, "scheduledQueryRuns", store.New(scheduledQueryRunHistoryKeyFn),
	)
}

// registerRegionTables constructs the four region-qualified "dirty" tables
// (see the package doc above): groups, streams, subscriptionFilters, and
// metricFilters. They are NOT registered on b.registry or b.ephemeralRegistry
// -- Snapshot/Restore drive them explicitly through DTOs (persistence.go) and
// Reset clears them with a direct .Reset() call (backend.go) -- but each still
// gets a store.Table plus the secondary index that replaces the old
// streams[region][group]-style nested-map lookup.
func registerRegionTables(b *InMemoryBackend) {
	b.groups = store.New(logGroupKeyFn)
	b.groupsByRegion = b.groups.AddIndex("byRegion", func(g *LogGroup) string { return g.region })

	b.streams = store.New(logStreamKeyFn)
	b.streamsByGroup = b.streams.AddIndex("byGroup", logStreamGroupIndexKeyFn)

	b.subscriptionFilters = store.New(subscriptionFilterKeyFn)
	b.subscriptionFiltersByGroup = b.subscriptionFilters.AddIndex("byGroup", subscriptionFilterGroupIndexKeyFn)

	b.metricFilters = store.New(metricFilterKeyFn)
	b.metricFiltersByGroup = b.metricFilters.AddIndex("byGroup", metricFilterGroupIndexKeyFn)
}
