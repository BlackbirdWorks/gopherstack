package cloudwatch

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend with a pure, value-derived
// key is registered exactly once, here, as a *store.Table[T] on b.registry.
// See pkgs/store's package doc and the services/ec2 conversion (commit
// 12e611a4) for the pattern this follows.
//
// Two resource fields are deliberately NOT registered here and remain plain
// maps:
//   - metrics (map[string]map[string]*metricRecord): PutMetricData is the
//     hottest op in this backend (up to cwMaxMetricDataPerRequest=1000 data
//     points per request) and every access site does a direct two-level
//     namespace->composite-key lookup; store.Table has no native concept of a
//     nested/composite-keyed collection, and flattening it into a single
//     Table keyed by "namespace|compositeKey" would require adding a
//     Namespace field to metricRecord purely to reconstruct identity for a
//     secondary Index, adding an extra key-concat + index-maintenance layer to
//     the single hottest write path for no behavioural benefit. The
//     b.totalMetrics running counter (#60) that avoids an O(namespaces) walk
//     would also need reworking around Index bookkeeping. Left inline,
//     matching the sqs pilot's decision to leave the (also hot, high-volume)
//     per-queue messages slice inline.
//   - alarmHistory (map[string][]AlarmHistoryItem): slice-valued, not
//     map[string]*T -- store.Table stores one *V per key, not a growable
//     per-key slice, so it does not fit this shape.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func alarmsKeyFn(v *MetricAlarm) string                 { return v.AlarmName }
func compositeAlarmsKeyFn(v *CompositeAlarm) string     { return v.AlarmName }
func logAlarmsKeyFn(v *LogAlarm) string                 { return v.AlarmName }
func dashboardsKeyFn(v *dashboardRecord) string         { return v.Name }
func datasetsKeyFn(v *Dataset) string                   { return v.DatasetID }
func otelEnrichmentKeyFn(v *OTelEnrichmentState) string { return v.Key }

func anomalyDetectorsKeyFn(v *AnomalyDetector) string {
	return anomalyDetectorKey(v.Namespace, v.MetricName, v.Stat, v.Dimensions)
}

func insightRulesKeyFn(v *InsightRule) string     { return v.Name }
func metricStreamsKeyFn(v *MetricStream) string   { return v.Name }
func alarmMuteRulesKeyFn(v *AlarmMuteRule) string { return v.Name }

// registerAllTables registers every converted resource map on b.registry
// exactly once. It must be called during construction only (immediately after
// b.registry is created), never on every Reset() -- store.Register panics on a
// duplicate name, so runtime resets go through registry.ResetAll() instead
// (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call to the concrete field and value type.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.alarms = store.Register(b.registry, "alarms", store.New(alarmsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.compositeAlarms = store.Register(b.registry, "compositeAlarms", store.New(compositeAlarmsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.logAlarms = store.Register(b.registry, "logAlarms", store.New(logAlarmsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.datasets = store.Register(b.registry, "datasets", store.New(datasetsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.otelEnrichment = store.Register(b.registry, "otelEnrichment", store.New(otelEnrichmentKeyFn))
	},
	func(b *InMemoryBackend) {
		b.dashboards = store.Register(b.registry, "dashboards", store.New(dashboardsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.anomalyDetectors = store.Register(b.registry, "anomalyDetectors", store.New(anomalyDetectorsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.insightRules = store.Register(b.registry, "insightRules", store.New(insightRulesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.metricStreams = store.Register(b.registry, "metricStreams", store.New(metricStreamsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.alarmMuteRules = store.Register(b.registry, "alarmMuteRules", store.New(alarmMuteRulesKeyFn))
	},
}
