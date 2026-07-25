package xray

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// xraySnapshotVersion identifies the shape of backendSnapshot. It must be
// bumped whenever a change here would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (rather than attempts to partially decode) any
// mismatch -- see Restore below. This mirrors the services/sqs pilot
// (commit 0f09d77c) and the services/ec2 conversion (commit 12e611a4).
const xraySnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the X-Ray backend.
//
// Groups, SamplingRules, Traces, Insights, ResourcePolicies, TraceRetrievals,
// and SamplingStats are each backed by a store.Table and persisted via that
// table's own deterministic, key-sorted Snapshot()/Restore() (see
// pkgs/store's package doc).
//
// parsedSegments and serviceWindows are ALSO store.Table-backed (see
// store_setup.go) but are deliberately NOT part of this snapshot:
//   - parsedSegments holds *Segment values whose Document field is
//     json:"-" (the raw segment JSON is stored once, on Trace.Segments,
//     to avoid duplicating it); a generic Table snapshot of Segment would
//     silently drop Document. Restore instead re-derives parsedSegments
//     (and its traceSegments index) from each restored Trace's raw
//     Segments JSON, exactly as before this conversion.
//   - serviceWindows is ephemeral insight fault-rate tracking state that
//     must always start empty after a restore (matching pre-conversion
//     behaviour), so it is simply Reset rather than round-tripped.
type backendSnapshot struct {
	LastRuleModification time.Time                    `json:"lastRuleModification"`
	RetrievedTraces      map[string][]*Trace          `json:"retrievedTraces,omitempty"`
	InsightEvents        map[string][]*InsightEvent   `json:"insightEvents"`
	ResourceTags         map[string]map[string]string `json:"resourceTags,omitempty"`
	EncryptionConfig     *EncryptionConfig            `json:"encryptionConfig,omitempty"`
	TraceSegmentDest     string                       `json:"traceSegmentDest,omitempty"`
	Groups               []*Group                     `json:"groups"`
	SamplingRules        []*SamplingRule              `json:"samplingRules"`
	Traces               []*Trace                     `json:"traces"`
	Insights             []*Insight                   `json:"insights"`
	ResourcePolicies     []*ResourcePolicy            `json:"resourcePolicies"`
	TraceRetrievals      []*TraceRetrieval            `json:"traceRetrievals"`
	SamplingStats        []*SamplingStatisticSummary  `json:"samplingStats,omitempty"`
	IndexingRules        []*IndexingRule              `json:"indexingRules,omitempty"`
	Version              int                          `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	cfgCopy := *b.encryptionConfig

	// Deep-copy indexing rules.
	rules := make([]*IndexingRule, len(b.indexingRules))
	for i, r := range b.indexingRules {
		cp := *r
		rules[i] = &cp
	}

	snap := backendSnapshot{
		Version:              xraySnapshotVersion,
		EncryptionConfig:     &cfgCopy,
		Groups:               b.groups.Snapshot(),
		SamplingRules:        b.samplingRules.Snapshot(),
		Traces:               b.traces.Snapshot(),
		Insights:             b.insights.Snapshot(),
		InsightEvents:        b.insightEvents,
		ResourcePolicies:     b.resourcePolicies.Snapshot(),
		TraceRetrievals:      b.traceRetrievals.Snapshot(),
		RetrievedTraces:      b.retrievedTraces,
		IndexingRules:        rules,
		SamplingStats:        b.samplingStats.Snapshot(),
		LastRuleModification: b.lastRuleModification,
		TraceSegmentDest:     b.traceSegmentDest,
		ResourceTags:         b.resourceTags,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "xray: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// ensureNonNilMaps guarantees the plain-map fields in the snapshot are
// non-nil. Table-backed fields don't need this: store.Table.Restore treats a
// nil slice the same as an empty one.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.InsightEvents == nil {
		snap.InsightEvents = make(map[string][]*InsightEvent)
	}

	if snap.RetrievedTraces == nil {
		snap.RetrievedTraces = make(map[string][]*Trace)
	}

	if snap.ResourceTags == nil {
		snap.ResourceTags = make(map[string]map[string]string)
	}
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "xray", data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != xraySnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never
		// be partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead
		// of erroring, since this is an expected, recoverable condition
		// (e.g. upgrading gopherstack across a snapshot-format change), not
		// data corruption.
		logger.Load(ctx).WarnContext(ctx,
			"xray: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", xraySnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	// Restoring groups also rebuilds the groupsByARN secondary index (see
	// store.Table.Restore).
	b.groups.Restore(snap.Groups)
	b.samplingRules.Restore(snap.SamplingRules)
	b.traces.Restore(snap.Traces)
	b.insights.Restore(snap.Insights)
	b.resourcePolicies.Restore(snap.ResourcePolicies)
	b.traceRetrievals.Restore(snap.TraceRetrievals)
	b.samplingStats.Restore(snap.SamplingStats)

	b.insightEvents = snap.InsightEvents
	b.retrievedTraces = snap.RetrievedTraces
	b.lastRuleModification = snap.LastRuleModification
	b.traceSegmentDest = snap.TraceSegmentDest
	b.resourceTags = snap.ResourceTags

	if snap.EncryptionConfig != nil {
		b.encryptionConfig = snap.EncryptionConfig
	}

	if len(snap.IndexingRules) > 0 {
		b.indexingRules = snap.IndexingRules
	}

	// Ensure the built-in Default sampling rule is always present after restore.
	if !b.samplingRules.Has(defaultSamplingRuleName) {
		b.samplingRules.Put(b.defaultSamplingRule())
	}

	// Rebuild parsed segment indexes from stored traces. This can't be a
	// direct table restore: Segment.Document is json:"-" (the raw segment
	// JSON lives once, on Trace.Segments), so it must be re-derived by
	// re-unmarshaling each trace's raw segment strings, exactly as before
	// this conversion.
	b.parsedSegments.Reset()
	b.retrievalTimes = make(map[string]time.Time)
	b.serviceWindows.Reset()

	for _, t := range b.traces.All() {
		for _, rawSeg := range t.Segments {
			var seg Segment
			if err := json.Unmarshal([]byte(rawSeg), &seg); err == nil {
				seg.Document = rawSeg
				b.parsedSegments.Put(&seg)
			}
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
