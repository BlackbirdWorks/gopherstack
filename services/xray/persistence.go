package xray

import (
	"context"
	"encoding/json"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	LastRuleModification time.Time                            `json:"lastRuleModification"`
	EncryptionConfig     *EncryptionConfig                    `json:"encryptionConfig,omitempty"`
	Groups               map[string]*Group                    `json:"groups"`
	SamplingRules        map[string]*SamplingRule             `json:"samplingRules"`
	Traces               map[string]*Trace                    `json:"traces"`
	Insights             map[string]*Insight                  `json:"insights"`
	InsightEvents        map[string][]*InsightEvent           `json:"insightEvents"`
	ResourcePolicies     map[string]*ResourcePolicy           `json:"resourcePolicies"`
	TraceRetrievals      map[string]*TraceRetrieval           `json:"traceRetrievals"`
	RetrievedTraces      map[string][]*Trace                  `json:"retrievedTraces,omitempty"`
	SamplingStats        map[string]*SamplingStatisticSummary `json:"samplingStats,omitempty"`
	IndexingRules        []*IndexingRule                      `json:"indexingRules,omitempty"`
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
		EncryptionConfig:     &cfgCopy,
		Groups:               b.groups,
		SamplingRules:        b.samplingRules,
		Traces:               b.traces,
		Insights:             b.insights,
		InsightEvents:        b.insightEvents,
		ResourcePolicies:     b.resourcePolicies,
		TraceRetrievals:      b.traceRetrievals,
		RetrievedTraces:      b.retrievedTraces,
		IndexingRules:        rules,
		SamplingStats:        b.samplingStats,
		LastRuleModification: b.lastRuleModification,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "xray: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// ensureNonNilMaps guarantees all map fields in the snapshot are non-nil.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Groups == nil {
		snap.Groups = make(map[string]*Group)
	}

	if snap.SamplingRules == nil {
		snap.SamplingRules = defaultSamplingRules()
	}

	if snap.Traces == nil {
		snap.Traces = make(map[string]*Trace)
	}

	if snap.Insights == nil {
		snap.Insights = make(map[string]*Insight)
	}

	if snap.InsightEvents == nil {
		snap.InsightEvents = make(map[string][]*InsightEvent)
	}

	if snap.ResourcePolicies == nil {
		snap.ResourcePolicies = make(map[string]*ResourcePolicy)
	}

	if snap.TraceRetrievals == nil {
		snap.TraceRetrievals = make(map[string]*TraceRetrieval)
	}

	if snap.RetrievedTraces == nil {
		snap.RetrievedTraces = make(map[string][]*Trace)
	}

	if snap.SamplingStats == nil {
		snap.SamplingStats = make(map[string]*SamplingStatisticSummary)
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

	b.groups = snap.Groups
	b.samplingRules = snap.SamplingRules
	b.traces = snap.Traces
	b.insights = snap.Insights
	b.insightEvents = snap.InsightEvents
	b.resourcePolicies = snap.ResourcePolicies
	b.traceRetrievals = snap.TraceRetrievals
	b.retrievedTraces = snap.RetrievedTraces
	b.samplingStats = snap.SamplingStats
	b.lastRuleModification = snap.LastRuleModification

	if snap.EncryptionConfig != nil {
		b.encryptionConfig = snap.EncryptionConfig
	}

	if len(snap.IndexingRules) > 0 {
		b.indexingRules = snap.IndexingRules
	}

	// Ensure the built-in Default sampling rule is always present after restore.
	if _, ok := b.samplingRules[defaultSamplingRuleName]; !ok {
		maps.Copy(b.samplingRules, defaultSamplingRules())
	}

	// Rebuild parsed segment indexes from stored traces.
	b.parsedSegments = make(map[string]*Segment)
	b.traceSegments = make(map[string][]*Segment)

	for traceID, t := range b.traces {
		for _, rawSeg := range t.Segments {
			var seg Segment
			if err := json.Unmarshal([]byte(rawSeg), &seg); err == nil {
				seg.Document = rawSeg
				segKey := traceID + ":" + seg.ID
				b.parsedSegments[segKey] = &seg
				b.traceSegments[traceID] = append(b.traceSegments[traceID], &seg)
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
