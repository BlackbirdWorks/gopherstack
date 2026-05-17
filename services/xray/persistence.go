package xray

import (
	"encoding/json"
	"log/slog"
	"time"
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
	SamplingStats        map[string]*SamplingStatisticSummary `json:"samplingStats,omitempty"`
	IndexingRules        []*IndexingRule                      `json:"indexingRules,omitempty"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
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
		IndexingRules:        rules,
		SamplingStats:        b.samplingStats,
		LastRuleModification: b.lastRuleModification,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("xray: failed to marshal snapshot", "error", err)

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
		snap.SamplingRules = make(map[string]*SamplingRule)
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

	if snap.SamplingStats == nil {
		snap.SamplingStats = make(map[string]*SamplingStatisticSummary)
	}
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
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
	b.samplingStats = snap.SamplingStats
	b.lastRuleModification = snap.LastRuleModification

	if snap.EncryptionConfig != nil {
		b.encryptionConfig = snap.EncryptionConfig
	}

	if len(snap.IndexingRules) > 0 {
		b.indexingRules = snap.IndexingRules
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
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
