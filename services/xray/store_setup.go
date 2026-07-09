package xray

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry. See pkgs/store's package doc and
// the services/ec2 (commit 12e611a4) and services/sqs (commit 0f09d77c)
// conversions this follows.
//
// A handful of fields are deliberately NOT converted and remain plain maps --
// see the field-level comments on InMemoryBackend in backend.go for the list
// and why (insightEvents/retrievedTraces are slice-valued; retrievalTimes and
// resourceTags do not hold *T values).
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func groupKeyFn(g *Group) string                           { return g.GroupName }
func groupARNKeyFn(g *Group) string                        { return g.GroupARN }
func samplingRuleKeyFn(r *SamplingRule) string             { return r.RuleName }
func traceKeyFn(t *Trace) string                           { return t.TraceID }
func segmentKeyFn(s *Segment) string                       { return s.TraceID + ":" + s.ID }
func segmentTraceKeyFn(s *Segment) string                  { return s.TraceID }
func insightKeyFn(i *Insight) string                       { return i.InsightID }
func resourcePolicyKeyFn(p *ResourcePolicy) string         { return p.PolicyName }
func traceRetrievalKeyFn(t *TraceRetrieval) string         { return t.RetrievalToken }
func samplingStatKeyFn(s *SamplingStatisticSummary) string { return s.RuleName }
func serviceWindowKeyFn(w *serviceInsightWindow) string    { return w.Name }

// registerAllTables registers every converted resource map on b.registry
// exactly once. It must be called during construction only (immediately
// after b.registry is created), never on every Reset() -- store.Register
// panics on a duplicate name, so runtime resets go through
// registry.ResetAll() instead (see InMemoryBackend.Reset in backend.go).
//
// groupsByARN and traceSegments are secondary indexes rather than their own
// tables: groupsByARN groups Group values by ARN ("group lookup by ARN"),
// and traceSegments groups Segment values by TraceID ("segments of a
// trace" -- the classic nested-collection case store.Index exists for).
//
// parsedSegments and serviceWindows are registered here (so Reset() covers
// them via registry.ResetAll()) but are deliberately excluded from
// persistence -- see persistence.go for why (Segment.Document is json:"-",
// and serviceWindows is ephemeral insight-detection state that must not
// survive a restore).
func registerAllTables(b *InMemoryBackend) {
	b.groups = store.Register(b.registry, "groups", store.New(groupKeyFn))
	b.groupsByARN = b.groups.AddIndex("byARN", groupARNKeyFn)

	b.samplingRules = store.Register(b.registry, "samplingRules", store.New(samplingRuleKeyFn))
	b.traces = store.Register(b.registry, "traces", store.New(traceKeyFn))

	b.parsedSegments = store.Register(b.registry, "parsedSegments", store.New(segmentKeyFn))
	b.traceSegments = b.parsedSegments.AddIndex("byTrace", segmentTraceKeyFn)

	b.insights = store.Register(b.registry, "insights", store.New(insightKeyFn))
	b.resourcePolicies = store.Register(b.registry, "resourcePolicies", store.New(resourcePolicyKeyFn))
	b.traceRetrievals = store.Register(b.registry, "traceRetrievals", store.New(traceRetrievalKeyFn))
	b.samplingStats = store.Register(b.registry, "samplingStats", store.New(samplingStatKeyFn))
	b.serviceWindows = store.Register(b.registry, "serviceWindows", store.New(serviceWindowKeyFn))
}
