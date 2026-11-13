package gcpdatastore

import (
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	scopeCollection = "Collection"
	scopeGroup      = "Collection group"
	keyAlias        = "__name__"
)

func dirText(desc bool) string {
	if desc {
		return "DESC"
	}

	return "ASC"
}

// indexProperties renders the index a plan reads in the format of the PlanSummary.indexes_used example.
func (p *queryPlan) indexProperties() string {
	eq := make([]string, 0, len(p.eqProps))

	for name := range p.eqProps {
		if name != keyProperty {
			eq = append(eq, name)
		}
	}

	slices.Sort(eq)

	cols := make([]string, 0, len(eq)+len(p.orders))
	for _, n := range eq {
		cols = append(cols, n+" ASC")
	}

	for _, o := range p.orders {
		name := o.name
		if name == keyProperty {
			name = keyAlias
		}

		cols = append(cols, name+" "+dirText(o.desc))
	}

	return "(" + strings.Join(cols, ", ") + ")"
}

func (p *queryPlan) planSummary() *datastorepb.PlanSummary {
	scope := scopeGroup
	if p.ancestor != nil {
		scope = scopeCollection
	}

	idx, err := structpb.NewStruct(map[string]any{"query_scope": scope, "properties": p.indexProperties()})
	if err != nil {
		return &datastorepb.PlanSummary{}
	}

	return &datastorepb.PlanSummary{IndexesUsed: []*structpb.Struct{idx}}
}

func explainMetrics(
	p *queryPlan, opts *datastorepb.ExplainOptions, returned int, took time.Duration,
) *datastorepb.ExplainMetrics {
	m := &datastorepb.ExplainMetrics{PlanSummary: p.planSummary()}
	if !opts.GetAnalyze() {
		return m
	}

	debug, err := structpb.NewStruct(map[string]any{
		"documents_scanned": float64(returned), "index_entries_scanned": float64(returned),
	})
	if err != nil {
		debug = &structpb.Struct{}
	}

	m.ExecutionStats = &datastorepb.ExecutionStats{
		ResultsReturned:   int64(returned),
		ExecutionDuration: durationpb.New(took),
		ReadOperations:    int64(max(returned, 1)),
		DebugStats:        debug,
	}

	return m
}
