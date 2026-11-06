package gremlin_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeSteps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		script  string
		check   string
		wantErr string
		want    []string
	}{
		{"mergev_matches_existing", `g.mergeV([(T.label):'person', name:'marko'])`, `g.V().hasLabel('person').count()`,
			"", []string{"3"}},
		{"mergev_creates_missing", `g.mergeV([(T.label):'person', name:'ripple'])`, `g.V().hasLabel('person').count()`,
			"", []string{"4"}},
		{"mergev_default_label", `g.mergeV([name:'ghost'])`, `g.V().has('name','ghost').label()`,
			"", []string{"vertex"}},
		{"mergev_by_id", `g.mergeV([(T.id):'marko'])`, `g.V().count()`, "", []string{"4"}},
		{
			"mergev_on_create", `g.mergeV([(T.label):'person', name:'ripple']).option(Merge.onCreate, [age:99])`,
			`g.V().has('name','ripple').values('age')`, "", []string{"99"},
		},
		{
			"mergev_on_create_skipped_when_matched",
			`g.mergeV([(T.label):'person', name:'marko']).option(Merge.onCreate, [age:99])`,
			`g.V('marko').values('age')`, "", []string{"29"},
		},
		{
			"mergev_on_match", `g.mergeV([(T.label):'person', name:'marko']).option(Merge.onMatch, [age:30])`,
			`g.V('marko').values('age')`, "", []string{"30"},
		},
		{
			"mergev_on_match_skipped_when_created",
			`g.mergeV([(T.label):'person', name:'new']).option(Merge.onMatch, [age:30]).option(Merge.onCreate, [age:1])`,
			`g.V().has('name','new').values('age')`, "", []string{"1"},
		},
		{"mergev_emits_matches", `g.mergeV([(T.label):'person']).values('name')`, `g.V().count()`,
			"", []string{"4"}},
		{"mergev_from_traverser", `g.inject([(T.label):'person', name:'josh']).mergeV()`,
			`g.V().hasLabel('person').count()`, "", []string{"3"}},
		{"mergev_on_match_rejects_id", `g.mergeV([name:'marko']).option(Merge.onMatch, [(T.id):'x'])`,
			`g.V().count()`, "T.id", nil},
		{"mergev_on_create_conflict", `g.mergeV([(T.label):'a', name:'zz']).option(Merge.onCreate, [(T.label):'b'])`,
			`g.V().count()`, "conflicts", nil},
		{
			"mergee_creates", `g.mergeE([(T.label):'knows', (Direction.OUT):'vadas', (Direction.IN):'josh'])`,
			`g.E().hasLabel('knows').count()`, "", []string{"3"},
		},
		{
			"mergee_matches", `g.mergeE([(T.label):'knows', (Direction.OUT):'marko', (Direction.IN):'josh'])`,
			`g.E().hasLabel('knows').count()`, "", []string{"2"},
		},
		{
			"mergee_on_match",
			`g.mergeE([(T.label):'knows', (Direction.OUT):'marko', (Direction.IN):'josh']).option(Merge.onMatch, [since:2020])`,
			`g.E().has('since',2020).count()`, "", []string{"1"},
		},
		{
			"mergee_on_create",
			`g.mergeE([(T.label):'knows', (Direction.OUT):'vadas', (Direction.IN):'josh']).option(Merge.onCreate, [weight:0.1])`,
			`g.E().has('weight',0.1).count()`, "", []string{"1"},
		},
		{
			"mergee_missing_vertex", `g.mergeE([(T.label):'knows', (Direction.OUT):'nobody', (Direction.IN):'josh'])`,
			`g.E().count()`, "Could not find", nil,
		},
		{"mergee_needs_label", `g.mergeE([(Direction.OUT):'vadas', (Direction.IN):'josh'])`,
			`g.E().count()`, "T.label", nil},
		{
			"mergee_vertex_maps",
			`g.mergeE([(T.label):'knows']).option(Merge.outV, [name:'vadas']).option(Merge.inV, [name:'josh'])`,
			`g.E().hasLabel('knows').count()`, "", []string{"3"},
		},
		{
			"mergee_by_edge_id", `g.mergeE([(T.id):'e-new', (T.label):'x', (Direction.OUT):'marko', (Direction.IN):'lop'])`,
			`g.E('e-new').label()`, "", []string{"x"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := seeded(t)
			_, err := evalAll(t, g, tt.script)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)

			got, err := evalAll(t, g, tt.check)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
