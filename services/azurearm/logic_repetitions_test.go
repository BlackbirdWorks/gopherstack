package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogicSDK_ActionRepetitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		actions map[string]any
		name    string
		action  string
		want    []string
		indexes [][]int32
		outputs []any
		scope   bool
	}{
		{
			name: "foreach inner action",
			actions: map[string]any{"Loop": map[string]any{
				"type": "Foreach", "foreach": []any{"a", "b", "c"}, "operationOptions": "Sequential",
				"runAfter": map[string]any{},
				"actions": map[string]any{"Echo": map[string]any{
					"type": "Compose", "inputs": "@item()", "runAfter": map[string]any{},
				}},
			}},
			action:  "Echo",
			want:    []string{"000000", "000001", "000002"},
			indexes: [][]int32{{0}, {1}, {2}},
			outputs: []any{"a", "b", "c"},
		},
		{
			name: "foreach scope iterations",
			actions: map[string]any{"Loop": map[string]any{
				"type": "Foreach", "foreach": []any{1, 2}, "operationOptions": "Sequential",
				"runAfter": map[string]any{},
				"actions": map[string]any{"Echo": map[string]any{
					"type": "Compose", "inputs": "@item()", "runAfter": map[string]any{},
				}},
			}},
			action:  "Loop",
			scope:   true,
			want:    []string{"000000", "000001"},
			indexes: [][]int32{{0}, {1}},
		},
		{
			name: "nested foreach full path",
			actions: map[string]any{"Outer": map[string]any{
				"type": "Foreach", "foreach": []any{1, 2}, "operationOptions": "Sequential",
				"runAfter": map[string]any{},
				"actions": map[string]any{"Inner": map[string]any{
					"type": "Foreach", "foreach": []any{"x", "y"}, "operationOptions": "Sequential",
					"runAfter": map[string]any{},
					"actions": map[string]any{"Echo": map[string]any{
						"type": "Compose", "inputs": "@item()", "runAfter": map[string]any{},
					}},
				}},
			}},
			action:  "Echo",
			want:    []string{"000000000000", "000000000001", "000001000000", "000001000001"},
			indexes: [][]int32{{0, 0}, {0, 1}, {1, 0}, {1, 1}},
			outputs: []any{"x", "y", "x", "y"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			s.create(t, "wf", logicDef(requestTrigger(nil), tc.actions))
			s.invoke(t, "wf", `{}`)

			run := s.latestRun(t, "wf")
			reps := []*armlogic.WorkflowRunActionRepetitionDefinition{}

			if tc.scope {
				pg := s.factory.NewWorkflowRunActionScopeRepetitionsClient().NewListPager(
					logicRG, "wf", *run.Name, tc.action, nil)
				for pg.More() {
					p, err := pg.NextPage(t.Context())
					require.NoError(t, err)

					reps = append(reps, p.Value...)
				}
			} else {
				pg := s.factory.NewWorkflowRunActionRepetitionsClient().NewListPager(
					logicRG, "wf", *run.Name, tc.action, nil)
				for pg.More() {
					p, err := pg.NextPage(t.Context())
					require.NoError(t, err)

					reps = append(reps, p.Value...)
				}
			}

			require.Len(t, reps, len(tc.want))

			byName := map[string]*armlogic.WorkflowRunActionRepetitionDefinition{}
			for _, r := range reps {
				byName[*r.Name] = r
			}

			for i, name := range tc.want {
				r := byName[name]
				require.NotNil(t, r, name)
				assert.Equal(t, armlogic.WorkflowStatusSucceeded, *r.Properties.Status)

				var got []int32
				for _, ix := range r.Properties.RepetitionIndexes {
					got = append(got, *ix.ItemIndex)
				}

				assert.Equal(t, tc.indexes[i], got)

				if !tc.scope {
					assert.Equal(t, tc.outputs[i], s.content(t, r.Properties.OutputsLink))
				}

				if tc.scope {
					continue
				}

				one, err := s.factory.NewWorkflowRunActionRepetitionsClient().Get(
					t.Context(), logicRG, "wf", *run.Name, tc.action, name, nil)
				require.NoError(t, err)
				assert.Equal(t, name, *one.Name)
			}
		})
	}

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		s := newLogicStack(t)
		s.create(t, "wf", logicDef(requestTrigger(nil), map[string]any{"Loop": map[string]any{
			"type": "Foreach", "foreach": []any{1}, "runAfter": map[string]any{},
			"actions": map[string]any{"Echo": map[string]any{
				"type": "Compose", "inputs": "@item()", "runAfter": map[string]any{},
			}},
		}}))
		s.invoke(t, "wf", `{}`)

		run := s.latestRun(t, "wf")
		base := "/workflows/wf/runs/" + *run.Name + "/actions/"

		for _, p := range []string{"Nope/repetitions", "Echo/repetitions/000099", "Loop/scopeRepetitions/000099"} {
			code, body := s.rawJSON(t, http.MethodGet, base+p, "")
			assert.Equal(t, http.StatusNotFound, code, p)
			assert.NotNil(t, body["error"], p)
		}
	})
}
