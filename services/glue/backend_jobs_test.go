package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestRefinement2_SeedHelpers tests backend seed helper functions.
func TestRefinement2_SeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seed  func(*glue.InMemoryBackend)
		check func(*testing.T, *glue.InMemoryBackend)
		name  string
	}{
		{
			name: "job_run_seed",
			seed: func(b *glue.InMemoryBackend) {
				b.AddJobRunInternal(&glue.JobRun{
					ID: "jr-seed", JobName: "seed-job", JobRunState: "SUCCEEDED",
				})
			},
			check: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.JobRunCount(b))
			},
		},
		{
			name: "dq_ruleset_seed",
			seed: func(b *glue.InMemoryBackend) {
				b.AddDataQualityRulesetInternal(&glue.DataQualityRuleset{
					Name: "seed-rs", Ruleset: "Rules = [ RowCount > 0 ]",
				})
			},
			check: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.DataQualityRulesetCount(b))
			},
		},
		{
			name: "dq_eval_run_seed",
			seed: func(b *glue.InMemoryBackend) {
				b.AddDataQualityEvalRunInternal(&glue.DataQualityEvaluationRun{
					RunID: "seed-run-1", Status: "SUCCEEDED",
				})
			},
			check: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.DataQualityEvalRunCount(b))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend(testAccountID, testRegion)
			tt.seed(b)
			tt.check(t, b)
		})
	}
}
