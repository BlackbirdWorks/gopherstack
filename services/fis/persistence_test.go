package fis_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

func TestFIS_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *fis.ExportedInMemoryBackend)
		verify func(t *testing.T, b *fis.ExportedInMemoryBackend)
		name   string
	}{
		{
			name:  "empty_backend",
			setup: func(_ *fis.ExportedInMemoryBackend) {},
			verify: func(t *testing.T, b *fis.ExportedInMemoryBackend) {
				t.Helper()

				assert.Equal(t, 0, b.TemplateCount())
				assert.Equal(t, 0, b.ExperimentCount())
			},
		},
		{
			name: "experiment_template_preserved",
			setup: func(b *fis.ExportedInMemoryBackend) {
				b.InjectTemplate(&fis.ExperimentTemplate{
					ID:             "EXT-abc123",
					Arn:            "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-abc123",
					Description:    "test template",
					Tags:           map[string]string{"env": "test"},
					Actions:        map[string]fis.ExperimentTemplateAction{},
					Targets:        map[string]fis.ExperimentTemplateTarget{},
					CreationTime:   time.Now().UTC(),
					LastUpdateTime: time.Now().UTC(),
				})
			},
			verify: func(t *testing.T, b *fis.ExportedInMemoryBackend) {
				t.Helper()

				assert.Equal(t, 1, b.TemplateCount())

				tpl, err := b.GetExperimentTemplate("EXT-abc123")
				require.NoError(t, err)
				assert.Equal(t, "test template", tpl.Description)
				assert.Equal(t, "test", tpl.Tags["env"])
			},
		},
		{
			name: "experiment_preserved",
			setup: func(b *fis.ExportedInMemoryBackend) {
				endTime := time.Now().UTC()
				b.InjectExperiment(&fis.Experiment{
					ID:        "EXP-xyz999",
					Arn:       "arn:aws:fis:us-east-1:000000000000:experiment/EXP-xyz999",
					Status:    fis.ExperimentStatus{Status: "completed"},
					StartTime: time.Now().UTC().Add(-time.Hour),
					EndTime:   &endTime,
					Tags:      map[string]string{"run": "1"},
				})
			},
			verify: func(t *testing.T, b *fis.ExportedInMemoryBackend) {
				t.Helper()

				assert.Equal(t, 1, b.ExperimentCount())

				exp, err := b.GetExperiment("EXP-xyz999")
				require.NoError(t, err)
				assert.Equal(t, "completed", exp.Status.Status)
				assert.Equal(t, "1", exp.Tags["run"])
			},
		},
		{
			name: "arn_indexes_rebuilt",
			setup: func(b *fis.ExportedInMemoryBackend) {
				b.InjectTemplate(&fis.ExperimentTemplate{
					ID:      "EXT-arn1",
					Arn:     "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-arn1",
					Actions: map[string]fis.ExperimentTemplateAction{},
					Targets: map[string]fis.ExperimentTemplateTarget{},
				})

				b.InjectExperiment(&fis.Experiment{
					ID:     "EXP-arn2",
					Arn:    "arn:aws:fis:us-east-1:000000000000:experiment/EXP-arn2",
					Status: fis.ExperimentStatus{Status: "stopped"},
					Tags:   map[string]string{},
				})
			},
			verify: func(t *testing.T, b *fis.ExportedInMemoryBackend) {
				t.Helper()

				// ListTagsForResource uses ARN index — this verifies indexes were rebuilt.
				tagMap, err := b.ListTagsForResource(
					"arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-arn1",
				)
				require.NoError(t, err)
				assert.NotNil(t, tagMap)

				tagMap2, err := b.ListTagsForResource(
					"arn:aws:fis:us-east-1:000000000000:experiment/EXP-arn2",
				)
				require.NoError(t, err)
				assert.NotNil(t, tagMap2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := fis.NewTestBackend()
			tt.setup(b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := fis.NewTestBackend()
			err := b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}
