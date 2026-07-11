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

// TestFIS_PersistenceFullStateRoundTrip exercises a Snapshot->Restore round
// trip across every pkgs/store-backed table at once (templates, experiments,
// and targetAccountConfigs -- see store_setup.go), plus the cascade-delete
// path that walks the targetAccountConfigsByTemplate secondary index.
func TestFIS_PersistenceFullStateRoundTrip(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()

	b.InjectTemplate(&fis.ExperimentTemplate{
		ID:             "EXT-full1",
		Arn:            "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-full1",
		Description:    "full state template",
		Tags:           map[string]string{"env": "full"},
		Actions:        map[string]fis.ExperimentTemplateAction{},
		Targets:        map[string]fis.ExperimentTemplateTarget{},
		CreationTime:   time.Now().UTC(),
		LastUpdateTime: time.Now().UTC(),
	})

	_, err := b.CreateTargetAccountConfiguration(
		"EXT-full1", "111111111111", "arn:aws:iam::111111111111:role/fis-role", "cross-account",
	)
	require.NoError(t, err)

	endTime := time.Now().UTC()
	b.InjectExperiment(&fis.Experiment{
		ID:                   "EXP-full1",
		Arn:                  "arn:aws:fis:us-east-1:000000000000:experiment/EXP-full1",
		ExperimentTemplateID: "EXT-full1",
		Status:               fis.ExperimentStatus{Status: "completed"},
		StartTime:            time.Now().UTC().Add(-time.Hour),
		EndTime:              &endTime,
		Tags:                 map[string]string{"run": "full"},
	})

	require.Equal(t, 1, b.TemplateCount())
	require.Equal(t, 1, b.ExperimentCount())
	require.Equal(t, 1, b.TargetAccountConfigCount())

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := fis.NewTestBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, b2.TemplateCount())
	assert.Equal(t, 1, b2.ExperimentCount())
	assert.Equal(t, 1, b2.TargetAccountConfigCount())

	tpl, err := b2.GetExperimentTemplate("EXT-full1")
	require.NoError(t, err)
	assert.Equal(t, "full state template", tpl.Description)
	assert.Equal(t, "full", tpl.Tags["env"])

	cfg, err := b2.GetTargetAccountConfiguration("EXT-full1", "111111111111")
	require.NoError(t, err)
	assert.Equal(t, "cross-account", cfg.Description)

	cfgs, err := b2.ListTargetAccountConfigurations("EXT-full1")
	require.NoError(t, err)
	assert.Len(t, cfgs, 1)

	exp, err := b2.GetExperiment("EXP-full1")
	require.NoError(t, err)
	assert.Equal(t, "completed", exp.Status.Status)
	assert.Equal(t, "full", exp.Tags["run"])

	// Deleting the template must cascade-delete its target account configs
	// via the byTemplate secondary index.
	require.NoError(t, b2.DeleteExperimentTemplate("EXT-full1"))
	assert.Equal(t, 0, b2.TargetAccountConfigCount())
}

// TestFIS_PersistenceIncompatibleVersionDiscarded verifies that Restore
// discards (rather than partially decodes) a snapshot whose version does not
// match the current backend shape, resetting to an empty backend without
// returning an error.
func TestFIS_PersistenceIncompatibleVersionDiscarded(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.InjectTemplate(&fis.ExperimentTemplate{
		ID:      "EXT-stale",
		Arn:     "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-stale",
		Actions: map[string]fis.ExperimentTemplateAction{},
		Targets: map[string]fis.ExperimentTemplateTarget{},
	})
	require.Equal(t, 1, b.TemplateCount())

	badSnap := []byte(`{"version":999,"tables":{}}`)
	require.NoError(t, b.Restore(t.Context(), badSnap))

	assert.Equal(t, 0, b.TemplateCount())
	assert.Equal(t, 0, b.ExperimentCount())
	assert.Equal(t, 0, b.TargetAccountConfigCount())
}

// TestFIS_PersistenceMalformedSnapshotErrors verifies that Restore returns an
// error (rather than silently resetting) when the snapshot bytes are not
// valid JSON at all.
func TestFIS_PersistenceMalformedSnapshotErrors(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()

	err := b.Restore(t.Context(), []byte("{not valid json"))
	assert.Error(t, err)
}
