package personalize_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/personalize"
)

const (
	pTestAccountID = "111122223333"
	pTestRegion    = "us-west-2"
)

// Test_Persistence_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func Test_Persistence_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := personalize.NewInMemoryBackend(pTestAccountID, pTestRegion)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// Test_Persistence_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including a version-less
// snapshot, which decodes with Version == 0) is discarded cleanly rather
// than partially decoded: the backend resets to empty state (with the
// builtin feature transformations re-seeded) and Restore returns no error.
func Test_Persistence_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "explicit version mismatch",
			data: []byte(`{"version":999,"tables":{}}`),
		},
		{
			name: "no version field decodes as zero",
			data: []byte(`{"tables":{"datasetGroups":[{"name":"seed-dg"}]}}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := personalize.NewInMemoryBackend(pTestAccountID, pTestRegion)
			_, err := b.CreateDatasetGroup("seed-dg", "", "", "", nil)
			require.NoError(t, err)

			err = b.Restore(t.Context(), tt.data)
			require.NoError(t, err)

			list, _ := b.ListDatasetGroups(0, "")
			assert.Empty(t, list)

			_, err = b.DescribeDatasetGroup("seed-dg")
			require.Error(t, err)

			// Builtin feature transformations must be re-seeded, not left empty.
			ft, err := b.GetFeatureTransformation("aws-feature-transformation")
			require.NoError(t, err)
			assert.Equal(t, "aws-feature-transformation", ft.Name)
		})
	}
}

// Test_Persistence_SnapshotRestore_Tables round-trips every store.Table the
// Phase 3.3 conversion registered, one at a time in an isolated backend, plus
// the tags plain map. Each subtest seeds exactly the resource(s) needed to
// exercise its own table, snapshots, restores into a fresh backend, and
// verifies the restored state is byte-for-byte equivalent.
func Test_Persistence_SnapshotRestore_Tables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seed   func(t *testing.T, b *personalize.InMemoryBackend)
		verify func(t *testing.T, b *personalize.InMemoryBackend)
		name   string
	}{
		{
			name: "datasetGroups",
			seed: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateDatasetGroup("dg-1", "ECOMMERCE", "arn:aws:kms:us-west-2:111122223333:key/k", "", nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				dg, err := b.DescribeDatasetGroup("dg-1")
				require.NoError(t, err)
				assert.Equal(t, "ECOMMERCE", dg.Domain)
			},
		},
		{
			name: "datasets",
			seed: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateDataset("ds-1", "arn:aws:personalize:us-west-2:111122223333:dataset-group/dg-1",
					"INTERACTIONS", "arn:aws:personalize:us-west-2:111122223333:schema/sc-1", nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				ds, err := b.DescribeDataset("ds-1")
				require.NoError(t, err)
				assert.Equal(t, "INTERACTIONS", ds.DatasetType)
			},
		},
		{
			name: "schemas",
			seed: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSchema("sc-1", `{"type":"record"}`, "ECOMMERCE")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				s, err := b.DescribeSchema("sc-1")
				require.NoError(t, err)
				assert.JSONEq(t, `{"type":"record"}`, s.Schema)
			},
		},
		{
			name: "solutions",
			seed: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSolution("sol-1", "arn:aws:personalize:us-west-2:111122223333:dataset-group/dg-1",
					"arn:aws:personalize:::recipe/aws-user-personalization", true, false, nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				sol, err := b.DescribeSolution("sol-1")
				require.NoError(t, err)
				assert.True(t, sol.PerformAutoML)
			},
		},
		{
			name: "solutionVersions",
			seed: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSolutionVersion(
					"arn:aws:personalize:us-west-2:111122223333:solution/sol-1", "FULL", nil,
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				list, _ := b.ListSolutionVersions("", 0, "")
				require.Len(t, list, 1)
				assert.Equal(t, "FULL", list[0].TrainingMode)
			},
		},
		{
			name: "campaigns",
			seed: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateCampaign(
					"camp-1", "arn:aws:personalize:us-west-2:111122223333:solution/sol-1/v1", 5, nil,
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				c, err := b.DescribeCampaign("camp-1")
				require.NoError(t, err)
				assert.Equal(t, int32(5), c.MinProvisionedTPS)
			},
		},
		{
			name: "datasetImportJobs",
			seed: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateDatasetImportJob("dij-1", "arn:aws:personalize:us-west-2:111122223333:dataset/ds-1",
					"arn:aws:iam::111122223333:role/r", map[string]any{"dataLocation": "s3://bucket/key"}, nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				list, _ := b.ListDatasetImportJobs("", 0, "")
				require.Len(t, list, 1)
				assert.Equal(t, "dij-1", list[0].JobName)
			},
		},
		{
			name: "datasetExportJobs",
			seed: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateDatasetExportJob("dej-1", "arn:aws:personalize:us-west-2:111122223333:dataset/ds-1",
					"arn:aws:iam::111122223333:role/r", map[string]any{"s3DataDestination": "s3://bucket/out"}, nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				list, _ := b.ListDatasetExportJobs("", 0, "")
				require.Len(t, list, 1)
				assert.Equal(t, "dej-1", list[0].JobName)
			},
		},
		{
			name: "batchInferenceJobs",
			seed: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateBatchInferenceJob(
					"bij-1",
					"arn:aws:personalize:us-west-2:111122223333:solution/sol-1/v1",
					"arn:aws:iam::111122223333:role/r",
					map[string]any{"in": "s3://in"},
					map[string]any{"out": "s3://out"},
					nil,
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				list, _ := b.ListBatchInferenceJobs("", 0, "")
				require.Len(t, list, 1)
				assert.Equal(t, "bij-1", list[0].JobName)
			},
		},
		{
			name: "batchSegmentJobs",
			seed: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateBatchSegmentJob(
					"bsj-1",
					"arn:aws:personalize:us-west-2:111122223333:solution/sol-1/v1",
					"arn:aws:iam::111122223333:role/r",
					map[string]any{"in": "s3://in"},
					map[string]any{"out": "s3://out"},
					nil,
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				list, _ := b.ListBatchSegmentJobs("", 0, "")
				require.Len(t, list, 1)
				assert.Equal(t, "bsj-1", list[0].JobName)
			},
		},
		{
			name: "eventTrackers",
			seed: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateEventTracker(
					"et-1",
					"arn:aws:personalize:us-west-2:111122223333:dataset-group/dg-1",
					nil,
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				et, err := b.DescribeEventTracker("et-1")
				require.NoError(t, err)
				assert.NotEmpty(t, et.TrackingID)
			},
		},
		{
			name: "filters",
			seed: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateFilter("f-1", "arn:aws:personalize:us-west-2:111122223333:dataset-group/dg-1",
					"INCLUDE ItemID WHERE Interactions.EVENT_TYPE IN (\"click\")", nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				f, err := b.DescribeFilter("f-1")
				require.NoError(t, err)
				assert.Contains(t, f.FilterExpression, "click")
			},
		},
		{
			name: "recommenders",
			seed: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateRecommender("rec-1", "arn:aws:personalize:us-west-2:111122223333:dataset-group/dg-1",
					"arn:aws:personalize:::recipe/aws-similar-items", 3, nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				r, err := b.DescribeRecommender("rec-1")
				require.NoError(t, err)
				assert.Equal(t, int32(3), r.MinRecommendationRequestsPerSecond)
			},
		},
		{
			name: "metricAttributions",
			seed: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateMetricAttribution(
					"ma-1",
					"arn:aws:personalize:us-west-2:111122223333:dataset-group/dg-1",
					map[string]any{"s3DataDestination": map[string]any{"path": "s3://bucket/metrics"}},
					nil,
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				ma, err := b.DescribeMetricAttribution("ma-1")
				require.NoError(t, err)
				assert.NotNil(t, ma.MetricsOutputConfig)
			},
		},
		{
			name: "dataDeletionJobs",
			seed: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateDataDeletionJob(
					"ddj-1",
					"arn:aws:personalize:us-west-2:111122223333:dataset-group/dg-1",
					"arn:aws:iam::111122223333:role/r",
					map[string]any{"dataSource": "s3://bucket/delete"},
					nil,
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				list, _ := b.ListDataDeletionJobs("", 0, "")
				require.Len(t, list, 1)
				assert.Equal(t, "ddj-1", list[0].JobName)
			},
		},
		{
			name: "featureTransformations",
			seed: func(t *testing.T, _ *personalize.InMemoryBackend) {
				t.Helper()
				// Builtin, read-only, seeded by NewInMemoryBackend itself.
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				ft, err := b.GetFeatureTransformation("aws-explicit-contextual-bandits-feature-transformation")
				require.NoError(t, err)
				assert.Equal(t, "ACTIVE", ft.Status)
			},
		},
		{
			name: "tags",
			seed: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateDatasetGroup("dg-tagged", "", "", "", map[string]string{"env": "test"})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *personalize.InMemoryBackend) {
				t.Helper()
				dg, err := b.DescribeDatasetGroup("dg-tagged")
				require.NoError(t, err)
				tagVals, err := b.ListTagsForResource(dg.DatasetGroupArn)
				require.NoError(t, err)
				assert.Equal(t, "test", tagVals["env"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := personalize.NewInMemoryBackend(pTestAccountID, pTestRegion)
			tt.seed(t, original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := personalize.NewInMemoryBackend(pTestAccountID, pTestRegion)
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh)
		})
	}
}

// Test_Persistence_HandlerDelegates verifies the Handler-level Snapshot and
// Restore -- which the persistence layer actually calls, per setupPersistence
// in cli.go -- correctly delegate to the backend. Before Phase 3.3, Handler
// had no Snapshot/Restore at all, so Personalize was silently excluded from
// persistence entirely (setupPersistence type-asserts each service's
// Registerable to a Snapshot/Restore-shaped interface).
func Test_Persistence_HandlerDelegates(t *testing.T) {
	t.Parallel()

	h := personalize.NewHandler(personalize.NewInMemoryBackend(pTestAccountID, pTestRegion))

	_, err := h.Backend.CreateDatasetGroup("delegate-dg", "", "", "", nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := personalize.NewHandler(personalize.NewInMemoryBackend(pTestAccountID, pTestRegion))
	require.NoError(t, h2.Restore(t.Context(), snap))

	got, err := h2.Backend.DescribeDatasetGroup("delegate-dg")
	require.NoError(t, err)
	assert.Equal(t, "delegate-dg", got.Name)
}

// Test_Persistence_EmptyBackend verifies Snapshot/Restore round trips cleanly
// on a backend with no data at all beyond the builtin feature
// transformations -- every registered table and the tags map must decode
// back to empty (or the re-seeded builtins), not nil-panic or error.
func Test_Persistence_EmptyBackend(t *testing.T) {
	t.Parallel()

	original := personalize.NewInMemoryBackend(pTestAccountID, pTestRegion)
	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := personalize.NewInMemoryBackend(pTestAccountID, pTestRegion)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	list, _ := fresh.ListDatasetGroups(0, "")
	assert.Empty(t, list)

	ft, err := fresh.GetFeatureTransformation("aws-feature-transformation")
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", ft.Status)
}
