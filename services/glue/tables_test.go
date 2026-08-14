package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestExtendedStateSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seed  func(*testing.T, *glue.InMemoryBackend)
		check func(*testing.T, *glue.InMemoryBackend)
		name  string
	}{
		{
			name: "partition_index",
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
				require.NoError(t, err)
				_, err = b.CreateTable("db", glue.TableInput{
					Name: "table", PartitionKeys: []glue.Column{{Name: "day"}},
				})
				require.NoError(t, err)
				require.NoError(t, b.CreatePartitionIndex("db", "table", glue.PartitionIndex{
					IndexName: "day_idx", Keys: []string{"day"},
				}))
			},
			check: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				indexes, err := b.GetPartitionIndexes("db", "table")
				require.NoError(t, err)
				require.Len(t, indexes, 1)
				assert.Equal(t, "day_idx", indexes[0].IndexName)
			},
		},
		{
			name: "trigger_workflow_classifier",
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateTrigger(glue.Trigger{Name: "trigger"}, nil)
				require.NoError(t, err)
				_, err = b.CreateWorkflow(glue.Workflow{Name: "workflow"}, nil)
				require.NoError(t, err)
				_, err = b.StartWorkflowRun("workflow")
				require.NoError(t, err)
				require.NoError(t, b.CreateClassifier(glue.Classifier{
					JSONClassifier: &glue.JSONClassifier{Name: "json"},
				}))
			},
			check: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				_, err := b.GetTrigger("trigger")
				require.NoError(t, err)
				runs, err := b.GetWorkflowRuns("workflow")
				require.NoError(t, err)
				require.Len(t, runs, 1)
				_, err = b.GetClassifier("json")
				require.NoError(t, err)
			},
		},
		{
			name: "registry_schema_udf_security",
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateRegistry("registry", "description", nil)
				require.NoError(t, err)
				_, _, err = b.CreateSchema("registry", "schema", "JSON", "NONE", "", "", nil)
				require.NoError(t, err)
				_, err = b.RegisterSchemaVersion("registry", "schema", "{}")
				require.NoError(t, err)
				_, err = b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
				require.NoError(t, err)
				_, err = b.CreateUserDefinedFunction("db", glue.UserDefinedFunction{FunctionName: "fn"}, nil)
				require.NoError(t, err)
				_, err = b.CreateSecurityConfiguration("security", glue.EncryptionConfiguration{})
				require.NoError(t, err)
			},
			check: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				require.Len(t, b.ListSchemaVersions("registry", "schema"), 1)
				_, err := b.GetUserDefinedFunction("db", "fn")
				require.NoError(t, err)
				_, err = b.GetSecurityConfiguration("security")
				require.NoError(t, err)
			},
		},
		{
			name: "session_optimizer_statistics_policy",
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
				require.NoError(t, err)
				_, err = b.CreateTable("db", glue.TableInput{
					Name: "table", PartitionKeys: []glue.Column{{Name: "day"}},
				})
				require.NoError(t, err)
				_, errs := b.BatchCreatePartition("db", "table", []glue.PartitionInput{{Values: []string{"today"}}})
				require.Empty(t, errs)
				_, err = b.CreateSession("session", "role", glue.SessionCommand{}, glue.Session{})
				require.NoError(t, err)
				_, err = b.RunStatement("session", "select 1")
				require.NoError(t, err)
				require.NoError(
					t,
					b.CreateTableOptimizer("", "db", "table", "compaction", glue.TableOptimizerConfiguration{}),
				)
				require.NoError(t, b.UpdateColumnStatisticsForTable("db", "table", []*glue.ColumnStatistics{
					{ColumnName: "id", StatisticsData: glue.ColumnStatisticsData{Type: "LONG"}},
				}))
				require.NoError(t, b.UpdateColumnStatisticsForPartition(
					"db",
					"table",
					[]string{"today"},
					[]*glue.ColumnStatistics{
						{ColumnName: "id", StatisticsData: glue.ColumnStatisticsData{Type: "LONG"}},
					},
				))
				_, err = b.PutResourcePolicy("policy", "", "", "", "")
				require.NoError(t, err)
			},
			check: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				statements, err := b.GetStatements("session")
				require.NoError(t, err)
				require.Len(t, statements, 1)
				_, err = b.GetTableOptimizer("db", "table", "compaction")
				require.NoError(t, err)
				stats, err := b.GetColumnStatisticsForTable("db", "table", []string{"id"})
				require.NoError(t, err)
				require.Len(t, stats, 1)
				stats, err = b.GetColumnStatisticsForPartition("db", "table", []string{"today"}, []string{"id"})
				require.NoError(t, err)
				require.Len(t, stats, 1)
				policy, _, err := b.GetResourcePolicy("")
				require.NoError(t, err)
				assert.Equal(t, "policy", policy)
			},
		},
		{
			name: "ml_catalog_encryption",
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateMLTransform("transform", "", "", nil, glue.MLTransformParameter{}, nil)
				require.NoError(t, err)
				require.NoError(t, b.CreateCatalog("catalog", "catalog", "", nil))
				require.NoError(t, b.PutDataCatalogEncryptionSettings("", glue.DataCatalogEncryptionSettings{
					EncryptionAtRest: &glue.EncryptionAtRest{CatalogEncryptionMode: "SSE-KMS"},
				}))
			},
			check: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				require.Len(t, b.GetMLTransforms(), 1)
				_, err := b.GetCatalog("catalog")
				require.NoError(t, err)
				settings, err := b.GetDataCatalogEncryptionSettings("")
				require.NoError(t, err)
				assert.Equal(t, "SSE-KMS", settings.EncryptionAtRest.CatalogEncryptionMode)
			},
		},
		{
			name: "batch2_resource_families",
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateBlueprint("blueprint", "s3://bucket/blueprint", "", nil)
				require.NoError(t, err)
				_, err = b.StartBlueprintRun("blueprint")
				require.NoError(t, err)
				_, err = b.CreateUsageProfile("usage", "", nil)
				require.NoError(t, err)
				_, err = b.StartDataQualityRuleRecommendationRun("s3://source")
				require.NoError(t, err)
				_, err = b.CreateColumnStatisticsTaskSettings("db", "table", "role", []string{"id"})
				require.NoError(t, err)
				_, err = b.StartColumnStatisticsTaskRun("db", "table")
				require.NoError(t, err)
				_, err = b.StartMaterializedViewRefreshTaskRun("db", "view")
				require.NoError(t, err)
				_, err = b.CreateIntegration(
					"integration", "arn:aws:s3:::source", "arn:aws:redshift:us-east-1:123456789012:cluster/target", nil,
				)
				require.NoError(t, err)
				_, err = b.CreateGlueIdentityCenterConfiguration("instance")
				require.NoError(t, err)
			},
			check: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				require.Len(t, b.GetBlueprintRuns("blueprint"), 1)
				_, err := b.GetUsageProfile("usage")
				require.NoError(t, err)
				require.Len(t, b.ListDataQualityRuleRecommendationRuns(), 1)
				require.Len(t, b.GetColumnStatisticsTaskRuns(), 1)
				require.Len(t, b.ListMaterializedViewRefreshTaskRuns(), 1)
				require.Len(t, b.ListIntegrations(), 1)
				config, err := b.GetGlueIdentityCenterConfiguration()
				require.NoError(t, err)
				assert.Equal(t, "instance", config.InstanceARN)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := glue.NewInMemoryBackend("123456789012", "us-east-1")
			tt.seed(t, original)
			snapshot := original.Snapshot(t.Context())
			require.NotNil(t, snapshot)

			restored := glue.NewInMemoryBackend("123456789012", "us-east-1")
			require.NoError(t, restored.Restore(t.Context(), snapshot))
			tt.check(t, restored)
		})
	}
}

func TestBatchDeleteTable_NotFound(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
	require.NoError(t, err)

	errs := b.BatchDeleteTable("db", []string{"missing"})

	assert.Len(t, errs, 1)
	assert.Equal(t, "EntityNotFoundException", errs[0].ErrorDetail.ErrorCode)
}

func TestBatchDeleteTableVersion_NotFound(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	errs := b.BatchDeleteTableVersion("db", "tbl", []string{"v1"})

	assert.Len(t, errs, 1)
	assert.Equal(t, "EntityNotFoundException", errs[0].ErrorDetail.ErrorCode)
	assert.Equal(t, "v1", errs[0].VersionID)
}

func TestBatchDeleteTableVersion_RoundTrip(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddTableVersionInternal("db", "tbl", &glue.TableVersion{VersionID: "1"})
	b.AddTableVersionInternal("db", "tbl", &glue.TableVersion{VersionID: "2"})

	errs := b.BatchDeleteTableVersion("db", "tbl", []string{"1"})

	assert.Empty(t, errs)
	assert.Equal(t, 1, glue.TableVersionCount(b))
}
