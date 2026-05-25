package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func createPartitionedTable(t *testing.T, h *glue.Handler, dbName, tableName string) {
	t.Helper()

	rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": dbName},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": dbName,
		"TableInput": map[string]any{
			"Name": tableName,
			"PartitionKeys": []map[string]any{
				{"Name": "year", "Type": "string"},
				{"Name": "month", "Type": "string"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestBatch4_PartitionIndexes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		request    map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "valid_index",
			request: map[string]any{
				"DatabaseName":   "db",
				"TableName":      "events",
				"PartitionIndex": map[string]any{"IndexName": "ym", "Keys": []string{"year", "month"}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "unknown_partition_key",
			request: map[string]any{
				"DatabaseName":   "db",
				"TableName":      "events",
				"PartitionIndex": map[string]any{"IndexName": "bad", "Keys": []string{"region"}},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate_key",
			request: map[string]any{
				"DatabaseName":   "db",
				"TableName":      "events",
				"PartitionIndex": map[string]any{"IndexName": "duplicate", "Keys": []string{"year", "year"}},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "table_missing",
			request: map[string]any{
				"DatabaseName":   "db",
				"TableName":      "missing",
				"PartitionIndex": map[string]any{"IndexName": "ym", "Keys": []string{"year"}},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createPartitionedTable(t, h, "db", "events")
			rec := doGlueRequest(t, h, "CreatePartitionIndex", tt.request)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestBatch4_PartitionIndexLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createPartitionedTable(t, h, "db", "events")

	for _, index := range []map[string]any{
		{"IndexName": "year_idx", "Keys": []string{"year"}},
		{"IndexName": "ym_idx", "Keys": []string{"year", "month"}},
	} {
		rec := doGlueRequest(t, h, "CreatePartitionIndex", map[string]any{
			"DatabaseName": "db", "TableName": "events", "PartitionIndex": index,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doGlueRequest(t, h, "GetPartitionIndexes", map[string]any{
		"DatabaseName": "db", "TableName": "events",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var out struct {
		Indexes []*glue.PartitionIndex `json:"PartitionIndexDescriptorList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Indexes, 2)
	assert.Equal(t, "year_idx", out.Indexes[0].IndexName)
	assert.Equal(t, "ACTIVE", out.Indexes[0].IndexStatus)

	rec = doGlueRequest(t, h, "DeletePartitionIndex", map[string]any{
		"DatabaseName": "db", "TableName": "events", "IndexName": "year_idx",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doGlueRequest(t, h, "DeletePartitionIndex", map[string]any{
		"DatabaseName": "db", "TableName": "events", "IndexName": "year_idx",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = doGlueRequest(t, h, "DeleteTable", map[string]any{"DatabaseName": "db", "Name": "events"})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doGlueRequest(t, h, "GetPartitionIndexes", map[string]any{
		"DatabaseName": "db", "TableName": "events",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch4_ExtendedStateSnapshotRestore(t *testing.T) {
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
				_, err = b.CreateSchema("registry", "schema", "JSON", "NONE", "", nil)
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
				_, err = b.PutResourcePolicy("policy", "")
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
				require.NoError(t, b.CreateBlueprint("blueprint"))
				_, err := b.StartBlueprintRun("blueprint")
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
				_, err = b.CreateIntegration("integration", nil)
				require.NoError(t, err)
				require.NoError(t, b.CreateGlueIdentityCenterConfiguration("instance"))
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
			snapshot := original.Snapshot()
			require.NotNil(t, snapshot)

			restored := glue.NewInMemoryBackend("123456789012", "us-east-1")
			require.NoError(t, restored.Restore(snapshot))
			tt.check(t, restored)
		})
	}
}
