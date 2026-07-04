package glue_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/glue"
)

// --- Reset ---

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("111111111111", "us-west-2")
	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db1"}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, glue.DatabaseCount(b))

	b.Reset()

	assert.Equal(t, 0, glue.DatabaseCount(b))
	assert.Equal(t, 0, glue.TableCount(b))
	assert.Equal(t, 0, glue.CrawlerCount(b))
	assert.Equal(t, 0, glue.JobCount(b))
	assert.Equal(t, 0, glue.PartitionCount(b))
	assert.Equal(t, 0, glue.ConnectionCount(b))
	assert.Equal(t, 0, glue.BlueprintCount(b))
}

func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("111111111111", "us-west-2")

	for range 3 {
		_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
		require.NoError(t, err)
		require.Equal(t, 1, glue.DatabaseCount(b))

		b.Reset()

		assert.Equal(t, 0, glue.DatabaseCount(b))
	}
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend("111111111111", "us-east-1")
	h := glue.NewHandler(backend)

	_, err := backend.CreateDatabase(glue.DatabaseInput{Name: "db1"}, nil)
	require.NoError(t, err)

	h.Reset()

	assert.Equal(t, 0, glue.DatabaseCount(backend))
}

// --- Provider nil ctx ---

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &glue.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, glue.ErrNilAppContext)
}

// --- Ops pre-built ---

func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend("000000000000", "us-east-1")
	h := glue.NewHandler(backend)

	assert.Positive(t, h.HandlerOpsLen())
}

func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend("000000000000", "us-east-1")
	h := glue.NewHandler(backend)

	ops := h.GetSupportedOperations()
	assert.Len(t, ops, h.HandlerOpsLen(), "supported ops count should match dispatch table size")

	expectedBatch := []string{
		"BatchCreatePartition",
		"BatchDeleteConnection",
		"BatchDeletePartition",
		"BatchDeleteTable",
		"BatchDeleteTableVersion",
		"BatchGetBlueprints",
		"BatchGetCrawlers",
		"BatchGetCustomEntityTypes",
		"BatchGetDataQualityResult",
		"BatchGetDevEndpoints",
	}

	for _, op := range expectedBatch {
		assert.Contains(t, ops, op, "expected batch op %s in supported operations", op)
	}
}

// --- Seed helpers ---

func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *glue.InMemoryBackend)
		checkFunc func(t *testing.T, b *glue.InMemoryBackend)
		name      string
	}{
		{
			name: "AddConnectionInternal",
			setup: func(b *glue.InMemoryBackend) {
				b.AddConnectionInternal(&glue.Connection{Name: "conn1"})
			},
			checkFunc: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.ConnectionCount(b))
			},
		},
		{
			name: "AddBlueprintInternal",
			setup: func(b *glue.InMemoryBackend) {
				b.AddBlueprintInternal(&glue.Blueprint{Name: "bp1", Status: "ACTIVE"})
			},
			checkFunc: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.BlueprintCount(b))
			},
		},
		{
			name: "AddCustomEntityTypeInternal",
			setup: func(b *glue.InMemoryBackend) {
				b.AddCustomEntityTypeInternal(&glue.CustomEntityType{Name: "cet1", RegexString: `\d+`})
			},
			checkFunc: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.CustomEntityTypeCount(b))
			},
		},
		{
			name: "AddDataQualityResultInternal",
			setup: func(b *glue.InMemoryBackend) {
				b.AddDataQualityResultInternal(&glue.DataQualityResult{ResultID: "r1", Score: 0.9})
			},
			checkFunc: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.DataQualityResultCount(b))
			},
		},
		{
			name: "AddDevEndpointInternal",
			setup: func(b *glue.InMemoryBackend) {
				b.AddDevEndpointInternal(&glue.DevEndpoint{EndpointName: "ep1", Status: "READY"})
			},
			checkFunc: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.DevEndpointCount(b))
			},
		},
		{
			name: "AddTableVersionInternal",
			setup: func(b *glue.InMemoryBackend) {
				b.AddTableVersionInternal("db1", "tbl1", &glue.TableVersion{VersionID: "1"})
			},
			checkFunc: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.TableVersionCount(b))
			},
		},
		{
			name: "AddPartitionInternal",
			setup: func(b *glue.InMemoryBackend) {
				b.AddPartitionInternal("db1", "tbl1", &glue.Partition{Values: []string{"2024", "01"}})
			},
			checkFunc: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.PartitionCount(b))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)
			tt.checkFunc(t, b)
		})
	}
}

// --- Export count helpers ---

func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	assert.Equal(t, 0, glue.DatabaseCount(b))
	assert.Equal(t, 0, glue.TableCount(b))
	assert.Equal(t, 0, glue.CrawlerCount(b))
	assert.Equal(t, 0, glue.JobCount(b))
	assert.Equal(t, 0, glue.PartitionCount(b))
	assert.Equal(t, 0, glue.TableVersionCount(b))
	assert.Equal(t, 0, glue.ConnectionCount(b))
	assert.Equal(t, 0, glue.BlueprintCount(b))
	assert.Equal(t, 0, glue.CustomEntityTypeCount(b))
	assert.Equal(t, 0, glue.DataQualityResultCount(b))
	assert.Equal(t, 0, glue.DevEndpointCount(b))

	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db1"}, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, glue.DatabaseCount(b))
}

// --- BatchCreatePartition ---

func TestRefinement1_BatchCreatePartition_RoundTrip(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
	require.NoError(t, err)
	_, err = b.CreateTable("db", glue.TableInput{Name: "tbl"})
	require.NoError(t, err)

	inputs := []glue.PartitionInput{
		{Values: []string{"2024", "01"}},
		{Values: []string{"2024", "02"}},
	}

	created, errs := b.BatchCreatePartition("db", "tbl", inputs)

	assert.Len(t, created, 2)
	assert.Empty(t, errs)
	assert.Equal(t, 2, glue.PartitionCount(b))

	// Duplicate should produce an error.
	created2, errs2 := b.BatchCreatePartition("db", "tbl", inputs)
	assert.Empty(t, created2)
	assert.Len(t, errs2, 2)
}

// --- BatchDeletePartition ---

func TestRefinement1_BatchDeletePartition_RemovesPartition(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddPartitionInternal("db", "tbl", &glue.Partition{Values: []string{"2024", "01"}})
	b.AddPartitionInternal("db", "tbl", &glue.Partition{Values: []string{"2024", "02"}})

	errs := b.BatchDeletePartition("db", "tbl", []glue.PartitionValueList{
		{Values: []string{"2024", "01"}},
	})

	assert.Empty(t, errs)
	assert.Equal(t, 1, glue.PartitionCount(b))

	// Delete a non-existent partition.
	errs2 := b.BatchDeletePartition("db", "tbl", []glue.PartitionValueList{
		{Values: []string{"9999", "99"}},
	})

	assert.Len(t, errs2, 1)
	assert.Equal(t, "EntityNotFoundException", errs2[0].ErrorDetail.ErrorCode)
}

// --- BatchDeleteTable cascade ---

func TestRefinement1_BatchDeleteTable_CascadesPartitions(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
	require.NoError(t, err)
	_, err = b.CreateTable("db", glue.TableInput{Name: "tbl"})
	require.NoError(t, err)

	b.AddPartitionInternal("db", "tbl", &glue.Partition{Values: []string{"2024"}})
	b.AddTableVersionInternal("db", "tbl", &glue.TableVersion{VersionID: "1"})

	require.Equal(t, 1, glue.PartitionCount(b))
	require.Equal(t, 1, glue.TableVersionCount(b))

	tableErrs := b.BatchDeleteTable("db", []string{"tbl"})

	assert.Empty(t, tableErrs)
	assert.Equal(t, 0, glue.TableCount(b))
	assert.Equal(t, 0, glue.PartitionCount(b))
	assert.Equal(t, 0, glue.TableVersionCount(b))
}

func TestRefinement1_BatchDeleteTable_NotFound(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
	require.NoError(t, err)

	errs := b.BatchDeleteTable("db", []string{"missing"})

	assert.Len(t, errs, 1)
	assert.Equal(t, "EntityNotFoundException", errs[0].ErrorDetail.ErrorCode)
}

// --- BatchDeleteTableVersion ---

func TestRefinement1_BatchDeleteTableVersion_NotFound(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	errs := b.BatchDeleteTableVersion("db", "tbl", []string{"v1"})

	assert.Len(t, errs, 1)
	assert.Equal(t, "EntityNotFoundException", errs[0].ErrorDetail.ErrorCode)
	assert.Equal(t, "v1", errs[0].VersionID)
}

func TestRefinement1_BatchDeleteTableVersion_RoundTrip(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddTableVersionInternal("db", "tbl", &glue.TableVersion{VersionID: "1"})
	b.AddTableVersionInternal("db", "tbl", &glue.TableVersion{VersionID: "2"})

	errs := b.BatchDeleteTableVersion("db", "tbl", []string{"1"})

	assert.Empty(t, errs)
	assert.Equal(t, 1, glue.TableVersionCount(b))
}

// --- BatchDeleteConnection ---

func TestRefinement1_BatchDeleteConnection_MixedResults(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddConnectionInternal(&glue.Connection{Name: "conn1"})
	b.AddConnectionInternal(&glue.Connection{Name: "conn2"})

	succeeded, errs := b.BatchDeleteConnection([]string{"conn1", "missing", "conn2"})

	assert.Len(t, succeeded, 2)
	assert.Contains(t, succeeded, "conn1")
	assert.Contains(t, succeeded, "conn2")
	assert.Len(t, errs, 1)
	assert.Equal(t, "EntityNotFoundException", errs[0].ErrorCode)
	assert.Equal(t, 0, glue.ConnectionCount(b))
}

// --- BatchGetBlueprints ---

func TestRefinement1_BatchGetBlueprints_FoundAndMissing(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddBlueprintInternal(&glue.Blueprint{Name: "bp1", Status: "ACTIVE"})

	found, missing := b.BatchGetBlueprints([]string{"bp1", "bp2"})

	assert.Len(t, found, 1)
	assert.Equal(t, "bp1", found[0].Name)
	assert.Len(t, missing, 1)
	assert.Contains(t, missing, "bp2")
}

func TestRefinement1_BatchGetBlueprints_NonNilSlices(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	found, missing := b.BatchGetBlueprints([]string{})

	assert.NotNil(t, found)
	assert.NotNil(t, missing)
}

// --- BatchGetCrawlers ---

func TestRefinement1_BatchGetCrawlers_FoundAndMissing(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
	require.NoError(t, err)
	_, err = b.CreateCrawler("c1", "role", "db", glue.CrawlerTarget{}, nil)
	require.NoError(t, err)

	found, missing := b.BatchGetCrawlers([]string{"c1", "c2"})

	assert.Len(t, found, 1)
	assert.Equal(t, "c1", found[0].Name)
	assert.Len(t, missing, 1)
	assert.Contains(t, missing, "c2")
}

// --- BatchGetCustomEntityTypes ---

func TestRefinement1_BatchGetCustomEntityTypes_FoundAndMissing(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddCustomEntityTypeInternal(&glue.CustomEntityType{Name: "cet1", RegexString: `\d+`})

	found, missing := b.BatchGetCustomEntityTypes([]string{"cet1", "cet2"})

	assert.Len(t, found, 1)
	assert.Equal(t, "cet1", found[0].Name)
	assert.Len(t, missing, 1)
}

// --- BatchGetDataQualityResult ---

func TestRefinement1_BatchGetDataQualityResult_FoundAndMissing(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddDataQualityResultInternal(&glue.DataQualityResult{ResultID: "r1", Score: 0.95})

	found, errs := b.BatchGetDataQualityResult([]string{"r1", "r2"})

	assert.Len(t, found, 1)
	assert.InDelta(t, 0.95, found[0].Score, 0.001)
	assert.Len(t, errs, 1)
	assert.Equal(t, "EntityNotFoundException", errs[0].ErrorCode)
}

// --- BatchGetDevEndpoints ---

func TestRefinement1_BatchGetDevEndpoints_FoundAndMissing(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddDevEndpointInternal(&glue.DevEndpoint{EndpointName: "ep1", Status: "READY"})

	found, missing := b.BatchGetDevEndpoints([]string{"ep1", "ep2"})

	assert.Len(t, found, 1)
	assert.Equal(t, "ep1", found[0].EndpointName)
	assert.Len(t, missing, 1)
}

// --- Sorted outputs ---

func TestRefinement1_SortedGetDatabases(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	for _, name := range []string{"gamma", "alpha", "beta"} {
		_, err := b.CreateDatabase(glue.DatabaseInput{Name: name}, nil)
		require.NoError(t, err)
	}

	dbs := b.GetDatabases()
	require.Len(t, dbs, 3)

	assert.Equal(t, "alpha", dbs[0].Name)
	assert.Equal(t, "beta", dbs[1].Name)
	assert.Equal(t, "gamma", dbs[2].Name)
}

func TestRefinement1_SortedGetCrawlers(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
	require.NoError(t, err)

	for _, name := range []string{"zeta", "alpha", "mu"} {
		_, crawlerErr := b.CreateCrawler(name, "role", "db", glue.CrawlerTarget{}, nil)
		require.NoError(t, crawlerErr)
	}

	crawlers := b.GetCrawlers()
	require.Len(t, crawlers, 3)

	assert.Equal(t, "alpha", crawlers[0].Name)
	assert.Equal(t, "mu", crawlers[1].Name)
	assert.Equal(t, "zeta", crawlers[2].Name)
}

// --- Persistence round-trip ---

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db1"}, nil)
	require.NoError(t, err)
	_, err = b.CreateTable("db1", glue.TableInput{Name: "tbl1"})
	require.NoError(t, err)
	b.AddBlueprintInternal(&glue.Blueprint{Name: "bp1"})
	b.AddDevEndpointInternal(&glue.DevEndpoint{EndpointName: "ep1", Status: "READY"})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := glue.NewInMemoryBackend("000000000000", "us-east-1")
	err = b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	assert.Equal(t, 1, glue.DatabaseCount(b2))
	assert.Equal(t, 1, glue.TableCount(b2))
	assert.Equal(t, 1, glue.BlueprintCount(b2))
	assert.Equal(t, 1, glue.DevEndpointCount(b2))
}

func TestRefinement1_PersistenceEmpty(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := glue.NewInMemoryBackend("000000000000", "us-east-1")
	err := b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	assert.Equal(t, 0, glue.DatabaseCount(b2))
	assert.Equal(t, 0, glue.BlueprintCount(b2))
}

// --- Non-nil slices in batch outputs ---

func TestRefinement1_NonNilSlices_BatchOutputs(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	tests := []struct {
		validate func(t *testing.T)
		name     string
	}{
		{
			name: "BatchGetBlueprints_empty",
			validate: func(t *testing.T) {
				t.Helper()
				found, missing := b.BatchGetBlueprints([]string{})
				assert.NotNil(t, found)
				assert.NotNil(t, missing)
			},
		},
		{
			name: "BatchGetCrawlers_empty",
			validate: func(t *testing.T) {
				t.Helper()
				found, missing := b.BatchGetCrawlers([]string{})
				assert.NotNil(t, found)
				assert.NotNil(t, missing)
			},
		},
		{
			name: "BatchGetCustomEntityTypes_empty",
			validate: func(t *testing.T) {
				t.Helper()
				found, missing := b.BatchGetCustomEntityTypes([]string{})
				assert.NotNil(t, found)
				assert.NotNil(t, missing)
			},
		},
		{
			name: "BatchGetDataQualityResult_empty",
			validate: func(t *testing.T) {
				t.Helper()
				found, errs := b.BatchGetDataQualityResult([]string{})
				assert.NotNil(t, found)
				assert.NotNil(t, errs)
			},
		},
		{
			name: "BatchGetDevEndpoints_empty",
			validate: func(t *testing.T) {
				t.Helper()
				found, missing := b.BatchGetDevEndpoints([]string{})
				assert.NotNil(t, found)
				assert.NotNil(t, missing)
			},
		},
		{
			name: "BatchDeleteConnection_empty",
			validate: func(t *testing.T) {
				t.Helper()
				succeeded, errs := b.BatchDeleteConnection([]string{})
				assert.NotNil(t, succeeded)
				assert.NotNil(t, errs)
			},
		},
		{
			name: "BatchDeletePartition_empty",
			validate: func(t *testing.T) {
				t.Helper()
				errs := b.BatchDeletePartition("db", "tbl", []glue.PartitionValueList{})
				assert.NotNil(t, errs)
			},
		},
		{
			name: "BatchDeleteTable_empty",
			validate: func(t *testing.T) {
				t.Helper()
				errs := b.BatchDeleteTable("db", []string{})
				assert.NotNil(t, errs)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.validate(t)
		})
	}
}

// --- HTTP handler integration for batch operations ---

func TestRefinement1_HTTPBatchCreatePartition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a database and table first via HTTP.
	postGlue(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "testdb"},
	})
	postGlue(t, h, "CreateTable", map[string]any{
		"DatabaseName": "testdb",
		"TableInput":   map[string]any{"Name": "testtbl"},
	})

	rec := postGlue(t, h, "BatchCreatePartition", map[string]any{
		"DatabaseName": "testdb",
		"TableName":    "testtbl",
		"PartitionInputList": []map[string]any{
			{"Values": []string{"2024", "01"}},
			{"Values": []string{"2024", "02"}},
		},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotNil(t, out["Partitions"])
}

func TestRefinement1_HTTPBatchGetCrawlers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create database and crawler.
	postGlue(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "db1"},
	})
	postGlue(t, h, "CreateCrawler", map[string]any{
		"Name":         "crawler1",
		"Role":         "role",
		"DatabaseName": "db1",
	})

	rec := postGlue(t, h, "BatchGetCrawlers", map[string]any{
		"CrawlerNames": []string{"crawler1", "notexist"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	crawlers, ok := out["Crawlers"].([]any)
	require.True(t, ok)
	assert.Len(t, crawlers, 1)
	missing, ok := out["CrawlersNotFound"].([]any)
	require.True(t, ok)
	assert.Len(t, missing, 1)
}

func TestRefinement1_HTTPBatchDeleteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	postGlue(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "db1"},
	})
	postGlue(t, h, "CreateTable", map[string]any{
		"DatabaseName": "db1",
		"TableInput":   map[string]any{"Name": "tbl1"},
	})
	postGlue(t, h, "CreateTable", map[string]any{
		"DatabaseName": "db1",
		"TableInput":   map[string]any{"Name": "tbl2"},
	})

	rec := postGlue(t, h, "BatchDeleteTable", map[string]any{
		"DatabaseName":   "db1",
		"TablesToDelete": []string{"tbl1", "tbl2"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	errs, ok := out["Errors"].([]any)
	require.True(t, ok)
	assert.Empty(t, errs)
}

func TestRefinement1_ErrValidation(t *testing.T) {
	t.Parallel()

	require.Error(t, glue.ErrValidation)
	assert.Error(t, glue.ErrValidation)
}

// --- Handler Snapshot/Restore delegation ---

func TestRefinement1_HandlerSnapshotRestore(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend("000000000000", "us-east-1")
	h := glue.NewHandler(backend)

	_, err := backend.CreateDatabase(glue.DatabaseInput{Name: "db1"}, nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	backend2 := glue.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := glue.NewHandler(backend2)

	err = h2.Restore(t.Context(), snap)
	require.NoError(t, err)

	assert.Equal(t, 1, glue.DatabaseCount(backend2))
}

func TestRefinement1_ProviderInit_ValidCtx(t *testing.T) {
	t.Parallel()

	p := &glue.Provider{}
	ctx := &service.AppContext{}
	h, err := p.Init(ctx)

	require.NoError(t, err)
	assert.NotNil(t, h)
}

// postGlue is a helper for HTTP tests - reuses handler_test.go's infrastructure.
func postGlue(t *testing.T, h *glue.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	bs, err := json.Marshal(body)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bs))
	req.Header.Set("X-Amz-Target", "AWSGlue."+action)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err = h.Handler()(c)
	require.NoError(t, err)

	return rec
}
