package lakeformation_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

// --- ListTransactions with StatusFilter ---

func TestRefinement3_ListTransactions_StatusFilter_Active(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Start two transactions
	rec1 := postJSON(t, h, "/StartTransaction", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, jsonDecode(rec1.Body, &out1))
	txID1 := out1["TransactionId"].(string)

	// Commit one of them
	postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": txID1})
	postJSON(t, h, "/StartTransaction", nil) // second stays ACTIVE

	// Filter for only ACTIVE
	rec := postJSON(t, h, "/ListTransactions", map[string]any{"StatusFilter": "ACTIVE"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	txns := out["Transactions"].([]any)
	assert.Len(t, txns, 1, "only ACTIVE transaction should be returned")
	assert.Equal(t, "ACTIVE", txns[0].(map[string]any)["TransactionStatus"])
}

func TestRefinement3_ListTransactions_StatusFilter_Committed(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": "txn-commit-1"})
	require.Equal(t, http.StatusOK, rec.Code)

	postJSON(t, h, "/StartTransaction", nil) // active transaction

	rec2 := postJSON(t, h, "/ListTransactions", map[string]any{"StatusFilter": "COMMITTED"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))
	txns := out["Transactions"].([]any)
	assert.Len(t, txns, 1)
}

func TestRefinement3_ListTransactions_NoFilter_ReturnsAll(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/StartTransaction", nil)
	postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": "committed-1"})

	rec := postJSON(t, h, "/ListTransactions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	txns := out["Transactions"].([]any)
	assert.Len(t, txns, 2)
}

// --- DeleteObjectsOnCancel state check ---

func TestRefinement3_DeleteObjectsOnCancel_RequiresAborted(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartTransaction", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var txOut map[string]any
	require.NoError(t, jsonDecode(rec.Body, &txOut))
	txID := txOut["TransactionId"].(string)

	// Transaction is ACTIVE — should fail
	rec2 := postJSON(t, h, "/DeleteObjectsOnCancel", map[string]any{"TransactionId": txID})
	assert.Equal(t, http.StatusBadRequest, rec2.Code, "must be ABORTED before DeleteObjectsOnCancel")
}

func TestRefinement3_DeleteObjectsOnCancel_AfterCancel(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/CancelTransaction", map[string]any{"TransactionId": "txn-aborted"})

	rec := postJSON(t, h, "/DeleteObjectsOnCancel", map[string]any{"TransactionId": "txn-aborted"})
	assert.Equal(t, http.StatusOK, rec.Code, "should succeed when transaction is ABORTED")
}

func TestRefinement3_DeleteObjectsOnCancel_MissingID(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/DeleteObjectsOnCancel", map[string]any{"TransactionId": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- UpdateDataCellsFilter validation ---

func TestRefinement3_UpdateDataCellsFilter_RequiresAllFields(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	b.AddDataCellsFilterInternal(&lakeformation.DataCellsFilter{
		TableCatalogID: "123", DatabaseName: "db", TableName: "tbl", Name: "f",
	})

	tests := []struct {
		body   map[string]any
		name   string
		status int
	}{
		{
			name:   "missing_catalog",
			body:   map[string]any{"TableData": map[string]any{"DatabaseName": "db", "TableName": "tbl", "Name": "f"}},
			status: http.StatusBadRequest,
		},
		{
			name: "missing_database",
			body: map[string]any{
				"TableData": map[string]any{"TableCatalogId": "123", "TableName": "tbl", "Name": "f"},
			},
			status: http.StatusBadRequest,
		},
		{
			name: "missing_table",
			body: map[string]any{
				"TableData": map[string]any{"TableCatalogId": "123", "DatabaseName": "db", "Name": "f"},
			},
			status: http.StatusBadRequest,
		},
		{
			name: "not_found",
			body: map[string]any{"TableData": map[string]any{
				"TableCatalogId": "123", "DatabaseName": "db", "TableName": "tbl", "Name": "nonexistent",
			}},
			status: http.StatusNotFound,
		},
		{
			name: "success",
			body: map[string]any{"TableData": map[string]any{
				"TableCatalogId": "123", "DatabaseName": "db", "TableName": "tbl", "Name": "f",
			}},
			status: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h2 := lakeformation.NewHandler(b)
			rec := postJSON(t, h2, "/UpdateDataCellsFilter", tt.body)
			assert.Equal(t, tt.status, rec.Code)
		})
	}
}

// --- StartQueryPlanning validation ---

func TestRefinement3_StartQueryPlanning_RequiresDatabaseName(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartQueryPlanning", map[string]any{
		"QueryPlanningContext": map[string]any{},
		"QueryString":          "SELECT * FROM table",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Contains(t, out["message"].(string), "DatabaseName")
}

func TestRefinement3_StartQueryPlanning_RequiresQueryString(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartQueryPlanning", map[string]any{
		"QueryPlanningContext": map[string]any{"DatabaseName": "mydb"},
		"QueryString":          "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement3_StartQueryPlanning_Success(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartQueryPlanning", map[string]any{
		"QueryPlanningContext": map[string]any{"DatabaseName": "mydb"},
		"QueryString":          "SELECT * FROM mytable",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.NotEmpty(t, out["QueryId"])
}

// --- GetWorkUnits returns queryID in output ---

func TestRefinement3_GetWorkUnits_QueryIDInOutput(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartQueryPlanning", map[string]any{
		"QueryPlanningContext": map[string]any{"DatabaseName": "db"},
		"QueryString":          "SELECT 1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var planOut map[string]any
	require.NoError(t, jsonDecode(rec.Body, &planOut))
	queryID := planOut["QueryId"].(string)

	rec2 := postJSON(t, h, "/GetWorkUnits", map[string]any{"QueryId": queryID})
	require.Equal(t, http.StatusOK, rec2.Code)

	var workOut map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &workOut))
	assert.Equal(t, queryID, workOut["QueryId"], "QueryId should echo back the request QueryId")
	assert.NotNil(t, workOut["WorkUnitRanges"])
}

// --- GetEffectivePermissionsForPath ---

func TestRefinement3_GetEffectivePermissionsForPath_Empty(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetEffectivePermissionsForPath", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.NotNil(t, out["PrincipalResourcePermissions"])
}

// --- ListTableStorageOptimizers type filter ---

func TestRefinement3_ListTableStorageOptimizers_TypeFilter(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Add compaction + retention optimizers
	postJSON(t, h, "/UpdateTableStorageOptimizer", map[string]any{
		"CatalogId":    "123",
		"DatabaseName": "db",
		"TableName":    "tbl",
		"StorageOptimizerConfig": map[string]any{
			"COMPACTION": map[string]any{"enabled": "true"},
			"RETENTION":  map[string]any{"enabled": "true"},
		},
	})

	// Filter for COMPACTION only
	rec := postJSON(t, h, "/ListTableStorageOptimizers", map[string]any{
		"CatalogId":            "123",
		"DatabaseName":         "db",
		"TableName":            "tbl",
		"StorageOptimizerType": "COMPACTION",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	opts := out["StorageOptimizerList"].([]any)
	assert.Len(t, opts, 1)
	assert.Equal(t, "COMPACTION", opts[0].(map[string]any)["StorageOptimizerType"])
}

// --- GetDataCellsFilter ---

func TestRefinement3_GetDataCellsFilter_RoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddDataCellsFilterInternal(&lakeformation.DataCellsFilter{
		TableCatalogID: "cat",
		DatabaseName:   "db",
		TableName:      "tbl",
		Name:           "myfilter",
	})

	rec := postJSON(t, h, "/GetDataCellsFilter", map[string]any{
		"TableCatalogId": "cat",
		"DatabaseName":   "db",
		"TableName":      "tbl",
		"Name":           "myfilter",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.NotNil(t, out["DataCellsFilter"])
}

func TestRefinement3_GetDataCellsFilter_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetDataCellsFilter", map[string]any{
		"TableCatalogId": "cat",
		"DatabaseName":   "db",
		"TableName":      "tbl",
		"Name":           "missing",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- GetLFTagExpression ---

func TestRefinement3_GetLFTagExpression_RoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagExpressionInternal(&lakeformation.LFTagExpression{
		Name:        "myexpr",
		CatalogID:   "cat",
		Description: "test expression",
		Expression:  []lakeformation.LFTag{{TagKey: "env", TagValues: []string{"prod"}}},
	})

	rec := postJSON(t, h, "/GetLFTagExpression", map[string]any{
		"Name":      "myexpr",
		"CatalogId": "cat",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.Equal(t, "myexpr", out["Name"])
	assert.Equal(t, "test expression", out["Description"])
}

func TestRefinement3_UpdateLFTagExpression_Success(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagExpressionInternal(&lakeformation.LFTagExpression{
		Name:      "expr1",
		CatalogID: "cat",
		Expression: []lakeformation.LFTag{
			{TagKey: "env", TagValues: []string{"dev"}},
		},
	})

	rec := postJSON(t, h, "/UpdateLFTagExpression", map[string]any{
		"Name":        "expr1",
		"CatalogId":   "cat",
		"Description": "updated",
		"Expression":  []map[string]any{{"TagKey": "env", "TagValues": []string{"prod"}}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify via GetLFTagExpression
	rec2 := postJSON(t, h, "/GetLFTagExpression", map[string]any{"Name": "expr1", "CatalogId": "cat"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))
	assert.Equal(t, "updated", out["Description"])
}

// --- Identity Center configuration lifecycle ---

func TestRefinement3_IdentityCenter_DeleteAndDescribe(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	h.AccountID = "123456789012"

	// Create
	postJSON(t, h, "/CreateLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId":   "123456789012",
		"InstanceArn": "arn:aws:sso:::instance/ssoins-0000000000000000",
	})

	// Describe
	rec := postJSON(t, h, "/DescribeLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId": "123456789012",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.NotEmpty(t, out["ApplicationArn"])

	// Delete
	rec2 := postJSON(t, h, "/DeleteLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId": "123456789012",
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	// Describe after delete should 404
	rec3 := postJSON(t, h, "/DescribeLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId": "123456789012",
	})
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

func TestRefinement3_IdentityCenter_UpdateExternalFiltering(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	h.AccountID = "111111111111"

	// Create first
	postJSON(t, h, "/CreateLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId":   "111111111111",
		"InstanceArn": "arn:aws:sso:::instance/ssoins-abc",
	})

	// Update external filtering
	rec := postJSON(t, h, "/UpdateLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId": "111111111111",
		"ExternalFiltering": map[string]any{
			"Status":            "ENABLED",
			"AuthorizedTargets": []string{"arn:aws:s3:::my-bucket"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- GetQueryState and GetQueryStatistics ---

func TestRefinement3_QueryLifecycle(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Start planning
	planRec := postJSON(t, h, "/StartQueryPlanning", map[string]any{
		"QueryPlanningContext": map[string]any{"DatabaseName": "mydb"},
		"QueryString":          "SELECT 1",
	})
	require.Equal(t, http.StatusOK, planRec.Code)

	var planOut map[string]any
	require.NoError(t, jsonDecode(planRec.Body, &planOut))
	queryID := planOut["QueryId"].(string)

	// Get state
	stateRec := postJSON(t, h, "/GetQueryState", map[string]any{"QueryId": queryID})
	require.Equal(t, http.StatusOK, stateRec.Code)

	var stateOut map[string]any
	require.NoError(t, jsonDecode(stateRec.Body, &stateOut))
	assert.Equal(t, "WORKUNITS_AVAILABLE", stateOut["State"])

	// Get statistics
	statsRec := postJSON(t, h, "/GetQueryStatistics", map[string]any{"QueryId": queryID})
	require.Equal(t, http.StatusOK, statsRec.Code)

	// Get work unit results
	resultsRec := postJSON(t, h, "/GetWorkUnitResults", map[string]any{
		"QueryId":       queryID,
		"WorkUnitToken": "synthetic-token",
	})
	assert.Equal(t, http.StatusOK, resultsRec.Code)
}

func TestRefinement3_GetQueryState_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetQueryState", map[string]any{"QueryId": "nonexistent-query"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Temporary credentials ---

func TestRefinement3_GetTemporaryDataLocationCredentials_Success(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetTemporaryDataLocationCredentials", map[string]any{
		"ResourceArn": "arn:aws:s3:::my-bucket",
		"Permissions": []string{"DATA_LOCATION_ACCESS"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	require.NotNil(t, out["Credentials"])
	// Real AWS nests Expiration inside Credentials for
	// GetTemporaryDataLocationCredentials (unlike the Glue partition/table
	// credential ops, which return it at the top level).
	creds := out["Credentials"].(map[string]any)
	assert.NotEmpty(t, creds["Expiration"])
}

func TestRefinement3_GetTemporaryDataLocationCredentials_MissingARN(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetTemporaryDataLocationCredentials", map[string]any{
		"ResourceArn": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement3_GetTemporaryGlueTableCredentials_MissingTableArn(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetTemporaryGlueTableCredentials", map[string]any{
		"TableArn": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- ExtendTransaction ---

func TestRefinement3_ExtendTransaction_ActiveSucceeds(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	startRec := postJSON(t, h, "/StartTransaction", nil)
	require.Equal(t, http.StatusOK, startRec.Code)

	var txOut map[string]any
	require.NoError(t, jsonDecode(startRec.Body, &txOut))
	txID := txOut["TransactionId"].(string)

	rec := postJSON(t, h, "/ExtendTransaction", map[string]any{"TransactionId": txID})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRefinement3_ExtendTransaction_CommittedFails(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": "committed-tx"})

	rec := postJSON(t, h, "/ExtendTransaction", map[string]any{"TransactionId": "committed-tx"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- GetTableObjects ---

func TestRefinement3_GetTableObjects_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetTableObjects", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.Nil(t, out["Objects"])
}

// --- SearchDatabasesByLFTags and SearchTablesByLFTags ---

func TestRefinement3_SearchDatabasesByLFTags(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/SearchDatabasesByLFTags", map[string]any{
		"Expression": []map[string]any{{"TagKey": "env", "TagValues": []string{"prod"}}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.NotNil(t, out["DatabaseList"])
}

func TestRefinement3_SearchTablesByLFTags(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/SearchTablesByLFTags", map[string]any{
		"Expression": []map[string]any{{"TagKey": "env", "TagValues": []string{"prod"}}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.NotNil(t, out["TableList"])
}
