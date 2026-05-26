package lakeformation_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

// jsonDecode is a helper to decode a response body into a map.
func jsonDecode(r io.Reader, v any) error {
	return json.NewDecoder(r).Decode(v)
}

// --- UpdateResource tests ---

func TestRefinement2_UpdateResource_Success(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddResourceInternal("arn:aws:s3:::my-bucket", "old-role")

	rec := postJSON(t, h, "/UpdateResource", map[string]any{
		"ResourceArn": "arn:aws:s3:::my-bucket",
		"RoleArn":     "arn:aws:iam::000000000000:role/new-role",
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRefinement2_UpdateResource_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/UpdateResource", map[string]any{
		"ResourceArn": "arn:aws:s3:::missing",
		"RoleArn":     "arn:aws:iam::000000000000:role/r",
	})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement2_UpdateResource_MissingRoleArn(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddResourceInternal("arn:aws:s3:::my-bucket", "role")

	rec := postJSON(t, h, "/UpdateResource", map[string]any{
		"ResourceArn": "arn:aws:s3:::my-bucket",
		"RoleArn":     "",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- StartTransaction / DescribeTransaction / ListTransactions tests ---

func TestRefinement2_StartTransaction_ReturnsID(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartTransaction", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.NotEmpty(t, out["TransactionId"])
	assert.Equal(t, 1, b.TransactionCount())
}

func TestRefinement2_StartTransaction_MultipleUnique(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec1 := postJSON(t, h, "/StartTransaction", nil)
	rec2 := postJSON(t, h, "/StartTransaction", nil)

	require.Equal(t, http.StatusOK, rec1.Code)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out1, out2 map[string]any
	require.NoError(t, jsonDecode(rec1.Body, &out1))
	require.NoError(t, jsonDecode(rec2.Body, &out2))
	assert.NotEqual(t, out1["TransactionId"], out2["TransactionId"])
	assert.Equal(t, 2, b.TransactionCount())
}

func TestRefinement2_DescribeTransaction_Active(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartTransaction", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	txID := out["TransactionId"].(string)

	rec2 := postJSON(t, h, "/DescribeTransaction", map[string]any{"TransactionId": txID})
	require.Equal(t, http.StatusOK, rec2.Code)

	var desc map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &desc))
	assert.NotNil(t, desc["TransactionDescription"])
}

func TestRefinement2_DescribeTransaction_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/DescribeTransaction", map[string]any{"TransactionId": "nonexistent"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement2_DescribeTransaction_MissingID(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/DescribeTransaction", map[string]any{"TransactionId": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement2_ListTransactions(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/StartTransaction", nil)
	postJSON(t, h, "/StartTransaction", nil)

	rec := postJSON(t, h, "/ListTransactions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	txns := out["Transactions"].([]any)
	assert.Len(t, txns, 2)
}

// --- RemoveLFTagsFromResource tests ---

func TestRefinement2_RemoveLFTagsFromResource_Success(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagInternal("", "env", []string{"prod"})

	// First attach
	postJSON(t, h, "/AddLFTagsToResource", map[string]any{
		"Resource": map[string]any{"Database": map[string]any{"Name": "db1"}},
		"LFTags":   []any{map[string]any{"TagKey": "env", "TagValues": []string{"prod"}}},
	})

	assert.Equal(t, 1, b.ResourceLFTagCount())

	// Now remove
	rec := postJSON(t, h, "/RemoveLFTagsFromResource", map[string]any{
		"Resource": map[string]any{"Database": map[string]any{"Name": "db1"}},
		"LFTags":   []any{map[string]any{"TagKey": "env", "TagValues": []string{"prod"}}},
	})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, b.ResourceLFTagCount())
}

func TestRefinement2_RemoveLFTagsFromResource_NotAttached(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/RemoveLFTagsFromResource", map[string]any{
		"Resource": map[string]any{"Database": map[string]any{"Name": "db1"}},
		"LFTags":   []any{map[string]any{"TagKey": "missing", "TagValues": []string{"v"}}},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	failures := out["Failures"].([]any)
	assert.Len(t, failures, 1)
}

func TestRefinement2_RemoveLFTagsFromResource_MissingResource(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/RemoveLFTagsFromResource", map[string]any{
		"LFTags": []any{map[string]any{"TagKey": "k", "TagValues": []string{"v"}}},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- GetResourceLFTags tests ---

func TestRefinement2_GetResourceLFTags_Empty(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetResourceLFTags", map[string]any{
		"Resource": map[string]any{"Database": map[string]any{"Name": "db1"}},
	})

	require.Equal(t, http.StatusOK, rec.Code)
}

func TestRefinement2_GetResourceLFTags_MissingResource(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetResourceLFTags", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- ListDataCellsFilter tests ---

func TestRefinement2_ListDataCellsFilter_Empty(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Table is required per AWS behaviour (issue #15)
	rec := postJSON(t, h, "/ListDataCellsFilter", map[string]any{})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement2_ListDataCellsFilter_WithTable(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/ListDataCellsFilter", map[string]any{
		"Table": map[string]any{
			"DatabaseName": "mydb",
			"Name":         "mytable",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	filters, ok := out["DataCellsFilters"]
	if ok {
		assert.NotNil(t, filters)
	}
}

func TestRefinement2_ListDataCellsFilter_AfterCreate(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddDataCellsFilterInternal(&lakeformation.DataCellsFilter{
		TableCatalogID: "123456789012",
		DatabaseName:   "mydb",
		TableName:      "mytable",
		Name:           "myfilter",
	})

	rec := postJSON(t, h, "/ListDataCellsFilter", map[string]any{
		"Table": map[string]any{"DatabaseName": "mydb", "Name": "mytable"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	filters := out["DataCellsFilters"].([]any)
	assert.Len(t, filters, 1)
}

// --- ListLFTagExpressions tests ---

func TestRefinement2_ListLFTagExpressions_Empty(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/ListLFTagExpressions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.NotNil(t, out["LFTagExpressions"])
}

func TestRefinement2_ListLFTagExpressions_AfterCreate(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagExpressionInternal(&lakeformation.LFTagExpression{
		Name:      "myexpr",
		CatalogID: "123",
		Expression: []lakeformation.LFTag{
			{TagKey: "env", TagValues: []string{"prod"}},
		},
	})

	rec := postJSON(t, h, "/ListLFTagExpressions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	exprs := out["LFTagExpressions"].([]any)
	assert.Len(t, exprs, 1)
}

// --- DeleteLakeFormationOptIn tests ---

func TestRefinement2_DeleteLakeFormationOptIn_Success(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// First create
	postJSON(t, h, "/CreateLakeFormationOptIn", map[string]any{
		"Principal": map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::000000000000:user/alice"},
		"Resource":  map[string]any{"Database": map[string]any{"Name": "db1"}},
	})

	assert.Equal(t, 1, b.OptInCount())

	// Now delete
	rec := postJSON(t, h, "/DeleteLakeFormationOptIn", map[string]any{
		"Principal": map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::000000000000:user/alice"},
		"Resource":  map[string]any{"Database": map[string]any{"Name": "db1"}},
	})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, b.OptInCount())
}

func TestRefinement2_DeleteLakeFormationOptIn_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/DeleteLakeFormationOptIn", map[string]any{
		"Principal": map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::000000000000:user/nobody"},
		"Resource":  map[string]any{"Database": map[string]any{"Name": "db1"}},
	})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- ListLakeFormationOptIns tests ---

func TestRefinement2_ListLakeFormationOptIns(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/CreateLakeFormationOptIn", map[string]any{
		"Principal": map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::000000000000:user/alice"},
		"Resource":  map[string]any{"Database": map[string]any{"Name": "db1"}},
	})

	rec := postJSON(t, h, "/ListLakeFormationOptIns", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	entries := out["LakeFormationOptInsInfoList"].([]any)
	assert.Len(t, entries, 1)
}

// --- GetDataLakePrincipal tests ---

func TestRefinement2_GetDataLakePrincipal(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetDataLakePrincipal", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.NotEmpty(t, out["Identity"])
}

// --- Validation improvement tests ---

func TestRefinement2_RevokePermissions_NilEntry(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	err := b.RevokePermissions(nil)
	assert.Error(t, err)
}

func TestRefinement2_BatchGrant_ErrorCodeMapping(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Pass a nil principal entry to trigger validation error
	rec := postJSON(t, h, "/BatchGrantPermissions", map[string]any{
		"Entries": []any{
			map[string]any{
				// Missing Principal and Resource → validation error in GrantPermissions
				"Permissions": []string{"SELECT"},
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	failures := out["Failures"].([]any)
	assert.Len(t, failures, 1)
	failure := failures[0].(map[string]any)
	errDetail := failure["Error"].(map[string]any)
	// validation errors should map to InvalidInputException
	assert.Equal(t, "InvalidInputException", errDetail["ErrorCode"])
}

func TestRefinement2_CreateDataCellsFilter_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		status int
	}{
		{
			name: "missing_table_catalog_id",
			body: map[string]any{
				"TableData": map[string]any{
					"DatabaseName": "db", "TableName": "tbl", "Name": "f",
				},
			},
			status: http.StatusBadRequest,
		},
		{
			name: "missing_database_name",
			body: map[string]any{
				"TableData": map[string]any{
					"TableCatalogId": "123", "TableName": "tbl", "Name": "f",
				},
			},
			status: http.StatusBadRequest,
		},
		{
			name: "missing_table_name",
			body: map[string]any{
				"TableData": map[string]any{
					"TableCatalogId": "123", "DatabaseName": "db", "Name": "f",
				},
			},
			status: http.StatusBadRequest,
		},
		{
			name: "valid_all_fields",
			body: map[string]any{
				"TableData": map[string]any{
					"TableCatalogId": "123", "DatabaseName": "db", "TableName": "tbl", "Name": "f",
				},
			},
			status: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)
			rec := postJSON(t, h, "/CreateDataCellsFilter", tt.body)
			assert.Equal(t, tt.status, rec.Code)
		})
	}
}

func TestRefinement2_DeleteDataCellsFilter_MissingName(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/DeleteDataCellsFilter", map[string]any{
		"TableCatalogId": "123",
		"DatabaseName":   "db",
		"TableName":      "tbl",
		"Name":           "",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement2_UpdateLFTag_SortsTagValues(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	b.AddLFTagInternal("", "env", []string{"prod"})

	err := b.UpdateLFTag("", "env", []string{"dev", "staging"}, nil)
	require.NoError(t, err)

	tag, err := b.GetLFTag("", "env")
	require.NoError(t, err)
	assert.Equal(t, []string{"dev", "prod", "staging"}, tag.TagValues)
}

func TestRefinement2_CreateLFTagExpression_RequiresTagKey(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CreateLFTagExpression", map[string]any{
		"Name": "myexpr",
		"Expression": []any{
			map[string]any{"TagKey": "", "TagValues": []string{"v"}},
		},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement2_ListPermissions_SortedDeterministic(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	b.AddPermissionInternal(&lakeformation.PermissionEntry{
		Principal: &lakeformation.DataLakePrincipal{
			DataLakePrincipalIdentifier: "arn:aws:iam::000000000000:user/bob",
		},
		Resource:    &lakeformation.Resource{Database: &lakeformation.DatabaseResource{Name: "db1"}},
		Permissions: []string{"CREATE_TABLE"},
	})
	b.AddPermissionInternal(&lakeformation.PermissionEntry{
		Principal: &lakeformation.DataLakePrincipal{
			DataLakePrincipalIdentifier: "arn:aws:iam::000000000000:user/alice",
		},
		Resource:    &lakeformation.Resource{Database: &lakeformation.DatabaseResource{Name: "db1"}},
		Permissions: []string{"SELECT"},
	})

	perms, _ := b.ListPermissions("", 0, "", nil, "")
	require.Len(t, perms, 2)
	// alice sorts before bob
	assert.Equal(t, "arn:aws:iam::000000000000:user/alice", perms[0].Principal.DataLakePrincipalIdentifier)
	assert.Equal(t, "arn:aws:iam::000000000000:user/bob", perms[1].Principal.DataLakePrincipalIdentifier)
}

func TestRefinement2_DescribeResource_DeepCopy(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	b.AddResourceInternal("arn:aws:s3:::bucket1", "role")

	info, err := b.DescribeResource("arn:aws:s3:::bucket1")
	require.NoError(t, err)

	// Mutating the returned LastModified should not affect backend state.
	original := *info.LastModified
	mutated := original.AddDate(0, 0, 1)
	*info.LastModified = mutated

	info2, err := b.DescribeResource("arn:aws:s3:::bucket1")
	require.NoError(t, err)
	// The stored value should still be the original.
	assert.True(t, info2.LastModified.Equal(original) || info2.LastModified.Before(mutated))
}

func TestRefinement2_ExportHelpers(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	assert.Equal(t, 0, b.ResourceLFTagCount())
	assert.Equal(t, 0, b.IdentityCenterConfigCount())

	b.AddLFTagInternal("", "k", []string{"v"})
	_, _ = b.CreateLakeFormationIdentityCenterConfiguration("123", "arn:aws:sso:::instance/ssoins-abc", nil, nil)
	assert.Equal(t, 1, b.IdentityCenterConfigCount())
}
