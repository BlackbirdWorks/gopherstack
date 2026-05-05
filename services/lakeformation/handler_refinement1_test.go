package lakeformation_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	svc "github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

// newTestContext builds an echo.Context pointed at the given path with a JSON body.
func newTestContext(t *testing.T, path string, body any) (*echo.Context, *httptest.ResponseRecorder) {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(bodyBytes))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()

	c := e.NewContext(req, rec)

	return c, rec
}

func postJSON(t *testing.T, h *lakeformation.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	c, rec := newTestContext(t, path, body)
	fn := h.Handler()
	err := fn(c)
	require.NoError(t, err)

	return rec
}

// --- Refinement 1 Tests ---

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	b.AddLFTagInternal("123456789012", "env", []string{"dev", "prod"})
	b.AddResourceInternal("arn:aws:s3:::my-bucket", "arn:aws:iam::123456789012:role/MyRole")
	require.Equal(t, 1, b.TagCount())
	require.Equal(t, 1, b.ResourceCount())

	b.Reset()

	assert.Equal(t, 0, b.TagCount())
	assert.Equal(t, 0, b.ResourceCount())
	assert.Equal(t, 0, b.PermissionCount())
}

func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()

	for i := range 3 {
		_ = i
		b.AddLFTagInternal("cat", "key", []string{"v"})
		b.Reset()
		assert.Equal(t, 0, b.TagCount())
	}
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagInternal("cat", "key", []string{"v"})
	require.Equal(t, 1, b.TagCount())

	h.Reset()

	assert.Equal(t, 0, b.TagCount())
}

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &lakeformation.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, lakeformation.ErrNilAppContext)
}

func TestRefinement1_ProviderInit_Success(t *testing.T) {
	t.Parallel()

	p := &lakeformation.Provider{}
	ctx := &svc.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
}

func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	assert.Equal(t, len(h.GetSupportedOperations()), h.HandlerOpsLen())
}

func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	ops := h.GetSupportedOperations()
	assert.Len(t, ops, 61)

	expected := []string{
		"AddLFTagsToResource",
		"AssumeDecoratedRoleWithSAML",
		"BatchGrantPermissions",
		"BatchRevokePermissions",
		"CancelTransaction",
		"CommitTransaction",
		"CreateDataCellsFilter",
		"CreateLFTag",
		"CreateLFTagExpression",
		"CreateLakeFormationIdentityCenterConfiguration",
		"CreateLakeFormationOptIn",
		"DeleteDataCellsFilter",
		"DeleteLFTag",
		"DeleteLFTagExpression",
		"DeleteLakeFormationIdentityCenterConfiguration",
		"DeleteLakeFormationOptIn",
		"DeleteObjectsOnCancel",
		"DeregisterResource",
		"DescribeLakeFormationIdentityCenterConfiguration",
		"DescribeResource",
		"DescribeTransaction",
		"ExtendTransaction",
		"GetDataCellsFilter",
		"GetDataLakePrincipal",
		"GetDataLakeSettings",
		"GetEffectivePermissionsForPath",
		"GetLFTag",
		"GetLFTagExpression",
		"GetQueryState",
		"GetQueryStatistics",
		"GetResourceLFTags",
		"GetTableObjects",
		"GetTemporaryDataLocationCredentials",
		"GetTemporaryGluePartitionCredentials",
		"GetTemporaryGlueTableCredentials",
		"GetWorkUnitResults",
		"GetWorkUnits",
		"GrantPermissions",
		"ListDataCellsFilter",
		"ListLFTagExpressions",
		"ListLFTags",
		"ListLakeFormationOptIns",
		"ListPermissions",
		"ListResources",
		"ListTableStorageOptimizers",
		"ListTransactions",
		"PutDataLakeSettings",
		"RegisterResource",
		"RemoveLFTagsFromResource",
		"RevokePermissions",
		"SearchDatabasesByLFTags",
		"SearchTablesByLFTags",
		"StartQueryPlanning",
		"StartTransaction",
		"UpdateDataCellsFilter",
		"UpdateLFTag",
		"UpdateLFTagExpression",
		"UpdateLakeFormationIdentityCenterConfiguration",
		"UpdateResource",
		"UpdateTableObjects",
		"UpdateTableStorageOptimizer",
	}

	assert.Equal(t, expected, ops)
}

func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()

	b.AddLFTagInternal("cat", "k1", []string{"v1"})
	b.AddLFTagInternal("cat", "k2", []string{"v2"})
	assert.Equal(t, 2, b.TagCount())

	b.AddResourceInternal("arn:aws:s3:::bucket1", "role")
	assert.Equal(t, 1, b.ResourceCount())

	b.AddPermissionInternal(&lakeformation.PermissionEntry{
		Principal: &lakeformation.DataLakePrincipal{
			DataLakePrincipalIdentifier: "arn:aws:iam::123456789012:user/alice",
		},
		Resource: &lakeformation.Resource{Catalog: &lakeformation.CatalogResource{}},
	})
	assert.Equal(t, 1, b.PermissionCount())

	b.AddDataCellsFilterInternal(&lakeformation.DataCellsFilter{
		TableCatalogID: "cat",
		DatabaseName:   "db",
		TableName:      "tbl",
		Name:           "filter1",
	})
	assert.Equal(t, 1, b.DataCellsFilterCount())

	b.AddLFTagExpressionInternal(&lakeformation.LFTagExpression{
		Name:      "expr1",
		CatalogID: "cat",
	})
	assert.Equal(t, 1, b.LFTagExpressionCount())
}

func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	assert.Equal(t, 0, b.ResourceCount())
	assert.Equal(t, 0, b.TagCount())
	assert.Equal(t, 0, b.PermissionCount())
	assert.Equal(t, 0, b.DataCellsFilterCount())
	assert.Equal(t, 0, b.LFTagExpressionCount())
	assert.Equal(t, 0, b.OptInCount())
	assert.Equal(t, 0, b.TransactionCount())
}

func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body map[string]any
		path string
	}{
		{
			name: "create_lf_tag_empty_key",
			path: "/CreateLFTag",
			body: map[string]any{"CatalogId": "cat", "TagKey": "", "TagValues": []string{"v"}},
		},
		{
			name: "create_lf_tag_empty_values",
			path: "/CreateLFTag",
			body: map[string]any{"CatalogId": "cat", "TagKey": "env"},
		},
		{
			name: "delete_lf_tag_empty_key",
			path: "/DeleteLFTag",
			body: map[string]any{"CatalogId": "cat", "TagKey": ""},
		},
		{
			name: "get_lf_tag_empty_key",
			path: "/GetLFTag",
			body: map[string]any{"CatalogId": "cat", "TagKey": ""},
		},
		{
			name: "update_lf_tag_empty_key",
			path: "/UpdateLFTag",
			body: map[string]any{"CatalogId": "cat", "TagKey": "", "TagValuesToAdd": []string{"v"}},
		},
		{
			name: "register_resource_empty_arn",
			path: "/RegisterResource",
			body: map[string]any{"ResourceArn": "", "RoleArn": "arn:aws:iam::123:role/r"},
		},
		{
			name: "deregister_resource_empty_arn",
			path: "/DeregisterResource",
			body: map[string]any{"ResourceArn": ""},
		},
		{
			name: "describe_resource_empty_arn",
			path: "/DescribeResource",
			body: map[string]any{"ResourceArn": ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)
			rec := postJSON(t, h, tt.path, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "InvalidInputException", resp["__type"])
		})
	}
}

func TestRefinement1_CreateLFTag_RoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CreateLFTag", map[string]any{
		"CatalogId": "123456789012",
		"TagKey":    "team",
		"TagValues": []string{"eng", "ops"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, b.TagCount())

	rec2 := postJSON(t, h, "/GetLFTag", map[string]any{
		"CatalogId": "123456789012",
		"TagKey":    "team",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
	assert.Equal(t, "team", out["TagKey"])
}

func TestRefinement1_CreateLFTag_DuplicateReturns409(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagInternal("cat", "env", []string{"dev"})

	rec := postJSON(t, h, "/CreateLFTag", map[string]any{
		"CatalogId": "cat",
		"TagKey":    "env",
		"TagValues": []string{"prod"},
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestRefinement1_DeleteLFTag_NotFoundReturns404(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/DeleteLFTag", map[string]any{
		"CatalogId": "cat",
		"TagKey":    "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement1_RegisterResource_DuplicateReturns409(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddResourceInternal("arn:aws:s3:::existing", "role")

	rec := postJSON(t, h, "/RegisterResource", map[string]any{
		"ResourceArn": "arn:aws:s3:::existing",
		"RoleArn":     "arn:aws:iam::123:role/r",
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestRefinement1_ListResources_Sorted(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddResourceInternal("arn:aws:s3:::zz-bucket", "role")
	b.AddResourceInternal("arn:aws:s3:::aa-bucket", "role")
	b.AddResourceInternal("arn:aws:s3:::mm-bucket", "role")

	rec := postJSON(t, h, "/ListResources", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	list := out["ResourceInfoList"].([]any)
	require.Len(t, list, 3)
	assert.Equal(t, "arn:aws:s3:::aa-bucket", list[0].(map[string]any)["ResourceArn"])
	assert.Equal(t, "arn:aws:s3:::mm-bucket", list[1].(map[string]any)["ResourceArn"])
	assert.Equal(t, "arn:aws:s3:::zz-bucket", list[2].(map[string]any)["ResourceArn"])
}

func TestRefinement1_ListLFTags_Sorted(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagInternal("cat", "zzz", []string{"v"})
	b.AddLFTagInternal("cat", "aaa", []string{"v"})
	b.AddLFTagInternal("cat", "mmm", []string{"v"})

	rec := postJSON(t, h, "/ListLFTags", map[string]any{"CatalogId": "cat"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tags := out["LFTags"].([]any)
	require.Len(t, tags, 3)
	assert.Equal(t, "aaa", tags[0].(map[string]any)["TagKey"])
	assert.Equal(t, "mmm", tags[1].(map[string]any)["TagKey"])
	assert.Equal(t, "zzz", tags[2].(map[string]any)["TagKey"])
}

func TestRefinement1_AddLFTagsToResource_PartialFailure(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagInternal("cat", "env", []string{"dev", "prod"})

	rec := postJSON(t, h, "/AddLFTagsToResource", map[string]any{
		"CatalogId": "cat",
		"Resource":  map[string]any{"Database": map[string]any{"Name": "mydb"}},
		"LFTags": []map[string]any{
			{"TagKey": "env", "TagValues": []string{"dev"}},
			{"TagKey": "nonexistent", "TagValues": []string{"x"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	failures := out["Failures"].([]any)
	assert.Len(t, failures, 1, "only nonexistent tag should fail")
	assert.Equal(t, "nonexistent", failures[0].(map[string]any)["LFTag"].(map[string]any)["TagKey"])
}

func TestRefinement1_CancelTransaction_AlreadyCommitted(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Commit first.
	rec := postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": "tx-1"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, b.TransactionCount())

	// Cancel should fail.
	rec2 := postJSON(t, h, "/CancelTransaction", map[string]any{"TransactionId": "tx-1"})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestRefinement1_CommitTransaction_AlreadyCancelled(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Cancel first.
	rec := postJSON(t, h, "/CancelTransaction", map[string]any{"TransactionId": "tx-2"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Commit should fail.
	rec2 := postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": "tx-2"})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestRefinement1_CommitTransaction_ReturnsStatus(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CommitTransaction", map[string]any{"TransactionId": "tx-3"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "COMMITTED", out["TransactionStatus"])
}

func TestRefinement1_CreateDataCellsFilter_RoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CreateDataCellsFilter", map[string]any{
		"TableData": map[string]any{
			"TableCatalogId": "cat",
			"DatabaseName":   "db",
			"TableName":      "tbl",
			"Name":           "filter1",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, b.DataCellsFilterCount())
}

func TestRefinement1_CreateDataCellsFilter_DuplicateReturns409(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddDataCellsFilterInternal(&lakeformation.DataCellsFilter{
		TableCatalogID: "cat",
		DatabaseName:   "db",
		TableName:      "tbl",
		Name:           "f1",
	})

	rec := postJSON(t, h, "/CreateDataCellsFilter", map[string]any{
		"TableData": map[string]any{
			"TableCatalogId": "cat",
			"DatabaseName":   "db",
			"TableName":      "tbl",
			"Name":           "f1",
		},
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestRefinement1_DeleteDataCellsFilter_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/DeleteDataCellsFilter", map[string]any{
		"TableCatalogId": "cat",
		"DatabaseName":   "db",
		"TableName":      "tbl",
		"Name":           "missing",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement1_CreateLFTagExpression_RoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CreateLFTagExpression", map[string]any{
		"Name":        "my-expr",
		"Description": "a test expression",
		"CatalogId":   "cat",
		"Expression":  []map[string]any{{"TagKey": "env", "TagValues": []string{"dev"}}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, b.LFTagExpressionCount())
}

func TestRefinement1_DeleteLFTagExpression_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/DeleteLFTagExpression", map[string]any{
		"Name":      "nonexistent",
		"CatalogId": "cat",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement1_CreateLakeFormationOptIn_DuplicateReturns409(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	body := map[string]any{
		"Principal": map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/alice"},
		"Resource":  map[string]any{"Database": map[string]any{"Name": "db1"}},
	}

	rec1 := postJSON(t, h, "/CreateLakeFormationOptIn", body)
	require.Equal(t, http.StatusOK, rec1.Code)
	assert.Equal(t, 1, b.OptInCount())

	rec2 := postJSON(t, h, "/CreateLakeFormationOptIn", body)
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestRefinement1_IdentityCenterConfiguration_RoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CreateLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId":   "123456789012",
		"InstanceArn": "arn:aws:sso:::instance/ssoins-0000000000000000",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["ApplicationArn"])
	assert.Contains(t, out["ApplicationArn"].(string), "arn:aws:sso")
}

func TestRefinement1_AssumeDecoratedRoleWithSAML_RoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/AssumeDecoratedRoleWithSAML", map[string]any{
		"PrincipalArn":  "arn:aws:iam::123:saml-provider/MyProvider",
		"RoleArn":       "arn:aws:iam::123456789012:role/MyRole",
		"SAMLAssertion": "base64encodedsamlassertion",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["AccessKeyId"])
	assert.NotEmpty(t, out["SecretAccessKey"])
	assert.NotEmpty(t, out["SessionToken"])
	assert.NotEmpty(t, out["Expiration"])
}

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	b.AddLFTagInternal("cat", "env", []string{"dev", "prod"})
	b.AddResourceInternal("arn:aws:s3:::bucket", "role")
	b.AddDataCellsFilterInternal(&lakeformation.DataCellsFilter{
		TableCatalogID: "cat", DatabaseName: "db", TableName: "tbl", Name: "f1",
	})
	b.AddLFTagExpressionInternal(&lakeformation.LFTagExpression{Name: "e1", CatalogID: "cat"})
	err := b.CreateLakeFormationOptIn(
		&lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/alice"},
		&lakeformation.Resource{Database: &lakeformation.DatabaseResource{Name: "db1"}},
	)
	require.NoError(t, err)
	_, err = b.CommitTransaction("tx-persist")
	require.NoError(t, err)

	data, err := b.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	b2 := lakeformation.NewInMemoryBackend()
	require.NoError(t, b2.Restore(data))

	assert.Equal(t, b.TagCount(), b2.TagCount())
	assert.Equal(t, b.ResourceCount(), b2.ResourceCount())
	assert.Equal(t, b.DataCellsFilterCount(), b2.DataCellsFilterCount())
	assert.Equal(t, b.LFTagExpressionCount(), b2.LFTagExpressionCount())
	assert.Equal(t, b.OptInCount(), b2.OptInCount())
	assert.Equal(t, b.TransactionCount(), b2.TransactionCount())
}

func TestRefinement1_PersistenceEmpty(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	data, err := b.Snapshot()
	require.NoError(t, err)

	b2 := lakeformation.NewInMemoryBackend()
	require.NoError(t, b2.Restore(data))
	assert.Equal(t, 0, b2.TagCount())
	assert.Equal(t, 0, b2.ResourceCount())
}

func TestRefinement1_UnknownOperationReturns400(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	c, rec := newTestContext(t, "/UnknownOp", nil)
	fn := h.Handler()
	require.NoError(t, fn(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_NonPostMethodReturns405(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/GetDataLakeSettings", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	fn := h.Handler()
	require.NoError(t, fn(c))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestRefinement1_BatchGrantPermissions_EmptyEntries(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/BatchGrantPermissions", map[string]any{
		"Entries": []any{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out["Failures"])
}

func TestRefinement1_UpdateLFTag_AddAndRemove(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagInternal("cat", "env", []string{"dev", "staging"})

	rec := postJSON(t, h, "/UpdateLFTag", map[string]any{
		"CatalogId":         "cat",
		"TagKey":            "env",
		"TagValuesToAdd":    []string{"prod"},
		"TagValuesToDelete": []string{"staging"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify via GetLFTag.
	rec2 := postJSON(t, h, "/GetLFTag", map[string]any{
		"CatalogId": "cat",
		"TagKey":    "env",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))

	vals := out["TagValues"].([]any)
	strs := make([]string, len(vals))

	for i, v := range vals {
		strs[i] = v.(string)
	}

	assert.Contains(t, strs, "dev")
	assert.Contains(t, strs, "prod")
	assert.NotContains(t, strs, "staging")
}

func TestRefinement1_GrantAndListPermissions(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GrantPermissions", map[string]any{
		"Principal":   map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/alice"},
		"Resource":    map[string]any{"Database": map[string]any{"Name": "mydb"}},
		"Permissions": []string{"SELECT"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, b.PermissionCount())

	rec2 := postJSON(t, h, "/ListPermissions", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
	perms := out["PrincipalResourcePermissions"].([]any)
	assert.Len(t, perms, 1)
}

func TestRefinement1_AddLFTagsToResource_TableResource(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagInternal("cat", "classification", []string{"public", "private"})

	rec := postJSON(t, h, "/AddLFTagsToResource", map[string]any{
		"CatalogId": "cat",
		"Resource": map[string]any{
			"Table": map[string]any{"DatabaseName": "mydb", "Name": "mytbl"},
		},
		"LFTags": []map[string]any{
			{"TagKey": "classification", "TagValues": []string{"public"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out["Failures"])
}

func TestRefinement1_AddLFTagsToResource_CatalogResource(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagInternal("cat", "level", []string{"gold"})

	rec := postJSON(t, h, "/AddLFTagsToResource", map[string]any{
		"CatalogId": "cat",
		"Resource":  map[string]any{"Catalog": map[string]any{}},
		"LFTags": []map[string]any{
			{"TagKey": "level", "TagValues": []string{"gold"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out["Failures"])
}

func TestRefinement1_AddLFTagsToResource_DataLocationResource(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagInternal("cat", "zone", []string{"raw"})

	rec := postJSON(t, h, "/AddLFTagsToResource", map[string]any{
		"CatalogId": "cat",
		"Resource": map[string]any{
			"DataLocation": map[string]any{"ResourceArn": "arn:aws:s3:::mybucket"},
		},
		"LFTags": []map[string]any{
			{"TagKey": "zone", "TagValues": []string{"raw"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out["Failures"])
}

func TestRefinement1_DeregisterResourceCleansPermissions(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	arn := "arn:aws:s3:::cleanup-bucket"

	rec := postJSON(t, h, "/RegisterResource", map[string]any{
		"ResourceArn": arn,
		"RoleArn":     "arn:aws:iam::123:role/r",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	b.AddPermissionInternal(&lakeformation.PermissionEntry{
		Principal: &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/a"},
		Resource:  &lakeformation.Resource{DataLocation: &lakeformation.DataLocationResource{ResourceArn: arn}},
	})
	require.Equal(t, 1, b.PermissionCount())

	rec2 := postJSON(t, h, "/DeregisterResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, rec2.Code)

	assert.Equal(t, 0, b.ResourceCount())
	assert.Equal(t, 0, b.PermissionCount(), "permissions should be cleaned up on deregister")
}

func TestRefinement1_GrantPermissions_NilPrincipalReturns400(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GrantPermissions", map[string]any{
		"Resource":    map[string]any{"Database": map[string]any{"Name": "db"}},
		"Permissions": []string{"SELECT"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
