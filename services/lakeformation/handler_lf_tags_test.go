package lakeformation_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

func TestHandler_LFTagLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		catalogID  string
		tagKey     string
		tagValues  string
		wantStatus int
	}{
		{
			name:       "full_lifecycle",
			catalogID:  "123456789012",
			tagKey:     "env",
			tagValues:  `["dev","prod"]`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create
			createBody := `{"CatalogId":"` + tt.catalogID + `","TagKey":"` + tt.tagKey + `","TagValues":` + tt.tagValues + `}`
			rec := doLFRequest(t, h, "/CreateLFTag", createBody)
			assert.Equal(t, tt.wantStatus, rec.Code)

			// Get
			getBody := `{"CatalogId":"` + tt.catalogID + `","TagKey":"` + tt.tagKey + `"}`
			rec = doLFRequest(t, h, "/GetLFTag", getBody)
			assert.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.tagKey, getResp["TagKey"])

			// Update
			updateBody := `{"CatalogId":"` + tt.catalogID + `","TagKey":"` + tt.tagKey +
				`","TagValuesToAdd":["staging"],"TagValuesToDelete":["dev"]}`
			rec = doLFRequest(t, h, "/UpdateLFTag", updateBody)
			assert.Equal(t, http.StatusOK, rec.Code)

			// List
			listBody := `{"CatalogId":"` + tt.catalogID + `"}`
			rec = doLFRequest(t, h, "/ListLFTags", listBody)
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			tags, _ := listResp["LFTags"].([]any)
			assert.Len(t, tags, 1)

			// Delete
			rec = doLFRequest(t, h, "/DeleteLFTag", getBody)
			assert.Equal(t, http.StatusOK, rec.Code)

			// Get after delete → 404
			rec = doLFRequest(t, h, "/GetLFTag", getBody)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_AddLFTagsToResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupFn    func(b *lakeformation.InMemoryBackend)
		body       string
		wantStatus int
		wantFails  int
	}{
		{
			name: "success_with_existing_tag",
			setupFn: func(b *lakeformation.InMemoryBackend) {
				require.NoError(t, b.CreateLFTag("123456789012", "env", []string{"dev", "prod"}))
			},
			body: `{"CatalogId":"123456789012","Resource":{"Database":{"Name":"mydb"}},` +
				`"LFTags":[{"TagKey":"env","TagValues":["dev"]}]}`,
			wantStatus: http.StatusOK,
			wantFails:  0,
		},
		{
			name:    "tag_not_found_returns_failures",
			setupFn: nil,
			body: `{"CatalogId":"123456789012","Resource":{"Database":{"Name":"mydb"}},` +
				`"LFTags":[{"TagKey":"nonexistent","TagValues":["v1"]}]}`,
			wantStatus: http.StatusOK,
			wantFails:  1,
		},
		{
			name:       "missing_resource",
			body:       `{"LFTags":[{"TagKey":"env","TagValues":["dev"]}]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_lftags",
			body:       `{"Resource":{"Database":{"Name":"mydb"}}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json",
			body:       `not-json`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			if tt.setupFn != nil {
				tt.setupFn(b)
			}

			h := lakeformation.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doLFRequest(t, h, "/AddLFTagsToResource", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				failures, _ := resp["Failures"].([]any)
				assert.Len(t, failures, tt.wantFails)
			}
		})
	}
}

func TestHandler_AddLFTagsToResource_RouteMatches(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	matcher := h.RouteMatcher()

	tests := []struct {
		name       string
		path       string
		authHeader string
		want       bool
	}{
		{
			name:       "add_lf_tags_to_resource",
			path:       "/AddLFTagsToResource",
			authHeader: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/lakeformation/aws4_request",
			want:       true,
		},
		{
			name:       "cancel_transaction",
			path:       "/CancelTransaction",
			authHeader: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/lakeformation/aws4_request",
			want:       true,
		},
		{
			name:       "create_data_cells_filter",
			path:       "/CreateDataCellsFilter",
			authHeader: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/lakeformation/aws4_request",
			want:       true,
		},
		{
			name:       "create_lf_tag_expression",
			path:       "/CreateLFTagExpression",
			authHeader: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/lakeformation/aws4_request",
			want:       true,
		},
		{
			name:       "create_identity_center_config",
			path:       "/CreateLakeFormationIdentityCenterConfiguration",
			authHeader: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/lakeformation/aws4_request",
			want:       true,
		},
	}

	e := echo.New()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, tt.path, http.NoBody)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestCreateLFTag_RoundTrip(t *testing.T) {
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

func TestCreateLFTag_DuplicateReturns409(t *testing.T) {
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

func TestDeleteLFTag_NotFoundReturns404(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/DeleteLFTag", map[string]any{
		"CatalogId": "cat",
		"TagKey":    "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestListLFTags_Sorted(t *testing.T) {
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

func TestAddLFTagsToResource_PartialFailure(t *testing.T) {
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

func TestUpdateLFTag_AddAndRemove(t *testing.T) {
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

func TestAddLFTagsToResource_TableResource(t *testing.T) {
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

// TestAddLFTagsToResource_RejectsNonTaggableResourceKinds covers the
// resource-kind restriction documented on AddLFTagsToResourceInput.Resource:
// "The database, table, or column resource to which to attach an LF-tag."
// (api_op_AddLFTagsToResource.go:29-31, aws-sdk-go-v2/service/lakeformation
// @v1.50.4). Catalog/DataLocation/DataCellsFilter/LFTag/LFTagExpression/
// LFTagPolicy are real types.Resource union members but are not valid here --
// gopherstack previously accepted all of them (a superset of what AWS
// accepts), which these two cases used to assert as 200 OK.
func TestAddLFTagsToResource_RejectsNonTaggableResourceKinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resource map[string]any
		name     string
	}{
		{name: "catalog", resource: map[string]any{"Catalog": map[string]any{}}},
		{
			name:     "data-location",
			resource: map[string]any{"DataLocation": map[string]any{"ResourceArn": "arn:aws:s3:::mybucket"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)
			b.AddLFTagInternal("cat", "level", []string{"gold"})

			rec := postJSON(t, h, "/AddLFTagsToResource", map[string]any{
				"CatalogId": "cat",
				"Resource":  tt.resource,
				"LFTags": []map[string]any{
					{"TagKey": "level", "TagValues": []string{"gold"}},
				},
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestAddLFTagsToResource_TableWithColumnsIsolated proves resourceToKey
// previously had no TableWithColumns case at all (only Catalog/Database/
// Table/DataLocation/DataCellsFilter/LFTag/LFTagExpression/LFTagPolicy), so
// every TableWithColumns resource fell through to the same "" key --
// AddLFTagsToResource on one table-with-columns leaked into
// GetResourceLFTags on a completely different one.
func TestAddLFTagsToResource_TableWithColumnsIsolated(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagInternal("", "env", []string{"prod", "dev"})

	rec := postJSON(t, h, "/AddLFTagsToResource", map[string]any{
		"Resource": map[string]any{
			"TableWithColumns": map[string]any{
				"DatabaseName": "db1", "Name": "tbl1", "ColumnNames": []string{"col_a"},
			},
		},
		"LFTags": []any{map[string]any{"TagKey": "env", "TagValues": []string{"prod"}}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postJSON(t, h, "/GetResourceLFTags", map[string]any{
		"Resource": map[string]any{
			"TableWithColumns": map[string]any{
				"DatabaseName": "db2", "Name": "tbl2", "ColumnNames": []string{"col_b"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.Empty(t, out["LFTagsOnTable"], "tags on an unrelated TableWithColumns resource must not leak")
	assert.Empty(t, out["LFTagsOnColumns"], "tags on an unrelated TableWithColumns resource must not leak")
}

// TestGetResourceLFTags_ColumnsWireShape covers the real
// GetResourceLFTagsOutput.LFTagsOnColumns wire shape: []types.ColumnLFTag
// (Name + LFTags), not a flat []types.LFTagPair -- gopherstack's
// getResourceLFTagsOutput.LFTagsOnColumns was typed []LFTagPair, and the
// field was never populated by any code path (a disguised stub: present on
// the wire struct but dead).
func TestGetResourceLFTags_ColumnsWireShape(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagInternal("", "env", []string{"prod"})

	postJSON(t, h, "/AddLFTagsToResource", map[string]any{
		"Resource": map[string]any{
			"TableWithColumns": map[string]any{
				"DatabaseName": "db1", "Name": "tbl1", "ColumnNames": []string{"col_a", "col_b"},
			},
		},
		"LFTags": []any{map[string]any{"TagKey": "env", "TagValues": []string{"prod"}}},
	})

	rec := postJSON(t, h, "/GetResourceLFTags", map[string]any{
		"Resource": map[string]any{
			"TableWithColumns": map[string]any{
				"DatabaseName": "db1", "Name": "tbl1", "ColumnNames": []string{"col_a", "col_b"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	require.Empty(t, out["LFTagOnDatabase"])
	require.Empty(t, out["LFTagsOnTable"])

	cols, ok := out["LFTagsOnColumns"].([]any)
	require.True(t, ok, "LFTagsOnColumns must be a list of ColumnLFTag objects")
	require.Len(t, cols, 2)

	for _, c := range cols {
		col, colOK := c.(map[string]any)
		require.True(t, colOK)
		assert.Contains(t, []any{"col_a", "col_b"}, col["Name"])

		tags, tagsOK := col["LFTags"].([]any)
		require.True(t, tagsOK)
		require.Len(t, tags, 1)
		pair := tags[0].(map[string]any)
		assert.Equal(t, "env", pair["TagKey"])
	}
}

func TestRemoveLFTagsFromResource_Success(t *testing.T) {
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

func TestRemoveLFTagsFromResource_NotAttached(t *testing.T) {
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

func TestRemoveLFTagsFromResource_MissingResource(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/RemoveLFTagsFromResource", map[string]any{
		"LFTags": []any{map[string]any{"TagKey": "k", "TagValues": []string{"v"}}},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRemoveLFTagsFromResource_RejectsNonTaggableResourceKinds mirrors
// TestAddLFTagsToResource_RejectsNonTaggableResourceKinds: "Only database,
// table, or tableWithColumns resource are allowed."
// (api_op_RemoveLFTagsFromResource.go:12-14, aws-sdk-go-v2/service/
// lakeformation@v1.50.4).
func TestRemoveLFTagsFromResource_RejectsNonTaggableResourceKinds(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/RemoveLFTagsFromResource", map[string]any{
		"Resource": map[string]any{"Catalog": map[string]any{}},
		"LFTags":   []any{map[string]any{"TagKey": "k", "TagValues": []string{"v"}}},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- GetResourceLFTags tests ---

func TestGetResourceLFTags_RejectsNonTaggableResourceKind(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetResourceLFTags", map[string]any{
		"Resource": map[string]any{"DataLocation": map[string]any{"ResourceArn": "arn:aws:s3:::b"}},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetResourceLFTags_Empty(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetResourceLFTags", map[string]any{
		"Resource": map[string]any{"Database": map[string]any{"Name": "db1"}},
	})

	require.Equal(t, http.StatusOK, rec.Code)
}

func TestGetResourceLFTags_MissingResource(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetResourceLFTags", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- ListDataCellsFilter tests ---

func TestSearchDatabasesByLFTags(t *testing.T) {
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

func TestSearchTablesByLFTags(t *testing.T) {
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

func TestAddLFTagsToResource_ValidatesTagValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantFailureCode string
		allowedValues   []string
		tagValues       []any
		wantFailures    bool
	}{
		{
			name:          "valid value succeeds",
			allowedValues: []string{"dev", "prod"},
			tagValues:     []any{"dev"},
			wantFailures:  false,
		},
		{
			name:            "invalid value fails",
			allowedValues:   []string{"dev", "prod"},
			tagValues:       []any{"staging"},
			wantFailures:    true,
			wantFailureCode: "InvalidInputException",
		},
		{
			name:            "mix of valid and invalid fails for that tag",
			allowedValues:   []string{"dev", "prod"},
			tagValues:       []any{"dev", "nope"},
			wantFailures:    true,
			wantFailureCode: "InvalidInputException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)

			require.NoError(t, b.CreateLFTag("", "env", tt.allowedValues))

			rec := postJSON(t, h, "/AddLFTagsToResource", map[string]any{
				"Resource": map[string]any{"Database": map[string]any{"Name": "db1"}},
				"LFTags": []any{
					map[string]any{"TagKey": "env", "TagValues": tt.tagValues},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, jsonDecode(rec.Body, &out))
			failures, _ := out["Failures"].([]any)

			if tt.wantFailures {
				require.NotEmpty(t, failures, "should have failures")
				failure := failures[0].(map[string]any)
				errDetail := failure["Error"].(map[string]any)
				assert.Equal(t, tt.wantFailureCode, errDetail["ErrorCode"])
			} else {
				assert.Empty(t, failures)
			}
		})
	}
}
