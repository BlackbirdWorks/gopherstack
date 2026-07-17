package lakeformation_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestAddLFTagsToResource_CatalogResource(t *testing.T) {
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

func TestAddLFTagsToResource_DataLocationResource(t *testing.T) {
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

// --- GetResourceLFTags tests ---

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
