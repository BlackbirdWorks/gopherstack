package opensearch_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatch3_AddDataSource_DataSourceTypeJSON verifies that a structured
// DataSourceType sent as a JSON object is stored as JSON (not a Go map dump).
func TestAddDataSource_DataSourceTypeJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createTestDomain(t, h, "ds-json-domain")

	body := map[string]any{
		"Name":        "my-ds",
		"Description": "test",
		"DataSourceType": map[string]any{
			"S3GlueDataCatalog": map[string]any{
				"RoleArn": "arn:aws:iam::123456789012:role/MyRole",
			},
		},
	}

	resp := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/domain/ds-json-domain/dataSource", body)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Retrieve and verify that the DataSourceType is valid JSON, not a Go map dump.
	getResp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/ds-json-domain/dataSource/my-ds", nil)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&out))

	ds, ok := out["DataSource"].(map[string]any)
	require.True(t, ok, "expected DataSource key in response")

	rawType, hasType := ds["dataSourceType"]
	require.True(t, hasType, "DataSourceType must be present")

	rawStr, ok := rawType.(string)
	require.True(t, ok, "DataSourceType must be a string in storage")

	// Must be parseable JSON, not a Go map representation.
	var parsed map[string]any
	require.NoError(t, json.Unmarshal([]byte(rawStr), &parsed),
		"DataSourceType must be stored as JSON, got: %s", rawStr)

	assert.Contains(t, parsed, "S3GlueDataCatalog")
}

// TestBatch3_AddDirectQueryDataSource_DataSourceTypeJSON verifies that
// DataSourceType is stored as JSON for direct query data sources.
func TestAddDirectQueryDataSource_DataSourceTypeJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	body := map[string]any{
		"DataSourceName": "dq-ds",
		"Description":    "direct query test",
		"DataSourceType": map[string]any{
			"CloudWatchLog": map[string]any{},
		},
		"OpenSearchArns": []string{"arn:aws:es:us-east-1:123456789012:domain/my-domain"},
	}

	resp := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/directQueryDataSource", body)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Get and verify.
	getResp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/directQueryDataSource/dq-ds", nil)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&out))

	rawType, hasType := out["dataSourceType"]
	require.True(t, hasType, "dataSourceType must be present in response")

	rawStr, ok := rawType.(string)
	require.True(t, ok, "dataSourceType must be a string in storage")

	var parsed map[string]any
	require.NoError(t, json.Unmarshal([]byte(rawStr), &parsed),
		"dataSourceType must be stored as JSON, got: %s", rawStr)

	assert.Contains(t, parsed, "CloudWatchLog")
}

func TestAddDataSource_DomainNotFound(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.AddDataSource("nonexistent", "ds1", "desc", "S3GLUE")
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrDomainNotFound)
}

func TestAddDirectQueryDataSource_Duplicate(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.AddDirectQueryDataSource("ds-1", "desc", "CloudWatchLogs", nil)
	require.NoError(t, err)

	_, err = b.AddDirectQueryDataSource("ds-1", "desc2", "S3", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrDataSourceAlreadyExists)
}

func TestDirectQueryDataSource_ARN(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	dsARN, err := b.AddDirectQueryDataSource("my-dq", "desc", "CloudWatchLogs", nil)
	require.NoError(t, err)
	assert.Contains(t, dsARN, "directQueryDataSource/my-dq")
	assert.Contains(t, dsARN, testAccountID)
	assert.Contains(t, dsARN, testRegion)
}

func TestHTTPAddDataSource(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{"DomainName": "my-domain"})
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain/my-domain/dataSource",
		map[string]any{
			"Name":           "my-ds",
			"Description":    "test",
			"DataSourceType": map[string]string{"S3GlueDataCatalog": ""},
		})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestDataSources_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		getSourceName  string
		addSources     []map[string]string
		wantListCount  int
		wantGetSuccess bool
	}{
		{
			name:           "single_source_listed",
			addSources:     []map[string]string{{"name": "ds1", "type": "S3GLUE", "desc": "test"}},
			wantListCount:  1,
			getSourceName:  "ds1",
			wantGetSuccess: true,
		},
		{
			name: "multiple_sources_listed",
			addSources: []map[string]string{
				{"name": "ds1", "type": "S3GLUE", "desc": ""},
				{"name": "ds2", "type": "CloudWatchLogs", "desc": ""},
				{"name": "ds3", "type": "SecurityLake", "desc": ""},
			},
			wantListCount:  3,
			getSourceName:  "ds2",
			wantGetSuccess: true,
		},
		{
			name:           "empty_domain_no_sources",
			addSources:     []map[string]string{},
			wantListCount:  0,
			getSourceName:  "missing",
			wantGetSuccess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
			b.AddDomainInternal("ds-domain", "")

			for _, src := range tt.addSources {
				_, err := b.AddDataSource("ds-domain", src["name"], src["desc"], src["type"])
				require.NoError(t, err)
			}

			sources, err := b.ListDataSources("ds-domain")
			require.NoError(t, err)
			assert.Len(t, sources, tt.wantListCount)

			_, getErr := b.GetDataSource("ds-domain", tt.getSourceName)
			if tt.wantGetSuccess {
				require.NoError(t, getErr)
			} else {
				require.Error(t, getErr)
			}
		})
	}
}

func TestDirectQuery_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/directQueryDataSource/nonexistent", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestDirectQuery_DeleteAndList(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Add a direct-query data source.
	ar := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/directQueryDataSource",
		map[string]any{
			"DataSourceName": "dq-to-delete",
			"DataSourceType": map[string]any{"CloudWatchLogs": map[string]any{}},
			"Description":    "test",
		})
	ar.Body.Close()

	// List returns one entry.
	lr := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/directQueryDataSource", nil)
	defer lr.Body.Close()
	require.Equal(t, http.StatusOK, lr.StatusCode)

	var lOut map[string]any
	require.NoError(t, json.NewDecoder(lr.Body).Decode(&lOut))
	sources, ok := lOut["DirectQueryDataSources"].([]any)
	require.True(t, ok)
	assert.Len(t, sources, 1)

	// Delete it.
	del := doRequest(t, h, http.MethodDelete,
		"/2021-01-01/opensearch/directQueryDataSource/dq-to-delete", nil)
	defer del.Body.Close()
	require.Equal(t, http.StatusOK, del.StatusCode)

	// List should now be empty.
	lr2 := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/directQueryDataSource", nil)
	defer lr2.Body.Close()
	var lOut2 map[string]any
	require.NoError(t, json.NewDecoder(lr2.Body).Decode(&lOut2))
	sources2 := lOut2["DirectQueryDataSources"].([]any)
	assert.Empty(t, sources2)
}

func TestDirectQuery_UpdateReturnsARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Add first.
	ar := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/directQueryDataSource",
		map[string]any{
			"DataSourceName": "dq-upd",
			"DataSourceType": map[string]any{"CloudWatchLogs": map[string]any{}},
			"Description":    "before",
		})
	ar.Body.Close()

	// Update.
	ur := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/directQueryDataSource/dq-upd",
		map[string]any{
			"DataSourceName": "dq-upd",
			"DataSourceType": map[string]any{"CloudWatchLogs": map[string]any{}},
			"Description":    "after",
		})
	defer ur.Body.Close()
	require.Equal(t, http.StatusOK, ur.StatusCode)

	var uOut map[string]any
	require.NoError(t, json.NewDecoder(ur.Body).Decode(&uOut))
	assert.Contains(t, uOut, "DataSourceArn")
}

func TestDataSources_ListAndGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createTestDomain(t, h, "ds-list-domain")

	// Add two data sources via HTTP.
	for _, name := range []string{"ds-alpha", "ds-beta"} {
		ar := doRequest(t, h, http.MethodPost,
			"/2021-01-01/opensearch/domain/ds-list-domain/dataSource",
			map[string]any{"Name": name, "DataSourceType": "S3GLUE", "Description": name})
		ar.Body.Close()
	}

	// List.
	lr := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/ds-list-domain/dataSource", nil)
	defer lr.Body.Close()
	require.Equal(t, http.StatusOK, lr.StatusCode)

	var lOut map[string]any
	require.NoError(t, json.NewDecoder(lr.Body).Decode(&lOut))
	sources, ok := lOut["DataSources"].([]any)
	require.True(t, ok)
	assert.Len(t, sources, 2)

	// Get specific source.
	gr := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/ds-list-domain/dataSource/ds-alpha", nil)
	defer gr.Body.Close()
	require.Equal(t, http.StatusOK, gr.StatusCode)

	var gOut map[string]any
	require.NoError(t, json.NewDecoder(gr.Body).Decode(&gOut))
	ds, ok := gOut["DataSource"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ds-alpha", ds["name"])
}

func TestDataSources_UpdateAndDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createTestDomain(t, h, "ds-ud-domain")

	ar := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/domain/ds-ud-domain/dataSource",
		map[string]any{"Name": "ds-target", "DataSourceType": "S3GLUE", "Description": "before"})
	ar.Body.Close()

	// Update returns success message.
	ur := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/domain/ds-ud-domain/updateDataSource",
		map[string]any{"Name": "ds-target", "Description": "after"})
	defer ur.Body.Close()
	require.Equal(t, http.StatusOK, ur.StatusCode)

	var uOut map[string]any
	require.NoError(t, json.NewDecoder(ur.Body).Decode(&uOut))
	assert.Equal(t, "DataSource updated", uOut["Message"])

	// Delete.
	del := doRequest(t, h, http.MethodDelete,
		"/2021-01-01/opensearch/domain/ds-ud-domain/dataSource/ds-target", nil)
	defer del.Body.Close()
	require.Equal(t, http.StatusOK, del.StatusCode)

	var delOut map[string]any
	require.NoError(t, json.NewDecoder(del.Body).Decode(&delOut))
	assert.Equal(t, "DataSource deleted", delOut["Message"])

	// List should be empty after delete.
	lr := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/ds-ud-domain/dataSource", nil)
	defer lr.Body.Close()
	var lOut map[string]any
	require.NoError(t, json.NewDecoder(lr.Body).Decode(&lOut))
	remaining := lOut["DataSources"].([]any)
	assert.Empty(t, remaining)
}

func TestOpenSearchHandler_AddDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *opensearch.Handler)
		name         string
		domainName   string
		dsName       string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "success",
			domainName: "my-domain",
			dsName:     "my-datasource",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "my-domain"})
				r.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"Message"},
		},
		{
			name:       "domain_not_found",
			domainName: "nonexistent",
			dsName:     "ds",
			wantCode:   http.StatusNotFound,
		},
		{
			name:       "duplicate_datasource",
			domainName: "dup-domain",
			dsName:     "dup-ds",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "dup-domain"})
				r.Body.Close()
				r2 := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain/dup-domain/dataSource",
					map[string]any{"Name": "dup-ds", "DataSourceType": map[string]any{}})
				r2.Body.Close()
			},
			wantCode: http.StatusConflict,
		},
		{
			name:       "invalid_json",
			domainName: "my-domain",
			dsName:     "",
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			var body any
			if tt.name == "invalid_json" {
				req := httptest.NewRequest(
					http.MethodPost,
					"/2021-01-01/opensearch/domain/my-domain/dataSource",
					strings.NewReader("bad-json"),
				)
				req.Header.Set("Content-Type", "application/json")
				rw := httptest.NewRecorder()
				h.ServeHTTP(rw, req)
				resp := rw.Result()
				defer resp.Body.Close()
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

				return
			}

			if tt.dsName != "" {
				body = map[string]any{
					"Name":           tt.dsName,
					"DataSourceType": map[string]any{},
				}
			}

			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain/"+tt.domainName+"/dataSource", body)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if len(tt.wantContains) > 0 {
				bodyBytes, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				for _, s := range tt.wantContains {
					assert.Contains(t, string(bodyBytes), s)
				}
			}
		})
	}
}

func TestOpenSearchHandler_AddDirectQueryDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		dsName       string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			dsName:       "my-dq-source",
			wantCode:     http.StatusOK,
			wantContains: []string{"DataSourceArn"},
		},
		{
			name:     "no_name",
			dsName:   "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "duplicate",
			dsName:   "dup-source",
			wantCode: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.name == "duplicate" {
				r := doRequest(
					t,
					h,
					http.MethodPost,
					"/2021-01-01/opensearch/directQueryDataSource",
					map[string]any{
						"DataSourceName": "dup-source",
						"DataSourceType": map[string]any{},
						"OpenSearchArns": []string{},
					},
				)
				r.Body.Close()
			}

			body := map[string]any{
				"DataSourceName": tt.dsName,
				"DataSourceType": map[string]any{},
				"OpenSearchArns": []string{"arn:aws:opensearch:us-east-1:123456789012:domain/test"},
			}
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/directQueryDataSource", body)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if len(tt.wantContains) > 0 {
				bodyBytes, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				for _, s := range tt.wantContains {
					assert.Contains(t, string(bodyBytes), s)
				}
			}
		})
	}
}

func TestOpenSearchBackend_AddDataSource_MissingName(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: "my-domain"})
	require.NoError(t, err)

	_, err = b.AddDataSource("my-domain", "", "desc", "S3GLUE")
	require.Error(t, err)
}
