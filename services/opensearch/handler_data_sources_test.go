package opensearch_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// TestAddDataSource_DataSourceTypeJSON verifies that a structured
// DataSourceType sent as a JSON object round-trips as a real nested JSON
// object on GetDataSource (types.DataSourceType is a tagged union on the
// wire, e.g. {"S3GlueDataCatalog":{...}} -- not a plain enum string, and
// GetDataSourceOutput's fields are top-level, not wrapped in a "DataSource"
// envelope).
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

	getResp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/ds-json-domain/dataSource/my-ds", nil)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	var ds map[string]any
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&ds))

	// No "DataSource" envelope: fields are top-level.
	assert.Equal(t, "my-ds", ds["Name"])

	dsType, ok := ds["DataSourceType"].(map[string]any)
	require.True(t, ok, "DataSourceType must be a nested JSON object, not a string")
	assert.Contains(t, dsType, "S3GlueDataCatalog")
}

// TestAddDirectQueryDataSource_DataSourceTypeJSON verifies that
// DataSourceType round-trips as a real nested JSON object for direct-query
// data sources too.
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

	getResp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/directQueryDataSource/dq-ds", nil)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&out))
	assert.Equal(t, "dq-ds", out["DataSourceName"])

	dsType, ok := out["DataSourceType"].(map[string]any)
	require.True(t, ok, "DataSourceType must be a nested JSON object, not a string")
	assert.Contains(t, dsType, "CloudWatchLog")
}

func TestAddDataSource_DomainNotFound(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.AddDataSource("nonexistent", "ds1", "desc", json.RawMessage(`{"S3GlueDataCatalog":{}}`))
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrDomainNotFound)
}

func TestAddDirectQueryDataSource_Duplicate(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.AddDirectQueryDataSource("ds-1", "desc", json.RawMessage(`{"CloudWatchLog":{}}`), nil)
	require.NoError(t, err)

	_, err = b.AddDirectQueryDataSource("ds-1", "desc2", json.RawMessage(`{"SecurityLake":{}}`), nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrDataSourceAlreadyExists)
}

func TestDirectQueryDataSource_ARN(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	dsARN, err := b.AddDirectQueryDataSource("my-dq", "desc", json.RawMessage(`{"CloudWatchLog":{}}`), nil)
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
			"DataSourceType": map[string]any{"S3GlueDataCatalog": map[string]any{}},
		})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestDataSources_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		getSourceName  string
		addSources     []string
		wantListCount  int
		wantGetSuccess bool
	}{
		{
			name:           "single_source_listed",
			addSources:     []string{"ds1"},
			wantListCount:  1,
			getSourceName:  "ds1",
			wantGetSuccess: true,
		},
		{
			name:           "multiple_sources_listed",
			addSources:     []string{"ds1", "ds2", "ds3"},
			wantListCount:  3,
			getSourceName:  "ds2",
			wantGetSuccess: true,
		},
		{
			name:           "empty_domain_no_sources",
			addSources:     []string{},
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

			for _, name := range tt.addSources {
				_, err := b.AddDataSource("ds-domain", name, "", json.RawMessage(`{"S3GlueDataCatalog":{}}`))
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
			"DataSourceType": map[string]any{"CloudWatchLog": map[string]any{}},
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
	require.Len(t, sources, 1)
	assert.Equal(t, "dq-to-delete", sources[0].(map[string]any)["DataSourceName"])

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
			"DataSourceType": map[string]any{"CloudWatchLog": map[string]any{}},
			"Description":    "before",
			"OpenSearchArns": []string{"arn:aws:es:us-east-1:123456789012:domain/x"},
		})
	ar.Body.Close()

	// Update (DataSourceType and OpenSearchArns are required on update too).
	ur := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/directQueryDataSource/dq-upd",
		map[string]any{
			"DataSourceType": map[string]any{"CloudWatchLog": map[string]any{}},
			"Description":    "after",
			"OpenSearchArns": []string{"arn:aws:es:us-east-1:123456789012:domain/x"},
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
			map[string]any{
				"Name":           name,
				"DataSourceType": map[string]any{"S3GlueDataCatalog": map[string]any{}},
				"Description":    name,
			})
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

	// Get specific source. No "DataSource" envelope: fields are top-level.
	gr := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/ds-list-domain/dataSource/ds-alpha", nil)
	defer gr.Body.Close()
	require.Equal(t, http.StatusOK, gr.StatusCode)

	var ds map[string]any
	require.NoError(t, json.NewDecoder(gr.Body).Decode(&ds))
	assert.Equal(t, "ds-alpha", ds["Name"])
	assert.Equal(t, "ACTIVE", ds["Status"])
}

func TestDataSources_UpdateAndDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createTestDomain(t, h, "ds-ud-domain")

	ar := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/domain/ds-ud-domain/dataSource",
		map[string]any{
			"Name":           "ds-target",
			"DataSourceType": map[string]any{"S3GlueDataCatalog": map[string]any{}},
			"Description":    "before",
		})
	ar.Body.Close()

	// UpdateDataSource is a PUT to the same /dataSource/{Name} path GET uses,
	// not a POST to an invented /updateDataSource sub-path. DataSourceType is
	// required on every update call, matching real AWS.
	ur := doRequest(t, h, http.MethodPut,
		"/2021-01-01/opensearch/domain/ds-ud-domain/dataSource/ds-target",
		map[string]any{
			"DataSourceType": map[string]any{"S3GlueDataCatalog": map[string]any{}},
			"Description":    "after",
		})
	defer ur.Body.Close()
	require.Equal(t, http.StatusOK, ur.StatusCode)

	var uOut map[string]any
	require.NoError(t, json.NewDecoder(ur.Body).Decode(&uOut))
	assert.Contains(t, uOut["Message"], "updated")

	// Get reflects the updated description.
	gr := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/ds-ud-domain/dataSource/ds-target", nil)
	defer gr.Body.Close()
	var gOut map[string]any
	require.NoError(t, json.NewDecoder(gr.Body).Decode(&gOut))
	assert.Equal(t, "after", gOut["Description"])

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

func TestDataSources_UpdateUnknownReturnsNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createTestDomain(t, h, "ds-unknown-domain")

	resp := doRequest(t, h, http.MethodPut,
		"/2021-01-01/opensearch/domain/ds-unknown-domain/dataSource/nonexistent",
		map[string]any{"DataSourceType": map[string]any{"S3GlueDataCatalog": map[string]any{}}})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
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

	_, err = b.AddDataSource("my-domain", "", "desc", json.RawMessage(`{"S3GlueDataCatalog":{}}`))
	require.Error(t, err)
}
