package appsync_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

// ---- StartWorker ----

func TestHandler_StartWorker(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err := h.StartWorker(ctx)
	require.NoError(t, err)
}

// ---- Janitor ----

func TestJanitor_NewAndRun(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	j := appsync.NewJanitor(b)
	require.NotNil(t, j)

	j.Interval = 1 * time.Millisecond

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	// Run should exit cleanly when context is cancelled.
	done := make(chan struct{})
	go func() {
		j.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("janitor did not stop after context cancellation")
	}
}

// ---- SweepExpiredAPIKeys ----

func TestBackend_SweepExpiredAPIKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(b *appsync.InMemoryBackend) string
		name          string
		wantEvicted   int
		wantKeyExists bool
	}{
		{
			name: "no_keys",
			setup: func(b *appsync.InMemoryBackend) string {
				api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)

				return api.APIID
			},
			wantEvicted:   0,
			wantKeyExists: false,
		},
		{
			name: "expired_key_is_swept",
			setup: func(b *appsync.InMemoryBackend) string {
				api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)
				// Create a key that expires in the past.
				_, err = b.CreateAPIKey(api.APIID, "expired", time.Now().Add(-1*time.Hour).Unix())
				require.NoError(t, err)

				return api.APIID
			},
			wantEvicted:   1,
			wantKeyExists: false,
		},
		{
			name: "valid_key_not_swept",
			setup: func(b *appsync.InMemoryBackend) string {
				api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)
				// Create a key that expires far in the future.
				_, err = b.CreateAPIKey(api.APIID, "valid", time.Now().Add(24*time.Hour).Unix())
				require.NoError(t, err)

				return api.APIID
			},
			wantEvicted:   0,
			wantKeyExists: true,
		},
		{
			name: "mixed_keys",
			setup: func(b *appsync.InMemoryBackend) string {
				api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)
				_, err = b.CreateAPIKey(api.APIID, "expired", time.Now().Add(-1*time.Hour).Unix())
				require.NoError(t, err)
				_, err = b.CreateAPIKey(api.APIID, "valid", time.Now().Add(24*time.Hour).Unix())
				require.NoError(t, err)

				return api.APIID
			},
			wantEvicted:   1,
			wantKeyExists: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			apiID := tt.setup(b)

			evicted := b.SweepExpiredAPIKeys()
			assert.Equal(t, tt.wantEvicted, evicted)

			keys, err := b.ListAPIKeys(apiID)
			require.NoError(t, err)

			if tt.wantKeyExists {
				assert.NotEmpty(t, keys)
			} else {
				// Either no keys or only non-expired ones remain.
				for _, k := range keys {
					assert.True(t, k.Expires == 0 || k.Expires > time.Now().Unix())
				}
			}
		})
	}
}

// ---- EvaluateMappingTemplate (backend + HTTP) ----

func TestBackend_EvaluateMappingTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		template    string
		contextJSON string
		wantErr     bool
	}{
		{
			name:        "simple_template",
			template:    `{"version": "2017-02-28", "payload": {}}`,
			contextJSON: "",
		},
		{
			name:        "invalid_context_json",
			template:    `{"version": "2017-02-28"}`,
			contextJSON: "not-json",
			wantErr:     true,
		},
		{
			name:        "with_context",
			template:    `{"version": "2017-02-28", "payload": {}}`,
			contextJSON: `{"arguments": {"id": "1"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			out, err := b.EvaluateMappingTemplate(tt.template, tt.contextJSON)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, out)
		})
	}
}

func TestHandler_EvaluateMappingTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "valid_template",
			body: map[string]any{
				"template": `{"version": "2017-02-28", "payload": {}}`,
				"context":  "",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_body",
			body:       "not-json-string",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, http.MethodPost, "/v1/dataplane-evaluations/template", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---- EvaluateCode (backend + HTTP) ----

func TestBackend_EvaluateCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		code    string
		wantErr bool
	}{
		{
			name: "valid_code",
			code: `export function request(ctx) { return {}; }`,
		},
		{
			name:    "empty_code",
			code:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			out, err := b.EvaluateCode(tt.code, "", "", "")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, out)
		})
	}
}

func TestHandler_EvaluateCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "valid_code",
			body: map[string]any{
				"code":    `export function request(ctx) { return {}; }`,
				"context": "",
				"runtime": map[string]any{"name": "APPSYNC_JS"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_body",
			body:       "not-json-string",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, http.MethodPost, "/v1/dataplane-evaluations/code", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---- StartDataSourceIntrospection (backend + HTTP) ----

func TestBackend_StartDataSourceIntrospection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		dataSourceName string
		setupAPIID     bool
		setupDS        bool
		wantErr        bool
	}{
		{
			name:           "success",
			setupAPIID:     true,
			setupDS:        true,
			dataSourceName: "MyDS",
		},
		{
			name:       "api_not_found",
			setupAPIID: false,
			setupDS:    false,
			wantErr:    true,
		},
		{
			name:           "datasource_not_found",
			setupAPIID:     true,
			setupDS:        false,
			dataSourceName: "NoSuchDS",
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			apiID := "nonexistent"

			if tt.setupAPIID {
				api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)
				apiID = api.APIID
			}

			if tt.setupDS {
				_, err := b.CreateDataSource(apiID, &appsync.DataSource{
					Name: tt.dataSourceName,
					Type: appsync.DataSourceTypeNone,
				})
				require.NoError(t, err)
			}

			id, err := b.StartDataSourceIntrospection(apiID, tt.dataSourceName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, id)
		})
	}
}

func TestHandler_DataSourceIntrospections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantStatus int
		setupAPI   bool
		setupDS    bool
	}{
		{
			name:   "start_introspection_success",
			method: http.MethodPost,
			path:   "/v1/dataSource-introspections",
			body: map[string]any{
				"apiId":          "__APIID__",
				"dataSourceName": "MyDS",
			},
			setupAPI:   true,
			setupDS:    true,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "start_introspection_bad_body",
			method:     http.MethodPost,
			path:       "/v1/dataSource-introspections",
			body:       "not-json-string",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "start_introspection_api_not_found",
			method:     http.MethodPost,
			path:       "/v1/dataSource-introspections",
			body:       map[string]any{"apiId": "noexist", "dataSourceName": "DS"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "get_introspection_success",
			method:     http.MethodGet,
			path:       "/v1/dataSource-introspections/some-id",
			wantStatus: http.StatusOK,
		},
		{
			name:       "method_not_allowed_on_collection",
			method:     http.MethodGet,
			path:       "/v1/dataSource-introspections",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "method_not_allowed_on_item",
			method:     http.MethodPost,
			path:       "/v1/dataSource-introspections/some-id",
			wantStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()

			body := tt.body

			if tt.setupAPI {
				api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)

				if m, ok := body.(map[string]any); ok && m["apiId"] == "__APIID__" {
					m["apiId"] = api.APIID
				}

				if tt.setupDS {
					_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
						Name: "MyDS",
						Type: appsync.DataSourceTypeNone,
					})
					require.NoError(t, err)
				}
			}

			rec := doRequest(t, h, tt.method, tt.path, body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---- GetDataSourceIntrospection (backend) ----

func TestBackend_GetDataSourceIntrospection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		introspectionID string
		wantErr         bool
	}{
		{
			name:            "valid_id",
			introspectionID: "abc123",
		},
		{
			name:            "empty_id",
			introspectionID: "",
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			result, err := b.GetDataSourceIntrospection(tt.introspectionID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.introspectionID, result.IntrospectionID)
			assert.Equal(t, "SUCCESS", result.Status)
		})
	}
}

// ---- StartSchemaMerge (backend + HTTP) ----

func TestBackend_StartSchemaMerge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus appsync.SchemaStatus
		createAPI  bool
		wantErr    bool
	}{
		{
			name:       "success",
			createAPI:  true,
			wantStatus: appsync.SchemaStatusActive,
		},
		{
			name:      "api_not_found",
			createAPI: false,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			apiID := "nonexistent"

			if tt.createAPI {
				api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)
				apiID = api.APIID
			}

			status, err := b.StartSchemaMerge(apiID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, status)
		})
	}
}

func TestHandler_SchemaMerge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		createAPI  bool
		wantStatus int
	}{
		{
			name:       "post_success",
			method:     http.MethodPost,
			createAPI:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "api_not_found",
			method:     http.MethodPost,
			createAPI:  false,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "method_not_allowed",
			method:     http.MethodGet,
			createAPI:  true,
			wantStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			apiID := "nonexistent"

			if tt.createAPI {
				api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)
				apiID = api.APIID
			}

			rec := doRequest(t, h, tt.method, "/v1/apis/"+apiID+"/schemaMerge", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---- UpdateSourceAPIAssociation (backend + HTTP) ----

func TestBackend_UpdateSourceAPIAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		assocID     string
		mergedAPIID string
		description string
		wantDescr   string
		createAssoc bool
		wantErr     bool
	}{
		{
			name:        "success",
			createAssoc: true,
			description: "updated description",
			wantDescr:   "updated description",
		},
		{
			name:        "not_found",
			createAssoc: false,
			assocID:     "nonexistent",
			mergedAPIID: "nomerge",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			assocID := tt.assocID
			mergedAPIID := tt.mergedAPIID

			if tt.createAssoc {
				merged, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
				require.NoError(t, err)
				source, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)

				assoc, err := b.AssociateSourceGraphqlAPI(merged.APIID, source.APIID, "initial", "")
				require.NoError(t, err)
				assocID = assoc.AssociationID
				mergedAPIID = merged.APIID
			}

			result, err := b.UpdateSourceAPIAssociation(mergedAPIID, assocID, tt.description)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantDescr, result.Description)
		})
	}
}

func TestHandler_UpdateSourceAPIAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        any
		name        string
		wantStatus  int
		createAssoc bool
		useWrongID  bool
	}{
		{
			name:        "success",
			createAssoc: true,
			body:        map[string]any{"description": "new desc"},
			wantStatus:  http.StatusOK,
		},
		{
			name:        "not_found",
			createAssoc: false,
			useWrongID:  true,
			body:        map[string]any{"description": "desc"},
			wantStatus:  http.StatusNotFound,
		},
		{
			name:        "bad_body",
			createAssoc: true,
			body:        "not-json-string",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			mergedAPIID := "nomatch"
			assocID := "noassoc"

			if tt.createAssoc {
				merged, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
				require.NoError(t, err)
				source, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)

				assoc, err := b.AssociateSourceGraphqlAPI(merged.APIID, source.APIID, "initial", "")
				require.NoError(t, err)

				if !tt.useWrongID {
					assocID = assoc.AssociationID
					mergedAPIID = merged.APIID
				}
			}

			path := fmt.Sprintf("/v1/mergedApis/%s/sourceApiAssociations/%s", mergedAPIID, assocID)
			rec := doRequest(t, h, http.MethodPut, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---- ListTypesByAssociation (backend + HTTP) ----

func TestBackend_ListTypesByAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createAssoc bool
		createTypes bool
		wantErr     bool
		wantCount   int
	}{
		{
			name:        "success_no_types",
			createAssoc: true,
			createTypes: false,
			wantCount:   0,
		},
		{
			name:        "success_with_types",
			createAssoc: true,
			createTypes: true,
			wantCount:   1,
		},
		{
			name:        "api_not_found",
			createAssoc: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			mergedAPIID := "nonexistent"
			assocID := "noassoc"

			if tt.createAssoc {
				merged, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
				require.NoError(t, err)
				source, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)

				assoc, err := b.AssociateSourceGraphqlAPI(merged.APIID, source.APIID, "desc", "")
				require.NoError(t, err)
				mergedAPIID = merged.APIID
				assocID = assoc.AssociationID

				if tt.createTypes {
					_, err = b.CreateType(merged.APIID, "type Query { hello: String }", appsync.TypeFormatSDL)
					require.NoError(t, err)
				}
			}

			types, err := b.ListTypesByAssociation(mergedAPIID, assocID, "SDL")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, types, tt.wantCount)
		})
	}
}

func TestHandler_ListTypesByAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createAssoc bool
		wantStatus  int
	}{
		{
			name:        "success",
			createAssoc: true,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "not_found",
			createAssoc: false,
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			mergedAPIID := "nomatch"
			assocID := "noassoc"

			if tt.createAssoc {
				merged, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
				require.NoError(t, err)
				source, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)

				assoc, err := b.AssociateSourceGraphqlAPI(merged.APIID, source.APIID, "desc", "")
				require.NoError(t, err)
				mergedAPIID = merged.APIID
				assocID = assoc.AssociationID
			}

			path := fmt.Sprintf("/v1/mergedApis/%s/sourceApiAssociations/%s/types", mergedAPIID, assocID)
			rec := doRequest(t, h, http.MethodGet, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---- parseOperation coverage for new routes ----

func TestParseOperation_DataSourceIntrospections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "start_introspection",
			method: http.MethodPost,
			path:   "/v1/dataSource-introspections",
			wantOp: "StartDataSourceIntrospection",
		},
		{
			name:   "get_introspection",
			method: http.MethodGet,
			path:   "/v1/dataSource-introspections/abc123",
			wantOp: "GetDataSourceIntrospection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, tt.method, tt.path, nil)
			// The operation header is used by metrics; we only validate the handler was reached.
			_ = rec
		})
	}
}

func TestParseOperation_DataplaneEvaluations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{
			name:   "evaluate_template",
			method: http.MethodPost,
			path:   "/v1/dataplane-evaluations/template",
		},
		{
			name:   "evaluate_code",
			method: http.MethodPost,
			path:   "/v1/dataplane-evaluations/code",
		},
		{
			name:   "unknown_subpath",
			method: http.MethodPost,
			path:   "/v1/dataplane-evaluations/unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			// Just verify the handler is reached (no panic).
			_ = doRequest(t, h, tt.method, tt.path, map[string]any{"template": "x", "code": "x"})
		})
	}
}

func TestParseOperation_V2APIsItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		method     string
		wantStatus int
	}{
		{
			name:       "get_api",
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_api",
			method:     http.MethodDelete,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "update_api_put",
			method:     http.MethodPut,
			wantStatus: http.StatusNotFound,
			body:       map[string]any{"name": "new-name"},
		},
		{
			name:       "update_api_patch",
			method:     http.MethodPatch,
			wantStatus: http.StatusNotFound,
			body:       map[string]any{"name": "new-name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			r := doV2Request(t, h, tt.method, "/v2/apis/nonexistent-api-id", tt.body)
			assert.Equal(t, tt.wantStatus, r.Code)
		})
	}
}

func TestParseOperation_V2APIsNamedResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "get_channel_namespace",
			method:     http.MethodGet,
			path:       "/v2/apis/some-api/channelNamespaces/some-ns",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_channel_namespace",
			method:     http.MethodDelete,
			path:       "/v2/apis/some-api/channelNamespaces/some-ns",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "update_channel_namespace_put",
			method:     http.MethodPut,
			path:       "/v2/apis/some-api/channelNamespaces/some-ns",
			body:       map[string]any{"codeHandlers": ""},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "update_channel_namespace_patch",
			method:     http.MethodPatch,
			path:       "/v2/apis/some-api/channelNamespaces/some-ns",
			body:       map[string]any{"codeHandlers": ""},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "unknown_named_resource",
			method:     http.MethodGet,
			path:       "/v2/apis/some-api/unknown/some-name",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doV2Request(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestParseOperation_SubTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		wantStatus int
	}{
		{
			name:       "tag_resource",
			method:     http.MethodPost,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "list_tags",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
		},
		{
			name:       "untag_resource",
			method:     http.MethodDelete,
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			path := fmt.Sprintf("/v1/apis/%s/tags", api.APIID)

			var body any
			switch tt.method {
			case http.MethodPost:
				body = map[string]any{"tags": map[string]string{"key": "value"}}
			case http.MethodDelete:
				path += "?tagKeys=key"
			}

			rec := doRequest(t, h, tt.method, path, body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// doV2Request performs a request with the appsync User-Agent set so the v2 router matches.
func doV2Request(t *testing.T, handler *appsync.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var buf *bytes.Buffer
	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewBuffer(b)
	} else {
		buf = bytes.NewBuffer(nil)
	}

	req := httptest.NewRequest(method, path, buf)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "aws-sdk-go-v2/1.0 api/appsync/1.53.5")

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := handler.Handler()(c)
	require.NoError(t, err)

	return rec
}
