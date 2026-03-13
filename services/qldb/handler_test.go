package qldb_test

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
	"github.com/blackbirdworks/gopherstack/services/qldb"
)

func newTestHandler(t *testing.T) *qldb.Handler {
	t.Helper()

	return qldb.NewHandler(qldb.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doQLDBRequest(t *testing.T, h *qldb.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	return doQLDBRawRequest(t, h, method, path, bodyBytes)
}

func doQLDBRawRequest(t *testing.T, h *qldb.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/qldb/aws4_request")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "QLDB", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateLedger")
	assert.Contains(t, ops, "DescribeLedger")
	assert.Contains(t, ops, "ListLedgers")
	assert.Contains(t, ops, "UpdateLedger")
	assert.Contains(t, ops, "DeleteLedger")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "UntagResource")
	assert.Contains(t, ops, "ListTagsForResource")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 87, h.MatchPriority())
}

func TestHandler_CreateLedger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		bodyRaw    []byte
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"Name":            "my-ledger",
				"PermissionsMode": "ALLOW_ALL",
			},
			wantStatus: http.StatusOK,
			wantBody:   "my-ledger",
		},
		{
			name: "with_deletion_protection",
			body: map[string]any{
				"Name":               "protected-ledger",
				"PermissionsMode":    "ALLOW_ALL",
				"DeletionProtection": true,
			},
			wantStatus: http.StatusOK,
			wantBody:   "protected-ledger",
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_name",
			body:       map[string]any{"PermissionsMode": "ALLOW_ALL"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.bodyRaw != nil {
				rec = doQLDBRawRequest(t, h, http.MethodPost, "/ledgers", tt.bodyRaw)
			} else {
				rec = doQLDBRequest(t, h, http.MethodPost, "/ledgers", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_DescribeLedger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ledger     string
		wantBody   string
		wantStatus int
		create     bool
	}{
		{
			name:       "existing_ledger",
			ledger:     "describe-ledger",
			create:     true,
			wantStatus: http.StatusOK,
			wantBody:   "describe-ledger",
		},
		{
			name:       "not_found",
			ledger:     "missing-ledger",
			create:     false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.create {
				doQLDBRequest(t, h, http.MethodPost, "/ledgers", map[string]any{
					"Name":            tt.ledger,
					"PermissionsMode": "ALLOW_ALL",
				})
			}

			rec := doQLDBRequest(t, h, http.MethodGet, "/ledgers/"+tt.ledger, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_ListLedgers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doQLDBRequest(t, h, http.MethodPost, "/ledgers", map[string]any{
		"Name":            "list-ledger-1",
		"PermissionsMode": "ALLOW_ALL",
	})

	rec := doQLDBRequest(t, h, http.MethodGet, "/ledgers", nil)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Ledgers")
	assert.Contains(t, rec.Body.String(), "list-ledger-1")
}

func TestHandler_UpdateLedger(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doQLDBRequest(t, h, http.MethodPost, "/ledgers", map[string]any{
		"Name":            "update-ledger",
		"PermissionsMode": "ALLOW_ALL",
	})

	rec := doQLDBRequest(t, h, http.MethodPatch, "/ledgers/update-ledger", map[string]any{
		"DeletionProtection": true,
	})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "update-ledger")
}

func TestHandler_DeleteLedger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ledger     string
		wantStatus int
		create     bool
		protect    bool
	}{
		{
			name:       "success",
			ledger:     "del-ledger",
			create:     true,
			protect:    false,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			ledger:     "missing-ledger",
			create:     false,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "deletion_protected",
			ledger:     "protected-del-ledger",
			create:     true,
			protect:    true,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.create {
				doQLDBRequest(t, h, http.MethodPost, "/ledgers", map[string]any{
					"Name":               tt.ledger,
					"PermissionsMode":    "ALLOW_ALL",
					"DeletionProtection": tt.protect,
				})
			}

			rec := doQLDBRequest(t, h, http.MethodDelete, "/ledgers/"+tt.ledger, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DuplicateLedger(t *testing.T) {
	t.Parallel()

	b := qldb.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateLedger("dupe-ledger", "ALLOW_ALL", false, nil)
	require.NoError(t, err)

	_, err = b.CreateLedger("dupe-ledger", "ALLOW_ALL", false, nil)
	require.Error(t, err)
}

// createTestLedgerWithARN creates a ledger via the HTTP handler and returns its ARN.
func createTestLedgerWithARN(t *testing.T, h *qldb.Handler, ledgerName string) string {
	t.Helper()

	rec := doQLDBRequest(t, h, http.MethodPost, "/ledgers", map[string]any{
		"Name":            ledgerName,
		"PermissionsMode": "ALLOW_ALL",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["Arn"].(string)
}

// addTagsToLedger tags a ledger via the HTTP handler.
func addTagsToLedger(t *testing.T, h *qldb.Handler, arn string, tags map[string]string) {
	t.Helper()

	rec := doQLDBRequest(t, h, http.MethodPost, "/tags/"+arn, map[string]any{"Tags": tags})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *qldb.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "tag_resource",
			setup: func(t *testing.T, h *qldb.Handler) string {
				t.Helper()

				return createTestLedgerWithARN(t, h, "tag-test-ledger")
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_arn",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var arn string
			if tt.setup != nil {
				arn = tt.setup(t, h)
			}

			rec := doQLDBRequest(t, h, http.MethodPost, "/tags/"+arn,
				map[string]any{"Tags": map[string]string{"env": "test"}})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *qldb.Handler) string
		name       string
		wantTag    string
		wantStatus int
	}{
		{
			name: "list_tags",
			setup: func(t *testing.T, h *qldb.Handler) string {
				t.Helper()
				arn := createTestLedgerWithARN(t, h, "list-tags-ledger")
				addTagsToLedger(t, h, arn, map[string]string{"env": "prod"})

				return arn
			},
			wantStatus: http.StatusOK,
			wantTag:    "prod",
		},
		{
			name:       "missing_arn",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var arn string
			if tt.setup != nil {
				arn = tt.setup(t, h)
			}

			rec := doQLDBRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantTag != "" {
				assert.Contains(t, rec.Body.String(), tt.wantTag)
			}
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *qldb.Handler) string
		tagKeys    string
		wantStatus int
	}{
		{
			name: "untag_specific_key",
			setup: func(t *testing.T, h *qldb.Handler) string {
				t.Helper()
				arn := createTestLedgerWithARN(t, h, "untag-ledger")
				addTagsToLedger(t, h, arn, map[string]string{"env": "test", "team": "platform"})

				return arn
			},
			tagKeys:    "?tagKeys=env",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_arn",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var arn string
			if tt.setup != nil {
				arn = tt.setup(t, h)
			}

			rec := doQLDBRequest(t, h, http.MethodDelete, "/tags/"+arn+tt.tagKeys, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestBackend_TagResource(t *testing.T) {
	t.Parallel()

	b := qldb.NewInMemoryBackend("000000000000", "us-east-1")

	l, err := b.CreateLedger("tag-ledger", "ALLOW_ALL", false, nil)
	require.NoError(t, err)

	err = b.TagResource(l.ARN, map[string]string{"env": "test"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(l.ARN)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
}

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	b := qldb.NewInMemoryBackend("000000000000", "eu-west-1")
	assert.Equal(t, "eu-west-1", b.Region())
}

func TestHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "qldb", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		path        string
		authService string
		want        bool
	}{
		{
			name:        "matches_ledgers_path",
			path:        "/ledgers",
			authService: "qldb",
			want:        true,
		},
		{
			name:        "matches_ledger_by_name",
			path:        "/ledgers/my-ledger",
			authService: "qldb",
			want:        true,
		},
		{
			name:        "matches_tags_path",
			path:        "/tags/arn:aws:qldb:us-east-1:000000000000:ledger/test",
			authService: "qldb",
			want:        true,
		},
		{
			name:        "no_match_wrong_service",
			path:        "/ledgers",
			authService: "s3",
			want:        false,
		},
		{
			name:        "no_match_unrelated_path",
			path:        "/api/v1/other",
			authService: "qldb",
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)

			if tt.authService != "" {
				req.Header.Set("Authorization",
					"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/"+tt.authService+"/aws4_request")
			}

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		wantRes string
	}{
		{
			name:    "ledger_name_extracted",
			path:    "/ledgers/my-ledger",
			wantRes: "my-ledger",
		},
		{
			name:    "only_ledgers_path_returns_empty",
			path:    "/ledgers",
			wantRes: "",
		},
		{
			name:    "tags_path_returns_empty",
			path:    "/tags/some-arn",
			wantRes: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantRes, h.ExtractResource(c))
		})
	}
}

func TestHandler_UpdateLedger_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// "/ledgers/" has a trailing slash producing an empty name segment,
	// which triggers the missing-name validation path.
	rec := doQLDBRequest(t, h, http.MethodPatch, "/ledgers/", map[string]any{
		"DeletionProtection": false,
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateLedger_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doQLDBRequest(t, h, http.MethodPost, "/ledgers", map[string]any{
		"Name":            "upd-invalid-json",
		"PermissionsMode": "ALLOW_ALL",
	})

	rec := doQLDBRawRequest(t, h, http.MethodPatch, "/ledgers/upd-invalid-json", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteLedger_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doQLDBRequest(t, h, http.MethodDelete, "/ledgers/", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DescribeLedger_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doQLDBRequest(t, h, http.MethodGet, "/ledgers/", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DuplicateLedger_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doQLDBRequest(t, h, http.MethodPost, "/ledgers", map[string]any{
		"Name":            "conflict-ledger",
		"PermissionsMode": "ALLOW_ALL",
	})

	rec := doQLDBRequest(t, h, http.MethodPost, "/ledgers", map[string]any{
		"Name":            "conflict-ledger",
		"PermissionsMode": "ALLOW_ALL",
	})

	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &qldb.Provider{}
	assert.Equal(t, "QLDB", p.Name())

	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}
