package glacier_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

func newTestHandler() *glacier.Handler {
	bk := glacier.NewInMemoryBackend()
	// Disable the simulated asynchronous retrieval window so handler tests that
	// initiate a job and immediately read its output remain deterministic. The
	// async lifecycle itself is covered by dedicated tests in backend_test.go.
	glacier.SetRetrievalDelay(bk, 0)
	h := glacier.NewHandler(bk)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}

func doRequest(t *testing.T, h *glacier.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	var req *http.Request

	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func createVault(t *testing.T, h *glacier.Handler, vaultName string) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPut, "/"+testAccountID+"/vaults/"+vaultName, "")
	require.Equal(t, http.StatusCreated, rec.Code, "createVault %q: %s", vaultName, rec.Body.String())
}

// doRequestWithBody performs an HTTP request with a body and optional headers.

func doRequestWithHeaders(
	t *testing.T,
	h *glacier.Handler,
	method, path, body string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// ----------------------------------------
// InitiateMultipartUpload
// ----------------------------------------

func doRequestWithBody(
	t *testing.T,
	h *glacier.Handler,
	method, path, body string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestRouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		path  string
		match bool
	}{
		{
			name:  "vaults_root",
			path:  "/-/vaults",
			match: true,
		},
		{
			name:  "vault_name",
			path:  "/-/vaults/my-vault",
			match: true,
		},
		{
			name:  "archives",
			path:  "/-/vaults/my-vault/archives",
			match: true,
		},
		{
			name:  "jobs",
			path:  "/-/vaults/my-vault/jobs/jobId",
			match: true,
		},
		{
			name:  "policies",
			path:  "/-/policies/data-retrieval",
			match: true,
		},
		{
			name:  "s3_bucket",
			path:  "/my-bucket",
			match: false,
		},
		{
			name:  "fis_path",
			path:  "/experimentTemplates",
			match: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, http.NoBody)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.match, matcher(c))
		})
	}
}

func TestHandlerReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createVaults int
		wantAfter    int
	}{
		{
			name:         "reset clears all vaults",
			createVaults: 3,
			wantAfter:    0,
		},
		{
			name:         "reset on empty backend is a no-op",
			createVaults: 0,
			wantAfter:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			for i := range tt.createVaults {
				rec := doRequest(t, h, http.MethodPut, "/-/vaults/vault-"+strconv.Itoa(i), "")
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			h.Reset()

			rec := doRequest(t, h, http.MethodGet, "/-/vaults", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				VaultList []any `json:"VaultList"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.VaultList, tt.wantAfter)
		})
	}
}

// TestHandlerReset_DelegatesToBackend verifies that Handler.Reset() delegates to the backend.
func TestHandlerReset_DelegatesToBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantVault int
	}{
		{name: "handler_reset_clears_backend", wantVault: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			h := glacier.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			_, err := b.CreateVault(testAccountID, testRegion, "myvault")
			require.NoError(t, err)
			assert.Equal(t, 1, glacier.VaultCount(b))

			h.Reset()

			assert.Equal(t, tt.wantVault, glacier.VaultCount(b))
		})
	}
}

func TestHandlerReset_ClearsArchiveCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "archive_data_cleared_on_reset"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "cache-reset-vault")
			uploadArchiveData(t, h, "cache-reset-vault", []byte("secret data"))

			// Reset clears everything.
			h.Reset()

			// After reset: vault no longer exists.
			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/cache-reset-vault", "", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code, tt.name)
		})
	}
}

// TestGetSupportedOperations_AllOps verifies all 33 operations are present.
func TestGetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	wantOps := []string{
		"CreateVault", "DescribeVault", "DeleteVault", "ListVaults",
		"UploadArchive", "DeleteArchive",
		"InitiateJob", "DescribeJob", "ListJobs", "GetJobOutput",
		"SetVaultNotifications", "GetVaultNotifications", "DeleteVaultNotifications",
		"SetVaultAccessPolicy", "GetVaultAccessPolicy", "DeleteVaultAccessPolicy",
		"AddTagsToVault", "ListTagsForVault", "RemoveTagsFromVault",
		"InitiateVaultLock", "AbortVaultLock", "CompleteVaultLock", "GetVaultLock",
		"GetDataRetrievalPolicy", "SetDataRetrievalPolicy",
		"InitiateMultipartUpload", "UploadMultipartPart", "CompleteMultipartUpload",
		"AbortMultipartUpload", "ListMultipartUploads", "ListParts",
		"ListProvisionedCapacity", "PurchaseProvisionedCapacity",
	}

	tests := []struct {
		name    string
		wantLen int
	}{
		{name: "all_33_ops", wantLen: 33},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ops := h.GetSupportedOperations()

			assert.Len(t, ops, tt.wantLen)

			opSet := make(map[string]struct{}, len(ops))
			for _, op := range ops {
				opSet[op] = struct{}{}
			}

			for _, want := range wantOps {
				assert.Contains(t, opSet, want, "missing operation: %s", want)
			}
		})
	}
}

func TestGetSupportedOperations_Count(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantAtLeast int
	}{
		{name: "all_ops_present", wantAtLeast: 30},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ops := h.GetSupportedOperations()
			assert.GreaterOrEqual(t, len(ops), tt.wantAtLeast)
		})
	}
}

func TestGetSupportedOperations_IncludesNewOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
	}{
		{name: "InitiateMultipartUpload", op: "InitiateMultipartUpload"},
		{name: "UploadMultipartPart", op: "UploadMultipartPart"},
		{name: "CompleteMultipartUpload", op: "CompleteMultipartUpload"},
		{name: "AbortMultipartUpload", op: "AbortMultipartUpload"},
		{name: "ListMultipartUploads", op: "ListMultipartUploads"},
		{name: "ListParts", op: "ListParts"},
		{name: "GetVaultLock", op: "GetVaultLock"},
		{name: "ListProvisionedCapacity", op: "ListProvisionedCapacity"},
		{name: "PurchaseProvisionedCapacity", op: "PurchaseProvisionedCapacity"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ops := h.GetSupportedOperations()
			assert.Contains(t, ops, tt.op)
		})
	}
}

// ----------------------------------------
// Handler metadata
// ----------------------------------------

func TestMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(h *glacier.Handler)
		name  string
	}{
		{
			name:  "name",
			check: func(h *glacier.Handler) { assert.Equal(t, "Glacier", h.Name()) },
		},
		{
			name:  "chaos_service_name",
			check: func(h *glacier.Handler) { assert.Equal(t, "glacier", h.ChaosServiceName()) },
		},
		{
			name:  "chaos_operations",
			check: func(h *glacier.Handler) { assert.NotEmpty(t, h.ChaosOperations()) },
		},
		{
			name:  "chaos_regions",
			check: func(h *glacier.Handler) { assert.NotEmpty(t, h.ChaosRegions()) },
		},
		{
			name:  "match_priority",
			check: func(h *glacier.Handler) { assert.NotZero(t, h.MatchPriority()) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			tt.check(h)
		})
	}
}

// ----------------------------------------
// GetDataRetrievalPolicy / SetDataRetrievalPolicy
// ----------------------------------------

func TestExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "list_vaults",
			method: http.MethodGet,
			path:   "/-/vaults",
			wantOp: "ListVaults",
		},
		{
			name:   "create_vault",
			method: http.MethodPut,
			path:   "/-/vaults/my-vault",
			wantOp: "CreateVault",
		},
		{
			name:   "list_provisioned_capacity",
			method: http.MethodGet,
			path:   "/-/provisioned-capacity",
			wantOp: "ListProvisionedCapacity",
		},
		{
			name:   "purchase_provisioned_capacity",
			method: http.MethodPost,
			path:   "/-/provisioned-capacity",
			wantOp: "PurchaseProvisionedCapacity",
		},
		{
			name:   "list_multipart_uploads",
			method: http.MethodGet,
			path:   "/-/vaults/my-vault/multipart-uploads",
			wantOp: "ListMultipartUploads",
		},
		{
			name:   "get_vault_lock",
			method: http.MethodGet,
			path:   "/-/vaults/my-vault/lock-policy",
			wantOp: "GetVaultLock",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, http.NoBody)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			op := h.ExtractOperation(c)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		path         string
		wantResource string
	}{
		{
			name:         "vault_name",
			path:         "/-/vaults/my-vault",
			wantResource: "my-vault",
		},
		{
			name:         "no_vault",
			path:         "/-/vaults",
			wantResource: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, http.NoBody)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			resource := h.ExtractResource(c)
			assert.Equal(t, tt.wantResource, resource)
		})
	}
}

// ----------------------------------------
// Abort-then-check removes the upload
// ----------------------------------------

func TestXAmznRequestid_PresentOnEveryResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "reqid-vault")

	paths := []struct {
		method, path, body string
	}{
		{http.MethodPut, "/-/vaults/another-vault", ""},
		{http.MethodGet, "/-/vaults", ""},
		{http.MethodGet, "/-/vaults/reqid-vault", ""},
		{http.MethodDelete, "/-/vaults/reqid-vault", ""},
		{http.MethodGet, "/-/policies/data-retrieval", ""},
		{http.MethodGet, "/-/provisioned-capacity", ""},
	}

	for _, p := range paths {
		t.Run(p.method+"_"+p.path, func(t *testing.T) {
			t.Parallel()

			h2 := newTestHandler()
			createVault(t, h2, "reqid-vault")

			rec := doRequest(t, h2, p.method, p.path, p.body)
			assert.NotEmpty(t, rec.Header().Get("X-Amzn-Requestid"),
				"X-Amzn-Requestid missing for %s %s", p.method, p.path)
		})
	}
}

func TestErrorResponse_IncludesTypeField(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Trigger a 404 error by requesting non-existent vault.
	rec := doRequest(t, h, http.MethodGet, "/-/vaults/nonexistent", "")
	require.Equal(t, http.StatusNotFound, rec.Code)

	var errBody map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errBody))

	assert.NotEmpty(t, errBody["code"], "error body should have code field")
	assert.NotEmpty(t, errBody["__type"], "error body should have __type field")
	assert.Equal(t, errBody["code"], errBody["__type"], "__type should match code")
}

func TestErrorResponse_RequestIDUnique(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	ids := make(map[string]bool)

	for range 10 {
		rec := doRequest(t, h, http.MethodGet, "/-/vaults/nonexistent", "")
		id := rec.Header().Get("X-Amzn-Requestid")
		require.NotEmpty(t, id, "X-Amzn-Requestid should always be set")
		ids[id] = true
	}

	// All 10 request IDs should be unique.
	assert.Len(t, ids, 10, "each request should have a unique X-Amzn-Requestid")
}

// -------------------------------------------------------------------------
// Issue 29: CompleteVaultLock clears ExpirationDate
// -------------------------------------------------------------------------

func TestErrorResponse_FormatFields(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over minimal padding
		name        string
		method      string
		path        string
		wantStatus  int
		wantCodeKey string
	}{
		{
			name:        "vault_not_found_has_type_field",
			method:      http.MethodGet,
			path:        "/" + testAccountID + "/vaults/does-not-exist",
			wantStatus:  http.StatusNotFound,
			wantCodeKey: "ResourceNotFoundException",
		},
		{
			name:        "archive_not_found_has_type_field",
			method:      http.MethodDelete,
			path:        "/" + testAccountID + "/vaults/does-not-exist/archives/fake-id",
			wantStatus:  http.StatusNotFound,
			wantCodeKey: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequestWithHeaders(t, h, tt.method, tt.path, "", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			// Both "code" and "__type" must be present (SDK compatibility).
			assert.Equal(t, tt.wantCodeKey, errResp["code"])
			assert.Equal(t, tt.wantCodeKey, errResp["__type"])
			assert.NotEmpty(t, errResp["message"])
		})
	}
}

func TestErrorResponse_XAmznRequestID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "create_vault", method: http.MethodPut, path: "/" + testAccountID + "/vaults/req-id-vault"},
		{name: "list_vaults", method: http.MethodGet, path: "/" + testAccountID + "/vaults"},
		{name: "not_found", method: http.MethodGet, path: "/" + testAccountID + "/vaults/nosuchvault"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequestWithHeaders(t, h, tt.method, tt.path, "", nil)
			reqID := rec.Header().Get("X-Amzn-Requestid")
			assert.NotEmpty(t, reqID, "X-Amzn-Requestid must be present: %s", tt.name)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 15. GetJobOutput range header for inventory
// ─────────────────────────────────────────────────────────────────────────────

func TestPagination_MarkerNotFound_EmptyList(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over minimal padding
		name    string
		method  string
		path    string
		jsonKey string
		setupFn func(h *glacier.Handler)
	}{
		{
			name:    "list_vaults_unknown_marker",
			method:  http.MethodGet,
			path:    "/" + testAccountID + "/vaults?marker=nonexistent",
			jsonKey: "VaultList",
			setupFn: func(h *glacier.Handler) { createVault(t, h, "pag-vault") },
		},
		{
			name:    "list_jobs_unknown_marker",
			method:  http.MethodGet,
			path:    "/" + testAccountID + "/vaults/pagvault/jobs?marker=unknown-job-id",
			jsonKey: "JobList",
			setupFn: func(h *glacier.Handler) {
				createVault(t, h, "pagvault")
				initiateJobWithBody(t, h, "pagvault", `{"Type":"inventory-retrieval"}`)
			},
		},
		{
			name:    "list_multipart_uploads_unknown_marker",
			method:  http.MethodGet,
			path:    "/" + testAccountID + "/vaults/pagmpvault/multipart-uploads?marker=unknown",
			jsonKey: "UploadsList",
			setupFn: func(h *glacier.Handler) {
				createVault(t, h, "pagmpvault")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			tt.setupFn(h)

			rec := doRequestWithHeaders(t, h, tt.method, tt.path, "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			raw, ok := resp[tt.jsonKey]
			assert.True(t, ok, "%s key must be present", tt.jsonKey)
			assert.Equal(t, "[]", string(raw), "%s must be [] not null when marker not found", tt.jsonKey)
		})
	}
}
