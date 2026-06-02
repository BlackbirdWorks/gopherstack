package glacier_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

// doRequestWithHeaders performs an HTTP request with custom headers against the handler.
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

func TestHandler_InitiateMultipartUpload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vaultName    string
		description  string
		partSize     string
		wantStatus   int
		wantUploadID bool
	}{
		{
			name:         "success_with_description",
			vaultName:    "mp-vault",
			description:  "my backup",
			partSize:     "4194304",
			wantStatus:   http.StatusCreated,
			wantUploadID: true,
		},
		{
			name:         "missing_part_size_rejected",
			vaultName:    "mp-vault2",
			description:  "",
			partSize:     "",
			wantStatus:   http.StatusBadRequest,
			wantUploadID: false,
		},
		{
			name:       "vault_not_found",
			vaultName:  "nonexistent",
			partSize:   "1048576",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.wantStatus != http.StatusNotFound {
				rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName, "")
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			headers := map[string]string{}
			if tt.description != "" {
				headers["X-Amz-Archive-Description"] = tt.description
			}

			if tt.partSize != "" {
				headers["X-Amz-Part-Size"] = tt.partSize
			}

			rec := doRequestWithHeaders(
				t,
				h,
				http.MethodPost,
				"/-/vaults/"+tt.vaultName+"/multipart-uploads",
				"",
				headers,
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantUploadID {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["uploadId"])

				uploadID := rec.Header().Get("X-Amz-Multipart-Upload-Id")
				assert.NotEmpty(t, uploadID)
			}
		})
	}
}

// ----------------------------------------
// UploadMultipartPart
// ----------------------------------------

func TestHandler_UploadMultipartPart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		uploadID    string
		rangeHdr    string
		checksum    string
		wantStatus  int
		setupUpload bool
	}{
		{
			name:        "success",
			setupUpload: true,
			rangeHdr:    "bytes 0-4194303/*",
			checksum:    "abc123",
			wantStatus:  http.StatusNoContent,
		},
		{
			name:        "upload_not_found",
			setupUpload: false,
			uploadID:    "nonexistent-upload-id",
			rangeHdr:    "bytes 0-4194303/*",
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vaultName := "upload-part-vault"

			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+vaultName, "")
			require.Equal(t, http.StatusCreated, rec.Code)

			uploadID := tt.uploadID

			if tt.setupUpload {
				rec = doRequestWithHeaders(t, h, http.MethodPost, "/-/vaults/"+vaultName+"/multipart-uploads", "", map[string]string{"X-Amz-Part-Size": "1048576"})
				require.Equal(t, http.StatusCreated, rec.Code)
				uploadID = rec.Header().Get("X-Amz-Multipart-Upload-Id")
				require.NotEmpty(t, uploadID)
			}

			headers := map[string]string{
				"Content-Range":          tt.rangeHdr,
				"X-Amz-Sha256-Tree-Hash": tt.checksum,
			}

			rec = doRequestWithHeaders(
				t, h, http.MethodPut,
				"/-/vaults/"+vaultName+"/multipart-uploads/"+uploadID,
				"part-data", headers,
			)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ----------------------------------------
// CompleteMultipartUpload
// ----------------------------------------

func TestHandler_CompleteMultipartUpload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		uploadID    string
		wantStatus  int
		setupUpload bool
		wantArchive bool
	}{
		{
			name:        "success",
			setupUpload: true,
			wantStatus:  http.StatusCreated,
			wantArchive: true,
		},
		{
			name:       "upload_not_found",
			uploadID:   "nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vaultName := "complete-mp-vault"

			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+vaultName, "")
			require.Equal(t, http.StatusCreated, rec.Code)

			uploadID := tt.uploadID

			if tt.setupUpload {
				rec = doRequestWithHeaders(t, h, http.MethodPost, "/-/vaults/"+vaultName+"/multipart-uploads", "", map[string]string{"X-Amz-Part-Size": "1048576"})
				require.Equal(t, http.StatusCreated, rec.Code)
				uploadID = rec.Header().Get("X-Amz-Multipart-Upload-Id")
				require.NotEmpty(t, uploadID)
			}

			headers := map[string]string{
				"X-Amz-Archive-Size":     "8388608",
				"X-Amz-Sha256-Tree-Hash": "deadbeef",
			}

			rec = doRequestWithHeaders(
				t, h, http.MethodPost,
				"/-/vaults/"+vaultName+"/multipart-uploads/"+uploadID,
				"", headers,
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantArchive {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["archiveId"])
				assert.NotEmpty(t, rec.Header().Get("X-Amz-Archive-Id"))
			}
		})
	}
}

// ----------------------------------------
// AbortMultipartUpload
// ----------------------------------------

func TestHandler_AbortMultipartUpload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		uploadID    string
		wantStatus  int
		setupUpload bool
	}{
		{
			name:        "success",
			setupUpload: true,
			wantStatus:  http.StatusNoContent,
		},
		{
			name:       "upload_not_found",
			uploadID:   "nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vaultName := "abort-mp-vault"

			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+vaultName, "")
			require.Equal(t, http.StatusCreated, rec.Code)

			uploadID := tt.uploadID

			if tt.setupUpload {
				rec = doRequestWithHeaders(t, h, http.MethodPost, "/-/vaults/"+vaultName+"/multipart-uploads", "", map[string]string{"X-Amz-Part-Size": "1048576"})
				require.Equal(t, http.StatusCreated, rec.Code)
				uploadID = rec.Header().Get("X-Amz-Multipart-Upload-Id")
				require.NotEmpty(t, uploadID)
			}

			rec = doRequest(t, h, http.MethodDelete, "/-/vaults/"+vaultName+"/multipart-uploads/"+uploadID, "")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ----------------------------------------
// ListMultipartUploads
// ----------------------------------------

func TestHandler_ListMultipartUploads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		numUploads int
		wantCount  int
		wantStatus int
	}{
		{
			name:       "empty_list",
			numUploads: 0,
			wantCount:  0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "two_uploads",
			numUploads: 2,
			wantCount:  2,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vaultName := "list-mp-vault"

			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+vaultName, "")
			require.Equal(t, http.StatusCreated, rec.Code)

			for range tt.numUploads {
				rec = doRequestWithHeaders(t, h, http.MethodPost, "/-/vaults/"+vaultName+"/multipart-uploads", "", map[string]string{"X-Amz-Part-Size": "1048576"})
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+vaultName+"/multipart-uploads", "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp struct {
				UploadsList []any `json:"UploadsList"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.UploadsList, tt.wantCount)
		})
	}
}

// ----------------------------------------
// ListParts
// ----------------------------------------

func TestHandler_ListParts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		uploadID    string
		uploadParts int
		wantStatus  int
		wantParts   int
	}{
		{
			name:        "empty_parts",
			uploadParts: 0,
			wantStatus:  http.StatusOK,
			wantParts:   0,
		},
		{
			name:        "two_parts",
			uploadParts: 2,
			wantStatus:  http.StatusOK,
			wantParts:   2,
		},
		{
			name:       "upload_not_found",
			uploadID:   "nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vaultName := "list-parts-vault"

			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+vaultName, "")
			require.Equal(t, http.StatusCreated, rec.Code)

			uploadID := tt.uploadID

			if tt.wantStatus == http.StatusOK {
				rec = doRequestWithHeaders(t, h, http.MethodPost, "/-/vaults/"+vaultName+"/multipart-uploads", "", map[string]string{"X-Amz-Part-Size": "1048576"})
				require.Equal(t, http.StatusCreated, rec.Code)
				uploadID = rec.Header().Get("X-Amz-Multipart-Upload-Id")
				require.NotEmpty(t, uploadID)

				for i := range tt.uploadParts {
					rangeHdr := "bytes 0-4194303/*"
					if i > 0 {
						rangeHdr = "bytes 4194304-8388607/*"
					}

					headers := map[string]string{
						"Content-Range":          rangeHdr,
						"X-Amz-Sha256-Tree-Hash": "checksum" + string(rune('0'+i)),
					}

					pr := doRequestWithHeaders(
						t, h, http.MethodPut,
						"/-/vaults/"+vaultName+"/multipart-uploads/"+uploadID,
						"part-data", headers,
					)
					require.Equal(t, http.StatusNoContent, pr.Code)
				}
			}

			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+vaultName+"/multipart-uploads/"+uploadID, "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp struct {
					Parts []any `json:"Parts"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Len(t, resp.Parts, tt.wantParts)
			}
		})
	}
}

// ----------------------------------------
// GetVaultLock
// ----------------------------------------

func TestHandler_GetVaultLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantState    string
		wantStatus   int
		initiateLock bool
	}{
		{
			name:       "unlocked_vault",
			wantStatus: http.StatusOK,
			wantState:  "Unlocked",
		},
		{
			name:         "locked_vault",
			initiateLock: true,
			wantStatus:   http.StatusOK,
			wantState:    "InProgress",
		},
		{
			name:       "vault_not_found",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vaultName := "lock-vault"

			if tt.wantStatus != http.StatusNotFound {
				rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+vaultName, "")
				require.Equal(t, http.StatusCreated, rec.Code)
			} else {
				vaultName = "nonexistent-vault"
			}

			if tt.initiateLock {
				rec := doRequest(t, h, http.MethodPost, "/-/vaults/"+vaultName+"/lock-policy", `{"Policy":"{}"}`)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/-/vaults/"+vaultName+"/lock-policy", "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantState, resp["State"])
			}
		})
	}
}

// ----------------------------------------
// ListProvisionedCapacity
// ----------------------------------------

func TestHandler_ListProvisionedCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		purchaseFirst int
		wantStatus    int
		wantCount     int
	}{
		{
			name:       "empty_list",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:          "two_units",
			purchaseFirst: 2,
			wantStatus:    http.StatusOK,
			wantCount:     2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			for range tt.purchaseFirst {
				rec := doRequest(t, h, http.MethodPost, "/-/provisioned-capacity", "")
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/-/provisioned-capacity", "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp struct {
				ProvisionedCapacityList []any `json:"ProvisionedCapacityList"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.ProvisionedCapacityList, tt.wantCount)
		})
	}
}

// ----------------------------------------
// PurchaseProvisionedCapacity
// ----------------------------------------

func TestHandler_PurchaseProvisionedCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doRequest(t, h, http.MethodPost, "/-/provisioned-capacity", "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["capacityId"])
			assert.NotEmpty(t, rec.Header().Get("X-Amz-Capacity-Id"))
		})
	}
}

// ----------------------------------------
// Multipart upload full round-trip
// ----------------------------------------

func TestHandler_MultipartUpload_FullRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
	}{
		{
			name:      "initiate_upload_complete",
			vaultName: "roundtrip-vault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create vault
			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName, "")
			require.Equal(t, http.StatusCreated, rec.Code)

			// Initiate multipart upload
			headers := map[string]string{
				"X-Amz-Archive-Description": "my-backup",
				"X-Amz-Part-Size":           "4194304",
			}
			rec = doRequestWithHeaders(
				t,
				h,
				http.MethodPost,
				"/-/vaults/"+tt.vaultName+"/multipart-uploads",
				"",
				headers,
			)
			require.Equal(t, http.StatusCreated, rec.Code)
			uploadID := rec.Header().Get("X-Amz-Multipart-Upload-Id")
			require.NotEmpty(t, uploadID)

			// Upload a part
			partHeaders := map[string]string{
				"Content-Range":          "bytes 0-4194303/*",
				"X-Amz-Sha256-Tree-Hash": "abcdef",
			}
			rec = doRequestWithHeaders(
				t, h, http.MethodPut,
				"/-/vaults/"+tt.vaultName+"/multipart-uploads/"+uploadID,
				"part-data", partHeaders,
			)
			require.Equal(t, http.StatusNoContent, rec.Code)

			// List parts — should have 1
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/multipart-uploads/"+uploadID, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var partsResp struct {
				Parts []any `json:"Parts"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &partsResp))
			assert.Len(t, partsResp.Parts, 1)

			// List uploads — should have 1
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/multipart-uploads", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var uploadsResp struct {
				UploadsList []any `json:"UploadsList"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &uploadsResp))
			assert.Len(t, uploadsResp.UploadsList, 1)

			// Complete the upload
			completeHeaders := map[string]string{
				"X-Amz-Archive-Size":     "4194304",
				"X-Amz-Sha256-Tree-Hash": "abcdef",
			}
			rec = doRequestWithHeaders(
				t, h, http.MethodPost,
				"/-/vaults/"+tt.vaultName+"/multipart-uploads/"+uploadID,
				"", completeHeaders,
			)
			require.Equal(t, http.StatusCreated, rec.Code)

			var completeResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &completeResp))
			assert.NotEmpty(t, completeResp["archiveId"])

			// After completion, list uploads should be empty
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/multipart-uploads", "")
			require.Equal(t, http.StatusOK, rec.Code)
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &uploadsResp))
			assert.Empty(t, uploadsResp.UploadsList)
		})
	}
}

// ----------------------------------------
// Persistence round-trip for new operations
// ----------------------------------------

func TestHandler_NewOps_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{
			name: "snapshot_restore_preserves_multipart_and_capacity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create vault and initiate multipart upload
			rec := doRequest(t, h, http.MethodPut, "/-/vaults/persist-vault", "")
			require.Equal(t, http.StatusCreated, rec.Code)

			rec = doRequestWithHeaders(t, h, http.MethodPost, "/-/vaults/persist-vault/multipart-uploads", "", map[string]string{"X-Amz-Part-Size": "1048576"})
			require.Equal(t, http.StatusCreated, rec.Code)

			// Purchase provisioned capacity
			rec = doRequest(t, h, http.MethodPost, "/-/provisioned-capacity", "")
			require.Equal(t, http.StatusCreated, rec.Code)

			// Snapshot and restore
			snap := h.Snapshot()
			require.NotEmpty(t, snap)

			h2 := newTestHandler()
			require.NoError(t, h2.Restore(snap))

			// Verify multipart uploads are restored
			rec = doRequest(t, h2, http.MethodGet, "/-/vaults/persist-vault/multipart-uploads", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var uploadsResp struct {
				UploadsList []any `json:"UploadsList"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &uploadsResp))
			assert.Len(t, uploadsResp.UploadsList, 1)

			// Verify provisioned capacity is restored
			rec = doRequest(t, h2, http.MethodGet, "/-/provisioned-capacity", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var capsResp struct {
				ProvisionedCapacityList []any `json:"ProvisionedCapacityList"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &capsResp))
			assert.Len(t, capsResp.ProvisionedCapacityList, 1)
		})
	}
}

// ----------------------------------------
// GetSupportedOperations includes new ops
// ----------------------------------------

func TestHandler_GetSupportedOperations_NewOps(t *testing.T) {
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

func TestHandler_Metadata(t *testing.T) {
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

func TestHandler_DataRetrievalPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		wantStatus int
		wantPolicy bool
	}{
		{
			name:       "get_policy",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
			wantPolicy: true,
		},
		{
			name:       "set_policy",
			method:     http.MethodPut,
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			body := `{"Policy":{"Rules":[{"Strategy":"BytesPerHour","BytesPerHour":10737418240}]}}`
			rec := doRequest(t, h, tt.method, "/-/policies/data-retrieval", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantPolicy {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "Policy")
			}
		})
	}
}

// ----------------------------------------
// ExtractOperation and ExtractResource
// ----------------------------------------

func TestHandler_ExtractOperation(t *testing.T) {
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

func TestHandler_ExtractResource(t *testing.T) {
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

func TestHandler_AbortMultipartUpload_ThenListParts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
	}{
		{
			name:      "abort_removes_upload",
			vaultName: "abort-check-vault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName, "")
			require.Equal(t, http.StatusCreated, rec.Code)

			rec = doRequestWithHeaders(t, h, http.MethodPost, "/-/vaults/"+tt.vaultName+"/multipart-uploads", "", map[string]string{"X-Amz-Part-Size": "1048576"})
			require.Equal(t, http.StatusCreated, rec.Code)
			uploadID := rec.Header().Get("X-Amz-Multipart-Upload-Id")
			require.NotEmpty(t, uploadID)

			// Abort the upload
			rec = doRequest(t, h, http.MethodDelete, "/-/vaults/"+tt.vaultName+"/multipart-uploads/"+uploadID, "")
			require.Equal(t, http.StatusNoContent, rec.Code)

			// ListParts should now return 404
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/multipart-uploads/"+uploadID, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// ----------------------------------------
// Invalid archive size for CompleteMultipartUpload
// ----------------------------------------

func TestHandler_CompleteMultipartUpload_InvalidArchiveSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		archSize   string
		wantStatus int
	}{
		{
			name:       "invalid_archive_size_header",
			archSize:   "not-a-number",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vaultName := "invalid-size-vault"

			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+vaultName, "")
			require.Equal(t, http.StatusCreated, rec.Code)

			rec = doRequestWithHeaders(t, h, http.MethodPost, "/-/vaults/"+vaultName+"/multipart-uploads", "", map[string]string{"X-Amz-Part-Size": "1048576"})
			require.Equal(t, http.StatusCreated, rec.Code)
			uploadID := rec.Header().Get("X-Amz-Multipart-Upload-Id")
			require.NotEmpty(t, uploadID)

			headers := map[string]string{
				"X-Amz-Archive-Size":     tt.archSize,
				"X-Amz-Sha256-Tree-Hash": "checksum",
			}

			rec = doRequestWithHeaders(
				t, h, http.MethodPost,
				"/-/vaults/"+vaultName+"/multipart-uploads/"+uploadID,
				"", headers,
			)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
