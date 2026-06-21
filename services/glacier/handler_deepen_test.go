package glacier_test

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

const (
	deepenAccountID = "999988887777"
	deepenRegion    = "us-west-2"
)

func newDeepenHandler() *glacier.Handler {
	bk := glacier.NewInMemoryBackend()
	glacier.SetRetrievalDelay(bk, 0)
	h := glacier.NewHandler(bk)
	h.AccountID = deepenAccountID
	h.DefaultRegion = deepenRegion

	return h
}

// doRequestFull issues a request with optional headers and returns the recorder.
func doRequestFull(
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
	require.NoError(t, h.Handler()(c))

	return rec
}

// deepenCreateVault creates a vault and returns 201.
func deepenCreateVault(t *testing.T, h *glacier.Handler, vaultName string) {
	t.Helper()
	rec := doRequestFull(t, h, http.MethodPut, "/"+deepenAccountID+"/vaults/"+vaultName, "", nil)
	require.Equal(t, http.StatusCreated, rec.Code)
}

// deepenUploadArchive uploads bytes as an archive and returns the archiveId.
func deepenUploadArchive(t *testing.T, h *glacier.Handler, vaultName string, data []byte) string {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/"+deepenAccountID+"/vaults/"+vaultName+"/archives",
		strings.NewReader(string(data)))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	require.Equal(t, http.StatusCreated, rec.Code)
	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	id := resp["archiveId"]
	require.NotEmpty(t, id)

	return id
}

// deepenInitiateJob initiates a job and returns the jobId.
func deepenInitiateJob(t *testing.T, h *glacier.Handler, vaultName, body string) string {
	t.Helper()
	rec := doRequestFull(t, h, http.MethodPost, "/"+deepenAccountID+"/vaults/"+vaultName+"/jobs", body, nil)
	require.Equal(t, http.StatusAccepted, rec.Code)
	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	id := resp["jobId"]
	require.NotEmpty(t, id)

	return id
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Full archive-retrieval lifecycle
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_ArchiveRetrieval_FullLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
	}{
		{name: "small_archive", content: "hello glacier retrieval"},
		{name: "binary_like_content", content: "\x00\x01\x02\x03binary"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "arch-lifecycle")
			archiveID := deepenUploadArchive(t, h, "arch-lifecycle", []byte(tt.content))

			jobID := deepenInitiateJob(t, h, "arch-lifecycle",
				`{"Type":"archive-retrieval","ArchiveId":"`+archiveID+`"}`)

			// DescribeJob should show Succeeded (zero delay).
			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/arch-lifecycle/jobs/"+jobID, "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var desc map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
			assert.Equal(t, "Succeeded", desc["StatusCode"])
			assert.Equal(t, true, desc["Completed"])

			// GetJobOutput should return the archive bytes.
			rec = doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/arch-lifecycle/jobs/"+jobID+"/output", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tt.content, rec.Body.String())
		})
	}
}

func TestDeepen_ArchiveRetrieval_RetrievalByteRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		content    string
		byteRange  string
		wantOutput string
	}{
		{
			name:       "first_5_bytes",
			content:    "hello glacier",
			byteRange:  "0-4",
			wantOutput: "hello",
		},
		{
			name:       "middle_bytes",
			content:    "abcdefghijklmnop",
			byteRange:  "3-7",
			wantOutput: "defgh",
		},
		{
			name:       "last_byte",
			content:    "xyz",
			byteRange:  "2-2",
			wantOutput: "z",
		},
		{
			name:       "full_range",
			content:    "fullcontent",
			byteRange:  "0-10",
			wantOutput: "fullcontent",
		},
		{
			name:       "range_beyond_end_clamped",
			content:    "short",
			byteRange:  "0-9999",
			wantOutput: "short",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "range-vault-"+tt.name)
			archiveID := deepenUploadArchive(t, h, "range-vault-"+tt.name, []byte(tt.content))

			jobID := deepenInitiateJob(t, h, "range-vault-"+tt.name,
				`{"Type":"archive-retrieval","ArchiveId":"`+archiveID+`","RetrievalByteRange":"`+tt.byteRange+`"}`)

			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/range-vault-"+tt.name+"/jobs/"+jobID+"/output", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tt.wantOutput, rec.Body.String())
		})
	}
}

func TestDeepen_ArchiveRetrieval_SHA256TreeHashHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
	}{
		{name: "header_set_from_archive", content: "checksum test content"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "checksum-vault")
			data := []byte(tt.content)
			archiveID := deepenUploadArchive(t, h, "checksum-vault", data)
			expectedHash := glacier.ComputeTreeHash(data)

			jobID := deepenInitiateJob(t, h, "checksum-vault",
				`{"Type":"archive-retrieval","ArchiveId":"`+archiveID+`"}`)

			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/checksum-vault/jobs/"+jobID+"/output", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, expectedHash, rec.Header().Get("X-Amz-Sha256-Tree-Hash"))
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Inventory retrieval lifecycle
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_InventoryRetrieval_JSONLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		archiveCount int
	}{
		{name: "empty_vault", archiveCount: 0},
		{name: "three_archives", archiveCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "inv-json-"+tt.name)
			for i := range tt.archiveCount {
				deepenUploadArchive(t, h, "inv-json-"+tt.name, fmt.Appendf(nil, "archive-%d", i))
			}

			jobID := deepenInitiateJob(t, h, "inv-json-"+tt.name, `{"Type":"inventory-retrieval"}`)

			// GetJobOutput returns JSON inventory.
			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/inv-json-"+tt.name+"/jobs/"+jobID+"/output", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

			var inv map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &inv))
			archList, ok := inv["ArchiveList"].([]any)
			require.True(t, ok)
			assert.Len(t, archList, tt.archiveCount)
			assert.NotEmpty(t, inv["VaultARN"])
		})
	}
}

func TestDeepen_InventoryRetrieval_CSVLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		archiveCount int
	}{
		{name: "empty_vault_csv", archiveCount: 0},
		{name: "two_archives_csv", archiveCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "inv-csv-"+tt.name)
			for i := range tt.archiveCount {
				deepenUploadArchive(t, h, "inv-csv-"+tt.name, fmt.Appendf(nil, "data-%d", i))
			}

			jobID := deepenInitiateJob(t, h, "inv-csv-"+tt.name,
				`{"Type":"inventory-retrieval","Format":"CSV"}`)

			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/inv-csv-"+tt.name+"/jobs/"+jobID+"/output", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "text/csv", rec.Header().Get("Content-Type"))

			r := csv.NewReader(strings.NewReader(rec.Body.String()))
			rows, err := r.ReadAll()
			require.NoError(t, err)
			// Header row + one row per archive.
			assert.Len(t, rows, 1+tt.archiveCount)
			assert.Equal(t, "ArchiveId", rows[0][0])
		})
	}
}

func TestDeepen_InventoryRetrieval_InventorySizeInBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		archiveCount int
	}{
		{name: "populated_after_get_output", archiveCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "invsize-vault")
			for i := range tt.archiveCount {
				deepenUploadArchive(t, h, "invsize-vault", fmt.Appendf(nil, "data-%d", i))
			}

			jobID := deepenInitiateJob(t, h, "invsize-vault", `{"Type":"inventory-retrieval"}`)

			// Before GetJobOutput: InventorySizeInBytes may be 0.
			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/invsize-vault/jobs/"+jobID+"/output", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			payloadSize := rec.Body.Len()
			require.Positive(t, payloadSize)

			// After GetJobOutput: DescribeJob should have InventorySizeInBytes set.
			rec2 := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/invsize-vault/jobs/"+jobID, "", nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			var desc map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &desc))
			raw, present := desc["InventorySizeInBytes"]
			assert.True(t, present, "InventorySizeInBytes must be present after GetJobOutput")
			got := int64(raw.(float64))
			assert.Equal(t, int64(payloadSize), got, "InventorySizeInBytes must match actual payload size")
		})
	}
}

func TestDeepen_InventoryRetrieval_LastInventoryDateUpdated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "last_inventory_date_set_on_job"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "lastinv-vault")

			// Before any job: no LastInventoryDate.
			rec := doRequestFull(t, h, http.MethodGet, "/"+deepenAccountID+"/vaults/lastinv-vault", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var vaultDesc map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &vaultDesc))
			assert.Empty(t, vaultDesc["LastInventoryDate"])

			deepenInitiateJob(t, h, "lastinv-vault", `{"Type":"inventory-retrieval"}`)

			// After job: LastInventoryDate set.
			rec = doRequestFull(t, h, http.MethodGet, "/"+deepenAccountID+"/vaults/lastinv-vault", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &vaultDesc))
			assert.NotEmpty(t, vaultDesc["LastInventoryDate"], tt.name)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Vault isolation
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_Vault_CrossVaultIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "different_vaults_isolated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "vault-a")
			deepenCreateVault(t, h, "vault-b")

			archAID := deepenUploadArchive(t, h, "vault-a", []byte("in vault A"))

			// Archive exists in vault-a.
			jobID := deepenInitiateJob(t, h, "vault-a",
				`{"Type":"archive-retrieval","ArchiveId":"`+archAID+`"}`)
			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/vault-a/jobs/"+jobID+"/output", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			// Archive does NOT exist in vault-b.
			rec2 := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/vault-b/jobs",
				`{"Type":"archive-retrieval","ArchiveId":"`+archAID+`"}`, nil)
			assert.Equal(t, http.StatusNotFound, rec2.Code, tt.name)
		})
	}
}

func TestDeepen_Vault_CrossAccountIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "different_accounts_isolated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			glacier.SetRetrievalDelay(bk, 0)

			hA := glacier.NewHandler(bk)
			hA.AccountID = "account-aaa"
			hA.DefaultRegion = deepenRegion

			hB := glacier.NewHandler(bk)
			hB.AccountID = "account-bbb"
			hB.DefaultRegion = deepenRegion

			// Create same-named vault in both accounts.
			recA := doRequestFull(t, hA, http.MethodPut, "/account-aaa/vaults/shared-name", "", nil)
			require.Equal(t, http.StatusCreated, recA.Code)
			recB := doRequestFull(t, hB, http.MethodPut, "/account-bbb/vaults/shared-name", "", nil)
			require.Equal(t, http.StatusCreated, recB.Code)

			// Upload to account-a.
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/account-aaa/vaults/shared-name/archives",
				strings.NewReader("secret"))
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, hA.Handler()(c))
			require.Equal(t, http.StatusCreated, rec.Code)

			// account-b's vault has no archives.
			listRecB := doRequestFull(t, hB, http.MethodGet, "/account-bbb/vaults", "", nil)
			require.Equal(t, http.StatusOK, listRecB.Code)
			var listResp map[string]any
			require.NoError(t, json.Unmarshal(listRecB.Body.Bytes(), &listResp))
			vl := listResp["VaultList"].([]any)
			require.Len(t, vl, 1, tt.name)
			v := vl[0].(map[string]any)
			// account-b vault has 0 archives (not account-a's archive).
			assert.EqualValues(t, 0, v["NumberOfArchives"])
		})
	}
}

func TestDeepen_Vault_CrossRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "different_regions_isolated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			glacier.SetRetrievalDelay(bk, 0)

			hE := glacier.NewHandler(bk)
			hE.AccountID = "same-acct"
			hE.DefaultRegion = "us-east-1"

			hW := glacier.NewHandler(bk)
			hW.AccountID = "same-acct"
			hW.DefaultRegion = "us-west-2"

			// Create in east, list in west → 0 vaults.
			recE := doRequestFull(t, hE, http.MethodPut, "/same-acct/vaults/east-vault", "", nil)
			require.Equal(t, http.StatusCreated, recE.Code)

			recW := doRequestFull(t, hW, http.MethodGet, "/same-acct/vaults", "", nil)
			require.Equal(t, http.StatusOK, recW.Code)
			var listResp map[string]any
			require.NoError(t, json.Unmarshal(recW.Body.Bytes(), &listResp))
			vl := listResp["VaultList"].([]any)
			assert.Empty(t, vl, tt.name)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. VaultARN format
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_VaultARN_Format(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
	}{
		{name: "arn_follows_aws_format", vaultName: "my-arn-vault"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, tt.vaultName)

			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/"+tt.vaultName, "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			arn, _ := resp["VaultARN"].(string)
			assert.NotEmpty(t, arn)
			wantARN := "arn:aws:glacier:" + deepenRegion + ":" + deepenAccountID + ":vaults/" + tt.vaultName
			assert.Equal(t, wantARN, arn)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. CreateVault idempotency
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_CreateVault_Idempotent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		vaultName   string
		createCount int
	}{
		{name: "double_create_returns_201", vaultName: "idempotent-vault", createCount: 2},
		{name: "triple_create_returns_201", vaultName: "triple-vault", createCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			for range tt.createCount {
				rec := doRequestFull(t, h, http.MethodPut,
					"/"+deepenAccountID+"/vaults/"+tt.vaultName, "", nil)
				assert.Equal(t, http.StatusCreated, rec.Code)
			}

			// Only one vault should exist.
			rec := doRequestFull(t, h, http.MethodGet, "/"+deepenAccountID+"/vaults", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			vl := listResp["VaultList"].([]any)
			assert.Len(t, vl, 1)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. Vault stats (SizeInBytes, NumberOfArchives)
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_VaultStats_UploadAndDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content []byte
	}{
		{name: "stats_update_correctly", content: []byte("hello")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "stats-vault")

			// Initial state.
			descVault := func() map[string]any {
				rec := doRequestFull(t, h, http.MethodGet,
					"/"+deepenAccountID+"/vaults/stats-vault", "", nil)
				require.Equal(t, http.StatusOK, rec.Code)
				var v map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &v))

				return v
			}

			v0 := descVault()
			assert.EqualValues(t, 0, v0["NumberOfArchives"])
			assert.EqualValues(t, 0, v0["SizeInBytes"])

			// Upload.
			archiveID := deepenUploadArchive(t, h, "stats-vault", tt.content)
			v1 := descVault()
			assert.EqualValues(t, 1, v1["NumberOfArchives"])
			assert.EqualValues(t, len(tt.content), v1["SizeInBytes"])

			// Delete.
			rec := doRequestFull(t, h, http.MethodDelete,
				"/"+deepenAccountID+"/vaults/stats-vault/archives/"+archiveID, "", nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			v2 := descVault()
			assert.EqualValues(t, 0, v2["NumberOfArchives"])
			assert.EqualValues(t, 0, v2["SizeInBytes"])
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. Pagination nil-safety (marker not found → empty list, not null)
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_Pagination_MarkerNotFound_EmptyList(t *testing.T) {
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
			path:    "/" + deepenAccountID + "/vaults?marker=nonexistent",
			jsonKey: "VaultList",
			setupFn: func(h *glacier.Handler) { deepenCreateVault(t, h, "pag-vault") },
		},
		{
			name:    "list_jobs_unknown_marker",
			method:  http.MethodGet,
			path:    "/" + deepenAccountID + "/vaults/pagvault/jobs?marker=unknown-job-id",
			jsonKey: "JobList",
			setupFn: func(h *glacier.Handler) {
				deepenCreateVault(t, h, "pagvault")
				deepenInitiateJob(t, h, "pagvault", `{"Type":"inventory-retrieval"}`)
			},
		},
		{
			name:    "list_multipart_uploads_unknown_marker",
			method:  http.MethodGet,
			path:    "/" + deepenAccountID + "/vaults/pagmpvault/multipart-uploads?marker=unknown",
			jsonKey: "UploadsList",
			setupFn: func(h *glacier.Handler) {
				deepenCreateVault(t, h, "pagmpvault")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			tt.setupFn(h)

			rec := doRequestFull(t, h, tt.method, tt.path, "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			raw, ok := resp[tt.jsonKey]
			assert.True(t, ok, "%s key must be present", tt.jsonKey)
			assert.Equal(t, "[]", string(raw), "%s must be [] not null when marker not found", tt.jsonKey)
		})
	}
}

func TestDeepen_Pagination_ListParts_MarkerNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "parts_empty_on_unknown_marker"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "parts-marker-vault")

			// Initiate multipart upload.
			rec := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/parts-marker-vault/multipart-uploads", "",
				map[string]string{"X-Amz-Part-Size": "1048576"})
			require.Equal(t, http.StatusCreated, rec.Code)
			var initResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			uploadID := initResp["uploadId"]
			require.NotEmpty(t, uploadID)

			// List parts with unknown marker.
			rec2 := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/parts-marker-vault/multipart-uploads/"+uploadID+"?marker=nonexistent",
				"", nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			var resp map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
			partsRaw := resp["Parts"]
			assert.Equal(t, "[]", string(partsRaw), tt.name)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. Tag limit and validation
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_Tags_ExactLimitAccepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tagCount int
		wantOK   bool
	}{
		{name: "exactly_10_tags_accepted", tagCount: 10, wantOK: true},
		{name: "11_tags_rejected", tagCount: 11, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "tag-limit-vault-"+tt.name)

			tags := make(map[string]string, tt.tagCount)
			for i := range tt.tagCount {
				tags[fmt.Sprintf("key-%02d", i)] = fmt.Sprintf("val-%02d", i)
			}

			body, err := json.Marshal(map[string]any{"Tags": tags})
			require.NoError(t, err)

			rec := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/tag-limit-vault-"+tt.name+"/tags?operation=add",
				string(body), nil)

			if tt.wantOK {
				assert.Equal(t, http.StatusNoContent, rec.Code)
			} else {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			}
		})
	}
}

func TestDeepen_Tags_ReservedPrefixRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tagKey  string
		wantBad bool
	}{
		{name: "aws_prefix_rejected", tagKey: "aws:reserved", wantBad: true},
		{name: "normal_key_accepted", tagKey: "mykey", wantBad: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "tag-prefix-"+tt.name)

			body := `{"Tags":{"` + tt.tagKey + `":"value"}}`
			rec := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/tag-prefix-"+tt.name+"/tags?operation=add", body, nil)

			if tt.wantBad {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			} else {
				assert.Equal(t, http.StatusNoContent, rec.Code)
			}
		})
	}
}

func TestDeepen_Tags_RemoveNonExistentKeyIsNoop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "remove_missing_key_returns_204"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "tag-remove-vault")

			rec := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/tag-remove-vault/tags?operation=remove",
				`{"TagKeys":["nonexistent-key"]}`, nil)
			assert.Equal(t, http.StatusNoContent, rec.Code, tt.name)
		})
	}
}

func TestDeepen_Tags_ListTagsEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "list_tags_returns_empty_map"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "empty-tags-vault")

			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/empty-tags-vault/tags", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			tags, ok := resp["Tags"].(map[string]any)
			assert.True(t, ok, tt.name)
			assert.Empty(t, tags)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. ListJobs filter combinations
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_ListJobs_CompletedFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		completedFilter string
		wantCount       int
	}{
		{name: "completed_true_returns_succeeded", completedFilter: "true", wantCount: 1},
		{name: "completed_false_returns_empty", completedFilter: "false", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "list-jobs-completed-"+tt.name)
			deepenInitiateJob(t, h, "list-jobs-completed-"+tt.name, `{"Type":"inventory-retrieval"}`)

			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/list-jobs-completed-"+tt.name+"/jobs?completed="+tt.completedFilter,
				"", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			jobs := resp["JobList"].([]any)
			assert.Len(t, jobs, tt.wantCount)
		})
	}
}

func TestDeepen_ListJobs_StatusCodeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		statuscodeParam string
		wantCount       int
	}{
		{name: "succeeded_filter", statuscodeParam: "Succeeded", wantCount: 1},
		{name: "in_progress_filter", statuscodeParam: "InProgress", wantCount: 0},
		{name: "failed_filter", statuscodeParam: "Failed", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "list-jobs-sc-"+tt.name)
			deepenInitiateJob(t, h, "list-jobs-sc-"+tt.name, `{"Type":"inventory-retrieval"}`)

			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/list-jobs-sc-"+tt.name+"/jobs?statuscode="+tt.statuscodeParam,
				"", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			jobs := resp["JobList"].([]any)
			assert.Len(t, jobs, tt.wantCount)
		})
	}
}

func TestDeepen_ListJobs_InvalidCompletedParam(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		completedFilter string
	}{
		{name: "invalid_value", completedFilter: "yes"},
		{name: "numeric_value", completedFilter: "1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "listjobs-invalid-"+tt.name)

			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/listjobs-invalid-"+tt.name+"/jobs?completed="+tt.completedFilter,
				"", nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 10. VaultLock full lifecycle
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_VaultLock_FullLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{name: "initiate_complete_verify_locked", policy: `{"Version":"2012-10-17"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "lock-lifecycle-vault")

			// GetVaultLock on unlocked vault.
			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/lock-lifecycle-vault/lock-policy", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var lockState map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lockState))
			assert.Equal(t, "Unlocked", lockState["State"])

			// Initiate.
			body := `{"Policy":"` + strings.ReplaceAll(tt.policy, `"`, `\"`) + `"}`
			rec = doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/lock-lifecycle-vault/lock-policy", body, nil)
			require.Equal(t, http.StatusCreated, rec.Code)
			var initResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			lockID := initResp["lockId"]
			require.NotEmpty(t, lockID)

			// GetVaultLock shows InProgress.
			rec = doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/lock-lifecycle-vault/lock-policy", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lockState))
			assert.Equal(t, "InProgress", lockState["State"])
			assert.NotEmpty(t, lockState["ExpirationDate"])

			// Complete with correct lockID.
			rec = doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/lock-lifecycle-vault/lock-policy/"+lockID, "", nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			// GetVaultLock shows Locked.
			rec = doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/lock-lifecycle-vault/lock-policy", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lockState))
			assert.Equal(t, "Locked", lockState["State"], tt.name)
		})
	}
}

func TestDeepen_VaultLock_WrongLockIDFails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "wrong_lock_id_returns_400"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "lock-wrong-id-vault")

			rec := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/lock-wrong-id-vault/lock-policy", `{"Policy":"p"}`, nil)
			require.Equal(t, http.StatusCreated, rec.Code)

			rec = doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/lock-wrong-id-vault/lock-policy/WRONGID", "", nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
		})
	}
}

func TestDeepen_VaultLock_AbortRemovesLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "abort_then_state_is_unlocked"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "lock-abort-vault")

			rec := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/lock-abort-vault/lock-policy", `{"Policy":"p"}`, nil)
			require.Equal(t, http.StatusCreated, rec.Code)

			rec = doRequestFull(t, h, http.MethodDelete,
				"/"+deepenAccountID+"/vaults/lock-abort-vault/lock-policy", "", nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			rec = doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/lock-abort-vault/lock-policy", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var lockState map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lockState))
			assert.Equal(t, "Unlocked", lockState["State"], tt.name)
		})
	}
}

func TestDeepen_VaultLock_DoubleInitiateConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "second_initiate_returns_409"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "double-lock-vault")

			rec1 := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/double-lock-vault/lock-policy", `{"Policy":"p"}`, nil)
			require.Equal(t, http.StatusCreated, rec1.Code)

			rec2 := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/double-lock-vault/lock-policy", `{"Policy":"p"}`, nil)
			assert.Equal(t, http.StatusConflict, rec2.Code, tt.name)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 11. DataRetrievalPolicy full roundtrip
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_DataRetrievalPolicy_BytesPerHour(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bytesPerHour int64
		wantOK       bool
	}{
		{name: "valid_bytes_per_hour", bytesPerHour: 1073741824, wantOK: true},
		{name: "zero_bytes_per_hour_rejected", bytesPerHour: 0, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			body := fmt.Sprintf(`{"Policy":{"Rules":[{"Strategy":"BytesPerHour","BytesPerHour":%d}]}}`,
				tt.bytesPerHour)

			rec := doRequestFull(t, h, http.MethodPut,
				"/"+deepenAccountID+"/policies/data-retrieval", body, nil)
			if tt.wantOK {
				assert.Equal(t, http.StatusNoContent, rec.Code)
			} else {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			}
		})
	}
}

func TestDeepen_DataRetrievalPolicy_GetDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantStrategyKey string
	}{
		{name: "default_policy_is_free_tier", wantStrategyKey: "FreeTier"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/policies/data-retrieval", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			policy := resp["Policy"].(map[string]any)
			rules := policy["Rules"].([]any)
			require.NotEmpty(t, rules)
			rule := rules[0].(map[string]any)
			assert.Equal(t, tt.wantStrategyKey, rule["Strategy"])
		})
	}
}

func TestDeepen_DataRetrievalPolicy_SetAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy string
	}{
		{name: "set_none_and_get", strategy: "None"},
		{name: "set_free_tier_and_get", strategy: "FreeTier"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			body := `{"Policy":{"Rules":[{"Strategy":"` + tt.strategy + `"}]}}`
			rec := doRequestFull(t, h, http.MethodPut,
				"/"+deepenAccountID+"/policies/data-retrieval", body, nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			rec = doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/policies/data-retrieval", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			policy := resp["Policy"].(map[string]any)
			rules := policy["Rules"].([]any)
			require.NotEmpty(t, rules)
			rule := rules[0].(map[string]any)
			assert.Equal(t, tt.strategy, rule["Strategy"])
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 12. ProvisionedCapacity
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_ProvisionedCapacity_PurchaseAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		purchases  int
		wantCount  int
		wantStatus int
	}{
		{name: "purchase_one", purchases: 1, wantCount: 1, wantStatus: http.StatusCreated},
		{name: "purchase_two", purchases: 2, wantCount: 2, wantStatus: http.StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			for range tt.purchases {
				rec := doRequestFull(t, h, http.MethodPost,
					"/"+deepenAccountID+"/provisioned-capacity", "", nil)
				require.Equal(t, tt.wantStatus, rec.Code)
			}

			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/provisioned-capacity", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			list := resp["ProvisionedCapacityList"].([]any)
			assert.Len(t, list, tt.wantCount)
		})
	}
}

func TestDeepen_ProvisionedCapacity_LimitEnforced(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "third_purchase_rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			for range 2 {
				rec := doRequestFull(t, h, http.MethodPost,
					"/"+deepenAccountID+"/provisioned-capacity", "", nil)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/provisioned-capacity", "", nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
		})
	}
}

func TestDeepen_ProvisionedCapacity_DatesPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "start_and_expiration_dates_set"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			rec := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/provisioned-capacity", "", nil)
			require.Equal(t, http.StatusCreated, rec.Code)
			var purchaseResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &purchaseResp))
			capID := purchaseResp["capacityId"]
			require.NotEmpty(t, capID)

			rec = doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/provisioned-capacity", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			list := listResp["ProvisionedCapacityList"].([]any)
			require.Len(t, list, 1)
			capItem := list[0].(map[string]any)
			assert.NotEmpty(t, capItem["StartDate"], tt.name)
			assert.NotEmpty(t, capItem["ExpirationDate"])
			assert.Equal(t, capID, capItem["CapacityId"])
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 13. Multipart upload complete lifecycle
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_MultipartUpload_FullLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		description string
		partSize    string
	}{
		{name: "two_parts_complete", description: "my big file", partSize: "1048576"},
		{name: "no_description", description: "", partSize: "4194304"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "mp-lifecycle-"+tt.name)

			// Initiate.
			headers := map[string]string{"X-Amz-Part-Size": tt.partSize}
			if tt.description != "" {
				headers["X-Amz-Archive-Description"] = tt.description
			}
			rec := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/mp-lifecycle-"+tt.name+"/multipart-uploads",
				"", headers)
			require.Equal(t, http.StatusCreated, rec.Code)
			var initResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			uploadID := initResp["uploadId"]
			require.NotEmpty(t, uploadID)
			// Location header must be set.
			assert.Contains(t, rec.Header().Get("Location"), uploadID)
			assert.Equal(t, uploadID, rec.Header().Get("X-Amz-Multipart-Upload-Id"))

			// Upload part 1.
			part1Data := strings.Repeat("a", 1<<20)
			e := echo.New()
			reqP := httptest.NewRequest(http.MethodPut,
				"/"+deepenAccountID+"/vaults/mp-lifecycle-"+tt.name+"/multipart-uploads/"+uploadID,
				strings.NewReader(part1Data))
			reqP.Header.Set("Content-Range", "bytes 0-1048575/*")
			recP := httptest.NewRecorder()
			cp := e.NewContext(reqP, recP)
			require.NoError(t, h.Handler()(cp))
			require.Equal(t, http.StatusNoContent, recP.Code)

			// List parts: should have one.
			rec2 := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/mp-lifecycle-"+tt.name+"/multipart-uploads/"+uploadID,
				"", nil)
			require.Equal(t, http.StatusOK, rec2.Code)
			var listParts map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listParts))
			parts := listParts["Parts"].([]any)
			assert.Len(t, parts, 1)

			// Complete.
			checksum := glacier.ComputeTreeHash([]byte(part1Data))
			rec3 := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/mp-lifecycle-"+tt.name+"/multipart-uploads/"+uploadID,
				"", map[string]string{
					"X-Amz-Archive-Size":     "1048576",
					"X-Amz-Sha256-Tree-Hash": checksum,
				})
			require.Equal(t, http.StatusCreated, rec3.Code)
			var completeResp map[string]string
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &completeResp))
			archiveID := completeResp["archiveId"]
			assert.NotEmpty(t, archiveID)
			assert.Equal(t, checksum, rec3.Header().Get("X-Amz-Sha256-Tree-Hash"))

			// Upload no longer listed after completion.
			rec4 := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/mp-lifecycle-"+tt.name+"/multipart-uploads", "", nil)
			require.Equal(t, http.StatusOK, rec4.Code)
			var listUploads map[string]any
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &listUploads))
			uploads := listUploads["UploadsList"].([]any)
			assert.Empty(t, uploads)

			// Vault now has the archive.
			descVault := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/mp-lifecycle-"+tt.name, "", nil)
			require.Equal(t, http.StatusOK, descVault.Code)
			var vaultDesc map[string]any
			require.NoError(t, json.Unmarshal(descVault.Body.Bytes(), &vaultDesc))
			assert.EqualValues(t, 1, vaultDesc["NumberOfArchives"])
		})
	}
}

func TestDeepen_MultipartUpload_AbortLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "abort_clears_upload"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "mp-abort-vault")

			rec := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/mp-abort-vault/multipart-uploads",
				"", map[string]string{"X-Amz-Part-Size": "1048576"})
			require.Equal(t, http.StatusCreated, rec.Code)
			var initResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			uploadID := initResp["uploadId"]

			// Abort.
			rec2 := doRequestFull(t, h, http.MethodDelete,
				"/"+deepenAccountID+"/vaults/mp-abort-vault/multipart-uploads/"+uploadID, "", nil)
			require.Equal(t, http.StatusNoContent, rec2.Code)

			// ListMultipartUploads should be empty.
			rec3 := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/mp-abort-vault/multipart-uploads", "", nil)
			require.Equal(t, http.StatusOK, rec3.Code)
			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &listResp))
			uploads := listResp["UploadsList"].([]any)
			assert.Empty(t, uploads, tt.name)

			// Listing parts for aborted upload returns 404.
			rec4 := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/mp-abort-vault/multipart-uploads/"+uploadID, "", nil)
			assert.Equal(t, http.StatusNotFound, rec4.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 14. Error response fidelity
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_ErrorResponse_FormatFields(t *testing.T) {
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
			path:        "/" + deepenAccountID + "/vaults/does-not-exist",
			wantStatus:  http.StatusNotFound,
			wantCodeKey: "ResourceNotFoundException",
		},
		{
			name:        "archive_not_found_has_type_field",
			method:      http.MethodDelete,
			path:        "/" + deepenAccountID + "/vaults/does-not-exist/archives/fake-id",
			wantStatus:  http.StatusNotFound,
			wantCodeKey: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			rec := doRequestFull(t, h, tt.method, tt.path, "", nil)
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

func TestDeepen_ErrorResponse_XAmznRequestID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "create_vault", method: http.MethodPut, path: "/" + deepenAccountID + "/vaults/req-id-vault"},
		{name: "list_vaults", method: http.MethodGet, path: "/" + deepenAccountID + "/vaults"},
		{name: "not_found", method: http.MethodGet, path: "/" + deepenAccountID + "/vaults/nosuchvault"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			rec := doRequestFull(t, h, tt.method, tt.path, "", nil)
			reqID := rec.Header().Get("X-Amzn-Requestid")
			assert.NotEmpty(t, reqID, "X-Amzn-Requestid must be present: %s", tt.name)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 15. GetJobOutput range header for inventory
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_GetJobOutput_RangeOnInventory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		rangeHeader string
		wantStatus  int
	}{
		{
			name:        "first_10_bytes",
			rangeHeader: "bytes=0-9",
			wantStatus:  http.StatusPartialContent,
		},
		{
			name:        "invalid_range",
			rangeHeader: "bytes=9999-10000",
			wantStatus:  http.StatusRequestedRangeNotSatisfiable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "inv-range-vault-"+tt.name)
			deepenUploadArchive(t, h, "inv-range-vault-"+tt.name, []byte("some archive data"))

			jobID := deepenInitiateJob(t, h, "inv-range-vault-"+tt.name, `{"Type":"inventory-retrieval"}`)

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet,
				"/"+deepenAccountID+"/vaults/inv-range-vault-"+tt.name+"/jobs/"+jobID+"/output",
				http.NoBody)
			req.Header.Set("Range", tt.rangeHeader)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 16. Vault notifications
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_VaultNotifications_SetGetDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		topic  string
		events []string
	}{
		{
			name:   "set_and_get_notifications",
			topic:  "arn:aws:sns:us-east-1:123456789012:my-topic",
			events: []string{"ArchiveRetrievalCompleted", "InventoryRetrievalCompleted"},
		},
		{
			name:   "single_event",
			topic:  "arn:aws:sns:us-east-1:111111111111:single",
			events: []string{"InventoryRetrievalCompleted"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "notif-vault-"+tt.name)

			// GET before set → 404.
			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/notif-vault-"+tt.name+"/notification-configuration", "", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)

			// SET.
			eventsJSON, _ := json.Marshal(tt.events)
			body := `{"SNSTopic":"` + tt.topic + `","Events":` + string(eventsJSON) + `}`
			rec = doRequestFull(t, h, http.MethodPut,
				"/"+deepenAccountID+"/vaults/notif-vault-"+tt.name+"/notification-configuration", body, nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			// GET → matches.
			rec = doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/notif-vault-"+tt.name+"/notification-configuration", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var notifResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &notifResp))
			assert.Equal(t, tt.topic, notifResp["SNSTopic"])
			events, ok := notifResp["Events"].([]any)
			require.True(t, ok)
			assert.Len(t, events, len(tt.events))

			// DELETE.
			rec = doRequestFull(t, h, http.MethodDelete,
				"/"+deepenAccountID+"/vaults/notif-vault-"+tt.name+"/notification-configuration", "", nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			// GET after delete → 404.
			rec = doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/notif-vault-"+tt.name+"/notification-configuration", "", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestDeepen_VaultNotifications_InvalidEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		event      string
		wantStatus int
	}{
		{name: "invalid_event_rejected", event: "BogusEvent", wantStatus: http.StatusBadRequest},
		{name: "valid_event_accepted", event: "ArchiveRetrievalCompleted", wantStatus: http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "notif-event-"+tt.name)

			body := `{"SNSTopic":"arn:aws:sns:us-east-1:000:t","Events":["` + tt.event + `"]}`
			rec := doRequestFull(t, h, http.MethodPut,
				"/"+deepenAccountID+"/vaults/notif-event-"+tt.name+"/notification-configuration", body, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 17. Access policy
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_AccessPolicy_SetGetDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{name: "iam_policy_roundtrip", policy: `{"Version":"2012-10-17","Statement":[]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "access-policy-vault")

			// GET before set → 404.
			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/access-policy-vault/access-policy", "", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)

			// SET.
			body := `{"Policy":"` + strings.ReplaceAll(tt.policy, `"`, `\"`) + `"}`
			rec = doRequestFull(t, h, http.MethodPut,
				"/"+deepenAccountID+"/vaults/access-policy-vault/access-policy", body, nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			// GET.
			rec = doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/access-policy-vault/access-policy", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var policyResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &policyResp))
			assert.NotEmpty(t, policyResp["Policy"])

			// DELETE.
			rec = doRequestFull(t, h, http.MethodDelete,
				"/"+deepenAccountID+"/vaults/access-policy-vault/access-policy", "", nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			// GET after delete → 404.
			rec = doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/access-policy-vault/access-policy", "", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 18. DescribeJob response fidelity
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_DescribeJob_Fidelity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		jobBody    string
		wantAction string
		wantTier   string
	}{
		{
			name:       "inventory_retrieval_fields",
			jobBody:    `{"Type":"inventory-retrieval","Tier":"Bulk"}`,
			wantAction: "InventoryRetrieval",
			wantTier:   "Bulk",
		},
		{
			name:       "default_tier_is_standard",
			jobBody:    `{"Type":"inventory-retrieval"}`,
			wantAction: "InventoryRetrieval",
			wantTier:   "Standard",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "describe-job-fidelity-"+tt.name)
			jobID := deepenInitiateJob(t, h, "describe-job-fidelity-"+tt.name, tt.jobBody)

			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/describe-job-fidelity-"+tt.name+"/jobs/"+jobID, "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantAction, resp["Action"])
			assert.Equal(t, tt.wantTier, resp["Tier"])
			assert.NotEmpty(t, resp["JobId"])
			assert.NotEmpty(t, resp["VaultARN"])
			assert.NotEmpty(t, resp["CreationDate"])
			assert.NotEmpty(t, resp["StatusCode"])
		})
	}
}

func TestDeepen_DescribeJob_ArchiveSizePopulated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
	}{
		{name: "archive_size_in_bytes_from_archive", content: "archive content here"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "archsize-job-vault")
			archiveID := deepenUploadArchive(t, h, "archsize-job-vault", []byte(tt.content))

			jobID := deepenInitiateJob(t, h, "archsize-job-vault",
				`{"Type":"archive-retrieval","ArchiveId":"`+archiveID+`"}`)

			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/archsize-job-vault/jobs/"+jobID, "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			archiveSize, ok := resp["ArchiveSizeInBytes"].(float64)
			assert.True(t, ok, "ArchiveSizeInBytes must be present for archive-retrieval job")
			assert.InDelta(t, float64(len(tt.content)), archiveSize, 0)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 19. ListVaults limit and marker together
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_ListVaults_LimitAndMarkerCombined(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over minimal padding
		name       string
		vaultNames []string
		limit      int
		marker     string
		wantNames  []string
		wantMarker bool
	}{
		{
			name:       "limit_1_first_page",
			vaultNames: []string{"alpha", "beta", "gamma"},
			limit:      1,
			marker:     "",
			wantNames:  []string{"alpha"},
			wantMarker: true,
		},
		{
			name:       "marker_at_alpha_limit_1",
			vaultNames: []string{"alpha", "beta", "gamma"},
			limit:      1,
			marker:     "alpha",
			wantNames:  []string{"beta"},
			wantMarker: true,
		},
		{
			name:       "marker_at_beta_limit_2",
			vaultNames: []string{"alpha", "beta", "gamma"},
			limit:      2,
			marker:     "beta",
			wantNames:  []string{"gamma"},
			wantMarker: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			for _, vn := range tt.vaultNames {
				deepenCreateVault(t, h, vn)
			}

			queryStr := fmt.Sprintf("?limit=%d", tt.limit)
			if tt.marker != "" {
				queryStr += "&marker=" + tt.marker
			}

			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults"+queryStr, "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			list := resp["VaultList"].([]any)
			require.Len(t, list, len(tt.wantNames))
			for i, want := range tt.wantNames {
				v := list[i].(map[string]any)
				assert.Equal(t, want, v["VaultName"])
			}
			_, hasMarker := resp["Marker"]
			assert.Equal(t, tt.wantMarker, hasMarker)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 20. InitiateJob validation
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_InitiateJob_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "invalid_type_rejected",
			body:       `{"Type":"bogus-type"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "archive_retrieval_missing_archive_id",
			body:       `{"Type":"archive-retrieval"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_tier_rejected",
			body:       `{"Type":"inventory-retrieval","Tier":"Express"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "bulk_tier_accepted",
			body:       `{"Type":"inventory-retrieval","Tier":"Bulk"}`,
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "expedited_tier_accepted",
			body:       `{"Type":"inventory-retrieval","Tier":"Expedited"}`,
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "standard_tier_accepted",
			body:       `{"Type":"inventory-retrieval","Tier":"Standard"}`,
			wantStatus: http.StatusAccepted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "init-job-val-"+tt.name)

			rec := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/init-job-val-"+tt.name+"/jobs", tt.body, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 21. GetJobOutput for incomplete job
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_GetJobOutput_IncompleteJobReturns400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "in_progress_job_output_rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			// Long delay keeps job InProgress.
			glacier.SetRetrievalDelay(bk, 30_000_000_000) // 30 seconds
			h := glacier.NewHandler(bk)
			h.AccountID = deepenAccountID
			h.DefaultRegion = deepenRegion

			deepenCreateVault(t, h, "incomplete-job-vault")
			jobID := deepenInitiateJob(t, h, "incomplete-job-vault", `{"Type":"inventory-retrieval"}`)

			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/incomplete-job-vault/jobs/"+jobID+"/output", "", nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 22. DeleteVault errors
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_DeleteVault_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "delete_missing_vault_returns_404"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			rec := doRequestFull(t, h, http.MethodDelete,
				"/"+deepenAccountID+"/vaults/nonexistent-vault", "", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code, tt.name)
		})
	}
}

func TestDeepen_DeleteVault_WithActiveMultipartUploads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "delete_vault_with_uploads_succeeds_after_abort"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "del-mp-vault")

			// Initiate multipart upload.
			rec := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/del-mp-vault/multipart-uploads",
				"", map[string]string{"X-Amz-Part-Size": "1048576"})
			require.Equal(t, http.StatusCreated, rec.Code)
			var initResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			uploadID := initResp["uploadId"]

			// Delete vault (empty archives = ok, multipart uploads get cleaned).
			rec = doRequestFull(t, h, http.MethodDelete,
				"/"+deepenAccountID+"/vaults/del-mp-vault", "", nil)
			require.Equal(t, http.StatusNoContent, rec.Code, tt.name)

			// Multipart upload no longer accessible.
			rec = doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/del-mp-vault/multipart-uploads/"+uploadID, "", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 23. ListParts marker pagination
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_ListParts_MarkerPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantParts int
	}{
		{name: "two_parts_paginated_by_marker", wantParts: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "listparts-pag-vault")

			// Initiate.
			rec := doRequestFull(t, h, http.MethodPost,
				"/"+deepenAccountID+"/vaults/listparts-pag-vault/multipart-uploads",
				"", map[string]string{"X-Amz-Part-Size": "1048576"})
			require.Equal(t, http.StatusCreated, rec.Code)
			var initResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			uploadID := initResp["uploadId"]

			// Upload 2 parts.
			e := echo.New()
			for i := range tt.wantParts {
				start := i * (1 << 20)
				end := start + (1 << 20) - 1
				rangeHdr := fmt.Sprintf("bytes %d-%d/*", start, end)
				req := httptest.NewRequest(http.MethodPut,
					"/"+deepenAccountID+"/vaults/listparts-pag-vault/multipart-uploads/"+uploadID,
					strings.NewReader(strings.Repeat("x", 1<<20)))
				req.Header.Set("Content-Range", rangeHdr)
				rec2 := httptest.NewRecorder()
				c := e.NewContext(req, rec2)
				require.NoError(t, h.Handler()(c))
				require.Equal(t, http.StatusNoContent, rec2.Code)
			}

			// List first part with limit=1.
			rec = doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/listparts-pag-vault/multipart-uploads/"+uploadID+"?limit=1",
				"", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var page1 map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
			parts1 := page1["Parts"].([]any)
			require.Len(t, parts1, 1)
			markerVal, hasMarker := page1["Marker"].(string)
			assert.True(t, hasMarker, "Marker must be set when there are more parts")

			// Second page using marker (URL-encode because RangeInBytes may contain spaces).
			secondPage := "/" + deepenAccountID + "/vaults/listparts-pag-vault/multipart-uploads/" +
				uploadID + "?limit=1&marker=" + url.QueryEscape(markerVal)
			rec = doRequestFull(t, h, http.MethodGet, secondPage, "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var page2 map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))
			parts2 := page2["Parts"].([]any)
			assert.Len(t, parts2, 1)
			_, hasMarker2 := page2["Marker"]
			assert.False(t, hasMarker2, "no Marker on last page")
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 24. DescribeJob for non-existent vault/job
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_DescribeJob_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupFn    func(h *glacier.Handler)
		vaultName  string
		jobID      string
		wantStatus int
	}{
		{
			name:       "vault_not_found",
			setupFn:    func(_ *glacier.Handler) {},
			vaultName:  "nonexistent",
			jobID:      "fakejob",
			wantStatus: http.StatusNotFound,
		},
		{
			name: "job_not_found",
			setupFn: func(h *glacier.Handler) {
				deepenCreateVault(t, h, "existvault")
			},
			vaultName:  "existvault",
			jobID:      "fakejob",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			tt.setupFn(h)

			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/"+tt.vaultName+"/jobs/"+tt.jobID, "", nil)
			assert.Equal(t, tt.wantStatus, rec.Code, tt.name)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 25. Handler.Reset clears archiveData cache
// ─────────────────────────────────────────────────────────────────────────────

func TestDeepen_Handler_Reset_ClearsArchiveCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "archive_data_cleared_on_reset"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDeepenHandler()
			deepenCreateVault(t, h, "cache-reset-vault")
			deepenUploadArchive(t, h, "cache-reset-vault", []byte("secret data"))

			// Reset clears everything.
			h.Reset()

			// After reset: vault no longer exists.
			rec := doRequestFull(t, h, http.MethodGet,
				"/"+deepenAccountID+"/vaults/cache-reset-vault", "", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code, tt.name)
		})
	}
}
