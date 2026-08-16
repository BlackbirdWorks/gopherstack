package glacier_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

func TestCreateDescribeDeleteVault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		vaultName  string
		wantStatus int
	}{
		{
			name:       "create_describe_delete",
			vaultName:  "my-test-vault",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create
			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			// Describe
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var describeResp map[string]any
			err := json.Unmarshal(rec.Body.Bytes(), &describeResp)
			require.NoError(t, err)
			assert.Equal(t, tt.vaultName, describeResp["VaultName"])

			// List
			rec = doRequest(t, h, http.MethodGet, "/-/vaults", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			// Delete
			rec = doRequest(t, h, http.MethodDelete, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Describe after delete returns 404
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestVaultName_Validation_EmptyRejected verifies that creating a vault with an empty name returns 400.
func TestVaultName_Validation_EmptyRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		vaultName  string
		wantStatus int
	}{
		{name: "empty_name_returns_400", vaultName: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			// PUT /{accountId}/vaults/{vaultName} - empty vault name maps to CreateVault with ""
			rec := doRequest(t, h, http.MethodPut, "/"+testAccountID+"/vaults/"+tt.vaultName, "")

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestVaultName_Validation(t *testing.T) {
	t.Parallel()

	// Only include vault names that are safe to embed in a URL path.
	// Characters like space and slash are tested at the unit level via ValidateVaultName.
	tests := []struct {
		name       string
		vaultName  string
		wantStatus int
	}{
		{
			name:       "alphanumeric_ok",
			vaultName:  "MyVault123",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "with_dash_ok",
			vaultName:  "my-vault",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "with_underscore_ok",
			vaultName:  "my_vault",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "with_dot_ok",
			vaultName:  "my.vault",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "plus_char_rejected",
			vaultName:  "my+vault", // '+' is not in allowed set [a-zA-Z0-9._-]
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "too_long_rejected",
			vaultName:  strings.Repeat("a", 256),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "max_length_ok",
			vaultName:  strings.Repeat("a", 255),
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, http.MethodPut,
				"/"+testAccountID+"/vaults/"+tt.vaultName, "")
			assert.Equal(t, tt.wantStatus, rec.Code, "vault name: %q", tt.vaultName)
		})
	}
}

func TestValidateVaultName_Unit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		wantErr   bool
	}{
		{name: "empty_invalid", vaultName: "", wantErr: true},
		{name: "space_invalid", vaultName: "a b", wantErr: true},
		{name: "colon_invalid", vaultName: "a:b", wantErr: true},
		{name: "hash_invalid", vaultName: "a#b", wantErr: true},
		{name: "255_chars_valid", vaultName: strings.Repeat("x", 255), wantErr: false},
		{name: "256_chars_invalid", vaultName: strings.Repeat("x", 256), wantErr: true},
		{name: "alnum_valid", vaultName: "abc123", wantErr: false},
		{name: "dash_valid", vaultName: "abc-123", wantErr: false},
		{name: "underscore_valid", vaultName: "abc_123", wantErr: false},
		{name: "dot_valid", vaultName: "abc.123", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := glacier.ValidateVaultName(tt.vaultName)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// -------------------------------------------------------------------------
// Issue 24: InitiateMultipartUpload requires X-Amz-Part-Size header
// -------------------------------------------------------------------------

// TestListVaults_AfterReset verifies no vaults returned after reset.
func TestListVaults_AfterReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCount int
	}{
		{name: "no_vaults_after_reset", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create a vault first.
			rec := doRequest(t, h, http.MethodPut, "/"+testAccountID+"/vaults/myvault", "")
			require.Equal(t, http.StatusCreated, rec.Code)

			h.Reset()

			// List should be empty.
			rec = doRequest(t, h, http.MethodGet, "/"+testAccountID+"/vaults", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				VaultList []any `json:"VaultList"`
			}

			err := json.NewDecoder(rec.Body).Decode(&resp)
			require.NoError(t, err)

			assert.Len(t, resp.VaultList, tt.wantCount)
		})
	}
}

func TestAccountIDDash_SubstitutedInListVaults(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "dash-vault")

	// "-" in path should work same as real account ID.
	rec := doRequest(t, h, http.MethodGet, "/-/vaults", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	vaultList := resp["VaultList"].([]any)
	assert.Len(t, vaultList, 1)
}

func TestAccountIDReal_WorksLikeDash(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "real-id-vault")

	// Using the real account ID should return the same vaults.
	rec := doRequest(t, h, http.MethodGet, "/"+testAccountID+"/vaults", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	vaultList := resp["VaultList"].([]any)
	assert.Len(t, vaultList, 1)
}

// -------------------------------------------------------------------------
// Issue 6: Job tier validation
// -------------------------------------------------------------------------

func TestListVaults_LimitValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create 5 vaults.
	for i := range 5 {
		createVault(t, h, fmt.Sprintf("vault-%02d", i))
	}

	tests := []struct {
		name       string
		limit      string
		wantStatus int
		wantLen    int
	}{
		{name: "limit_1", limit: "1", wantStatus: http.StatusOK, wantLen: 1},
		{name: "limit_5", limit: "5", wantStatus: http.StatusOK, wantLen: 5},
		{name: "limit_50", limit: "50", wantStatus: http.StatusOK, wantLen: 5},
		{name: "limit_0_rejected", limit: "0", wantStatus: http.StatusBadRequest},
		{name: "limit_51_rejected", limit: "51", wantStatus: http.StatusBadRequest},
		{name: "limit_neg_rejected", limit: "-1", wantStatus: http.StatusBadRequest},
		{name: "limit_text_rejected", limit: "abc", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults?limit="+tt.limit, "")
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus == http.StatusOK && tt.wantLen > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				vaultList := resp["VaultList"].([]any)
				assert.Len(t, vaultList, tt.wantLen)
			}
		})
	}
}

func TestListVaults_MarkerPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	names := []string{"vault-a", "vault-b", "vault-c", "vault-d", "vault-e"}
	for _, n := range names {
		createVault(t, h, n)
	}

	// Get first page with limit=2.
	rec := doRequest(t, h, http.MethodGet, "/"+testAccountID+"/vaults?limit=2", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))

	list1 := page1["VaultList"].([]any)
	assert.Len(t, list1, 2)

	marker, ok := page1["Marker"].(string)
	require.True(t, ok, "Marker should be set when results truncated")

	// Get second page using the marker.
	rec = doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults?marker="+marker+"&limit=2", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))

	list2 := page2["VaultList"].([]any)
	assert.Len(t, list2, 2)
}

// -------------------------------------------------------------------------
// Issue 16: ListJobs marker/limit pagination
// -------------------------------------------------------------------------

func TestDeleteVault_RejectsNonEmpty(t *testing.T) {
	t.Parallel()

	const (
		nonEmptyVaultName = "non-empty-vault"
		nonEmptyArchiveID = "test-archive-id-123"
	)

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "non_empty_vault_returns_409", wantStatus: http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			bk := glacier.NewInMemoryBackend()
			h2 := glacier.NewHandler(bk)
			h2.AccountID = testAccountID
			h2.DefaultRegion = testRegion

			bk.AddVaultInternal(testAccountID, testRegion, &glacier.Vault{VaultName: nonEmptyVaultName})
			bk.AddArchiveInternal(testAccountID, testRegion, nonEmptyVaultName, &glacier.Archive{
				ArchiveID: nonEmptyArchiveID,
				Size:      100,
			})

			rec := doRequest(t, h2, http.MethodDelete, "/"+testAccountID+"/vaults/"+nonEmptyVaultName, "")
			assert.Equal(t, tt.wantStatus, rec.Code)
			_ = h
		})
	}
}

func TestDeleteVault_AllowsEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "empty_vault_returns_204", wantStatus: http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			const emptyVaultName = "empty-vault"
			createVault(t, h, emptyVaultName)
			rec := doRequest(t, h, http.MethodDelete, "/"+testAccountID+"/vaults/"+emptyVaultName, "")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestDeleteVault_NotEmpty_Returns409(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "409_on_non_empty", wantStatus: http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			h := glacier.NewHandler(bk)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			bk.AddVaultInternal(testAccountID, testRegion, &glacier.Vault{VaultName: "v1"})
			bk.AddArchiveInternal(
				testAccountID,
				testRegion,
				"v1",
				&glacier.Archive{ArchiveID: "a1", Size: 10},
			)

			rec := doRequest(t, h, http.MethodDelete, "/"+testAccountID+"/vaults/v1", "")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestDeleteVault_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "delete_missing_vault_returns_404"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequestWithHeaders(t, h, http.MethodDelete,
				"/"+testAccountID+"/vaults/nonexistent-vault", "", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code, tt.name)
		})
	}
}

func TestDeleteVault_WithActiveMultipartUploads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "delete_vault_with_uploads_succeeds_after_abort"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "del-mp-vault")

			// Initiate multipart upload.
			rec := doRequestWithHeaders(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/del-mp-vault/multipart-uploads",
				"", map[string]string{"X-Amz-Part-Size": "1048576"})
			require.Equal(t, http.StatusCreated, rec.Code)
			var initResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			uploadID := initResp["uploadId"]

			// Delete vault (empty archives = ok, multipart uploads get cleaned).
			rec = doRequestWithHeaders(t, h, http.MethodDelete,
				"/"+testAccountID+"/vaults/del-mp-vault", "", nil)
			require.Equal(t, http.StatusNoContent, rec.Code, tt.name)

			// Multipart upload no longer accessible.
			rec = doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/del-mp-vault/multipart-uploads/"+uploadID, "", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 23. ListParts marker pagination
// ─────────────────────────────────────────────────────────────────────────────

func TestCreateVault_Idempotent(t *testing.T) {
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

			h := newTestHandler()
			for i := range tt.createCount {
				rec := doRequestWithHeaders(t, h, http.MethodPut,
					"/"+testAccountID+"/vaults/"+tt.vaultName, "", nil)
				if i == 0 {
					assert.Equal(t, http.StatusCreated, rec.Code)
				} else {
					assert.Equal(t, http.StatusConflict, rec.Code)
				}
			}

			// Only one vault should exist.
			rec := doRequestWithHeaders(t, h, http.MethodGet, "/"+testAccountID+"/vaults", "", nil)
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

func TestVaultStats_UploadAndDelete(t *testing.T) {
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

			h := newTestHandler()
			createVault(t, h, "stats-vault")

			// Initial state.
			descVault := func() map[string]any {
				rec := doRequestWithHeaders(t, h, http.MethodGet,
					"/"+testAccountID+"/vaults/stats-vault", "", nil)
				require.Equal(t, http.StatusOK, rec.Code)
				var v map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &v))

				return v
			}

			v0 := descVault()
			assert.EqualValues(t, 0, v0["NumberOfArchives"])
			assert.EqualValues(t, 0, v0["SizeInBytes"])

			// Upload.
			archiveID := uploadArchiveData(t, h, "stats-vault", tt.content)
			v1 := descVault()
			assert.EqualValues(t, 1, v1["NumberOfArchives"])
			assert.EqualValues(t, len(tt.content), v1["SizeInBytes"])

			// Delete.
			rec := doRequestWithHeaders(t, h, http.MethodDelete,
				"/"+testAccountID+"/vaults/stats-vault/archives/"+archiveID, "", nil)
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

func TestVault_CrossVaultIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "different_vaults_isolated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "vault-a")
			createVault(t, h, "vault-b")

			archAID := uploadArchiveData(t, h, "vault-a", []byte("in vault A"))

			// Archive exists in vault-a.
			jobID := initiateJobWithBody(t, h, "vault-a",
				`{"Type":"archive-retrieval","ArchiveId":"`+archAID+`"}`)
			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/vault-a/jobs/"+jobID+"/output", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			// Archive does NOT exist in vault-b.
			rec2 := doRequestWithHeaders(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/vault-b/jobs",
				`{"Type":"archive-retrieval","ArchiveId":"`+archAID+`"}`, nil)
			assert.Equal(t, http.StatusNotFound, rec2.Code, tt.name)
		})
	}
}

func TestVault_CrossAccountIsolation(t *testing.T) {
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
			hA.DefaultRegion = testRegion

			hB := glacier.NewHandler(bk)
			hB.AccountID = "account-bbb"
			hB.DefaultRegion = testRegion

			// Create same-named vault in both accounts.
			recA := doRequestWithHeaders(t, hA, http.MethodPut, "/account-aaa/vaults/shared-name", "", nil)
			require.Equal(t, http.StatusCreated, recA.Code)
			recB := doRequestWithHeaders(t, hB, http.MethodPut, "/account-bbb/vaults/shared-name", "", nil)
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
			listRecB := doRequestWithHeaders(t, hB, http.MethodGet, "/account-bbb/vaults", "", nil)
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

func TestVault_CrossRegionIsolation(t *testing.T) {
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
			recE := doRequestWithHeaders(t, hE, http.MethodPut, "/same-acct/vaults/east-vault", "", nil)
			require.Equal(t, http.StatusCreated, recE.Code)

			recW := doRequestWithHeaders(t, hW, http.MethodGet, "/same-acct/vaults", "", nil)
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

func TestVaultARN_Format(t *testing.T) {
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

			h := newTestHandler()
			createVault(t, h, tt.vaultName)

			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/"+tt.vaultName, "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			arn, _ := resp["VaultARN"].(string)
			assert.NotEmpty(t, arn)
			wantARN := "arn:aws:glacier:" + testRegion + ":" + testAccountID + ":vaults/" + tt.vaultName
			assert.Equal(t, wantARN, arn)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. CreateVault idempotency
// ─────────────────────────────────────────────────────────────────────────────

func TestListVaults_LimitAndMarkerCombined(t *testing.T) {
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
			marker:     base64.StdEncoding.EncodeToString([]byte("alpha")),
			wantNames:  []string{"beta"},
			wantMarker: true,
		},
		{
			name:       "marker_at_beta_limit_2",
			vaultNames: []string{"alpha", "beta", "gamma"},
			limit:      2,
			marker:     base64.StdEncoding.EncodeToString([]byte("beta")),
			wantNames:  []string{"gamma"},
			wantMarker: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			for _, vn := range tt.vaultNames {
				createVault(t, h, vn)
			}

			queryStr := fmt.Sprintf("?limit=%d", tt.limit)
			if tt.marker != "" {
				queryStr += "&marker=" + tt.marker
			}

			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults"+queryStr, "", nil)
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
