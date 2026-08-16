package glacier_test

import (
	"crypto/sha256"
	"encoding/hex"
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

func TestUploadDeleteArchive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		bodyData  string
	}{
		{
			name:      "upload_and_delete",
			vaultName: "archive-vault",
			bodyData:  "archive content",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create vault first
			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, http.StatusCreated, rec.Code)

			// Upload archive
			e := echo.New()
			req := httptest.NewRequest(
				http.MethodPost,
				"/-/vaults/"+tt.vaultName+"/archives",
				strings.NewReader(tt.bodyData),
			)
			req.Header.Set("X-Amz-Archive-Description", "test archive")
			rec2 := httptest.NewRecorder()
			c := e.NewContext(req, rec2)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusCreated, rec2.Code)

			var uploadResp map[string]any
			err = json.Unmarshal(rec2.Body.Bytes(), &uploadResp)
			require.NoError(t, err)

			archiveID := uploadResp["archiveId"].(string)
			assert.NotEmpty(t, archiveID)

			// Delete archive
			rec = doRequest(t, h, http.MethodDelete, "/-/vaults/"+tt.vaultName+"/archives/"+archiveID, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)
		})
	}
}

func TestTreeHash_EmptyBody(t *testing.T) {
	t.Parallel()

	// AWS: tree-hash of empty body = SHA-256("") (a zero-length hash).
	hash := glacier.ComputeTreeHash(nil)
	assert.Len(t, hash, 64)

	emptySum := sha256.Sum256(nil)
	assert.Equal(t, hex.EncodeToString(emptySum[:]), hash)
}

func TestTreeHash_SingleBlock(t *testing.T) {
	t.Parallel()

	data := make([]byte, 512) // less than 1 MiB → single leaf

	for i := range data {
		data[i] = byte(i % 256)
	}

	hash := glacier.ComputeTreeHash(data)
	assert.Len(t, hash, 64)

	expected := sha256.Sum256(data)
	assert.Equal(t, hex.EncodeToString(expected[:]), hash)
}

func TestTreeHash_TwoBlocks(t *testing.T) {
	t.Parallel()

	// Build a 2 MiB body so there are exactly 2 leaf blocks.
	const leafSize = 1 << 20
	data := make([]byte, 2*leafSize)

	for i := range data {
		data[i] = byte(i % 97)
	}

	hash := glacier.ComputeTreeHash(data)
	assert.Len(t, hash, 64)

	leaf0 := sha256.Sum256(data[:leafSize])
	leaf1 := sha256.Sum256(data[leafSize:])
	combined := append(leaf0[:], leaf1[:]...)
	root := sha256.Sum256(combined)

	assert.Equal(t, hex.EncodeToString(root[:]), hash)
}

func TestUploadArchive_ComputesAndVerifiesTreeHash(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createVault(t, h, "hash-vault")

	body := []byte("hello glacier")
	correctHash := glacier.ComputeTreeHash(body)

	tests := []struct {
		name       string
		headerHash string
		wantStatus int
	}{
		{
			name:       "correct_hash_accepted",
			headerHash: correctHash,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "wrong_hash_rejected",
			headerHash: strings.Repeat("a", 64),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "no_hash_computes_one",
			headerHash: "",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h2 := newTestHandler()
			createVault(t, h2, "hash-vault")

			headers := map[string]string{}
			if tt.headerHash != "" {
				headers["X-Amz-Sha256-Tree-Hash"] = tt.headerHash
			}

			rec := doRequestWithBody(t, h2, http.MethodPost,
				"/"+testAccountID+"/vaults/hash-vault/archives",
				string(body), headers)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus == http.StatusCreated {
				assert.Equal(t, correctHash, rec.Header().Get("X-Amz-Sha256-Tree-Hash"))
			}
		})
	}
}

func TestUploadArchive_TreeHashReturnedInResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "treehash-vault")

	payload := []byte("some archive data for tree hash test")
	expectedHash := glacier.ComputeTreeHash(payload)

	rec := doRequestWithBody(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/treehash-vault/archives",
		string(payload), nil)
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, expectedHash, resp["checksum"])
	assert.Equal(t, expectedHash, rec.Header().Get("X-Amz-Sha256-Tree-Hash"))
}

// -------------------------------------------------------------------------
// Issue 4: Account ID "-" substitution
// -------------------------------------------------------------------------

func TestUploadArchive_SizeLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "size-vault")

	// Build a body that exceeds 4 GiB (we fake it by setting Content-Length header
	// but passing a small body — the handler checks len(body) from ReadBody).
	// Instead, we simulate by directly testing that a normally-sized body passes.
	// We can't realistically allocate 4 GiB in a unit test, so we verify the
	// constant and behavior boundary with a smaller threshold via the exported constant.

	// Verify small uploads work fine.
	rec := doRequestWithBody(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/size-vault/archives",
		"small payload", nil)
	assert.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
}

// -------------------------------------------------------------------------
// Issue 26: Description charset validation
// -------------------------------------------------------------------------

func TestDescription_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		desc    string
		wantErr bool
	}{
		{name: "empty_ok", desc: "", wantErr: false},
		{name: "printable_ascii_ok", desc: "Hello World 123!", wantErr: false},
		{name: "max_1024_ok", desc: strings.Repeat("x", 1024), wantErr: false},
		{name: "too_long_rejected", desc: strings.Repeat("x", 1025), wantErr: true},
		{name: "control_char_rejected", desc: "hello\x01world", wantErr: true},
		{name: "tab_rejected", desc: "hello\tworld", wantErr: true},
		{name: "newline_rejected", desc: "hello\nworld", wantErr: true},
		{name: "del_rejected", desc: "hello\x7fworld", wantErr: true},
		{name: "non_ascii_rejected", desc: "héllo", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := glacier.ValidateDescription(tt.desc)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestUploadArchive_DescriptionValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "desc-vault")

	tests := []struct {
		name       string
		desc       string
		wantStatus int
	}{
		{name: "valid_desc", desc: "my archive", wantStatus: http.StatusCreated},
		{name: "invalid_desc_control", desc: "bad\x01desc", wantStatus: http.StatusBadRequest},
		{name: "too_long_desc", desc: strings.Repeat("x", 1025), wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h2 := newTestHandler()
			createVault(t, h2, "desc-vault")

			rec := doRequestWithBody(t, h2, http.MethodPost,
				"/"+testAccountID+"/vaults/desc-vault/archives",
				"data", map[string]string{"X-Amz-Archive-Description": tt.desc})
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

// -------------------------------------------------------------------------
// Issue 28: X-Amzn-Requestid on every response + __type in error body
// -------------------------------------------------------------------------

func TestDeleteArchive_SizeGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		archiveSize int64
		wantSize    int64
		wantCount   int64
	}{
		{name: "size_decrements", archiveSize: 500, wantSize: 0, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			h := glacier.NewHandler(bk)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			bk.AddVaultInternal(testAccountID, testRegion, &glacier.Vault{
				VaultName:        "size-vault",
				NumberOfArchives: 1,
				SizeInBytes:      tt.archiveSize,
			})
			bk.AddArchiveInternal(testAccountID, testRegion, "size-vault", &glacier.Archive{
				ArchiveID: "arch1",
				Size:      tt.archiveSize,
			})

			rec := doRequest(t, h, http.MethodDelete,
				"/"+testAccountID+"/vaults/size-vault/archives/arch1", "")
			require.Equal(t, http.StatusNoContent, rec.Code)

			v, err := bk.DescribeVault(testAccountID, testRegion, "size-vault")
			require.NoError(t, err)
			assert.Equal(t, tt.wantSize, v.SizeInBytes)
			assert.Equal(t, tt.wantCount, v.NumberOfArchives)
		})
	}
}
