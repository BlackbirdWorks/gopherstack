package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// dispatchNewOp sends an operation and expects HTTP 200.
func dispatchNewOp(t *testing.T, h *glue.Handler, op string, body any) map[string]any {
	t.Helper()
	rr := doGlueOp(t, h, op, body)
	if rr.Code != http.StatusOK {
		t.Fatalf("op %s: got status %d, body: %s", op, rr.Code, rr.Body.String())
	}
	var out map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	return out
}

func newGlueHandler(t *testing.T) *glue.Handler {
	t.Helper()
	b := glue.NewInMemoryBackend("123456789012", "us-east-1")

	return glue.NewHandler(b)
}

// TestNewOps_Catalog tests Catalog CRUD.
func TestNewOps_Catalog(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// CreateCatalog
	dispatchNewOp(t, h, "CreateCatalog", map[string]any{
		"CatalogId": "my-catalog",
		"CatalogInput": map[string]any{
			"Description": "test catalog",
		},
	})

	// GetCatalog
	out := dispatchNewOp(t, h, "GetCatalog", map[string]any{"CatalogId": "my-catalog"})
	catalog := out["Catalog"].(map[string]any)
	if catalog["CatalogId"] != "my-catalog" {
		t.Errorf("CatalogId mismatch: %v", catalog)
	}

	// GetCatalogs
	out2 := dispatchNewOp(t, h, "GetCatalogs", map[string]any{})
	catalogs, _ := out2["CatalogList"].([]any)
	if len(catalogs) != 1 {
		t.Errorf("expected 1 catalog, got %d", len(catalogs))
	}

	// UpdateCatalog
	dispatchNewOp(t, h, "UpdateCatalog", map[string]any{
		"CatalogId": "my-catalog",
		"CatalogInput": map[string]any{
			"Description": "updated catalog",
		},
	})

	// DeleteCatalog
	dispatchNewOp(t, h, "DeleteCatalog", map[string]any{"CatalogId": "my-catalog"})

	// Verify deletion
	dispatchNewOpExpectError(t, h, "GetCatalog", map[string]any{"CatalogId": "my-catalog"})
}

// TestNewOps_DataCatalogEncryptionSettings tests encryption settings.
func TestNewOps_DataCatalogEncryptionSettings(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// PutDataCatalogEncryptionSettings
	dispatchNewOp(t, h, "PutDataCatalogEncryptionSettings", map[string]any{
		"DataCatalogEncryptionSettings": map[string]any{
			"EncryptionAtRest": map[string]any{
				"CatalogEncryptionMode": "SSE-KMS",
				"SseAwsKmsKeyId":        "key-id",
			},
		},
	})

	// GetDataCatalogEncryptionSettings
	out := dispatchNewOp(t, h, "GetDataCatalogEncryptionSettings", map[string]any{})
	settings := out["DataCatalogEncryptionSettings"].(map[string]any)
	ear, _ := settings["EncryptionAtRest"].(map[string]any)
	if ear == nil || ear["CatalogEncryptionMode"] != "SSE-KMS" {
		t.Errorf("EncryptionAtRest mismatch: %v", settings)
	}
}

// TestCatalogImport_Lifecycle verifies that ImportCatalogToGlue sets the
// import state and GetCatalogImportStatus reflects it.
func TestCatalogImport_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		catalogID     string
		importFirst   bool
		wantCompleted bool
	}{
		{
			name:          "not_yet_imported_returns_false",
			catalogID:     "my-catalog",
			importFirst:   false,
			wantCompleted: false,
		},
		{
			name:          "after_import_returns_completed",
			catalogID:     "my-catalog",
			importFirst:   true,
			wantCompleted: true,
		},
		{
			name:          "empty_catalog_id_uses_account_default",
			catalogID:     "",
			importFirst:   true,
			wantCompleted: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.importFirst {
				rec := doGlueRequest(t, h, "ImportCatalogToGlue", map[string]any{
					"CatalogId": tc.catalogID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			getRec := doGlueRequest(t, h, "GetCatalogImportStatus", map[string]any{
				"CatalogId": tc.catalogID,
			})
			require.Equal(t, http.StatusOK, getRec.Code)

			var out struct {
				ImportStatus struct {
					ImportedBy      string  `json:"ImportedBy"`
					ImportTime      float64 `json:"ImportTime"`
					ImportCompleted bool    `json:"ImportCompleted"`
				} `json:"ImportStatus"`
			}
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
			assert.Equal(t, tc.wantCompleted, out.ImportStatus.ImportCompleted)

			if tc.wantCompleted {
				assert.NotEmpty(t, out.ImportStatus.ImportedBy)
				assert.Greater(t, out.ImportStatus.ImportTime, float64(0))
			}
		})
	}
}

// TestCatalogImport_Idempotent verifies repeated imports overwrite state.
func TestCatalogImport_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for range 3 {
		rec := doGlueRequest(t, h, "ImportCatalogToGlue", map[string]any{"CatalogId": "c1"})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	getRec := doGlueRequest(t, h, "GetCatalogImportStatus", map[string]any{"CatalogId": "c1"})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out struct {
		ImportStatus struct {
			ImportCompleted bool `json:"ImportCompleted"`
		} `json:"ImportStatus"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	assert.True(t, out.ImportStatus.ImportCompleted)
}

// TestCatalogImportStatus tests GetCatalogImportStatus and ImportCatalogToGlue.
func TestCatalogImportStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		catalogID     string
		importFirst   bool
		wantCompleted bool
	}{
		{
			name:          "default_not_imported",
			importFirst:   false,
			catalogID:     "account-level",
			wantCompleted: false,
		},
		{
			name:          "after_import_completed",
			importFirst:   true,
			catalogID:     "account-level",
			wantCompleted: true,
		},
		{
			name:          "empty_catalog_id_defaults",
			importFirst:   false,
			catalogID:     "",
			wantCompleted: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.importFirst {
				importRec := doGlueRequest(t, h, "ImportCatalogToGlue", map[string]any{
					"CatalogId": tc.catalogID,
				})
				require.Equal(t, http.StatusOK, importRec.Code)
			}

			input := map[string]any{}
			if tc.catalogID != "" {
				input["CatalogId"] = tc.catalogID
			}

			rec := doGlueRequest(t, h, "GetCatalogImportStatus", input)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				ImportStatus struct {
					ImportedBy      string  `json:"ImportedBy"`
					ImportTime      float64 `json:"ImportTime"`
					ImportCompleted bool    `json:"ImportCompleted"`
				} `json:"ImportStatus"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tc.wantCompleted, out.ImportStatus.ImportCompleted)

			if tc.wantCompleted {
				assert.NotZero(t, out.ImportStatus.ImportTime)
				assert.Equal(t, "Glue", out.ImportStatus.ImportedBy)
			}
		})
	}
}
