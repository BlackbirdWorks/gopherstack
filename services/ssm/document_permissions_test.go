package ssm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestFull_Document_Permissions(t *testing.T) {
	t.Parallel()
	h := newHandler()

	postJSON(t, h, "CreateDocument", map[string]any{
		"Name":    "SharedDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})

	// Describe permissions
	code, out := postJSON(t, h, "DescribeDocumentPermission", map[string]any{
		"Name":           "SharedDoc",
		"PermissionType": "Share",
	})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["AccountIds"])

	// Modify permissions
	code, _ = postJSON(t, h, "ModifyDocumentPermission", map[string]any{
		"Name":            "SharedDoc",
		"PermissionType":  "Share",
		"AccountIdsToAdd": []string{"123456789012"},
	})
	assert.Equal(t, http.StatusOK, code)

	// Describe after modification
	code, out = postJSON(t, h, "DescribeDocumentPermission", map[string]any{
		"Name":           "SharedDoc",
		"PermissionType": "Share",
	})
	assert.Equal(t, http.StatusOK, code)
	ids := out["AccountIds"].([]any)
	assert.Contains(t, ids, "123456789012")
}

func TestDocumentPermissions_AddRemoveAccounts(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:         "SharedDoc",
		Content:      `{"schemaVersion":"2.2"}`,
		DocumentType: "Command",
	})
	require.NoError(t, err)

	// Add account permission.
	rec := doRequest(t, h, "ModifyDocumentPermission", `{
		"Name": "SharedDoc",
		"PermissionType": "Share",
		"AccountIdsToAdd": ["111111111111", "222222222222"]
	}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe — should list both accounts.
	rec = doRequest(t, h, "DescribeDocumentPermission", `{
		"Name": "SharedDoc",
		"PermissionType": "Share"
	}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "111111111111")
	assert.Contains(t, rec.Body.String(), "222222222222")

	// Remove one account.
	rec = doRequest(t, h, "ModifyDocumentPermission", `{
		"Name": "SharedDoc",
		"PermissionType": "Share",
		"AccountIdsToRemove": ["111111111111"]
	}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe — should only have second account.
	rec = doRequest(t, h, "DescribeDocumentPermission", `{
		"Name": "SharedDoc",
		"PermissionType": "Share"
	}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "111111111111")
	assert.Contains(t, rec.Body.String(), "222222222222")
}

func TestDistributorPackage_CreateAndGet(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create a Package type document (SSM Distributor).
	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:         "MyPackage",
		Content:      `{"schemaVersion":"2.0","packages":[]}`,
		DocumentType: "Package",
	})
	require.NoError(t, err)

	// GetDocument should return DocumentType=Package.
	rec := doRequest(t, h, "GetDocument", `{"Name":"MyPackage"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Package")

	// ListDocuments with DocumentType filter should include the package.
	rec = doRequest(t, h, "ListDocuments", `{"Filters":[{"Key":"DocumentType","Values":["Package"]}]}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "MyPackage")
}

func TestDistributorPackage_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		docType      string
		filterType   string
		wantInResult bool
	}{
		{
			name:         "package_matches_package_filter",
			docType:      "Package",
			filterType:   "Package",
			wantInResult: true,
		},
		{
			name:         "command_not_in_package_filter",
			docType:      "Command",
			filterType:   "Package",
			wantInResult: false,
		},
		{
			name:         "command_matches_command_filter",
			docType:      "Command",
			filterType:   "Command",
			wantInResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			docName := "filter-test-" + tt.name
			_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
				Name:         docName,
				Content:      `{"schemaVersion":"2.0"}`,
				DocumentType: tt.docType,
			})
			require.NoError(t, err)

			body, _ := json.Marshal(map[string]any{
				"Filters": []map[string]any{
					{"Key": "DocumentType", "Values": []string{tt.filterType}},
				},
			})

			rec := doRequest(t, h, "ListDocuments", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.wantInResult {
				assert.Contains(t, rec.Body.String(), docName)
			} else {
				assert.NotContains(t, rec.Body.String(), docName)
			}
		})
	}
}

func TestHandler_CreateDocument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantName   string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"Name":"MyDoc","Content":"{\"schemaVersion\":\"2.2\"}","DocumentType":"Command"}`,
			wantStatus: http.StatusOK,
			wantName:   "MyDoc",
		},
		{
			name:       "invalid_json",
			body:       `not-json`,
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CreateDocument", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var out ssm.CreateDocumentOutput
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.Equal(t, tt.wantName, out.DocumentDescription.Name)
				assert.Equal(t, "1", out.DocumentDescription.DocumentVersion)
			}
		})
	}
}

// TestHandler_GetDocument_VersionedContent verifies that requesting a specific
// version returns that version's content rather than the latest.
func TestHandler_GetDocument_VersionedContent(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()

	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{Name: "VerContent", Content: `{"v":1}`})
	require.NoError(t, err)

	_, err = b.UpdateDocument(context.TODO(), &ssm.UpdateDocumentInput{Name: "VerContent", Content: `{"v":2}`})
	require.NoError(t, err)

	// Request version "1" — should return first content
	out1, err := b.GetDocument(context.TODO(), &ssm.GetDocumentInput{Name: "VerContent", DocumentVersion: "1"})
	require.NoError(t, err)
	assert.Equal(t, `{"v":1}`, out1.Content)
	assert.Equal(t, "1", out1.DocumentVersion)

	// Request version "2" — should return second content
	out2, err := b.GetDocument(context.TODO(), &ssm.GetDocumentInput{Name: "VerContent", DocumentVersion: "2"})
	require.NoError(t, err)
	assert.Equal(t, `{"v":2}`, out2.Content)
	assert.Equal(t, "2", out2.DocumentVersion)

	// $LATEST — should return the latest (version 2)
	outLatest, err := b.GetDocument(
		context.TODO(),
		&ssm.GetDocumentInput{Name: "VerContent", DocumentVersion: "$LATEST"},
	)
	require.NoError(t, err)
	assert.Equal(t, `{"v":2}`, outLatest.Content)

	// Non-existent version — should return ErrInvalidDocumentVersion
	_, err = b.GetDocument(context.TODO(), &ssm.GetDocumentInput{Name: "VerContent", DocumentVersion: "99"})
	require.ErrorIs(t, err, ssm.ErrInvalidDocumentVersion)
}

func TestHandler_UpdateDocument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ssm.Handler)
		name       string
		body       string
		wantVer    string
		wantStatus int
	}{
		{
			name: "success_no_version",
			setup: func(t *testing.T, h *ssm.Handler) {
				t.Helper()
				doRequest(t, h, "CreateDocument", `{"Name":"UpdDoc","Content":"{}","DocumentType":"Command"}`)
			},
			body:       `{"Name":"UpdDoc","Content":"{\"updated\":true}"}`,
			wantStatus: http.StatusOK,
			wantVer:    "2",
		},
		{
			name: "success_with_latest_version",
			setup: func(t *testing.T, h *ssm.Handler) {
				t.Helper()
				doRequest(t, h, "CreateDocument", `{"Name":"UpdDocVer","Content":"{}","DocumentType":"Command"}`)
			},
			body:       `{"Name":"UpdDocVer","Content":"{\"updated\":true}","DocumentVersion":"1"}`,
			wantStatus: http.StatusOK,
			wantVer:    "2",
		},
		{
			name: "invalid_version",
			setup: func(t *testing.T, h *ssm.Handler) {
				t.Helper()
				doRequest(t, h, "CreateDocument", `{"Name":"UpdDocInv","Content":"{}","DocumentType":"Command"}`)
			},
			body:       `{"Name":"UpdDocInv","Content":"{}","DocumentVersion":"99"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			body:       `{"Name":"NoDoc","Content":"{}"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRequest(t, h, "UpdateDocument", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantVer != "" {
				var out ssm.UpdateDocumentOutput
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.Equal(t, tt.wantVer, out.DocumentDescription.DocumentVersion)
			}
		})
	}
}

func TestHandler_DescribeDocumentPermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*testing.T, *ssm.Handler)
		body       string
		wantStatus int
		wantLen    int
	}{
		{
			name: "empty_permissions",
			setup: func(t *testing.T, h *ssm.Handler) {
				t.Helper()
				doRequest(t, h, "CreateDocument", `{"Name":"PermDoc","Content":"{}"}`)
			},
			body:       `{"Name":"PermDoc","PermissionType":"Share"}`,
			wantStatus: http.StatusOK,
			wantLen:    0,
		},
		{
			name:       "not_found",
			body:       `{"Name":"NoDoc","PermissionType":"Share"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRequest(t, h, "DescribeDocumentPermission", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out ssm.DescribeDocumentPermissionOutput
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.Len(t, out.AccountIDs, tt.wantLen)
			}
		})
	}
}

func TestHandler_ModifyDocumentPermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*testing.T, *ssm.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "add_accounts",
			setup: func(t *testing.T, h *ssm.Handler) {
				t.Helper()
				doRequest(t, h, "CreateDocument", `{"Name":"ModPermDoc","Content":"{}"}`)
			},
			body:       `{"Name":"ModPermDoc","PermissionType":"Share","AccountIDsToAdd":["111111111111"]}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			body:       `{"Name":"NoDoc","PermissionType":"Share","AccountIDsToAdd":["111111111111"]}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRequest(t, h, "ModifyDocumentPermission", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListDocumentVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*testing.T, *ssm.Handler)
		body       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "single_version",
			setup: func(t *testing.T, h *ssm.Handler) {
				t.Helper()
				doRequest(t, h, "CreateDocument", `{"Name":"VerDoc","Content":"{}"}`)
			},
			body:       `{"Name":"VerDoc"}`,
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name: "multiple_versions",
			setup: func(t *testing.T, h *ssm.Handler) {
				t.Helper()
				doRequest(t, h, "CreateDocument", `{"Name":"MultiVer","Content":"{}"}`)
				doRequest(t, h, "UpdateDocument", `{"Name":"MultiVer","Content":"{\"v\":2}"}`)
				doRequest(t, h, "UpdateDocument", `{"Name":"MultiVer","Content":"{\"v\":3}"}`)
			},
			body:       `{"Name":"MultiVer"}`,
			wantStatus: http.StatusOK,
			wantCount:  3,
		},
		{
			name:       "not_found",
			body:       `{"Name":"NoDoc"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRequest(t, h, "ListDocumentVersions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCount > 0 {
				var out ssm.ListDocumentVersionsOutput
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.Len(t, out.DocumentVersions, tt.wantCount)
			}
		})
	}
}

func TestInMemoryBackend_DocumentPermissions(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	_, err := backend.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:    "PermDoc",
		Content: `{}`,
	})
	require.NoError(t, err)

	_, err = backend.ModifyDocumentPermission(context.TODO(), &ssm.ModifyDocumentPermissionInput{
		Name:            "PermDoc",
		PermissionType:  "Share",
		AccountIDsToAdd: []string{"111111111111", "222222222222"},
	})
	require.NoError(t, err)

	permOut, err := backend.DescribeDocumentPermission(context.TODO(), &ssm.DescribeDocumentPermissionInput{
		Name:           "PermDoc",
		PermissionType: "Share",
	})
	require.NoError(t, err)
	assert.Len(t, permOut.AccountIDs, 2)
	assert.Contains(t, permOut.AccountIDs, "111111111111")

	_, err = backend.ModifyDocumentPermission(context.TODO(), &ssm.ModifyDocumentPermissionInput{
		Name:               "PermDoc",
		PermissionType:     "Share",
		AccountIDsToRemove: []string{"111111111111"},
	})
	require.NoError(t, err)

	permOut2, err := backend.DescribeDocumentPermission(context.TODO(), &ssm.DescribeDocumentPermissionInput{
		Name:           "PermDoc",
		PermissionType: "Share",
	})
	require.NoError(t, err)
	assert.Len(t, permOut2.AccountIDs, 1)
	assert.Contains(t, permOut2.AccountIDs, "222222222222")
}

// TestInMemoryBackend_DeleteDocumentCleansUp previously asserted that
// deleting a still-shared document succeeded -- real AWS rejects that with
// InvalidDocumentOperation (ErrDocumentStillShared,
// deserializers.go:2225-2226: "You attempted to delete a document while it
// is still shared. You must stop sharing the document before you can delete
// it."). Corrected to prove both halves: the rejection while shared, and
// that unsharing then lets delete through and clean up.
func TestInMemoryBackend_DeleteDocumentCleansUp(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	_, err := backend.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{Name: "ToDelete", Content: "{}"})
	require.NoError(t, err)

	_, err = backend.ModifyDocumentPermission(context.TODO(), &ssm.ModifyDocumentPermissionInput{
		Name:            "ToDelete",
		PermissionType:  "Share",
		AccountIDsToAdd: []string{"123456789012"},
	})
	require.NoError(t, err)

	_, err = backend.DeleteDocument(context.TODO(), &ssm.DeleteDocumentInput{Name: "ToDelete"})
	require.ErrorIs(t, err, ssm.ErrDocumentStillShared)

	_, err = backend.ModifyDocumentPermission(context.TODO(), &ssm.ModifyDocumentPermissionInput{
		Name:               "ToDelete",
		PermissionType:     "Share",
		AccountIDsToRemove: []string{"123456789012"},
	})
	require.NoError(t, err)

	_, err = backend.DeleteDocument(context.TODO(), &ssm.DeleteDocumentInput{Name: "ToDelete"})
	require.NoError(t, err)

	_, err = backend.GetDocument(context.TODO(), &ssm.GetDocumentInput{Name: "ToDelete"})
	require.ErrorIs(t, err, ssm.ErrDocumentNotFound)
}
