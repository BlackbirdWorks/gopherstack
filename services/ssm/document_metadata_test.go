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

// Test_GetDocument_DefaultVersionSelector verifies that an explicit
// "$DEFAULT" DocumentVersion resolves to the document's DefaultVersion (as
// set by UpdateDocumentDefaultVersion), which can genuinely differ from the
// latest version. Previously $DEFAULT was conflated with $LATEST and always
// served the newest content even when the default had been pinned to an
// older version.
func Test_GetDocument_DefaultVersionSelector(t *testing.T) {
	t.Parallel()

	newBackendWithTwoVersions := func(t *testing.T) *ssm.InMemoryBackend {
		t.Helper()

		b := ssm.NewInMemoryBackend()
		ctx := context.Background()

		_, err := b.CreateDocument(ctx, &ssm.CreateDocumentInput{Name: "Doc", Content: `{"v":1}`})
		require.NoError(t, err)
		_, err = b.UpdateDocument(ctx, &ssm.UpdateDocumentInput{Name: "Doc", Content: `{"v":2}`})
		require.NoError(t, err)

		// Pin the default back to version 1, diverging it from LatestVersion (2).
		_, err = b.UpdateDocumentDefaultVersion(ctx, &ssm.UpdateDocumentDefaultVersionInput{
			Name: "Doc", DocumentVersion: "1",
		})
		require.NoError(t, err)

		return b
	}

	cases := []struct {
		name            string
		documentVersion string
		wantContent     string
	}{
		{
			name:            "explicit $DEFAULT resolves to the pinned default (v1)",
			documentVersion: "$DEFAULT",
			wantContent:     `{"v":1}`,
		},
		{
			name:            "explicit $LATEST resolves to the newest version (v2)",
			documentVersion: "$LATEST",
			wantContent:     `{"v":2}`,
		},
		{name: "explicit version 1 resolves to v1 regardless of default", documentVersion: "1", wantContent: `{"v":1}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackendWithTwoVersions(t)
			ctx := context.Background()

			out, err := b.GetDocument(ctx, &ssm.GetDocumentInput{
				Name: "Doc", DocumentVersion: tc.documentVersion,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantContent, out.Content)
		})
	}
}

// Test_DescribeDocument_OmitsContentAndHonorsVersionSelector verifies two
// wire-shape facts about DescribeDocument that CreateDocument/UpdateDocument
// share: (1) real AWS's DocumentDescription response never includes the
// document Content field — that's GetDocument's job — and (2) an explicit
// DocumentVersion selector changes the per-version metadata (DocumentVersion,
// DocumentFormat, Status) returned, not just the latest version's.
func Test_DescribeDocument_OmitsContentAndHonorsVersionSelector(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.Background()

	createOut, err := b.CreateDocument(ctx, &ssm.CreateDocumentInput{Name: "Doc", Content: `{"v":1}`})
	require.NoError(t, err)

	updateOut, err := b.UpdateDocument(ctx, &ssm.UpdateDocumentInput{Name: "Doc", Content: `{"v":2}`})
	require.NoError(t, err)

	// The wire-serialized form of DocumentDescription must not carry a
	// "Content" field at all — assert this at the JSON level since a Go
	// zero-value string would round-trip identically to "field absent" in a
	// struct-based comparison.
	createJSON, marshalErr := json.Marshal(createOut.DocumentDescription)
	require.NoError(t, marshalErr)
	assert.NotContains(t, string(createJSON), "Content",
		"CreateDocumentOutput.DocumentDescription must not serialize a Content field")

	updateJSON, marshalErr := json.Marshal(updateOut.DocumentDescription)
	require.NoError(t, marshalErr)
	assert.NotContains(t, string(updateJSON), "Content",
		"UpdateDocumentOutput.DocumentDescription must not serialize a Content field")

	cases := []struct {
		name            string
		documentVersion string
		wantVersion     string
	}{
		{name: "no version selector describes the latest version", documentVersion: "", wantVersion: "2"},
		{name: "explicit version 1 describes that older version", documentVersion: "1", wantVersion: "1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			describeOut, describeErr := b.DescribeDocument(ctx, &ssm.DescribeDocumentInput{
				Name: "Doc", DocumentVersion: tc.documentVersion,
			})
			require.NoError(t, describeErr)
			assert.Equal(t, tc.wantVersion, describeOut.Document.DocumentVersion)

			describeJSON, jsonErr := json.Marshal(describeOut.Document)
			require.NoError(t, jsonErr)
			assert.NotContains(t, string(describeJSON), "Content",
				"DescribeDocumentOutput.Document must not serialize a Content field")
		})
	}
}
func TestBackendOps_UpdateDocumentDefaultVersion(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Create a document with two versions.
	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:    "TestDoc",
		Content: `{"schemaVersion":"2.2"}`,
	})
	require.NoError(t, err)

	_, err = b.UpdateDocument(context.TODO(), &ssm.UpdateDocumentInput{
		Name:    "TestDoc",
		Content: `{"schemaVersion":"2.2","updated":true}`,
	})
	require.NoError(t, err)

	// Set default version to "1".
	out, err := b.UpdateDocumentDefaultVersion(context.TODO(), &ssm.UpdateDocumentDefaultVersionInput{
		Name:            "TestDoc",
		DocumentVersion: "1",
	})
	require.NoError(t, err)
	assert.Equal(t, "TestDoc", out.Description.Name)
	assert.Equal(t, "1", out.Description.DefaultVersion)
}
func TestBackendOps_UpdateDocumentDefaultVersion_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.UpdateDocumentDefaultVersion(context.TODO(), &ssm.UpdateDocumentDefaultVersionInput{
		Name:            "NoSuchDoc",
		DocumentVersion: "1",
	})
	require.Error(t, err)
}
func TestBackendOps_UpdateDocumentMetadata(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:    "MetaDoc",
		Content: `{"schemaVersion":"2.2"}`,
	})
	require.NoError(t, err)

	out, err := b.UpdateDocumentMetadata(context.TODO(), &ssm.UpdateDocumentMetadataInput{
		Name: "MetaDoc",
	})
	require.NoError(t, err)
	assert.NotNil(t, out)
}
func TestBackendOps_ListDocumentMetadataHistory(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:    "HistoryDoc",
		Content: `{"schemaVersion":"2.2"}`,
	})
	require.NoError(t, err)

	out, err := b.ListDocumentMetadataHistory(context.TODO(), &ssm.ListDocumentMetadataHistoryInput{
		Name: "HistoryDoc",
	})
	require.NoError(t, err)
	assert.Equal(t, "HistoryDoc", out.Name)
	assert.NotNil(t, out.Metadata)
	assert.Empty(t, out.Metadata.ReviewerResponse)
}
func TestFull_Document_CreateGetUpdateDelete(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	// Create
	code, out := mustPost(t, h, "CreateDocument", map[string]any{
		"Name":         "TestDoc",
		"Content":      `{"schemaVersion":"2.2","description":"test"}`,
		"DocumentType": "Command",
	})
	assert.Equal(t, http.StatusOK, code)
	doc := out["DocumentDescription"].(map[string]any)
	assert.Equal(t, "TestDoc", doc["Name"])
	assert.Equal(t, "Active", doc["Status"])

	// Get
	code, out = mustPost(t, h, "GetDocument", map[string]any{"Name": "TestDoc"})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["Content"])

	// Describe
	code, out = mustPost(t, h, "DescribeDocument", map[string]any{"Name": "TestDoc"})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "TestDoc", out["Document"].(map[string]any)["Name"])

	// List
	code, out = mustPost(t, h, "ListDocuments", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["DocumentIdentifiers"])

	// Update → new version
	code, _ = mustPost(t, h, "UpdateDocument", map[string]any{
		"Name":            "TestDoc",
		"Content":         `{"schemaVersion":"2.2","description":"v2"}`,
		"DocumentVersion": "$LATEST",
	})
	assert.Equal(t, http.StatusOK, code)

	// ListDocumentVersions
	code, out = mustPost(t, h, "ListDocumentVersions", map[string]any{"Name": "TestDoc"})
	assert.Equal(t, http.StatusOK, code)
	versions := out["DocumentVersions"].([]any)
	assert.GreaterOrEqual(t, len(versions), 2)

	// Delete
	code, _ = mustPost(t, h, "DeleteDocument", map[string]any{"Name": "TestDoc"})
	assert.Equal(t, http.StatusOK, code)

	// Gone
	code, _ = mustPost(t, h, "GetDocument", map[string]any{"Name": "TestDoc"})
	assert.Equal(t, http.StatusBadRequest, code)
}
func TestFull_Document_Permissions(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "CreateDocument", map[string]any{
		"Name":    "SharedDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})

	// Describe permissions
	code, out := mustPost(t, h, "DescribeDocumentPermission", map[string]any{
		"Name":           "SharedDoc",
		"PermissionType": "Share",
	})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["AccountIds"])

	// Modify permissions
	code, _ = mustPost(t, h, "ModifyDocumentPermission", map[string]any{
		"Name":            "SharedDoc",
		"PermissionType":  "Share",
		"AccountIdsToAdd": []string{"123456789012"},
	})
	assert.Equal(t, http.StatusOK, code)

	// Describe after modification
	code, out = mustPost(t, h, "DescribeDocumentPermission", map[string]any{
		"Name":           "SharedDoc",
		"PermissionType": "Share",
	})
	assert.Equal(t, http.StatusOK, code)
	ids := out["AccountIds"].([]any)
	assert.Contains(t, ids, "123456789012")
}
func TestFull_Document_UpdateDefaultVersion(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "CreateDocument", map[string]any{
		"Name":    "VersionDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})
	mustPost(t, h, "UpdateDocument", map[string]any{
		"Name":            "VersionDoc",
		"Content":         `{"schemaVersion":"2.2","v":2}`,
		"DocumentVersion": "$LATEST",
	})

	code, _ := mustPost(t, h, "UpdateDocumentDefaultVersion", map[string]any{
		"Name":            "VersionDoc",
		"DocumentVersion": "2",
	})
	assert.Equal(t, http.StatusOK, code)
}

// TestGetDocumentVersion exercises GetDocument with version branches.
func TestGetDocumentVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		documentVersion string
		wantErr         bool
	}{
		{
			name:            "latest_version",
			documentVersion: "$LATEST",
			wantErr:         false,
		},
		{
			name:            "default_version",
			documentVersion: "$DEFAULT",
			wantErr:         false,
		},
		{
			name:            "explicit_version_1",
			documentVersion: "1",
			wantErr:         false,
		},
		{
			name:            "nonexistent_version",
			documentVersion: "99",
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
				Name:    "TestVersionDoc",
				Content: `{"schemaVersion":"2.2"}`,
			})
			require.NoError(t, err)

			_, err = b.GetDocument(context.TODO(), &ssm.GetDocumentInput{
				Name:            "TestVersionDoc",
				DocumentVersion: tt.documentVersion,
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ssm.ErrInvalidDocumentVersion)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestListDocumentVersions_Pagination exercises ListDocumentVersions pagination.
func TestListDocumentVersions_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults *int64
		nextToken  string
		wantCount  int
		wantToken  bool
	}{
		{
			name:      "all_versions",
			wantCount: 3,
			wantToken: false,
		},
		{
			name: "paginate_2",
			maxResults: func() *int64 {
				v := int64(2)

				return &v
			}(),
			wantCount: 2,
			wantToken: true,
		},
		{
			name:      "beyond_end",
			nextToken: "999",
			wantCount: 0,
			wantToken: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
				Name:    "PagDoc",
				Content: `{"schemaVersion":"2.2","v":"1"}`,
			})
			require.NoError(t, err)
			_, err = b.UpdateDocument(context.TODO(), &ssm.UpdateDocumentInput{
				Name:    "PagDoc",
				Content: `{"schemaVersion":"2.2","v":"2"}`,
			})
			require.NoError(t, err)
			_, err = b.UpdateDocument(context.TODO(), &ssm.UpdateDocumentInput{
				Name:    "PagDoc",
				Content: `{"schemaVersion":"2.2","v":"3"}`,
			})
			require.NoError(t, err)

			out, err := b.ListDocumentVersions(context.TODO(), &ssm.ListDocumentVersionsInput{
				Name:       "PagDoc",
				MaxResults: tt.maxResults,
				NextToken:  tt.nextToken,
			})
			require.NoError(t, err)
			assert.Len(t, out.DocumentVersions, tt.wantCount)
			if tt.wantToken {
				assert.NotEmpty(t, out.NextToken)
			}
		})
	}
}

// TestUpdateDocument_Version exercises UpdateDocument version validation.
func TestUpdateDocument_Version(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		documentVersion string
		wantErr         bool
	}{
		{
			name:            "no_version",
			documentVersion: "",
			wantErr:         false,
		},
		{
			name:            "latest_version",
			documentVersion: "$LATEST",
			wantErr:         false,
		},
		{
			name:            "default_version",
			documentVersion: "$DEFAULT",
			wantErr:         false,
		},
		{
			name:            "explicit_current_version",
			documentVersion: "1",
			wantErr:         false,
		},
		{
			name:            "wrong_version",
			documentVersion: "99",
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
				Name:    "UpdDoc",
				Content: `{"schemaVersion":"2.2"}`,
			})
			require.NoError(t, err)

			_, err = b.UpdateDocument(context.TODO(), &ssm.UpdateDocumentInput{
				Name:            "UpdDoc",
				Content:         `{"schemaVersion":"2.2","v":"2"}`,
				DocumentVersion: tt.documentVersion,
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ssm.ErrInvalidDocumentVersion)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
func TestDocumentVersions_IsDefaultVersion(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create document with initial content v1.
	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:         "MyDoc",
		Content:      `{"schemaVersion":"2.2"}`,
		DocumentType: "Command",
	})
	require.NoError(t, err)

	// Update to create v2.
	_, err = b.UpdateDocument(context.TODO(), &ssm.UpdateDocumentInput{
		Name:            "MyDoc",
		Content:         `{"schemaVersion":"2.2","updated":true}`,
		DocumentVersion: "$LATEST",
	})
	require.NoError(t, err)

	// List versions — should show both, original default is 1.
	rec := doRequest(t, h, "ListDocumentVersions", `{"Name":"MyDoc"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	versions, ok := resp["DocumentVersions"].([]any)
	require.True(t, ok, "DocumentVersions must be an array")
	assert.GreaterOrEqual(t, len(versions), 2)

	// Explicitly set default to v2.
	body, _ := json.Marshal(map[string]any{
		"Name":            "MyDoc",
		"DocumentVersion": "2",
	})
	rec = doRequest(t, h, "UpdateDocumentDefaultVersion", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	// List versions again — v2 should be default.
	rec = doRequest(t, h, "ListDocumentVersions", `{"Name":"MyDoc"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	versions = resp["DocumentVersions"].([]any)

	var defaultFound bool
	for _, v := range versions {
		ver := v.(map[string]any)
		if ver["DocumentVersion"] == "2" {
			defaultFound = defaultFound || ver["IsDefaultVersion"].(bool)
		}
	}

	assert.True(t, defaultFound, "version 2 should be marked IsDefaultVersion after UpdateDocumentDefaultVersion")
}
func TestDocumentVersions_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setDefault   string
		wantDefault  string
		versions     int
		wantVersions int
	}{
		{
			name:         "single_version_default_is_1",
			versions:     1,
			setDefault:   "",
			wantDefault:  "1",
			wantVersions: 1,
		},
		{
			name:         "two_versions_switch_default",
			versions:     2,
			setDefault:   "2",
			wantDefault:  "2",
			wantVersions: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
				Name:         "td-doc-" + tt.name,
				Content:      `{"schemaVersion":"2.2"}`,
				DocumentType: "Command",
			})
			require.NoError(t, err)

			for i := 1; i < tt.versions; i++ {
				_, err = b.UpdateDocument(context.TODO(), &ssm.UpdateDocumentInput{
					Name:            "td-doc-" + tt.name,
					Content:         `{"schemaVersion":"2.2","v":true}`,
					DocumentVersion: "$LATEST",
				})
				require.NoError(t, err)
			}

			if tt.setDefault != "" {
				body, _ := json.Marshal(map[string]any{
					"Name":            "td-doc-" + tt.name,
					"DocumentVersion": tt.setDefault,
				})
				rec := doRequest(t, h, "UpdateDocumentDefaultVersion", string(body))
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "ListDocumentVersions",
				`{"Name":"td-doc-`+tt.name+`"}`)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			versions := resp["DocumentVersions"].([]any)
			assert.GreaterOrEqual(t, len(versions), tt.wantVersions)
		})
	}
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
func TestInMemoryBackend_DocumentVersioning(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	_, err := backend.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:    "MyDoc",
		Content: `{"v":1}`,
	})
	require.NoError(t, err)

	_, err = backend.UpdateDocument(context.TODO(), &ssm.UpdateDocumentInput{
		Name:    "MyDoc",
		Content: `{"v":2}`,
	})
	require.NoError(t, err)

	verOut, err := backend.ListDocumentVersions(context.TODO(), &ssm.ListDocumentVersionsInput{Name: "MyDoc"})
	require.NoError(t, err)
	require.Len(t, verOut.DocumentVersions, 2)
	assert.Equal(t, "1", verOut.DocumentVersions[0].DocumentVersion)
	assert.Equal(t, "2", verOut.DocumentVersions[1].DocumentVersion)
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
	require.NoError(t, err)

	_, err = backend.GetDocument(context.TODO(), &ssm.GetDocumentInput{Name: "ToDelete"})
	require.ErrorIs(t, err, ssm.ErrDocumentNotFound)
}
func TestInMemoryBackend_Snapshot_IncludesDocumentsAndCommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(*ssm.InMemoryBackend)
		verify        func(*testing.T, *ssm.InMemoryBackend)
		name          string
		skipRoundTrip bool // when true, verify receives a fresh backend and manages its own restore
	}{
		{
			name: "document_survives_round_trip",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
					Name:         "SnapDoc",
					Content:      `{"v":1}`,
					DocumentType: ssm.DocumentTypeCommand,
				})
				_, _ = b.UpdateDocument(context.TODO(), &ssm.UpdateDocumentInput{Name: "SnapDoc", Content: `{"v":2}`})
			},
			verify: func(t *testing.T, b *ssm.InMemoryBackend) {
				t.Helper()

				out, err := b.GetDocument(context.TODO(), &ssm.GetDocumentInput{Name: "SnapDoc"})
				require.NoError(t, err)
				assert.Equal(t, `{"v":2}`, out.Content)
				assert.Equal(t, "2", out.DocumentVersion)

				// Historic version content is also preserved
				v1, err := b.GetDocument(context.TODO(), &ssm.GetDocumentInput{Name: "SnapDoc", DocumentVersion: "1"})
				require.NoError(t, err)
				assert.Equal(t, `{"v":1}`, v1.Content)
			},
		},
		{
			name: "command_survives_round_trip",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.SendCommand(context.TODO(), &ssm.SendCommandInput{
					DocumentName: "AWS-RunShellScript",
					InstanceIDs:  []string{"i-snap"},
				})
			},
			verify: func(t *testing.T, b *ssm.InMemoryBackend) {
				t.Helper()

				out, err := b.ListCommands(context.TODO(), &ssm.ListCommandsInput{})
				require.NoError(t, err)
				require.Len(t, out.Commands, 1)

				inv, err := b.GetCommandInvocation(context.TODO(), &ssm.GetCommandInvocationInput{
					CommandID:  out.Commands[0].CommandID,
					InstanceID: "i-snap",
				})
				require.NoError(t, err)
				assert.Equal(t, "Success", inv.Status)
			},
		},
		{
			name:          "default_docs_restored_from_old_snapshot",
			skipRoundTrip: true,
			verify: func(t *testing.T, b *ssm.InMemoryBackend) {
				t.Helper()

				// Restore a pre-documents snapshot; defaults should be re-seeded
				oldSnap := `{"parameters":{},"history":{},"tags":{}}`
				require.NoError(t, b.Restore(t.Context(), []byte(oldSnap)))

				out, err := b.ListDocuments(context.TODO(), &ssm.ListDocumentsInput{})
				require.NoError(t, err)

				names := make([]string, 0, len(out.DocumentIdentifiers))
				for _, d := range out.DocumentIdentifiers {
					names = append(names, d.Name)
				}

				assert.Contains(t, names, "AWS-RunShellScript")
				assert.Contains(t, names, "AWS-RunPowerShellScript")
			},
		},
		{
			name: "permissions_survive_round_trip",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{Name: "PermSnap", Content: "{}"})
				_, _ = b.ModifyDocumentPermission(context.TODO(), &ssm.ModifyDocumentPermissionInput{
					Name:            "PermSnap",
					PermissionType:  "Share",
					AccountIDsToAdd: []string{"111111111111"},
				})
			},
			verify: func(t *testing.T, b *ssm.InMemoryBackend) {
				t.Helper()

				perm, err := b.DescribeDocumentPermission(context.TODO(), &ssm.DescribeDocumentPermissionInput{
					Name:           "PermSnap",
					PermissionType: "Share",
				})
				require.NoError(t, err)
				assert.Contains(t, perm.AccountIDs, "111111111111")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.skipRoundTrip {
				tt.verify(t, ssm.NewInMemoryBackend())

				return
			}

			orig := ssm.NewInMemoryBackend()
			tt.setup(orig)

			snap := orig.Snapshot(t.Context())
			require.NotNil(t, snap)

			restored := ssm.NewInMemoryBackend()
			require.NoError(t, restored.Restore(t.Context(), snap))

			tt.verify(t, restored)
		})
	}
}
