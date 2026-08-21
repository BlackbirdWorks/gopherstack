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

// Test_UpdateDocument_VersionCapNeverEvictsPinnedDefault locks in the fix for
// bd gopherstack-1hg: the version-cap eviction that trims documentVersions
// down to MaxDocumentVersionCap entries on each UpdateDocument must never
// evict the version currently pinned as DefaultVersion, even when it is the
// oldest entry — otherwise a caller that later requests the (explicit or
// omitted) $DEFAULT selector gets ErrInvalidDocumentVersion instead of the
// pinned content, silently orphaning the pointer.
func Test_UpdateDocument_VersionCapNeverEvictsPinnedDefault(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateDocument(ctx, &ssm.CreateDocumentInput{Name: "Doc", Content: `{"v":1}`})
	require.NoError(t, err)

	// Pin the default to version 1 -- the oldest version, and the one that
	// would be evicted first under a naive FIFO trim.
	_, err = b.UpdateDocumentDefaultVersion(ctx, &ssm.UpdateDocumentDefaultVersionInput{
		Name: "Doc", DocumentVersion: "1",
	})
	require.NoError(t, err)

	// Push well past the version cap so a naive FIFO trim would have evicted
	// version 1 many times over.
	for i := range ssm.MaxDocumentVersionCap + 50 {
		_, err = b.UpdateDocument(ctx, &ssm.UpdateDocumentInput{
			Name: "Doc", Content: `{"v":"bump-` + string(rune('a'+i%26)) + `"}`,
		})
		require.NoError(t, err)
	}

	// The store may retain one entry beyond the cap to protect the pinned
	// default -- that is the accepted tradeoff, never orphaning $DEFAULT.
	assert.LessOrEqual(t, b.DocumentVersionCount("Doc"), ssm.MaxDocumentVersionCap+1)

	out, err := b.GetDocument(ctx, &ssm.GetDocumentInput{Name: "Doc", DocumentVersion: "$DEFAULT"})
	require.NoError(t, err, "$DEFAULT must still resolve after heavy version churn")
	assert.Equal(t, `{"v":1}`, out.Content)

	desc, err := b.DescribeDocument(ctx, &ssm.DescribeDocumentInput{Name: "Doc", DocumentVersion: "$DEFAULT"})
	require.NoError(t, err)
	assert.Equal(t, "1", desc.Document.DocumentVersion)
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

// TestUpdateDocumentDefaultVersion_RequiresFields locks in that Name and
// DocumentVersion are both required on the real op (aws-sdk-go-v2/service/ssm@v1.73.4
// api_op_UpdateDocumentDefaultVersion.go) -- previously an empty body
// returned a silent empty-success stub instead of ValidationException.
func TestUpdateDocumentDefaultVersion_RequiresFields(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:    "DefVerDoc",
		Content: `{"schemaVersion":"2.2"}`,
	})
	require.NoError(t, err)

	_, err = b.UpdateDocumentDefaultVersion(context.TODO(), &ssm.UpdateDocumentDefaultVersionInput{})
	require.ErrorIs(t, err, ssm.ErrValidationException)

	_, err = b.UpdateDocumentDefaultVersion(context.TODO(), &ssm.UpdateDocumentDefaultVersionInput{
		Name: "DefVerDoc",
	})
	require.ErrorIs(t, err, ssm.ErrValidationException)

	_, err = b.UpdateDocumentDefaultVersion(context.TODO(), &ssm.UpdateDocumentDefaultVersionInput{
		DocumentVersion: "1",
	})
	require.ErrorIs(t, err, ssm.ErrValidationException)
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
		Name:            "MetaDoc",
		DocumentReviews: &ssm.DocumentReviews{Action: "SendForReview"},
	})
	require.NoError(t, err)
	assert.NotNil(t, out)
}

// TestUpdateDocumentMetadata_RequiresDocumentReviews locks in that Name and
// DocumentReviews (with a valid Action) are both required on the real op
// (aws-sdk-go-v2/service/ssm@v1.73.4 api_op_UpdateDocumentMetadata.go) --
// previously an empty body silently succeeded instead of rejecting with
// ValidationException.
func TestUpdateDocumentMetadata_RequiresDocumentReviews(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:    "MetaDoc2",
		Content: `{"schemaVersion":"2.2"}`,
	})
	require.NoError(t, err)

	_, err = b.UpdateDocumentMetadata(context.TODO(), &ssm.UpdateDocumentMetadataInput{})
	require.ErrorIs(t, err, ssm.ErrValidationException)

	_, err = b.UpdateDocumentMetadata(context.TODO(), &ssm.UpdateDocumentMetadataInput{Name: "MetaDoc2"})
	require.ErrorIs(t, err, ssm.ErrValidationException)

	_, err = b.UpdateDocumentMetadata(context.TODO(), &ssm.UpdateDocumentMetadataInput{
		Name:            "MetaDoc2",
		DocumentReviews: &ssm.DocumentReviews{Action: "NotARealAction"},
	})
	require.ErrorIs(t, err, ssm.ErrValidationException)
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
		Name:     "HistoryDoc",
		Metadata: "DocumentReviews",
	})
	require.NoError(t, err)
	assert.Equal(t, "HistoryDoc", out.Name)
	assert.NotNil(t, out.Metadata)
	assert.Empty(t, out.Metadata.ReviewerResponse)
}

// TestListDocumentMetadataHistory_RequiresNameAndMetadata locks in that Name
// and Metadata are both required on the real op (aws-sdk-go-v2/service/ssm@v1.73.4
// api_op_ListDocumentMetadataHistory.go) -- previously an empty body silently
// succeeded instead of rejecting with ValidationException.
func TestListDocumentMetadataHistory_RequiresNameAndMetadata(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:    "HistoryDoc2",
		Content: `{"schemaVersion":"2.2"}`,
	})
	require.NoError(t, err)

	_, err = b.ListDocumentMetadataHistory(context.TODO(), &ssm.ListDocumentMetadataHistoryInput{})
	require.ErrorIs(t, err, ssm.ErrValidationException)

	_, err = b.ListDocumentMetadataHistory(context.TODO(), &ssm.ListDocumentMetadataHistoryInput{
		Name: "HistoryDoc2",
	})
	require.ErrorIs(t, err, ssm.ErrValidationException)

	_, err = b.ListDocumentMetadataHistory(context.TODO(), &ssm.ListDocumentMetadataHistoryInput{
		Name:     "HistoryDoc2",
		Metadata: "NotARealValue",
	})
	require.ErrorIs(t, err, ssm.ErrValidationException)
}

func TestFull_Document_CreateGetUpdateDelete(t *testing.T) {
	t.Parallel()
	h := newHandler()

	// Create
	code, out := postJSON(t, h, "CreateDocument", map[string]any{
		"Name":         "TestDoc",
		"Content":      `{"schemaVersion":"2.2","description":"test"}`,
		"DocumentType": "Command",
	})
	assert.Equal(t, http.StatusOK, code)
	doc := out["DocumentDescription"].(map[string]any)
	assert.Equal(t, "TestDoc", doc["Name"])
	assert.Equal(t, "Active", doc["Status"])

	// Get
	code, out = postJSON(t, h, "GetDocument", map[string]any{"Name": "TestDoc"})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["Content"])

	// Describe
	code, out = postJSON(t, h, "DescribeDocument", map[string]any{"Name": "TestDoc"})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "TestDoc", out["Document"].(map[string]any)["Name"])

	// List
	code, out = postJSON(t, h, "ListDocuments", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["DocumentIdentifiers"])

	// Update → new version
	code, _ = postJSON(t, h, "UpdateDocument", map[string]any{
		"Name":            "TestDoc",
		"Content":         `{"schemaVersion":"2.2","description":"v2"}`,
		"DocumentVersion": "$LATEST",
	})
	assert.Equal(t, http.StatusOK, code)

	// ListDocumentVersions
	code, out = postJSON(t, h, "ListDocumentVersions", map[string]any{"Name": "TestDoc"})
	assert.Equal(t, http.StatusOK, code)
	versions := out["DocumentVersions"].([]any)
	assert.GreaterOrEqual(t, len(versions), 2)

	// Delete
	code, _ = postJSON(t, h, "DeleteDocument", map[string]any{"Name": "TestDoc"})
	assert.Equal(t, http.StatusOK, code)

	// Gone
	code, _ = postJSON(t, h, "GetDocument", map[string]any{"Name": "TestDoc"})
	assert.Equal(t, http.StatusBadRequest, code)
}

func TestFull_Document_UpdateDefaultVersion(t *testing.T) {
	t.Parallel()
	h := newHandler()

	postJSON(t, h, "CreateDocument", map[string]any{
		"Name":    "VersionDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})
	postJSON(t, h, "UpdateDocument", map[string]any{
		"Name":            "VersionDoc",
		"Content":         `{"schemaVersion":"2.2","v":2}`,
		"DocumentVersion": "$LATEST",
	})

	code, _ := postJSON(t, h, "UpdateDocumentDefaultVersion", map[string]any{
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
