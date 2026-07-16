package ssm_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func makeLevels(n int) []string {
	levels := make([]string, n)
	for i := range levels {
		levels[i] = fmt.Sprintf("L%d", i)
	}

	return levels
}

// countParameterHistory paginates through GetParameterHistory (page size 50)
// and returns the total number of history entries for name.
func countParameterHistory(ctx context.Context, t *testing.T, b *ssm.InMemoryBackend, name string) int {
	t.Helper()

	total := 0
	nextToken := ""

	for {
		out, err := b.GetParameterHistory(ctx, &ssm.GetParameterHistoryInput{
			Name:      name,
			NextToken: nextToken,
		})
		require.NoError(t, err)

		total += len(out.Parameters)
		if out.NextToken == "" {
			break
		}

		nextToken = out.NextToken
	}

	return total
}
func TestCreateDocument_WithRequires(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	postAudit1(t, h, "CreateDocument", map[string]any{
		"Name":    "BaseDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})

	code, out := postAudit1(t, h, "CreateDocument", map[string]any{
		"Name":    "DerivedDoc",
		"Content": `{"schemaVersion":"2.2"}`,
		"Requires": []map[string]any{
			{"Name": "BaseDoc", "Version": "1"},
		},
	})

	assert.Equal(t, http.StatusOK, code)
	doc := out["DocumentDescription"].(map[string]any)
	requires, ok := doc["Requires"].([]any)
	require.True(t, ok, "Requires field should be present")
	assert.Len(t, requires, 1)
	assert.Equal(t, "BaseDoc", requires[0].(map[string]any)["Name"])
}
func TestCreateDocument_WithAttachments(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, out := postAudit1(t, h, "CreateDocument", map[string]any{
		"Name":    "DocWithAttach",
		"Content": `{"schemaVersion":"2.2"}`,
		"Attachments": []map[string]any{
			{"Key": "SourceUrl", "Values": []string{"https://example.com/script.ps1"}, "Name": "script"},
		},
	})

	assert.Equal(t, http.StatusOK, code)
	// The document should be created successfully even with Attachments source.
	require.NotNil(t, out["DocumentDescription"])
}
func TestCreateAssociation_DocumentNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "nonexistent_document",
			body:       `{"Name":"NoSuchDoc","InstanceId":"i-abc"}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "InvalidDocument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CreateAssociation", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantErr)
		})
	}
}

// TestRefinement2_DispatchTableCached verifies handler caches the dispatch table.
func TestDispatchTableCached(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "dispatch_table_works_after_creation"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ssm.NewHandler(ssm.NewInMemoryBackend())
			// Verify that multiple calls to the handler (which uses the cached ops) work fine
			rec1 := doRequest(t, h, "ListDocuments", `{}`)
			assert.Equal(t, http.StatusOK, rec1.Code)

			rec2 := doRequest(t, h, "ListDocuments", `{}`)
			assert.Equal(t, http.StatusOK, rec2.Code)

			_ = tt.name
		})
	}
}
func TestFull_Document_DuplicateCreate(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "CreateDocument", map[string]any{
		"Name":    "DupDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})

	code, _ := mustPost(t, h, "CreateDocument", map[string]any{
		"Name":    "DupDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})
	assert.Equal(t, http.StatusBadRequest, code)
}
func TestFull_Command_S3Output(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "CreateDocument", map[string]any{
		"Name":    "S3Doc",
		"Content": `{"schemaVersion":"2.2"}`,
	})

	code, out := mustPost(t, h, "SendCommand", map[string]any{
		"DocumentName":       "S3Doc",
		"InstanceIds":        []string{"i-s3"},
		"OutputS3BucketName": "results-bucket",
		"OutputS3KeyPrefix":  "cmd/output/",
		"TimeoutSeconds":     300,
	})
	assert.Equal(t, http.StatusOK, code)
	cmd := out["Command"].(map[string]any)
	assert.Equal(t, "results-bucket", cmd["OutputS3BucketName"])
	assert.Equal(t, "cmd/output/", cmd["OutputS3KeyPrefix"])
}
func TestFull_Association_Batch(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "CreateDocument", map[string]any{
		"Name":    "BatchDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})

	code, out := mustPost(t, h, "CreateAssociationBatch", map[string]any{
		"Entries": []map[string]any{
			{"Name": "BatchDoc", "InstanceId": "i-b1"},
			{"Name": "BatchDoc", "InstanceId": "i-b2"},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	successful := out["Successful"].([]any)
	assert.Len(t, successful, 2)
}

// TestDocumentMatchesFilters exercises the documentMatchesFilters branches.
func TestDocumentMatchesFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filters    []ssm.DocumentFilter
		wantResult int
	}{
		{
			name:       "no_filters_all_returned",
			filters:    nil,
			wantResult: 3, // 2 defaults + 1 created
		},
		{
			name: "filter_by_document_type",
			filters: []ssm.DocumentFilter{
				{Key: "DocumentType", Values: []string{"Command"}},
			},
			wantResult: 3, // default docs are Command type too
		},
		{
			name: "filter_by_name",
			filters: []ssm.DocumentFilter{
				{Key: "Name", Values: []string{"TestDoc"}},
			},
			wantResult: 1,
		},
		{
			name: "filter_no_match",
			filters: []ssm.DocumentFilter{
				{Key: "Name", Values: []string{"NonExistentDoc"}},
			},
			wantResult: 0,
		},
		{
			name: "unknown_filter_key_ignored",
			filters: []ssm.DocumentFilter{
				{Key: "UnknownKey", Values: []string{"anything"}},
			},
			wantResult: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
				Name:    "TestDoc",
				Content: `{"schemaVersion":"2.2"}`,
			})
			require.NoError(t, err)

			out, err := b.ListDocuments(context.TODO(), &ssm.ListDocumentsInput{
				Filters: tt.filters,
			})
			require.NoError(t, err)
			assert.Len(t, out.DocumentIdentifiers, tt.wantResult)
		})
	}
}

// TestProvider_NilContext exercises the nil-context error path.
func TestProvider_NilContext(t *testing.T) {
	t.Parallel()

	p := &ssm.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrNilAppContext)
}

// TestListDocuments_Pagination exercises ListDocuments pagination.
func TestListDocuments_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults *int64
		nextToken  string
		wantEmpty  bool
	}{
		{
			name: "paginate_1",
			maxResults: func() *int64 {
				v := int64(1)

				return &v
			}(),
			wantEmpty: false,
		},
		{
			name:      "beyond_end",
			nextToken: "999",
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			// There are already 2 default docs
			out, err := b.ListDocuments(context.TODO(), &ssm.ListDocumentsInput{
				MaxResults: tt.maxResults,
				NextToken:  tt.nextToken,
			})
			require.NoError(t, err)
			if tt.wantEmpty {
				assert.Empty(t, out.DocumentIdentifiers)
			} else {
				assert.NotEmpty(t, out.DocumentIdentifiers)
				assert.NotEmpty(t, out.NextToken)
			}
		})
	}
}
func TestGetCalendarState_WithCalendarDocument(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create a ChangeCalendar document.
	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:         "MyCalendar",
		Content:      `{"type":"DEFAULT_OPEN"}`,
		DocumentType: "ChangeCalendar",
	})
	require.NoError(t, err)

	// GetCalendarState with the calendar name should succeed.
	body, _ := json.Marshal(map[string]any{
		"CalendarNames": []string{"MyCalendar"},
	})
	rec := doRequest(t, h, "GetCalendarState", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "OPEN")
}
func TestGetCalendarState_NonCalendarDocumentReturnsError(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create a non-calendar document.
	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:         "NotACalendar",
		Content:      `{"schemaVersion":"2.2"}`,
		DocumentType: "Command",
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{
		"CalendarNames": []string{"NotACalendar"},
	})
	rec := doRequest(t, h, "GetCalendarState", string(body))
	assert.NotEqual(t, http.StatusOK, rec.Code, "non-ChangeCalendar doc should return error")
}
func TestHandler_CreateDocument_Duplicate(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	body := `{"Name":"DupDoc","Content":"{}","DocumentType":"Command"}`

	rec := doRequest(t, h, "CreateDocument", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, "CreateDocument", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	var errResp map[string]string
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&errResp))
	assert.Contains(t, errResp["__type"], "DocumentAlreadyExists")
}
func TestHandler_GetDocument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ssm.Handler)
		name       string
		body       string
		wantDoc    string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ssm.Handler) {
				t.Helper()
				doRequest(
					t,
					h,
					"CreateDocument",
					`{"Name":"TestDoc","Content":"{\"schemaVersion\":\"2.2\"}","DocumentType":"Command"}`,
				)
			},
			body:       `{"Name":"TestDoc"}`,
			wantStatus: http.StatusOK,
			wantDoc:    "TestDoc",
		},
		{
			name:       "not_found",
			body:       `{"Name":"Missing"}`,
			wantStatus: http.StatusBadRequest,
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
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRequest(t, h, "GetDocument", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantDoc != "" {
				var out ssm.GetDocumentOutput
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.Equal(t, tt.wantDoc, out.Name)
			}
		})
	}
}
func TestHandler_DescribeDocument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ssm.Handler)
		name       string
		body       string
		wantDoc    string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ssm.Handler) {
				t.Helper()
				doRequest(t, h, "CreateDocument", `{"Name":"DescDoc","Content":"{}","DocumentType":"Automation"}`)
			},
			body:       `{"Name":"DescDoc"}`,
			wantStatus: http.StatusOK,
			wantDoc:    "DescDoc",
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

			rec := doRequest(t, h, "DescribeDocument", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantDoc != "" {
				var out ssm.DescribeDocumentOutput
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.Equal(t, tt.wantDoc, out.Document.Name)
			}
		})
	}
}
func TestHandler_ListDocuments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*testing.T, *ssm.Handler)
		body       string
		wantStatus int
		wantMin    int
	}{
		{
			name:       "empty_has_defaults",
			body:       `{}`,
			wantStatus: http.StatusOK,
			wantMin:    2, // AWS-RunShellScript and AWS-RunPowerShellScript
		},
		{
			name: "with_custom_doc",
			setup: func(t *testing.T, h *ssm.Handler) {
				t.Helper()
				doRequest(t, h, "CreateDocument", `{"Name":"ListDoc","Content":"{}","DocumentType":"Command"}`)
			},
			body:       `{}`,
			wantStatus: http.StatusOK,
			wantMin:    3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRequest(t, h, "ListDocuments", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out ssm.ListDocumentsOutput
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.GreaterOrEqual(t, len(out.DocumentIdentifiers), tt.wantMin)
		})
	}
}
func TestHandler_DeleteDocument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*testing.T, *ssm.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ssm.Handler) {
				t.Helper()
				doRequest(t, h, "CreateDocument", `{"Name":"DelDoc","Content":"{}","DocumentType":"Command"}`)
			},
			body:       `{"Name":"DelDoc"}`,
			wantStatus: http.StatusOK,
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

			rec := doRequest(t, h, "DeleteDocument", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
func TestInMemoryBackend_DefaultDocuments(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	out, err := backend.ListDocuments(context.TODO(), &ssm.ListDocumentsInput{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.DocumentIdentifiers), 2)

	names := make([]string, 0, len(out.DocumentIdentifiers))
	for _, d := range out.DocumentIdentifiers {
		names = append(names, d.Name)
	}

	assert.Contains(t, names, "AWS-RunShellScript")
	assert.Contains(t, names, "AWS-RunPowerShellScript")
}
