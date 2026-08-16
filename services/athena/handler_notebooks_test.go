package athena_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

func TestHandler_CreateNotebook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantID     bool
	}{
		{
			name:       "success",
			body:       `{"WorkGroup":"primary","Name":"my-notebook"}`,
			wantStatus: http.StatusOK,
			wantID:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateNotebook", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["NotebookId"])
			}
		})
	}
}

func TestHandler_CreatePresignedNotebookUrl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		sessionID  string
		wantStatus int
		wantURL    bool
	}{
		{
			name:       "success",
			sessionID:  "sess-abc123",
			body:       `{"SessionId":"sess-abc123"}`,
			wantStatus: http.StatusOK,
			wantURL:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreatePresignedNotebookUrl", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantURL {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["NotebookUrl"], tt.sessionID)
				assert.NotEmpty(t, resp["AuthToken"])
				assert.Positive(t, resp["AuthTokenExpirationTime"])
			}
		})
	}
}

func TestHandler_DeleteNotebook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*athena.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *athena.Handler) string {
				createRec := doRequest(t, h, "CreateNotebook", `{"WorkGroup":"primary","Name":"del-nb"}`)
				require.Equal(t, http.StatusOK, createRec.Code)

				var cr map[string]string
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))

				return cr["NotebookId"]
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *athena.Handler) string {
				return "nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			notebookID := tt.setup(h)

			rec := doRequest(t, h, "DeleteNotebook", `{"NotebookId":"`+notebookID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ExportNotebook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*athena.Handler) string
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *athena.Handler) string {
				createRec := doRequest(t, h, "CreateNotebook", `{"WorkGroup":"primary","Name":"exp-nb"}`)
				require.Equal(t, http.StatusOK, createRec.Code)

				var cr map[string]string
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))

				return cr["NotebookId"]
			},
			wantStatus: http.StatusOK,
			wantName:   "exp-nb",
		},
		{
			name: "not_found",
			setup: func(_ *athena.Handler) string {
				return "nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			notebookID := tt.setup(h)

			rec := doRequest(t, h, "ExportNotebook", `{"NotebookId":"`+notebookID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				meta, _ := resp["NotebookMetadata"].(map[string]any)
				require.NotNil(t, meta, "NotebookMetadata should be present")
				assert.Equal(t, tt.wantName, meta["Name"])
			}
		})
	}
}

// TestHandler_CreateNotebook_UnknownFieldIgnored verifies that a "Tags" key
// in the CreateNotebook request body is silently ignored rather than causing
// a failure. Unlike CreateWorkGroup/CreateDataCatalog/CreateCapacityReservation,
// the real CreateNotebookInput has no Tags field at all (only Name, WorkGroup,
// and ClientRequestToken) -- a notebook can only be tagged after creation via
// TagResource against its ARN. gopherstack previously (incorrectly) accepted
// and stored an invented Tags input here; this test locks in that a client
// sending it (as no real AWS SDK client would) doesn't break the request.
func TestHandler_CreateNotebook_UnknownFieldIgnored(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "tags_field_ignored",
			body:       `{"WorkGroup":"primary","Name":"tagged-nb","Tags":[{"Key":"env","Value":"dev"}]}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateNotebook", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Validation tests ---

func TestHandler_CreateNotebook_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "missing_workgroup",
			body:       `{"Name":"nb1"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_name",
			body:       `{"WorkGroup":"primary"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "duplicate_name_in_workgroup",
			body:       `{"WorkGroup":"primary","Name":"dup-nb"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate_name_in_workgroup" {
				rec := doRequest(t, h, "CreateNotebook", `{"WorkGroup":"primary","Name":"dup-nb"}`)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateNotebook", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func importNotebook(t *testing.T, h *athena.Handler, name string) string {
	t.Helper()

	rec := doRequest(t, h, "ImportNotebook",
		`{"WorkGroup":"primary","Name":"`+name+`","Payload":"{}"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	return jsonField(t, rec.Body.Bytes(), "NotebookId")
}

// --- Session tests ---

func TestHandler_ImportNotebook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		setup      bool
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"WorkGroup":"primary","Name":"imported","Payload":"{}"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_no_workgroup",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "validation_no_name",
			body:       `{"WorkGroup":"primary"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "validation_no_payload",
			body:       `{"WorkGroup":"primary","Name":"x"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "duplicate",
			body:       `{"WorkGroup":"primary","Name":"imported","Payload":"{}"}`,
			setup:      true,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup {
				importNotebook(t, h, "imported")
			}

			rec := doRequest(t, h, "ImportNotebook", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetNotebookMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		notebookID string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			notebookID: "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			notebookID := tt.notebookID

			if notebookID == "" {
				notebookID = importNotebook(t, h, "imp1")
			}

			rec := doRequest(t, h, "GetNotebookMetadata", `{"NotebookId":"`+notebookID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListNotebookMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "filtered_match",
			body:         `{"WorkGroup":"primary","Filters":{"Name":"imp"}}`,
			wantStatus:   http.StatusOK,
			wantContains: "imported",
		},
		{
			name:       "no_match",
			body:       `{"WorkGroup":"other"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			importNotebook(t, h, "imported")

			rec := doRequest(t, h, "ListNotebookMetadata", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestHandler_UpdateNotebook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		notebookID string
		sessionID  string
		payload    string
		wantStatus int
	}{
		{
			name:       "success",
			payload:    "new",
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_no_notebook_id",
			notebookID: "",
			payload:    "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "validation_no_payload",
			notebookID: "x",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			notebookID: "missing",
			payload:    "x",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown_session",
			payload:    "x",
			sessionID:  "missing",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "with_valid_session",
			payload:    "x",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			notebookID := tt.notebookID
			sessionID := tt.sessionID

			if tt.name == "success" || tt.name == "unknown_session" || tt.name == "with_valid_session" {
				notebookID = importNotebook(t, h, "nb-"+tt.name)
			}

			if tt.name == "with_valid_session" {
				sessionID = startSession(t, h)
			}

			body := `{"NotebookId":"` + notebookID + `","Payload":"` + tt.payload + `"`
			if sessionID != "" {
				body += `,"SessionId":"` + sessionID + `"`
			}

			body += `}`

			rec := doRequest(t, h, "UpdateNotebook", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateNotebookMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		notebookID string
		newName    string
		conflict   bool
		wantStatus int
	}{
		{
			name:       "success",
			newName:    "renamed",
			wantStatus: http.StatusOK,
		},
		{
			name:       "idempotent",
			newName:    "renamed",
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_no_notebook_id",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "validation_no_name",
			notebookID: "x",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			notebookID: "missing",
			newName:    "x",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "conflict",
			newName:    "renamed-target",
			conflict:   true,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			notebookID := tt.notebookID

			switch tt.name {
			case "success", "idempotent":
				notebookID = importNotebook(t, h, "rename-me-"+tt.name)
				if tt.name == "idempotent" {
					doRequest(t, h, "UpdateNotebookMetadata",
						`{"NotebookId":"`+notebookID+`","Name":"renamed"}`)
				}
			case "conflict":
				importNotebook(t, h, "renamed-target")
				other := doRequest(t, h, "CreateNotebook", `{"WorkGroup":"primary","Name":"other"}`)
				require.Equal(t, http.StatusOK, other.Code)
				notebookID = jsonField(t, other.Body.Bytes(), "NotebookId")
			}

			body := `{"NotebookId":"` + notebookID + `","Name":"` + tt.newName + `"}`
			rec := doRequest(t, h, "UpdateNotebookMetadata", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Named query / prepared statement tests ---
