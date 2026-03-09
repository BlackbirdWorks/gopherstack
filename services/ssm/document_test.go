package ssm_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// --- CreateDocument ---

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

// --- GetDocument ---

func TestHandler_GetDocument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*ssm.Handler)
		name       string
		body       string
		wantDoc    string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *ssm.Handler) {
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
				tt.setup(h)
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

// --- DescribeDocument ---

func TestHandler_DescribeDocument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*ssm.Handler)
		name       string
		body       string
		wantDoc    string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *ssm.Handler) {
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
				tt.setup(h)
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

// --- ListDocuments ---

func TestHandler_ListDocuments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*ssm.Handler)
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
			setup: func(h *ssm.Handler) {
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
				tt.setup(h)
			}

			rec := doRequest(t, h, "ListDocuments", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out ssm.ListDocumentsOutput
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.GreaterOrEqual(t, len(out.DocumentIdentifiers), tt.wantMin)
		})
	}
}

// --- UpdateDocument ---

func TestHandler_UpdateDocument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*ssm.Handler)
		name       string
		body       string
		wantVer    string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *ssm.Handler) {
				doRequest(t, h, "CreateDocument", `{"Name":"UpdDoc","Content":"{}","DocumentType":"Command"}`)
			},
			body:       `{"Name":"UpdDoc","Content":"{\"updated\":true}"}`,
			wantStatus: http.StatusOK,
			wantVer:    "2",
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
				tt.setup(h)
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

// --- DeleteDocument ---

func TestHandler_DeleteDocument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*ssm.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *ssm.Handler) {
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
				tt.setup(h)
			}

			rec := doRequest(t, h, "DeleteDocument", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- DescribeDocumentPermission ---

func TestHandler_DescribeDocumentPermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*ssm.Handler)
		body       string
		wantStatus int
		wantLen    int
	}{
		{
			name: "empty_permissions",
			setup: func(h *ssm.Handler) {
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
				tt.setup(h)
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

// --- ModifyDocumentPermission ---

func TestHandler_ModifyDocumentPermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*ssm.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "add_accounts",
			setup: func(h *ssm.Handler) {
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
				tt.setup(h)
			}

			rec := doRequest(t, h, "ModifyDocumentPermission", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- ListDocumentVersions ---

func TestHandler_ListDocumentVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*ssm.Handler)
		body       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "single_version",
			setup: func(h *ssm.Handler) {
				doRequest(t, h, "CreateDocument", `{"Name":"VerDoc","Content":"{}"}`)
			},
			body:       `{"Name":"VerDoc"}`,
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name: "multiple_versions",
			setup: func(h *ssm.Handler) {
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
				tt.setup(h)
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

// --- SendCommand ---

func TestHandler_SendCommand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "success",
			body: `{"DocumentName":"AWS-RunShellScript",` +
				`"InstanceIDs":["i-1234567890abcdef0"],` +
				`"Parameters":{"commands":["echo hello"]}}`,
			wantStatus: http.StatusOK,
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
			rec := doRequest(t, h, "SendCommand", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out ssm.SendCommandOutput
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.NotEmpty(t, out.Command.CommandID)
				assert.Equal(t, "AWS-RunShellScript", out.Command.DocumentName)
			}
		})
	}
}

// --- ListCommands ---

func TestHandler_ListCommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*ssm.Handler)
		body       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "empty",
			body:       `{}`,
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "with_commands",
			setup: func(h *ssm.Handler) {
				doRequest(t, h, "SendCommand", `{"DocumentName":"AWS-RunShellScript","InstanceIDs":["i-abc"]}`)
				doRequest(t, h, "SendCommand", `{"DocumentName":"AWS-RunShellScript","InstanceIDs":["i-def"]}`)
			},
			body:       `{}`,
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "ListCommands", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out ssm.ListCommandsOutput
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Len(t, out.Commands, tt.wantCount)
		})
	}
}

// --- GetCommandInvocation ---

func TestHandler_GetCommandInvocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup             func(*ssm.Handler) string
		body              func(cmdID string) string
		name              string
		wantCommandStatus string
		wantStatus        int
	}{
		{
			name: "success",
			setup: func(h *ssm.Handler) string {
				rec := doRequest(t, h, "SendCommand", `{"DocumentName":"AWS-RunShellScript","InstanceIDs":["i-abc"]}`)
				var out ssm.SendCommandOutput
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

				return out.Command.CommandID
			},
			body: func(cmdID string) string {
				return `{"CommandId":"` + cmdID + `","InstanceId":"i-abc"}`
			},
			wantStatus:        http.StatusOK,
			wantCommandStatus: "Success",
		},
		{
			name:  "not_found",
			setup: nil,
			body: func(_ string) string {
				return `{"CommandId":"no-such-id","InstanceId":"i-abc"}`
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			var cmdID string
			if tt.setup != nil {
				cmdID = tt.setup(h)
			}

			rec := doRequest(t, h, "GetCommandInvocation", tt.body(cmdID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCommandStatus != "" {
				var out ssm.GetCommandInvocationOutput
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.Equal(t, tt.wantCommandStatus, out.Status)
				assert.Equal(t, tt.wantCommandStatus, out.StatusDetails)
			}
		})
	}
}

// --- ListCommandInvocations ---

func TestHandler_ListCommandInvocations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*ssm.Handler)
		body       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "empty",
			body:       `{}`,
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "with_invocations",
			setup: func(h *ssm.Handler) {
				doRequest(t, h, "SendCommand", `{"DocumentName":"AWS-RunShellScript","InstanceIDs":["i-abc","i-def"]}`)
			},
			body:       `{}`,
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "ListCommandInvocations", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out ssm.ListCommandInvocationsOutput
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Len(t, out.CommandInvocations, tt.wantCount)
		})
	}
}

// --- Backend direct tests ---

func TestInMemoryBackend_DefaultDocuments(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	out, err := backend.ListDocuments(&ssm.ListDocumentsInput{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.DocumentIdentifiers), 2)

	names := make([]string, 0, len(out.DocumentIdentifiers))
	for _, d := range out.DocumentIdentifiers {
		names = append(names, d.Name)
	}

	assert.Contains(t, names, "AWS-RunShellScript")
	assert.Contains(t, names, "AWS-RunPowerShellScript")
}

func TestInMemoryBackend_DocumentVersioning(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	_, err := backend.CreateDocument(&ssm.CreateDocumentInput{
		Name:    "MyDoc",
		Content: `{"v":1}`,
	})
	require.NoError(t, err)

	_, err = backend.UpdateDocument(&ssm.UpdateDocumentInput{
		Name:    "MyDoc",
		Content: `{"v":2}`,
	})
	require.NoError(t, err)

	verOut, err := backend.ListDocumentVersions(&ssm.ListDocumentVersionsInput{Name: "MyDoc"})
	require.NoError(t, err)
	require.Len(t, verOut.DocumentVersions, 2)
	assert.Equal(t, "1", verOut.DocumentVersions[0].DocumentVersion)
	assert.Equal(t, "2", verOut.DocumentVersions[1].DocumentVersion)
}

func TestInMemoryBackend_DocumentPermissions(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	_, err := backend.CreateDocument(&ssm.CreateDocumentInput{
		Name:    "PermDoc",
		Content: `{}`,
	})
	require.NoError(t, err)

	_, err = backend.ModifyDocumentPermission(&ssm.ModifyDocumentPermissionInput{
		Name:            "PermDoc",
		PermissionType:  "Share",
		AccountIDsToAdd: []string{"111111111111", "222222222222"},
	})
	require.NoError(t, err)

	permOut, err := backend.DescribeDocumentPermission(&ssm.DescribeDocumentPermissionInput{
		Name:           "PermDoc",
		PermissionType: "Share",
	})
	require.NoError(t, err)
	assert.Len(t, permOut.AccountIDs, 2)
	assert.Contains(t, permOut.AccountIDs, "111111111111")

	_, err = backend.ModifyDocumentPermission(&ssm.ModifyDocumentPermissionInput{
		Name:               "PermDoc",
		PermissionType:     "Share",
		AccountIDsToRemove: []string{"111111111111"},
	})
	require.NoError(t, err)

	permOut2, err := backend.DescribeDocumentPermission(&ssm.DescribeDocumentPermissionInput{
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

	_, err := backend.CreateDocument(&ssm.CreateDocumentInput{Name: "ToDelete", Content: "{}"})
	require.NoError(t, err)

	_, err = backend.ModifyDocumentPermission(&ssm.ModifyDocumentPermissionInput{
		Name:            "ToDelete",
		PermissionType:  "Share",
		AccountIDsToAdd: []string{"123456789012"},
	})
	require.NoError(t, err)

	_, err = backend.DeleteDocument(&ssm.DeleteDocumentInput{Name: "ToDelete"})
	require.NoError(t, err)

	_, err = backend.GetDocument(&ssm.GetDocumentInput{Name: "ToDelete"})
	require.ErrorIs(t, err, ssm.ErrDocumentNotFound)
}
