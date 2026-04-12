package ssm_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// --- CancelCommand ---

func TestCancelCommand_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "cancels_existing_command", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			sendRec := doRequest(
				t,
				h,
				"SendCommand",
				`{"DocumentName":"AWS-RunShellScript","Parameters":{"commands":["echo hi"]}}`,
			)
			require.Equal(t, http.StatusOK, sendRec.Code)

			var sendResp map[string]any
			require.NoError(t, json.Unmarshal(sendRec.Body.Bytes(), &sendResp))

			cmd, ok := sendResp["Command"].(map[string]any)
			require.True(t, ok)
			cmdID := cmd["CommandId"].(string)

			body := `{"CommandId":"` + cmdID + `"}`
			rec := doRequest(t, h, "CancelCommand", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestCancelCommand_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantErrMsg string
		wantStatus int
	}{
		{
			name:       "unknown_command_id",
			body:       `{"CommandId":"nonexistent-id"}`,
			wantStatus: http.StatusBadRequest,
			wantErrMsg: "InvalidCommandId",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CancelCommand", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantErrMsg)
		})
	}
}

// --- CancelMaintenanceWindowExecution ---

func TestCancelMaintenanceWindowExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantExecID string
		wantStatus int
	}{
		{
			name:       "returns_execution_id",
			body:       `{"WindowExecutionId":"wex-0123456789abcdef0"}`,
			wantStatus: http.StatusOK,
			wantExecID: "wex-0123456789abcdef0",
		},
		{
			name:       "empty_execution_id",
			body:       `{"WindowExecutionId":""}`,
			wantStatus: http.StatusOK,
			wantExecID: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CancelMaintenanceWindowExecution", tt.body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantExecID, resp["WindowExecutionId"])
		})
	}
}

// --- CreateActivation ---

func TestCreateActivation_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		body              string
		wantStatus        int
		wantActivationLen int
	}{
		{
			name:              "minimal_required_fields",
			body:              `{"IamRole":"arn:aws:iam::123456789012:role/SSMRole"}`,
			wantStatus:        http.StatusOK,
			wantActivationLen: 1,
		},
		{
			name: "with_optional_fields",
			body: `{"IamRole":"arn:aws:iam::123456789012:role/SSMRole",` +
				`"Description":"test","DefaultInstanceName":"my-instance","RegistrationLimit":5}`,
			wantStatus:        http.StatusOK,
			wantActivationLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)
			rec := doRequest(t, h, "CreateActivation", tt.body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var resp ssm.CreateActivationOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.ActivationID)
			assert.NotEmpty(t, resp.ActivationCode)
			assert.True(t, strings.HasPrefix(resp.ActivationID, "act-"))
			assert.Len(t, resp.ActivationCode, 20)
			assert.Equal(t, tt.wantActivationLen, backend.ActivationCount())
		})
	}
}

// --- CreateAssociation ---

func TestCreateAssociation_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "associates_existing_document", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)

			doRequest(
				t,
				h,
				"CreateDocument",
				`{"Name":"MyDoc","Content":"{\"schemaVersion\":\"2.2\"}","DocumentType":"Command"}`,
			)

			rec := doRequest(t, h, "CreateAssociation", `{"Name":"MyDoc","InstanceId":"i-abc123"}`)
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp ssm.CreateAssociationOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.AssociationDescription.AssociationID)
			assert.Equal(t, "MyDoc", resp.AssociationDescription.Name)
			assert.Equal(t, 1, backend.AssociationCount())
		})
	}
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

// --- CreateAssociationBatch ---

func TestCreateAssociationBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		body           string
		wantStatus     int
		wantSuccessful int
		wantFailed     int
		setupDoc       bool
	}{
		{
			name:           "all_succeed",
			setupDoc:       true,
			body:           `{"Entries":[{"Name":"MyDoc","InstanceId":"i-1"},{"Name":"MyDoc","InstanceId":"i-2"}]}`,
			wantStatus:     http.StatusOK,
			wantSuccessful: 2,
			wantFailed:     0,
		},
		{
			name:           "partial_failure",
			setupDoc:       true,
			body:           `{"Entries":[{"Name":"MyDoc","InstanceId":"i-1"},{"Name":"NoSuchDoc","InstanceId":"i-2"}]}`,
			wantStatus:     http.StatusOK,
			wantSuccessful: 1,
			wantFailed:     1,
		},
		{
			name:           "all_fail",
			setupDoc:       false,
			body:           `{"Entries":[{"Name":"BadDoc1"},{"Name":"BadDoc2"}]}`,
			wantStatus:     http.StatusOK,
			wantSuccessful: 0,
			wantFailed:     2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)

			if tt.setupDoc {
				doRequest(
					t,
					h,
					"CreateDocument",
					`{"Name":"MyDoc","Content":"{\"schemaVersion\":\"2.2\"}","DocumentType":"Command"}`,
				)
			}

			rec := doRequest(t, h, "CreateAssociationBatch", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp ssm.CreateAssociationBatchOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.Successful, tt.wantSuccessful)
			assert.Len(t, resp.Failed, tt.wantFailed)
			assert.Equal(t, tt.wantSuccessful, backend.AssociationCount())
		})
	}
}

// --- CreateMaintenanceWindow ---

func TestCreateMaintenanceWindow_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "valid_window",
			body: `{"Name":"MyWindow","Schedule":"cron(0 2 ? * SUN *)",` +
				`"Duration":4,"Cutoff":1,"AllowUnassociatedTargets":true}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "minimal_window",
			body:       `{"Name":"Min","Schedule":"rate(1 day)","Duration":2,"Cutoff":0}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)
			rec := doRequest(t, h, "CreateMaintenanceWindow", tt.body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var resp ssm.CreateMaintenanceWindowOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.WindowID)
			assert.True(t, strings.HasPrefix(resp.WindowID, "mw-"))
			assert.Equal(t, 1, backend.MaintenanceWindowCount())
		})
	}
}

func TestCreateMaintenanceWindow_ValidationError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "missing_name",
			body:       `{"Schedule":"cron(0 2 ? * SUN *)","Duration":4,"Cutoff":1}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CreateMaintenanceWindow", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantErr)
		})
	}
}

// --- CreateOpsItem ---

func TestCreateOpsItem_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "valid_ops_item",
			body: `{"Title":"My OpsItem","Source":"my-app",` +
				`"Description":"test issue","Severity":"2","Category":"Availability"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "minimal_ops_item",
			body:       `{"Title":"Minimal","Source":"svc"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)
			rec := doRequest(t, h, "CreateOpsItem", tt.body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var resp ssm.CreateOpsItemOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.OpsItemID)
			assert.NotEmpty(t, resp.OpsItemArn)
			assert.True(t, strings.HasPrefix(resp.OpsItemID, "oi-"))
			assert.Equal(t, 1, backend.OpsItemCount())
		})
	}
}

func TestCreateOpsItem_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "missing_title",
			body:       `{"Source":"my-app"}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
		{
			name:       "missing_source",
			body:       `{"Title":"My Title"}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CreateOpsItem", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantErr)
		})
	}
}

// --- AssociateOpsItemRelatedItem ---

func TestAssociateOpsItemRelatedItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStatus int
		createItem bool
	}{
		{
			name:       "success",
			createItem: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "ops_item_not_found",
			createItem: false,
			body: `{"OpsItemId":"oi-nonexistent","AssociationType":"RelatesTo",` +
				`"ResourceType":"AWS::EC2::Instance",` +
				`"ResourceUri":"arn:aws:ec2:us-east-1:123:instance/i-abc"}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "OpsItemNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			body := tt.body
			if tt.createItem {
				rec := doRequest(t, h, "CreateOpsItem", `{"Title":"item","Source":"src"}`)
				require.Equal(t, http.StatusOK, rec.Code)

				var createResp ssm.CreateOpsItemOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

				body = `{"OpsItemId":"` + createResp.OpsItemID +
					`","AssociationType":"RelatesTo","ResourceType":"AWS::EC2::Instance",` +
					`"ResourceUri":"arn:aws:ec2:us-east-1:123:instance/i-abc"}`
			}

			rec := doRequest(t, h, "AssociateOpsItemRelatedItem", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErr != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErr)
			} else {
				var resp ssm.AssociateOpsItemRelatedItemOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp.AssociationID)
			}
		})
	}
}

// --- CreateOpsMetadata ---

func TestCreateOpsMetadata_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "with_resource_id",
			body:       `{"ResourceId":"my-app-resource"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "with_metadata",
			body:       `{"ResourceId":"res-1","Metadata":{"env":{"Value":"prod"}}}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)
			rec := doRequest(t, h, "CreateOpsMetadata", tt.body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var resp ssm.CreateOpsMetadataOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.OpsMetadataArn)
			assert.Contains(t, resp.OpsMetadataArn, "arn:aws:ssm:")
			assert.Equal(t, 1, backend.OpsMetadataCount())
		})
	}
}

func TestCreateOpsMetadata_ValidationError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "missing_resource_id",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CreateOpsMetadata", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantErr)
		})
	}
}

// --- CreatePatchBaseline ---

func TestCreatePatchBaseline_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "minimal_baseline",
			body:       `{"Name":"MyBaseline"}`,
			wantStatus: http.StatusOK,
		},
		{
			name: "full_baseline",
			body: `{"Name":"FullBaseline","Description":"desc","OperatingSystem":"WINDOWS",` +
				`"ApprovedPatches":["KB123"],"RejectedPatches":["KB999"],` +
				`"ApprovedPatchesComplianceLevel":"HIGH"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)
			rec := doRequest(t, h, "CreatePatchBaseline", tt.body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var resp ssm.CreatePatchBaselineOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.BaselineID)
			assert.True(t, strings.HasPrefix(resp.BaselineID, "pb-"))
			assert.Equal(t, 1, backend.PatchBaselineCount())
		})
	}
}

func TestCreatePatchBaseline_ValidationError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "missing_name",
			body:       `{"OperatingSystem":"WINDOWS"}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CreatePatchBaseline", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantErr)
		})
	}
}

// --- BackendReset includes new maps ---

func TestNewOps_ResetClearsAllMaps(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandler(t)

	doRequest(t, h, "CreateActivation", `{"IamRole":"arn:aws:iam::123:role/r"}`)
	doRequest(t, h, "CreateOpsItem", `{"Title":"t","Source":"s"}`)
	doRequest(t, h, "CreateMaintenanceWindow", `{"Name":"w","Schedule":"rate(1 day)","Duration":2,"Cutoff":0}`)
	doRequest(t, h, "CreatePatchBaseline", `{"Name":"b"}`)
	doRequest(t, h, "CreateOpsMetadata", `{"ResourceId":"res"}`)

	require.Equal(t, 1, backend.ActivationCount())
	require.Equal(t, 1, backend.OpsItemCount())
	require.Equal(t, 1, backend.MaintenanceWindowCount())
	require.Equal(t, 1, backend.PatchBaselineCount())
	require.Equal(t, 1, backend.OpsMetadataCount())

	backend.Reset()

	assert.Equal(t, 0, backend.ActivationCount())
	assert.Equal(t, 0, backend.OpsItemCount())
	assert.Equal(t, 0, backend.MaintenanceWindowCount())
	assert.Equal(t, 0, backend.PatchBaselineCount())
	assert.Equal(t, 0, backend.OpsMetadataCount())
}
