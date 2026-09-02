package transfer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	transfersdk "github.com/aws/aws-sdk-go-v2/service/transfer"
	transfertypes "github.com/aws/aws-sdk-go-v2/service/transfer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestListServers_LoggingRole_RealClient covers a layer-3 bug
// (gopherstack-g8k9): Server.LoggingRole is real, tracked state --
// CreateServer stores it and DescribeServer already emits it correctly (the
// second-op signal, handler_servers.go's toDescribedServer) -- but
// ListServers' serverListItem never carried it through, so a real client's
// ListServers().Servers[i].LoggingRole was always empty regardless of what
// the server was configured with. Real field confirmed against
// transfer@v1.75.4 deserializers.go's
// awsAwsjson11_deserializeDocumentListedServer, which has a "LoggingRole"
// case identical to the one on ListedUser/DescribedServer.
func TestListServers_LoggingRole_RealClient(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := transfer.NewInMemoryBackend(ctx, "123456789012", "us-east-1")
	client := newTestTransferClient(t, transfer.NewHandler(backend))

	created, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{
		LoggingRole: aws.String("arn:aws:iam::123456789012:role/transfer-logging-role"),
	})
	require.NoError(t, err)

	listed, err := client.ListServers(ctx, &transfersdk.ListServersInput{})
	require.NoError(t, err)

	var found *string
	for _, s := range listed.Servers {
		if aws.ToString(s.ServerId) == aws.ToString(created.ServerId) {
			found = s.LoggingRole

			break
		}
	}

	require.NotNil(t, found,
		"ListServers: LoggingRole must round-trip; pre-fix it was always nil")
	assert.Equal(t, "arn:aws:iam::123456789012:role/transfer-logging-role", aws.ToString(found))
}

// TestDescribeWorkflow_CustomStepTimeoutSecondsKey_RealClient covers
// gopherstack-y1zn. workflowStepToMap emitted "Timeout" for a CUSTOM step's
// timeout; types.CustomStepDetails (transfer@v1.75.4 deserializers.go's
// real member) is "TimeoutSeconds". A typed client silently ignores the
// unknown key, so the proof is the raw body.
func TestDescribeWorkflow_CustomStepTimeoutSecondsKey_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWorkflow", map[string]any{
		"Steps": []map[string]any{
			{
				"Type": "CUSTOM",
				"CustomStepDetails": map[string]any{
					"Name":    "y1zn-custom-step",
					"Target":  "arn:aws:lambda:us-east-1:123456789012:function:my-func",
					"Timeout": 30,
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	workflowID := createResp["WorkflowId"].(string)

	rec := doTransferRequest(t, h, "DescribeWorkflow", map[string]any{
		"WorkflowId": workflowID,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	body := rec.Body.String()
	assert.NotContains(t, body, `"Timeout":`,
		"types.CustomStepDetails has no Timeout member")
	assert.Contains(t, body, `"TimeoutSeconds"`,
		"types.CustomStepDetails's real member is TimeoutSeconds")
}

// TestSendWorkflowStepState_CustomStepStatus_RealClient covers
// gopherstack's fabricated-enum-value bug (this campaign, 2026-08-23):
// SendWorkflowStepStateInput.Status is types.CustomStepStatus
// (transfer@v1.75.4 api_op_SendWorkflowStepState.go), whose only real
// values are SUCCESS/FAILURE. gopherstack previously required
// "COMPLETE"/"EXCEPTION" instead -- values that don't exist on
// CustomStepStatus, so a real SDK client (which can only ever send
// CustomStepStatusSuccess or CustomStepStatusFailure) always got rejected
// with a validation error. This drives the real client end to end and
// asserts the typed DescribedExecution.Status this backend returns decodes
// to the expected ExecutionStatus constant.
func TestSendWorkflowStepState_CustomStepStatus_RealClient(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := transfer.NewInMemoryBackend(ctx, "123456789012", "us-east-1")
	client := newTestTransferClient(t, transfer.NewHandler(backend))

	// CreateExecution has no public SDK operation (real executions are
	// triggered by file uploads); seed one directly against the backend, as
	// workflows_test.go does.
	wf, err := backend.CreateWorkflow("wire-field-fixes", nil, nil, nil)
	require.NoError(t, err)

	exec, err := backend.CreateExecution(wf.WorkflowID)
	require.NoError(t, err)

	_, err = client.SendWorkflowStepState(ctx, &transfersdk.SendWorkflowStepStateInput{
		WorkflowId:  aws.String(wf.WorkflowID),
		ExecutionId: aws.String(exec.ExecutionID),
		Token:       aws.String("tok-abc"),
		Status:      transfertypes.CustomStepStatusSuccess,
	})
	require.NoError(t, err,
		"a real client can only send CustomStepStatusSuccess/Failure (SUCCESS/FAILURE); "+
			"pre-fix, gopherstack rejected both with a validation error")

	described, err := client.DescribeExecution(ctx, &transfersdk.DescribeExecutionInput{
		WorkflowId:  aws.String(wf.WorkflowID),
		ExecutionId: aws.String(exec.ExecutionID),
	})
	require.NoError(t, err)
	assert.Equal(t, transfertypes.ExecutionStatusCompleted, described.Execution.Status)
}

// TestDescribeWebAppCustomization_Arn_RealClient covers a silent-drop bug:
// types.DescribedWebAppCustomization.Arn (transfer@v1.75.4
// api_op_DescribeWebAppCustomization.go) is a required response member, but
// the handler's output map never included it, so a real client always got
// a nil Arn regardless of the web app's real ARN.
func TestDescribeWebAppCustomization_Arn_RealClient(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := transfer.NewInMemoryBackend(ctx, "123456789012", "us-east-1")
	client := newTestTransferClient(t, transfer.NewHandler(backend))

	created, err := client.CreateWebApp(ctx, &transfersdk.CreateWebAppInput{
		IdentityProviderDetails: &transfertypes.WebAppIdentityProviderDetailsMemberIdentityCenterConfig{
			Value: transfertypes.IdentityCenterConfig{
				InstanceArn: aws.String("arn:aws:sso:::instance/ssoins-1234567890"),
				Role:        aws.String("arn:aws:iam::123456789012:role/access"),
			},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeWebAppCustomization(ctx, &transfersdk.DescribeWebAppCustomizationInput{
		WebAppId: created.WebAppId,
	})
	require.NoError(t, err)
	require.NotNil(t, desc.WebAppCustomization)
	assert.NotEmpty(t, aws.ToString(desc.WebAppCustomization.Arn),
		"DescribeWebAppCustomization: Arn is a required response member; pre-fix it was always nil")
	assert.Equal(t,
		"arn:aws:transfer:us-east-1:123456789012:webapp/"+aws.ToString(created.WebAppId),
		aws.ToString(desc.WebAppCustomization.Arn))
}

// TestUpdateWebAppCustomization_WebAppId_RealClient covers a silent-drop
// bug: types.UpdateWebAppCustomizationOutput.WebAppId (transfer@v1.75.4
// api_op_UpdateWebAppCustomization.go) is a required response member, but
// the handler returned an empty struct, so a real client always got a nil
// WebAppId back from UpdateWebAppCustomization regardless of which web app
// was updated.
func TestUpdateWebAppCustomization_WebAppId_RealClient(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := transfer.NewInMemoryBackend(ctx, "123456789012", "us-east-1")
	client := newTestTransferClient(t, transfer.NewHandler(backend))

	created, err := client.CreateWebApp(ctx, &transfersdk.CreateWebAppInput{
		IdentityProviderDetails: &transfertypes.WebAppIdentityProviderDetailsMemberIdentityCenterConfig{
			Value: transfertypes.IdentityCenterConfig{
				InstanceArn: aws.String("arn:aws:sso:::instance/ssoins-1234567890"),
				Role:        aws.String("arn:aws:iam::123456789012:role/access"),
			},
		},
	})
	require.NoError(t, err)

	updated, err := client.UpdateWebAppCustomization(ctx, &transfersdk.UpdateWebAppCustomizationInput{
		WebAppId: created.WebAppId,
		Title:    aws.String("My Portal"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.WebAppId), aws.ToString(updated.WebAppId),
		"UpdateWebAppCustomization: WebAppId is a required response member; pre-fix it was always nil")
}

// TestListExecutionsAndDescribeExecution_NoFabricatedWorkflowId covers an
// invented-field bug: types.ListedExecution and types.DescribedExecution
// (transfer@v1.75.4 api_op_ListExecutions.go / api_op_DescribeExecution.go)
// carry no WorkflowId member -- WorkflowId is only a sibling field at the
// top level of each response. gopherstack's per-execution maps duplicated
// it as an invented nested key. Harmless to a typed client (unknown JSON
// keys are ignored), so asserted on the raw body like
// TestDescribeWorkflow_CustomStepTimeoutSecondsKey_RealClient above.
func TestListExecutionsAndDescribeExecution_NoFabricatedWorkflowId(t *testing.T) {
	t.Parallel()

	backend := transfer.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := transfer.NewHandler(backend)

	wf, err := backend.CreateWorkflow("wfx-no-fab", nil, nil, nil)
	require.NoError(t, err)

	exec, err := backend.CreateExecution(wf.WorkflowID)
	require.NoError(t, err)

	listRec := doTransferRequest(t, h, "ListExecutions", map[string]any{"WorkflowId": wf.WorkflowID})
	require.Equal(t, http.StatusOK, listRec.Code, listRec.Body.String())

	var listResp struct {
		Executions []map[string]any `json:"Executions"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Executions, 1, "must exercise a non-empty collection")
	require.Contains(t, listResp.Executions[0], "ExecutionId")

	_, listHasWorkflowID := listResp.Executions[0]["WorkflowId"]
	assert.False(t, listHasWorkflowID,
		"ListExecutions: ListedExecution has no WorkflowId member on the real wire")

	descRec := doTransferRequest(t, h, "DescribeExecution", map[string]any{
		"WorkflowId":  wf.WorkflowID,
		"ExecutionId": exec.ExecutionID,
	})
	require.Equal(t, http.StatusOK, descRec.Code, descRec.Body.String())

	var descResp struct {
		Execution map[string]any `json:"Execution"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	require.Contains(t, descResp.Execution, "ExecutionId")

	_, descHasWorkflowID := descResp.Execution["WorkflowId"]
	assert.False(t, descHasWorkflowID,
		"DescribeExecution: DescribedExecution has no WorkflowId member on the real wire")
}

// TestListCertificates_Usage_RealClient covers a sibling-shape bug (gopherstack-6flj/21my):
// real types.ListedCertificate (transfer@v1.75.4 deserializers.go,
// awsAwsjson11_deserializeDocumentListedCertificate, case "Usage") declares a Usage member
// identical to types.DescribedCertificate's -- backed by real, tracked state
// (Certificate.Usage) and already emitted correctly by DescribeCertificate -- but
// ListCertificates never carried it through, so a real client's
// ListCertificates().Certificates[i].Usage was always empty regardless of what the
// certificate was imported with. A prior PARITY.md note claimed ListedCertificate "has no
// Usage member at all," which does not hold against the currently pinned SDK.
func TestListCertificates_Usage_RealClient(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := transfer.NewInMemoryBackend(ctx, "123456789012", "us-east-1")
	client := newTestTransferClient(t, transfer.NewHandler(backend))

	imported, err := client.ImportCertificate(ctx, &transfersdk.ImportCertificateInput{
		Certificate: aws.String(testCertPEM),
		Usage:       transfertypes.CertificateUsageTypeSigning,
	})
	require.NoError(t, err)

	other, err := client.ImportCertificate(ctx, &transfersdk.ImportCertificateInput{
		Certificate: aws.String(testCertPEM),
		Usage:       transfertypes.CertificateUsageTypeEncryption,
	})
	require.NoError(t, err)

	listed, err := client.ListCertificates(ctx, &transfersdk.ListCertificatesInput{})
	require.NoError(t, err)
	require.Len(t, listed.Certificates, 2, "must exercise a collection of at least two items")

	got := map[string]transfertypes.CertificateUsageType{}
	for _, c := range listed.Certificates {
		got[aws.ToString(c.CertificateId)] = c.Usage
	}

	assert.Equal(t, transfertypes.CertificateUsageTypeSigning, got[aws.ToString(imported.CertificateId)],
		"ListCertificates: Usage must round-trip from ImportCertificate, not decode empty")
	assert.Equal(t, transfertypes.CertificateUsageTypeEncryption, got[aws.ToString(other.CertificateId)],
		"ListCertificates: Usage must round-trip from ImportCertificate, not decode empty")
}
