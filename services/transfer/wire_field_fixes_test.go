package transfer_test

import (
	"context"
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

	backend := transfer.NewInMemoryBackend(context.Background(), "123456789012", "us-east-1")
	client := newTestTransferClient(t, transfer.NewHandler(backend))
	ctx := t.Context()

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

	backend := transfer.NewInMemoryBackend(context.Background(), "123456789012", "us-east-1")
	client := newTestTransferClient(t, transfer.NewHandler(backend))
	ctx := t.Context()

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
