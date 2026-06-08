package ssm_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// --- ResourceDataSync ---

func TestBatch2_ResourceDataSync_CRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// List empty
	rec := doRequest(t, h, "ListResourceDataSync", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Create
	rec = doRequest(t, h, "CreateResourceDataSync", `{"SyncName":"my-sync","SyncType":"SyncToDestination"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List shows it
	rec = doRequest(t, h, "ListResourceDataSync", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "my-sync")

	// Create duplicate → returns error (sync already exists)
	rec = doRequest(t, h, "CreateResourceDataSync", `{"SyncName":"my-sync"}`)
	assert.NotEqual(t, http.StatusNotFound, rec.Code)

	// Update
	rec = doRequest(t, h, "UpdateResourceDataSync", `{"SyncName":"my-sync"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doRequest(t, h, "DeleteResourceDataSync", `{"SyncName":"my-sync"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List empty again
	rec = doRequest(t, h, "ListResourceDataSync", `{}`)
	assertBodyContains(t, rec, "[]")
}

// --- Automation Execution ---

func TestBatch2_AutomationExecution_Lifecycle(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Start
	rec := doRequest(t, h, "StartAutomationExecution", `{"DocumentName":"AWS-RunShellScript"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "AutomationExecutionId")

	// Describe
	rec = doRequest(t, h, "DescribeAutomationExecutions", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "AWS-RunShellScript")

	// Stop (empty ID → still ok)
	rec = doRequest(t, h, "StopAutomationExecution", `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestBatch2_ChangeRequest(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := doRequest(t, h, "StartChangeRequestExecution", `{"DocumentName":"AWS-ChangeRequest"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "AutomationExecutionId")
}

// --- Service Settings ---

func TestBatch2_ServiceSetting_GetPutReset(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Get default
	rec := doRequest(t, h, "GetServiceSetting", `{"SettingId":"/ssm/parameter-store/default-parameter-tier"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "Default")

	// Update
	updateBody := `{"SettingId":"/ssm/parameter-store/default-parameter-tier","SettingValue":"Advanced"}`
	rec = doRequest(t, h, "UpdateServiceSetting", updateBody)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get updated
	rec = doRequest(t, h, "GetServiceSetting", `{"SettingId":"/ssm/parameter-store/default-parameter-tier"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "Advanced")

	// Reset
	rec = doRequest(t, h, "ResetServiceSetting", `{"SettingId":"/ssm/parameter-store/default-parameter-tier"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Resource Policies ---

func TestBatch2_ResourcePolicies(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	arn := `"arn:aws:ssm:us-east-1:000000000000:parameter/my-param"`

	// Get empty
	rec := doRequest(t, h, "GetResourcePolicies", `{"ResourceArn":`+arn+`}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Put
	rec = doRequest(t, h, "PutResourcePolicy", `{"ResourceArn":`+arn+`,"Policy":"{\"Version\":\"2012-10-17\"}"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "PolicyId")

	// Get shows policy
	rec = doRequest(t, h, "GetResourcePolicies", `{"ResourceArn":`+arn+`}`)
	assertBodyContains(t, rec, "Policies")
}

// --- Parameter Labels ---

func TestBatch2_ParameterLabels(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create a parameter first
	_, err := b.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: "/my/param", Value: "val", Type: "String"})
	require.NoError(t, err)

	// Label it
	rec := doRequest(t, h, "LabelParameterVersion", `{"Name":"/my/param","Labels":["v1","stable"]}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "AddedLabels")

	// Unlabel
	rec = doRequest(t, h, "UnlabelParameterVersion", `{"Name":"/my/param","Labels":["v1"]}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "RemovedLabels")
}

// --- Session Manager ---

func TestBatch2_Sessions(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Start a session
	sess, err := b.StartSession(context.TODO(), &ssm.StartSessionInput{Target: "i-001"})
	require.NoError(t, err)

	// DescribeSessions
	rec := doRequest(t, h, "DescribeSessions", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, sess.SessionID)

	// GetConnectionStatus
	rec = doRequest(t, h, "GetConnectionStatus", `{"Target":"i-001"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// ResumeSession
	rec = doRequest(t, h, "ResumeSession", `{"SessionId":"`+sess.SessionID+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "SessionId")
}

// --- OpsItem / OpsMetadata ---

func TestBatch2_OpsMetadata_List(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{ResourceID: "res-1"})
	require.NoError(t, err)

	rec := doRequest(t, h, "ListOpsMetadata", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "OpsMetadataList")
}

func TestBatch2_GetOpsSummary(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := doRequest(t, h, "GetOpsSummary", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "Entities")
}

// --- Execution Preview ---

func TestBatch2_ExecutionPreview(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Start preview
	rec := doRequest(t, h, "StartExecutionPreview", `{"DocumentName":"AWS-Test"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "ExecutionPreviewId")

	// Get preview (with empty ID → returns default)
	rec = doRequest(t, h, "GetExecutionPreview", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

// --- CalendarState ---

func TestBatch2_GetCalendarState(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := doRequest(t, h, "GetCalendarState", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "OPEN")
}

// --- ListNodes ---

func TestBatch2_ListNodes(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create an activation so there's a node
	_, err := b.CreateActivation(context.TODO(), &ssm.CreateActivationInput{
		IamRole:           "arn:aws:iam::000000000000:role/SSMRole",
		RegistrationLimit: 1,
	})
	require.NoError(t, err)

	rec := doRequest(t, h, "ListNodes", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "Nodes")

	rec = doRequest(t, h, "ListNodesSummary", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "NodeCount")
}

// --- Maintenance Window Executions ---

func TestBatch2_MaintenanceWindowExecutions(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := doRequest(t, h, "DescribeMaintenanceWindowExecutions", `{"WindowId":"mw-001"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DescribeMaintenanceWindowExecutionTasks", `{"WindowExecutionId":"we-001"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DescribeMaintenanceWindowExecutionTaskInvocations", `{"WindowExecutionId":"we-001"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DescribeMaintenanceWindowSchedule", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetMaintenanceWindowExecution", `{"WindowId":"mw-001","WindowExecutionId":"we-001"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

// --- Association ops ---

func TestBatch2_AssociationOps(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create association
	assoc, err := b.CreateAssociation(context.TODO(), &ssm.CreateAssociationInput{
		Name:       "AWS-RunShellScript",
		InstanceID: "i-001",
	})
	require.NoError(t, err)

	assocID := assoc.AssociationDescription.AssociationID

	// ListAssociationVersions
	rec := doRequest(t, h, "ListAssociationVersions", `{"AssociationId":"`+assocID+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "AssociationVersions")

	// DescribeAssociationExecutions
	rec = doRequest(t, h, "DescribeAssociationExecutions", `{"AssociationId":"`+assocID+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeEffectiveInstanceAssociations
	rec = doRequest(t, h, "DescribeEffectiveInstanceAssociations", `{"InstanceId":"i-001"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "Associations")

	// DescribeInstanceAssociationsStatus
	rec = doRequest(t, h, "DescribeInstanceAssociationsStatus", `{"InstanceId":"i-001"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

// assertBodyContains checks that the response body contains the given substring.
func assertBodyContains(t *testing.T, rec *httptest.ResponseRecorder, s string) {
	t.Helper()
	assert.Contains(t, rec.Body.String(), s)
}
