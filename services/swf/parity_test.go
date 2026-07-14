package swf_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

// TestParity_DomainArn verifies DescribeDomain returns an ARN in domainInfo.
func TestParity_DomainArn(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)
	doSWFRequest(t, h, "RegisterDomain", map[string]any{
		"name":                                   "my-domain",
		"workflowExecutionRetentionPeriodInDays": "7",
	})
	rec := doSWFRequest(t, h, "DescribeDomain", map[string]any{"name": "my-domain"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	info, ok := resp["domainInfo"].(map[string]any)
	require.True(t, ok, "domainInfo missing")
	assert.Equal(t, "my-domain", info["name"])
	assert.Equal(t, "REGISTERED", info["status"])
	assert.NotEmpty(t, info["arn"], "domainInfo.arn must be present")
	_, hasRetention := info["workflowExecutionRetentionPeriodInDays"]
	assert.False(t, hasRetention, "domainInfo must not contain workflowExecutionRetentionPeriodInDays")

	cfg, ok := resp["configuration"].(map[string]any)
	require.True(t, ok, "configuration missing")
	assert.Equal(t, "7", cfg["workflowExecutionRetentionPeriodInDays"])
}

// TestParity_ListDomains_NoDomainInfoRetention verifies ListDomains omits retention from domainInfos.
func TestParity_ListDomains_NoDomainInfoRetention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		domains    []string
		wantCount  int
	}{
		{
			name:       "registered_domains",
			domains:    []string{"d1", "d2"},
			wantCount:  2,
			wantStatus: "REGISTERED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			for _, d := range tt.domains {
				doSWFRequest(t, h, "RegisterDomain", map[string]any{
					"name": d, "workflowExecutionRetentionPeriodInDays": "30",
				})
			}
			rec := doSWFRequest(t, h, "ListDomains", map[string]any{"registrationStatus": "REGISTERED"})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			infos, ok := resp["domainInfos"].([]any)
			require.True(t, ok)
			assert.Len(t, infos, tt.wantCount)

			for _, raw := range infos {
				info := raw.(map[string]any)
				_, hasRetention := info["workflowExecutionRetentionPeriodInDays"]
				assert.False(t, hasRetention, "domainInfos must not contain retention period")
				assert.NotEmpty(t, info["arn"])
			}
		})
	}
}

// TestParity_ExecutionStatus_OpenClosed verifies executionStatus is OPEN/CLOSED not internal values.
func TestParity_ExecutionStatus_OpenClosed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantExecStatus string
		terminate      bool
		wantHasClose   bool
	}{
		{name: "running_is_open", terminate: false, wantExecStatus: "OPEN", wantHasClose: false},
		{name: "terminated_is_closed", terminate: true, wantExecStatus: "CLOSED", wantHasClose: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
			_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
				Domain: "dom", WorkflowID: "wf-1",
			})
			require.NoError(t, err)

			if tt.terminate {
				require.NoError(t, b.TerminateWorkflowExecution("dom", "wf-1", "", "", ""))
			}

			h := swf.NewHandler(b)
			rec := doSWFRequest(t, h, "DescribeWorkflowExecution", map[string]any{
				"domain":    "dom",
				"execution": map[string]any{"workflowId": "wf-1", "runId": ""},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			info := resp["executionInfo"].(map[string]any)
			assert.Equal(t, tt.wantExecStatus, info["executionStatus"])

			_, hasClose := info["closeStatus"]
			assert.Equal(t, tt.wantHasClose, hasClose)
		})
	}
}

// TestParity_UndeprecateDomain_AlreadyActive verifies DomainAlreadyExistsFault on active domain.
func TestParity_UndeprecateDomain_AlreadyActive(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))

	err := b.UndeprecateDomain("dom")

	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrAlreadyExists)
}

// TestParity_UndeprecateWorkflowType_AlreadyActive verifies TypeAlreadyExistsFault on active type.
func TestParity_UndeprecateWorkflowType_AlreadyActive(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterWorkflowType("dom", "wf", "1.0", "", swf.WorkflowTypeDefaults{}))

	err := b.UndeprecateWorkflowType("dom", "wf", "1.0")

	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrTypeAlreadyExists)
}

// TestParity_UndeprecateActivityType_AlreadyActive verifies TypeAlreadyExistsFault on active type.
func TestParity_UndeprecateActivityType_AlreadyActive(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterActivityType("dom", "act", "1.0", "", swf.ActivityTypeDefaults{}))

	err := b.UndeprecateActivityType("dom", "act", "1.0")

	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrTypeAlreadyExists)
}

// TestParity_StartWorkflowExecution_AlreadyStarted verifies WorkflowExecutionAlreadyStartedFault.
func TestParity_StartWorkflowExecution_AlreadyStarted(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "wf-1",
	})
	require.NoError(t, err)

	_, err = b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "wf-1",
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrWorkflowAlreadyStarted)
}

// TestParity_TerminateWorkflowExecution_AlreadyClosed verifies UnknownResourceFault on closed exec.
func TestParity_TerminateWorkflowExecution_AlreadyClosed(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "wf-1", RunID: "run-1",
	})
	require.NoError(t, err)
	require.NoError(t, b.TerminateWorkflowExecution("dom", "wf-1", "", "", ""))

	err = b.TerminateWorkflowExecution("dom", "wf-1", "", "", "")

	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrNotFound)
}

// TestParity_PollForActivityTask_WorkflowExecution verifies nested workflowExecution in response.
func TestParity_PollForActivityTask_WorkflowExecution(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	b.EnqueueActivityTaskInternal("dom", "list", "act-1", "MyActivity", "1.0", "payload", "wf-10", "run-10")
	h := swf.NewHandler(b)

	rec := doSWFRequest(t, h, "PollForActivityTask", map[string]any{
		"domain":   "dom",
		"taskList": map[string]any{"name": "list"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()

	// Should have nested workflowExecution, not flat workflowId/runId at top level
	assert.Contains(t, body, `"workflowExecution"`, "workflowExecution nested object required")
	wePos := strings.Index(body, `"workflowExecution"`)
	wfPos := strings.Index(body, `"workflowId"`)
	assert.Greater(t, wfPos, wePos, "workflowId should appear inside workflowExecution")
}

// TestParity_PollForDecisionTask_NoFlatWorkflowId verifies no top-level workflowId/runId in response.
func TestParity_PollForDecisionTask_NoFlatWorkflowId(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	b.EnqueueDecisionTaskInternal("dom", "decisions", "wf-1", "run-1")
	h := swf.NewHandler(b)

	rec := doSWFRequest(t, h, "PollForDecisionTask", map[string]any{
		"domain":   "dom",
		"taskList": map[string]any{"name": "decisions"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	// workflowId and runId should only appear nested under workflowExecution
	_, hasTopWorkflowID := resp["workflowId"]
	_, hasTopRunID := resp["runId"]
	assert.False(t, hasTopWorkflowID, "workflowId must not be a top-level field")
	assert.False(t, hasTopRunID, "runId must not be a top-level field")
	assert.NotNil(t, resp["workflowExecution"], "workflowExecution nested object required")
}

// TestParity_OpenCounts_LambdaFunctions verifies openCounts includes openLambdaFunctions.
func TestParity_OpenCounts_LambdaFunctions(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "wf-1",
	})
	require.NoError(t, err)

	h := swf.NewHandler(b)
	rec := doSWFRequest(t, h, "DescribeWorkflowExecution", map[string]any{
		"domain":    "dom",
		"execution": map[string]any{"workflowId": "wf-1", "runId": ""},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	counts := resp["openCounts"].(map[string]any)
	_, hasLambda := counts["openLambdaFunctions"]
	assert.True(t, hasLambda, "openCounts must include openLambdaFunctions")
}

// TestParity_DeleteOps_RequireDeprecatedFirst verifies DeleteWorkflowType and
// DeleteActivityType reject deleting a type that hasn't been deprecated yet
// (real AWS: "Prior to deletion, [workflow/activity] types must first be
// deprecated"), succeed once deprecated, and 404 on an unknown type.
func TestParity_DeleteOps_RequireDeprecatedFirst(t *testing.T) {
	t.Parallel()

	tests := []struct {
		register   func(h *swf.Handler)
		deprecate  func(h *swf.Handler)
		name       string
		action     string
		typeRefKey string
	}{
		{
			name:   "DeleteWorkflowType",
			action: "DeleteWorkflowType",
			register: func(h *swf.Handler) {
				doSWFRequest(t, h, "RegisterWorkflowType",
					map[string]any{"domain": "d1", "name": "wf1", "version": "1.0"})
			},
			deprecate: func(h *swf.Handler) {
				doSWFRequest(t, h, "DeprecateWorkflowType",
					map[string]any{"domain": "d1", "workflowType": map[string]any{"name": "wf1", "version": "1.0"}})
			},
			typeRefKey: "workflowType",
		},
		{
			name:   "DeleteActivityType",
			action: "DeleteActivityType",
			register: func(h *swf.Handler) {
				doSWFRequest(t, h, "RegisterActivityType",
					map[string]any{"domain": "d1", "name": "act1", "version": "1.0"})
			},
			deprecate: func(h *swf.Handler) {
				doSWFRequest(t, h, "DeprecateActivityType",
					map[string]any{"domain": "d1", "activityType": map[string]any{"name": "act1", "version": "1.0"}})
			},
			typeRefKey: "activityType",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			doSWFRequest(t, h, "RegisterDomain", map[string]any{"name": "d1"})
			tt.register(h)

			typeName := "wf1"
			if tt.typeRefKey == "activityType" {
				typeName = "act1"
			}
			body := map[string]any{
				"domain":      "d1",
				tt.typeRefKey: map[string]any{"name": typeName, "version": "1.0"},
			}

			// Not yet deprecated -> TypeNotDeprecatedFault.
			rec := doSWFRequest(t, h, tt.action, body)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			var errResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "TypeNotDeprecatedFault", errResp["__type"])

			// Deprecate, then delete should succeed.
			tt.deprecate(h)
			rec = doSWFRequest(t, h, tt.action, body)
			assert.Equal(t, http.StatusOK, rec.Code)

			// Unknown type -> UnknownResourceFault.
			rec = doSWFRequest(t, h, tt.action, body)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestParity_NewDecisionTypes verifies 7 new decision types are processed without error.
func TestParity_NewDecisionTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		decisionType string
	}{
		{name: "RequestCancelActivityTask", decisionType: "RequestCancelActivityTask"},
		{name: "StartTimer", decisionType: "StartTimer"},
		{name: "CancelTimer", decisionType: "CancelTimer"},
		{name: "RecordMarker", decisionType: "RecordMarker"},
		{name: "StartChildWorkflowExecution", decisionType: "StartChildWorkflowExecution"},
		{name: "SignalExternalWorkflowExecution", decisionType: "SignalExternalWorkflowExecution"},
		{name: "RequestCancelExternalWorkflowExecution", decisionType: "RequestCancelExternalWorkflowExecution"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
			_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
				Domain: "dom", WorkflowID: "wf-1", TaskList: "tasks",
			})
			require.NoError(t, err)

			b.EnqueueDecisionTaskInternal("dom", "tasks", "wf-1", "run-1")
			token := pollDecisionToken(t, b)

			err = b.RespondDecisionTaskCompleted(token, "", []swf.Decision{
				{DecisionType: tt.decisionType},
			})
			require.NoError(t, err)

			events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", 0, "", false)
			assert.NotEmpty(t, events)
		})
	}
}

func pollDecisionToken(t *testing.T, b *swf.InMemoryBackend) string {
	t.Helper()

	task := b.PollForDecisionTask("dom", "tasks", 0, "")
	require.NotNil(t, task)

	return task.TaskToken
}
