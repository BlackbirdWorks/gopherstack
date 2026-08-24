package transfer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_CreateWorkflowInvalidStepTypeRejected verifies that CreateWorkflow returns 400
// when a step Type is not one of the real AWS-valid values (COPY, CUSTOM, DELETE, TAG, DECRYPT).
func TestHandler_CreateWorkflowInvalidStepTypeRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateWorkflow", map[string]any{
		"Steps": []map[string]any{
			{"Type": "INVALID_STEP"},
		},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"CreateWorkflow with invalid step Type must return 400; body: %s", rec.Body.String())
}

// TestHandler_CreateWorkflowValidStepTypesAccepted verifies that all five real AWS step types
// are accepted by CreateWorkflow.
func TestHandler_CreateWorkflowValidStepTypesAccepted(t *testing.T) {
	t.Parallel()

	validTypes := []string{"COPY", "CUSTOM", "DELETE", "TAG", "DECRYPT"}

	for _, stepType := range validTypes {
		t.Run(stepType, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTransferRequest(t, h, "CreateWorkflow", map[string]any{
				"Steps": []map[string]any{
					{"Type": stepType},
				},
			})

			assert.Equal(t, http.StatusOK, rec.Code,
				"CreateWorkflow with step Type %q must return 200; body: %s", stepType, rec.Body.String())
		})
	}
}

func TestHandler_DescribeWorkflowIncludesArnAndTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWorkflow", map[string]any{
		"Description": "test workflow",
		"Tags":        []map[string]string{{"Key": "stage", "Value": "prod"}},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	workflowID := createResp["WorkflowId"].(string)

	rec := doTransferRequest(t, h, "DescribeWorkflow", map[string]any{
		"WorkflowId": workflowID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wf := resp["Workflow"].(map[string]any)

	arn, hasArn := wf["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in DescribeWorkflow response")
	assert.Contains(t, arn, workflowID, "Arn must contain WorkflowId")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")

	tags, hasTags := wf["Tags"].([]any)
	assert.True(t, hasTags, "Tags must be present in DescribeWorkflow response")
	assert.Len(t, tags, 1)
}

func TestHandler_ListWorkflowsIncludesArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doTransferRequest(t, h, "CreateWorkflow", map[string]any{
		"Description": "wf for list test",
	})

	rec := doTransferRequest(t, h, "ListWorkflows", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	workflows := resp["Workflows"].([]any)
	require.NotEmpty(t, workflows)

	item := workflows[0].(map[string]any)
	arn, hasArn := item["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in ListWorkflows items")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")
}

// TestHandler_CreateWorkflowReturnsWorkflowID verifies response schema.
func TestHandler_CreateWorkflowReturnsWorkflowID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateWorkflow", map[string]any{
		"Description": "my workflow",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["WorkflowId"])
}

// TestHandler_SendWorkflowStepStateInvalidStatus verifies HTTP 400.
func TestHandler_SendWorkflowStepStateInvalidStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wf, err := h.Backend.CreateWorkflow("test", nil, nil, nil)
	require.NoError(t, err)

	exec, err := h.Backend.CreateExecution(wf.WorkflowID)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "SendWorkflowStepState", map[string]any{
		"WorkflowId":  wf.WorkflowID,
		"ExecutionId": exec.ExecutionID,
		"Token":       "tok-abc",
		"Status":      "INVALID_STATUS",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_SendWorkflowStepStateFailureStatus verifies FAILURE (the real
// CustomStepStatus value) is valid and moves the execution to EXCEPTION.
func TestHandler_SendWorkflowStepStateFailureStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wf, err := h.Backend.CreateWorkflow("test", nil, nil, nil)
	require.NoError(t, err)

	exec, err := h.Backend.CreateExecution(wf.WorkflowID)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "SendWorkflowStepState", map[string]any{
		"WorkflowId":  wf.WorkflowID,
		"ExecutionId": exec.ExecutionID,
		"Token":       "tok-abc",
		"Status":      "FAILURE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	updatedExec, err := h.Backend.DescribeExecution(wf.WorkflowID, exec.ExecutionID)
	require.NoError(t, err)
	assert.Equal(t, "EXCEPTION", updatedExec.Status)
}

func TestHandler_CreateWorkflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "success no description",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name: "success with description",
			body: map[string]any{
				"Description": "my workflow",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "success with tags",
			body: map[string]any{
				"Description": "tagged workflow",
				"Tags":        []map[string]string{{"Key": "team", "Value": "data"}},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTransferRequest(t, h, "CreateWorkflow", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["WorkflowId"])
		})
	}
}

func TestHandler_DescribeWorkflow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWorkflow", map[string]any{
		"Steps": []map[string]any{},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	workflowID := createResp["WorkflowId"].(string)

	rec := doTransferRequest(t, h, "DescribeWorkflow", map[string]any{
		"WorkflowId": workflowID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListWorkflows(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ListWorkflows", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteWorkflow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWorkflow", map[string]any{
		"Steps": []map[string]any{},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	workflowID := createResp["WorkflowId"].(string)

	rec := doTransferRequest(t, h, "DeleteWorkflow", map[string]any{
		"WorkflowId": workflowID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListExecutions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWorkflow", map[string]any{
		"Steps": []map[string]any{},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	workflowID := createResp["WorkflowId"].(string)

	rec := doTransferRequest(t, h, "ListExecutions", map[string]any{
		"WorkflowId": workflowID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_SendWorkflowStepState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "SendWorkflowStepState", map[string]any{
		"WorkflowId":  "wf-1234",
		"ExecutionId": "exec-1234",
		"Token":       "token-1234",
		"Status":      "SUCCESS",
	})
	// May fail if workflow not found; we're testing the handler path.
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)
}

// Test 16: DescribeWorkflow includes typed CopyStepDetails.
func TestHandler_DescribeWorkflowTypedCopyStepDetails(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWorkflow", map[string]any{
		"Description": "copy workflow",
		"Steps": []map[string]any{
			{
				"Type": "COPY",
				"CopyStepDetails": map[string]any{
					"Name":               "my-copy",
					"SourceFileLocation": "${original.file}",
					"OverwriteExisting":  "TRUE",
					"DestinationFileLocation": map[string]any{
						"S3FileLocation": map[string]any{
							"Bucket": "my-bucket",
							"Key":    "output/${original.file}",
						},
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	workflowID := createResp["WorkflowId"].(string)

	descRec := doTransferRequest(t, h, "DescribeWorkflow", map[string]any{
		"WorkflowId": workflowID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	wf := descResp["Workflow"].(map[string]any)
	steps := wf["Steps"].([]any)
	require.Len(t, steps, 1)

	step := steps[0].(map[string]any)
	assert.Equal(t, "COPY", step["Type"])

	copyDetails, ok := step["CopyStepDetails"].(map[string]any)
	require.True(t, ok, "CopyStepDetails must be present")
	assert.Equal(t, "my-copy", copyDetails["Name"])
	assert.Equal(t, "${original.file}", copyDetails["SourceFileLocation"])
	assert.Equal(t, "TRUE", copyDetails["OverwriteExisting"])
}

// Test 29: SendWorkflowStepState SUCCESS transitions execution to COMPLETED.
func TestHandler_SendWorkflowStepStateSuccess(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wf, err := h.Backend.CreateWorkflow("accuracy test workflow", nil, nil, nil)
	require.NoError(t, err)

	exec, err := h.Backend.CreateExecution(wf.WorkflowID)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "SendWorkflowStepState", map[string]any{
		"WorkflowId":  wf.WorkflowID,
		"ExecutionId": exec.ExecutionID,
		"Token":       "tok-abc",
		"Status":      "SUCCESS",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	updatedExec, err := h.Backend.DescribeExecution(wf.WorkflowID, exec.ExecutionID)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", updatedExec.Status)
}
