package transfer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestWorkflowCountExport verifies WorkflowCount export.
func TestWorkflowCountExport(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	assert.Equal(t, 0, transfer.WorkflowCount(b))

	_, err := b.CreateWorkflow("desc", nil, nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, transfer.WorkflowCount(b))
}

// TestAddWorkflowInternal verifies the AddWorkflowInternal seed helper.
func TestAddWorkflowInternal(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	b.AddWorkflowInternal("w-test123")

	assert.Equal(t, 1, transfer.WorkflowCount(b))
}

// TestSendWorkflowStepStateInvalidStatus verifies invalid status is rejected.
func TestSendWorkflowStepStateInvalidStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status string
		wantOK bool
	}{
		{status: "SUCCESS", wantOK: true},
		{status: "FAILURE", wantOK: true},
		{status: "COMPLETE", wantOK: false},  // fabricated value; real enum is SUCCESS/FAILURE
		{status: "EXCEPTION", wantOK: false}, // fabricated value; real enum is SUCCESS/FAILURE
		{status: "COMPLETED", wantOK: false}, // close but wrong
		{status: "", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			t.Parallel()

			b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
			wf, err := b.CreateWorkflow("test", nil, nil, nil)
			require.NoError(t, err)

			exec, err := b.CreateExecution(wf.WorkflowID)
			require.NoError(t, err)

			err = b.SendWorkflowStepStateRecord(wf.WorkflowID, exec.ExecutionID, "tok-1", tt.status)
			if tt.wantOK {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrInvalidParameter)
			}
		})
	}
}

func TestCreateWorkflow(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	wf, err := b.CreateWorkflow("my workflow", nil, nil, map[string]string{"team": "data"})
	require.NoError(t, err)
	assert.NotEmpty(t, wf.WorkflowID)
	assert.Equal(t, "my workflow", wf.Description)
}
