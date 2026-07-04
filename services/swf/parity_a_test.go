package swf_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_SignalWorkflowExecutionRequiresSignalName verifies that
// SignalWorkflowExecution rejects requests with an empty or absent signalName.
// Real AWS returns ValidationException for this case; the emulator previously
// accepted empty signalNames, recording a history event with no signal name.
func TestParity_SignalWorkflowExecutionRequiresSignalName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "empty_signal_name_rejected",
			body: map[string]any{
				"domain":     "test-domain",
				"workflowId": "wf-1",
				"signalName": "",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "absent_signal_name_rejected",
			body: map[string]any{
				"domain":     "test-domain",
				"workflowId": "wf-1",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "non_empty_signal_name_accepted",
			body: map[string]any{
				"domain":     "test-domain",
				"workflowId": "wf-1",
				"signalName": "my-signal",
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			rec := doSWFRequest(t, h, "SignalWorkflowExecution", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"SignalWorkflowExecution status for case %q", tt.name)

			var errResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			errType, _ := errResp["__type"].(string)

			if tt.name == "non_empty_signal_name_accepted" {
				assert.Contains(t, errType, "UnknownResource",
					"valid signalName but non-existent workflow must return UnknownResource")
			} else {
				assert.Contains(t, errType, "Validation",
					"empty signalName must return ValidationException")
			}
		})
	}
}
