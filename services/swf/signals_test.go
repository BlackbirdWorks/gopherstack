package swf_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

func TestSignalWorkflowExecution_AttributesInHistory(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:     "dom",
		WorkflowID: "wf-1",
	})
	require.NoError(t, err)

	require.NoError(t, b.SignalWorkflowExecution("dom", "wf-1", "", "my-signal", `{"key":"val"}`))

	events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", "", 0, "", false)
	var signalEvent *swf.HistoryEvent
	for i := range events {
		if events[i].EventType == "WorkflowExecutionSignaled" {
			signalEvent = &events[i]
		}
	}
	require.NotNil(t, signalEvent)
	attrs := signalEvent.Attributes["workflowExecutionSignaledEventAttributes"].(map[string]any)
	assert.Equal(t, "my-signal", attrs["signalName"])
	//nolint:testifylint // input is already a plain string
	assert.Equal(t, `{"key":"val"}`, attrs["input"])
}

// TestSignalWorkflowExecution_NotOpen_UnknownResourceFault verifies
// SignalWorkflowExecution on a closed execution fails with
// UnknownResourceFault, per the real SWF API doc: "If the specified workflow
// execution isn't open, this method fails with UnknownResource." --
// ValidationException isn't in this operation's fault model at all.
func TestSignalWorkflowExecution_NotOpen_UnknownResourceFault(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:     "dom",
		WorkflowID: "wf-1",
	})
	require.NoError(t, err)
	require.NoError(t, b.TerminateWorkflowExecution("dom", "wf-1", "", "", "", ""))

	err = b.SignalWorkflowExecution("dom", "wf-1", "", "my-signal", "")
	require.ErrorIs(t, err, swf.ErrNotFound)
	assert.NotErrorIs(t, err, swf.ErrValidation)
}
