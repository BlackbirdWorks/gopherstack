package swf_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

func TestGetWorkflowExecutionHistory_Pagination(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:     "dom",
		WorkflowID: "wf-1",
	})
	require.NoError(t, err)
	// Add more events via signals.
	for range 5 {
		require.NoError(t, b.SignalWorkflowExecution("dom", "wf-1", "", "sig", ""))
	}

	// Total events: 1 (started) + 5 (signaled) = 6
	all, tok := b.GetWorkflowExecutionHistory("dom", "wf-1", "", 0, "", false)
	assert.Len(t, all, 6)
	assert.Empty(t, tok)

	// Page of 3
	page1, tok1 := b.GetWorkflowExecutionHistory("dom", "wf-1", "", 3, "", false)
	assert.Len(t, page1, 3)
	assert.NotEmpty(t, tok1)

	page2, tok2 := b.GetWorkflowExecutionHistory("dom", "wf-1", "", 3, tok1, false)
	assert.Len(t, page2, 3)
	assert.Empty(t, tok2)
}

func TestGetWorkflowExecutionHistory_ReverseOrder(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:     "dom",
		WorkflowID: "wf-1",
	})
	require.NoError(t, err)
	require.NoError(t, b.SignalWorkflowExecution("dom", "wf-1", "", "sig", ""))

	events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", "", 0, "", true)
	require.Len(t, events, 2)
	assert.Equal(t, "WorkflowExecutionSignaled", events[0].EventType)
	assert.Equal(t, "WorkflowExecutionStarted", events[1].EventType)
}

func TestHistoryEvent_AttributesMarshal(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterWorkflowType("dom", "wf", "1.0", "", swf.WorkflowTypeDefaults{}))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:              "dom",
		WorkflowID:          "wf-1",
		WorkflowTypeName:    "wf",
		WorkflowTypeVersion: "1.0",
		Input:               "hello",
		ChildPolicy:         "TERMINATE",
	})
	require.NoError(t, err)

	events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", "", 0, "", false)
	require.NotEmpty(t, events)
	e := events[0]
	assert.Equal(t, "WorkflowExecutionStarted", e.EventType)
	attrs, ok := e.Attributes["workflowExecutionStartedEventAttributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "hello", attrs["input"])
	assert.Equal(t, "TERMINATE", attrs["childPolicy"])
}
