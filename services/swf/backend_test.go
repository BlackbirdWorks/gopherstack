package swf_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

func TestRegisterDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		name        string
		domain      string
		description string
		retention   string
		wantName    string
		wantStatus  string
		preRegister []string
		wantCount   int
	}{
		{
			name:        "success",
			domain:      "my-domain",
			description: "test domain",
			retention:   "30",
			wantCount:   1,
			wantName:    "my-domain",
			wantStatus:  "REGISTERED",
		},
		{
			name:        "success_none_retention",
			domain:      "my-domain2",
			description: "",
			retention:   "NONE",
			wantCount:   1,
			wantName:    "my-domain2",
			wantStatus:  "REGISTERED",
		},
		{
			name:        "AlreadyExists",
			preRegister: []string{"my-domain"},
			domain:      "my-domain",
			wantErr:     swf.ErrAlreadyExists,
		},
		{
			name:      "invalid_retention",
			domain:    "bad-ret",
			retention: "999",
			wantErr:   swf.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := swf.NewInMemoryBackend()
			for _, d := range tt.preRegister {
				require.NoError(t, b.RegisterDomain(d, "", "NONE"))
			}

			err := b.RegisterDomain(tt.domain, tt.description, tt.retention)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)

			domains, err := b.ListDomains("REGISTERED")
			require.NoError(t, err)
			require.Len(t, domains, tt.wantCount)
			assert.Equal(t, tt.wantName, domains[0].Name)
			assert.Equal(t, tt.wantStatus, domains[0].Status)
		})
	}
}

func TestDeprecateDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		name        string
		domain      string
		wantCount   int
		preRegister bool
	}{
		{
			name:        "success",
			preRegister: true,
			domain:      "my-domain",
			wantCount:   1,
		},
		{
			name:    "NotFound",
			domain:  "nonexistent",
			wantErr: swf.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := swf.NewInMemoryBackend()
			if tt.preRegister {
				require.NoError(t, b.RegisterDomain(tt.domain, "", "NONE"))
			}

			err := b.DeprecateDomain(tt.domain)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)

			domains, err := b.ListDomains("DEPRECATED")
			require.NoError(t, err)
			require.Len(t, domains, tt.wantCount)
		})
	}
}

func TestRegisterWorkflowType(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("my-domain", "", "NONE"))

	err := b.RegisterWorkflowType("my-domain", "my-workflow", "1.0", "", swf.WorkflowTypeDefaults{})
	require.NoError(t, err)

	wts, err := b.ListWorkflowTypes("my-domain", "")
	require.NoError(t, err)
	require.Len(t, wts, 1)
	assert.Equal(t, "my-workflow", wts[0].Name)
}

func TestWorkflowExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		name       string
		domain     string
		workflowID string
		runID      string
		wantStatus string
		wantRunID  string
		startFirst bool
	}{
		{
			name:       "StartAndDescribe",
			startFirst: true,
			domain:     "my-domain",
			workflowID: "wf-001",
			runID:      "run-001",
			wantStatus: "RUNNING",
			wantRunID:  "run-001",
		},
		{
			name:       "DescribeNotFound",
			domain:     "my-domain",
			workflowID: "nonexistent",
			wantErr:    swf.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := swf.NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain(tt.domain, "", "NONE"))

			if tt.startFirst {
				exec, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
					Domain:     tt.domain,
					WorkflowID: tt.workflowID,
					RunID:      tt.runID,
				})
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, exec.Status)
			}

			got, err := b.DescribeWorkflowExecution(tt.domain, tt.workflowID)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantRunID, got.RunID)
		})
	}
}

func TestListDomains(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("d1", "", "NONE"))
	require.NoError(t, b.RegisterDomain("d2", "", "NONE"))

	all, err := b.ListDomains("")
	require.NoError(t, err)
	assert.Len(t, all, 2)
}

func TestListDomains_InvalidStatus(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	_, err := b.ListDomains("INVALID")
	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrValidation)
}

func TestRegisterDomain_RetentionStoredAndReturned(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("ret-dom", "desc", "45"))

	d, err := b.DescribeDomain("ret-dom")
	require.NoError(t, err)
	assert.Equal(t, "45", d.WorkflowExecutionRetentionPeriodInDays)
}

func TestWorkflowTypeDefaults(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))

	defaults := swf.WorkflowTypeDefaults{
		DefaultTaskList:                     "my-task-list",
		DefaultTaskPriority:                 "10",
		DefaultTaskStartToCloseTimeout:      "3600",
		DefaultExecutionStartToCloseTimeout: "86400",
		DefaultChildPolicy:                  "TERMINATE",
		DefaultLambdaRole:                   "arn:aws:iam::123:role/swf-lambda",
	}
	require.NoError(t, b.RegisterWorkflowType("dom", "wf1", "1.0", "desc", defaults))

	wt, err := b.DescribeWorkflowType("dom", "wf1", "1.0")
	require.NoError(t, err)
	assert.Equal(t, defaults.DefaultTaskList, wt.Defaults.DefaultTaskList)
	assert.Equal(t, defaults.DefaultChildPolicy, wt.Defaults.DefaultChildPolicy)
	assert.Equal(t, defaults.DefaultExecutionStartToCloseTimeout, wt.Defaults.DefaultExecutionStartToCloseTimeout)
}

func TestActivityTypeDefaults(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))

	defaults := swf.ActivityTypeDefaults{
		DefaultTaskList:                   "act-list",
		DefaultTaskHeartbeatTimeout:       "300",
		DefaultTaskScheduleToCloseTimeout: "3600",
		DefaultTaskScheduleToStartTimeout: "600",
		DefaultTaskStartToCloseTimeout:    "3000",
	}
	require.NoError(t, b.RegisterActivityType("dom", "act1", "1.0", "activity", defaults))

	at, err := b.DescribeActivityType("dom", "act1", "1.0")
	require.NoError(t, err)
	assert.Equal(t, defaults.DefaultTaskHeartbeatTimeout, at.Defaults.DefaultTaskHeartbeatTimeout)
	assert.Equal(t, defaults.DefaultTaskScheduleToCloseTimeout, at.Defaults.DefaultTaskScheduleToCloseTimeout)
}

func TestStartWorkflowExecution_WithWorkflowType(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterWorkflowType("dom", "wf", "1.0", "", swf.WorkflowTypeDefaults{
		DefaultTaskList:    "default",
		DefaultChildPolicy: "TERMINATE",
	}))

	exec, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:              "dom",
		WorkflowID:          "wf-1",
		WorkflowTypeName:    "wf",
		WorkflowTypeVersion: "1.0",
		Input:               `{"key":"value"}`,
		TagList:             []string{"env:test"},
	})
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", exec.Status)
	assert.Equal(t, "wf", exec.WorkflowTypeName)
	assert.Equal(t, "default", exec.TaskList)
	assert.Equal(t, "TERMINATE", exec.ChildPolicy)
	assert.Equal(t, []string{"env:test"}, exec.TagList)
}

func TestStartWorkflowExecution_DeprecatedTypeRejected(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterWorkflowType("dom", "wf", "1.0", "", swf.WorkflowTypeDefaults{}))
	require.NoError(t, b.DeprecateWorkflowType("dom", "wf", "1.0"))

	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:              "dom",
		WorkflowID:          "wf-1",
		WorkflowTypeName:    "wf",
		WorkflowTypeVersion: "1.0",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrTypeDeprecated)
}

func TestTerminateWorkflowExecution_ReasonInHistory(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:     "dom",
		WorkflowID: "wf-1",
	})
	require.NoError(t, err)

	require.NoError(t, b.TerminateWorkflowExecution("dom", "wf-1", "", "out of budget", "details here"))

	events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", 0, "", false)
	require.NotEmpty(t, events)
	last := events[len(events)-1]
	assert.Equal(t, "WorkflowExecutionTerminated", last.EventType)
	attrKey := "workflowExecutionTerminatedEventAttributes"
	attrs, ok := last.Attributes[attrKey].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "out of budget", attrs["reason"])
}

func TestRequestCancelWorkflowExecution_SetsFlag(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:     "dom",
		WorkflowID: "wf-1",
		TaskList:   "default",
	})
	require.NoError(t, err)

	require.NoError(t, b.RequestCancelWorkflowExecution("dom", "wf-1", ""))

	exec, err := b.DescribeWorkflowExecution("dom", "wf-1")
	require.NoError(t, err)
	assert.True(t, exec.CancelRequested)
}

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

	events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", 0, "", false)
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
	all, tok := b.GetWorkflowExecutionHistory("dom", "wf-1", 0, "", false)
	assert.Len(t, all, 6)
	assert.Empty(t, tok)

	// Page of 3
	page1, tok1 := b.GetWorkflowExecutionHistory("dom", "wf-1", 3, "", false)
	assert.Len(t, page1, 3)
	assert.NotEmpty(t, tok1)

	page2, tok2 := b.GetWorkflowExecutionHistory("dom", "wf-1", 3, tok1, false)
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

	events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", 0, "", true)
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

	events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", 0, "", false)
	require.NotEmpty(t, events)
	e := events[0]
	assert.Equal(t, "WorkflowExecutionStarted", e.EventType)
	attrs, ok := e.Attributes["workflowExecutionStartedEventAttributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "hello", attrs["input"])
	assert.Equal(t, "TERMINATE", attrs["childPolicy"])
}

func TestValidateChildPolicy(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))

	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:      "dom",
		WorkflowID:  "wf-1",
		ChildPolicy: "INVALID_POLICY",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrValidation)
}

func TestCountPendingTasks_ActuallyCount(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	b.EnqueueActivityTaskInternal("dom", "list1", "act-1", "MyAct", "1.0", "", "wf-1", "run-1")
	b.EnqueueActivityTaskInternal("dom", "list1", "act-2", "MyAct", "1.0", "", "wf-1", "run-1")

	assert.Equal(t, 2, b.CountPendingActivityTasks("dom", "list1"))
	assert.Equal(t, 0, b.CountPendingActivityTasks("dom", "other-list"))
}
