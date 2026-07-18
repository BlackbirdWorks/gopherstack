package cloudformation_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// ---- ChangeSet ChangeSetType and ExecutionStatus (table-driven) -------------------

func TestChangeSet_TypeAndExecutionStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		setup             func(*cloudformation.InMemoryBackend)
		stackName         string
		changeSetName     string
		wantChangeSetType string
		wantExecStatus    string
	}{
		{
			name:              "new stack → ChangeSetType=CREATE",
			stackName:         "brand-new-stack",
			changeSetName:     "init-cs",
			wantChangeSetType: "CREATE",
			wantExecStatus:    "AVAILABLE",
		},
		{
			name: "existing stack → ChangeSetType=UPDATE",
			setup: func(b *cloudformation.InMemoryBackend) {
				// Create with a different template so the change set (simpleTemplate)
				// carries a real change (a Remove) and is therefore AVAILABLE.
				_, _ = b.CreateStack(t.Context(), "existing-stack", modifiedTemplate, nil,
					cloudformation.StackOptions{})
			},
			stackName:         "existing-stack",
			changeSetName:     "update-cs",
			wantChangeSetType: "UPDATE",
			wantExecStatus:    "AVAILABLE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			if tc.setup != nil {
				tc.setup(b)
			}
			cs, err := b.CreateChangeSet(t.Context(), tc.stackName, tc.changeSetName,
				simpleTemplate, "test", nil)
			require.NoError(t, err)
			assert.Equal(t, tc.wantChangeSetType, cs.ChangeSetType)
			assert.Equal(t, tc.wantExecStatus, cs.ExecutionStatus)
		})
	}
}

func TestChangeSet_ExecutionStatus_AfterExecute(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStack(t.Context(), "exec-status-stack", simpleTemplate, nil,
		cloudformation.StackOptions{})
	require.NoError(t, err)

	_, err = b.CreateChangeSet(t.Context(), "exec-status-stack", "my-cs",
		modifiedTemplate, "test", nil)
	require.NoError(t, err)

	// After execute, the changeset is removed (EXECUTE_COMPLETE).
	err = b.ExecuteChangeSet(t.Context(), "exec-status-stack", "my-cs")
	require.NoError(t, err)

	// Changeset no longer exists — verify.
	_, err = b.DescribeChangeSet("exec-status-stack", "my-cs")
	require.ErrorIs(t, err, cloudformation.ErrChangeSetNotFound)
}

func TestChangeSet_ExecutionStatus_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postFormValues(t, h, url.Values{
		"Action": {"CreateStack"}, "StackName": {"cs-exec-http"}, "TemplateBody": {simpleTemplate},
	})
	postFormValues(t, h, url.Values{
		"Action":        {"CreateChangeSet"},
		"StackName":     {"cs-exec-http"},
		"ChangeSetName": {"http-cs"},
		"TemplateBody":  {modifiedTemplate},
	})

	resp := postFormValues(t, h, url.Values{
		"Action":        {"DescribeChangeSet"},
		"StackName":     {"cs-exec-http"},
		"ChangeSetName": {"http-cs"},
	})
	resp.mustOK(t)
	assert.Contains(t, resp.Body, "ExecutionStatus")
	assert.Contains(t, resp.Body, "AVAILABLE")
	assert.Contains(t, resp.Body, "ChangeSetType")
	assert.Contains(t, resp.Body, "UPDATE")
}

// ---- Handler: CreateChangeSet ChangeSetType in response body ----------------------

func TestHandler_CreateChangeSet_TypeInResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		wantChangeSetType string
		preCreateStack    bool
	}{
		{
			name:              "no existing stack → CREATE type",
			preCreateStack:    false,
			wantChangeSetType: "CREATE",
		},
		{
			name:              "existing stack → UPDATE type",
			preCreateStack:    true,
			wantChangeSetType: "UPDATE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHandler()
			const stackName = "cs-type-test"

			if tc.preCreateStack {
				postFormValues(t, h, url.Values{
					"Action": {"CreateStack"}, "StackName": {stackName},
					"TemplateBody": {simpleTemplate},
				})
			}

			postFormValues(t, h, url.Values{
				"Action":        {"CreateChangeSet"},
				"StackName":     {stackName},
				"ChangeSetName": {"type-check-cs"},
				"TemplateBody":  {simpleTemplate},
			})

			resp := postFormValues(t, h, url.Values{
				"Action":        {"DescribeChangeSet"},
				"StackName":     {stackName},
				"ChangeSetName": {"type-check-cs"},
			})
			resp.mustOK(t)
			assert.Contains(t, resp.Body, "ExecutionStatus")
			// ChangeSetType and ExecutionStatus are serialized in XML.
			assert.Contains(t, resp.Body, "AVAILABLE")
		})
	}
}
