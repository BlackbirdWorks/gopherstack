package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		action  string
		wantMsg string
	}{
		{
			name:   "instance identifier",
			action: "CreateReplicationInstance",
			body: map[string]any{
				"ReplicationInstanceIdentifier": "1bad_ID", "ReplicationInstanceClass": "dms.t3.micro",
			},
			wantMsg: "ReplicationInstanceIdentifier",
		},
		{
			name:   "instance class",
			action: "CreateReplicationInstance",
			body: map[string]any{
				"ReplicationInstanceIdentifier": "ri", "ReplicationInstanceClass": "bogus",
			},
			wantMsg: "ReplicationInstanceClass",
		},
		{
			name:   "allocated storage",
			action: "CreateReplicationInstance",
			body: map[string]any{
				"ReplicationInstanceIdentifier": "ri", "ReplicationInstanceClass": "dms.t3.micro",
				"AllocatedStorage": 1,
			},
			wantMsg: "AllocatedStorage",
		},
		{
			name:    "garbage marker",
			action:  "DescribeReplicationTasks",
			body:    map[string]any{"Marker": "garbage!"},
			wantMsg: "Invalid marker",
		},
		{
			name:    "unknown endpoint",
			action:  "DeleteEndpoint",
			body:    map[string]any{"EndpointArn": "arn:aws:dms:us-east-1:000000000000:endpoint:NOPE"},
			wantMsg: "not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			rec := doDMS(t, h, tt.action, tt.body)
			require.GreaterOrEqual(t, rec.Code, http.StatusBadRequest)

			out := parseJSON(t, rec)
			assert.Contains(t, out["message"], tt.wantMsg)
			assert.NotContains(t, out["message"], "Fault:")
			assert.NotContains(t, out["message"], "Exception:")
		})
	}
}

func TestReplicationTaskStartsThenRuns(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddReplicationInstanceInternal("ri", "dms.t3.medium")
	h.Backend.AddEndpointInternal("src", "source", "mysql")
	h.Backend.AddEndpointInternal("tgt", "target", "postgres")
	h.Backend.AddReplicationTaskInternal("task", "src", "tgt", "ri", "full-load")

	taskArn := describeTaskArn(t, h, "task")
	rec := doDMS(t, h, "StartReplicationTask", map[string]any{"ReplicationTaskArn": taskArn})
	require.Equal(t, http.StatusOK, rec.Code)

	stopRec := doDMS(t, h, "DeleteReplicationTask", map[string]any{"ReplicationTaskArn": taskArn})
	assert.Equal(t, http.StatusBadRequest, stopRec.Code, "starting task cannot be deleted")

	for _, want := range []string{"starting", "running"} {
		list := doDMS(t, h, "DescribeReplicationTasks", map[string]any{})
		tasks := parseJSON(t, list)["ReplicationTasks"].([]any)
		require.Len(t, tasks, 1)
		assert.Equal(t, want, tasks[0].(map[string]any)["Status"])
	}
}
