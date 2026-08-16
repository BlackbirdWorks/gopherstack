package iot_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestBatch3_CommandCRUD tests Command create/get/update/list/delete.
func TestCommandCRUD(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Create
	out := iotOK(t, h, http.MethodPut, "/commands/my-cmd", map[string]any{
		"displayName": "My Command",
		"description": "test command",
		"namespace":   "AWS-IoT",
	})
	if out["commandId"] != "my-cmd" {
		t.Errorf("expected commandId=my-cmd, got %v", out)
	}

	// Get
	out2 := iotOK(t, h, http.MethodGet, "/commands/my-cmd", nil)
	if out2["commandId"] != "my-cmd" {
		t.Errorf("get mismatch: %v", out2)
	}

	// Update
	iotOK(t, h, http.MethodPatch, "/commands/my-cmd", map[string]any{
		"description": "updated",
	})

	// List
	out3 := iotOK(t, h, http.MethodGet, "/commands", nil)
	cmds, _ := out3["commands"].([]any)
	if len(cmds) != 1 {
		t.Errorf("expected 1 command, got %d", len(cmds))
	}

	// Delete
	iotOK(t, h, http.MethodDelete, "/commands/my-cmd", nil)
	iotExpectError(t, h, "/commands/my-cmd")
}

func TestDeleteCommandExecution(t *testing.T) {
	t.Parallel()

	t.Run("round_trip", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		b.AddCommandExecutionInternal("cmd-1", "exec-1", iot.IoTCommandExecution{
			CommandARN: "arn:aws:iot:us-east-1:123456789012:command/cmd-1",
			ThingARN:   "arn:aws:iot:us-east-1:123456789012:thing/my-thing",
			Status:     "SUCCEEDED",
		})

		rec := doRefRequest(t, h, http.MethodDelete,
			"/command-executions/exec-1?targetArn=arn:aws:iot:us-east-1:123456789012:thing/my-thing", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		_, err := b.GetCommandExecution("cmd-1", "exec-1")
		require.Error(t, err)
	})

	t.Run("unknown_execution_404", func(t *testing.T) {
		t.Parallel()

		h, _ := newRefHandler()

		rec := doRefRequest(t, h, http.MethodDelete, "/command-executions/no-such", nil, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// TestListCommands_SummaryScoping proves handleListCommands stops leaking
// payload/tags/description/namespace, none of which types.CommandSummary
// (iot@v1.77.4 types.go:1504-1527) declares. An SDK-driven client cannot
// prove this: its deserializer silently discards keys it does not
// recognize, so the over-wide response would decode "successfully" either
// way. Asserting on the raw JSON body is the only technique that actually
// distinguishes fixed from unfixed here.
func TestListCommands_SummaryScoping(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerForBatch3Test(t)

	iotOK(t, h, http.MethodPut, "/commands/my-cmd", map[string]any{
		"displayName": "My Command",
		"description": "must not leak",
		"namespace":   "must-not-leak",
		"payload":     map[string]any{"k": "v"},
		"tags":        []map[string]string{{"Key": "env", "Value": "prod"}},
	})

	out := iotOK(t, h, http.MethodGet, "/commands", nil)
	cmds, ok := out["commands"].([]any)
	require.True(t, ok)
	require.Len(t, cmds, 1)

	cmd, ok := cmds[0].(map[string]any)
	require.True(t, ok)

	for _, forbidden := range []string{"payload", "tags", "description", "namespace"} {
		assert.NotContainsf(t, cmd, forbidden, "%s is not a member of types.CommandSummary", forbidden)
	}
	for _, want := range []string{
		"commandArn", "commandId", "createdAt", "deprecated",
		"displayName", "lastUpdatedAt", "pendingDeletion",
	} {
		assert.Containsf(t, cmd, want, "%s is a member of types.CommandSummary", want)
	}
}

// TestSDKRoundTrip_ListCommandExecutions drives the real aws-sdk-go-v2 IoT
// client through POST /command-executions (iot@v1.77.4 serializers.go:13785)
// end to end. Unlike the over-wide summary bugs, a raw-body assertion is
// weak here: the bug is a wrong key name plus a route the real client could
// not previously reach at all (matchIoTPath never matched the bare
// "/command-executions" path, and resolveCommandOps never resolved
// ListCommandExecutions for it), so only a real client proves the fix
// actually reaches the wire. Before the fix this failed at require.NoError
// (unreachable route); after fixing only the field name it would still
// fail (TargetArn nil, since the wire key was "thingArn").
func TestSDKRoundTrip_ListCommandExecutions(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	client := newTestIoTClient(t, h)

	backend.AddCommandExecutionInternal("cmd-1", "exec-1", iot.IoTCommandExecution{
		CommandARN: "arn:aws:iot:us-east-1:123456789012:command/cmd-1",
		ThingARN:   "arn:aws:iot:us-east-1:123456789012:thing/my-thing",
		Status:     "SUCCEEDED",
	})

	out, err := client.ListCommandExecutions(t.Context(), &iotsdk.ListCommandExecutionsInput{
		TargetArn: aws.String("arn:aws:iot:us-east-1:123456789012:thing/my-thing"),
	})
	require.NoError(t, err)
	require.Len(t, out.CommandExecutions, 1)

	exec := out.CommandExecutions[0]
	assert.Equal(t, "arn:aws:iot:us-east-1:123456789012:thing/my-thing", aws.ToString(exec.TargetArn))
	assert.Equal(t, "exec-1", aws.ToString(exec.ExecutionId))
	assert.Equal(t, "arn:aws:iot:us-east-1:123456789012:command/cmd-1", aws.ToString(exec.CommandArn))
	assert.Nil(t, exec.CompletedAt)
	assert.Nil(t, exec.StartedAt)
}

// TestSDKRoundTrip_GetCommandExecution drives the real aws-sdk-go-v2 IoT
// client through GET /command-executions/{executionId} (iot@v1.77.4
// api_op_GetCommandExecution.go), GetCommandExecution's real route. The
// path already matched matchIoTPath (via the same pathCommandExecutions
// prefix rule ListCommandExecutions' fix relies on), but
// resolveFinalOpsGroupB only had a DELETE case for that prefix -- GET fell
// through to unknownOperation, so a real client's GetCommandExecution 400'd
// on "unknown operation" before ever reaching the handler. A raw-body
// assertion against the handler cannot show this: it bypasses resolveOperation
// and the RouteMatcher entirely, so it would pass against unfixed code too.
func TestSDKRoundTrip_GetCommandExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		executionID string
		wantErr     bool
	}{
		{name: "found_by_execution_id_and_target_arn", executionID: "exec-1"},
		{name: "unknown_execution_id_errors", executionID: "no-such-exec", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := iot.NewInMemoryBackend()
			h := iot.NewHandler(backend, nil)
			client := newTestIoTClient(t, h)

			backend.AddCommandExecutionInternal("cmd-1", "exec-1", iot.IoTCommandExecution{
				CommandARN: "arn:aws:iot:us-east-1:123456789012:command/cmd-1",
				ThingARN:   "arn:aws:iot:us-east-1:123456789012:thing/my-thing",
				Status:     "SUCCEEDED",
			})

			out, err := client.GetCommandExecution(t.Context(), &iotsdk.GetCommandExecutionInput{
				ExecutionId: aws.String(tt.executionID),
				TargetArn:   aws.String("arn:aws:iot:us-east-1:123456789012:thing/my-thing"),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "exec-1", aws.ToString(out.ExecutionId))
			assert.Equal(t, "arn:aws:iot:us-east-1:123456789012:command/cmd-1", aws.ToString(out.CommandArn))
			assert.Equal(t, "arn:aws:iot:us-east-1:123456789012:thing/my-thing", aws.ToString(out.TargetArn))
			assert.Equal(t, "SUCCEEDED", string(out.Status))
			assert.Nil(t, out.CompletedAt)
			assert.Nil(t, out.StartedAt)
		})
	}
}
