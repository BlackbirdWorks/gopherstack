package iot_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
