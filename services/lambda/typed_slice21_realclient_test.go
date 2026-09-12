package lambda_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypedSlice21RealClient drives lambda's DurableExecution family
// (gopherstack-n3zi typed slice 21) through the real aws-sdk-go-v2 lambda
// client. This backend has no StartDurableExecution entry point (correctly
// -- neither does the real API; a real execution starts implicitly on
// Invoke, and this emulator's Invoke path doesn't model durable-execution
// semantics either, both pre-existing, documented gaps in PARITY.md's
// durable_execution note). CheckpointDurableExecution auto-creates the
// execution record on first use (durableExecutionStore.checkpoint), which
// is the only test-reachable creation path -- used here instead of a
// fabricated backdoor.
func TestTypedSlice21RealClient(t *testing.T) {
	t.Parallel()

	t.Run("checkpoint lifecycle", func(t *testing.T) {
		t.Parallel()

		h, _ := newInMemoryHandler(t)
		client := newTestLambdaClient(t, h)
		ctx := t.Context()
		arn := "arn:aws:lambda:us-east-1:000000000000:function:s21-durable-fn:durable-execution:exec-lifecycle"

		cpOut, err := client.CheckpointDurableExecution(
			ctx,
			&lambdasdk.CheckpointDurableExecutionInput{
				DurableExecutionArn: aws.String(arn),
				CheckpointToken:     aws.String("seed"),
				Updates: []types.OperationUpdate{
					{
						Id:     aws.String("step1"),
						Type:   types.OperationTypeStep,
						Action: types.OperationActionStart,
					},
				},
			},
		)
		require.NoError(t, err)
		require.NotNil(t, cpOut.NewExecutionState)
		require.Len(t, cpOut.NewExecutionState.Operations, 1)
		assert.Equal(t, "step1", aws.ToString(cpOut.NewExecutionState.Operations[0].Id))
		assert.Equal(t, types.OperationStatusStarted, cpOut.NewExecutionState.Operations[0].Status)
		assert.NotEmpty(t, aws.ToString(cpOut.CheckpointToken))

		_, err = client.CheckpointDurableExecution(ctx, &lambdasdk.CheckpointDurableExecutionInput{
			DurableExecutionArn: aws.String(arn),
			CheckpointToken:     aws.String(aws.ToString(cpOut.CheckpointToken)),
			Updates: []types.OperationUpdate{
				{
					Id:      aws.String("step1"),
					Type:    types.OperationTypeStep,
					Action:  types.OperationActionSucceed,
					Payload: aws.String(`{"ok":true}`),
				},
			},
		})
		require.NoError(t, err)

		getOut, err := client.GetDurableExecution(ctx, &lambdasdk.GetDurableExecutionInput{
			DurableExecutionArn: aws.String(arn),
		})
		require.NoError(t, err)
		assert.Equal(t, arn, aws.ToString(getOut.DurableExecutionArn))
		assert.Equal(t, "exec-lifecycle", aws.ToString(getOut.DurableExecutionName))
		assert.Equal(t, types.ExecutionStatusRunning, getOut.Status)
		assert.False(t, aws.ToTime(getOut.StartTimestamp).IsZero())
		// FunctionArn is a required real member this backend never populates
		// -- no entry point threads function identity into a checkpoint-
		// created execution (PARITY.md durable_execution note, "entry-point/
		// architecture gap, not a wire-shape gap"). Documenting, not fixing.
		assert.Empty(t, aws.ToString(getOut.FunctionArn))

		histOut, err := client.GetDurableExecutionHistory(
			ctx,
			&lambdasdk.GetDurableExecutionHistoryInput{
				DurableExecutionArn: aws.String(arn),
			},
		)
		require.NoError(t, err)
		require.NotEmpty(t, histOut.Events)
		assert.Equal(t, types.EventTypeExecutionStarted, histOut.Events[0].EventType)

		var sawStepStarted, sawStepSucceeded bool

		for _, ev := range histOut.Events {
			switch ev.EventType { //nolint:exhaustive // only the two event types this test produces matter here
			case types.EventTypeStepStarted:
				sawStepStarted = true
				assert.Equal(t, "step1", aws.ToString(ev.Id))
			case types.EventTypeStepSucceeded:
				sawStepSucceeded = true
			}
		}

		assert.True(t, sawStepStarted, "expected a StepStarted event")
		assert.True(t, sawStepSucceeded, "expected a StepSucceeded event")

		stateOut, err := client.GetDurableExecutionState(
			ctx,
			&lambdasdk.GetDurableExecutionStateInput{
				DurableExecutionArn: aws.String(arn),
				CheckpointToken:     aws.String("ignored-by-this-backend"),
			},
		)
		require.NoError(t, err)
		require.Len(t, stateOut.Operations, 2)
		assert.Equal(t, types.OperationTypeExecution, stateOut.Operations[0].Type)
		assert.Equal(t, "step1", aws.ToString(stateOut.Operations[1].Id))
		assert.Equal(t, types.OperationStatusSucceeded, stateOut.Operations[1].Status)

		stopOut, err := client.StopDurableExecution(ctx, &lambdasdk.StopDurableExecutionInput{
			DurableExecutionArn: aws.String(arn),
			Error: &types.ErrorObject{
				ErrorType:    aws.String("CustomStopReason"),
				ErrorMessage: aws.String("stopped by typed slice 21 test"),
			},
		})
		require.NoError(t, err)
		assert.False(t, aws.ToTime(stopOut.StopTimestamp).IsZero())

		getOut2, err := client.GetDurableExecution(ctx, &lambdasdk.GetDurableExecutionInput{
			DurableExecutionArn: aws.String(arn),
		})
		require.NoError(t, err)
		assert.Equal(t, types.ExecutionStatusStopped, getOut2.Status)

		histOut2, err := client.GetDurableExecutionHistory(
			ctx,
			&lambdasdk.GetDurableExecutionHistoryInput{
				DurableExecutionArn: aws.String(arn),
			},
		)
		require.NoError(t, err)

		var stoppedEvent *types.Event

		for i := range histOut2.Events {
			if histOut2.Events[i].EventType == types.EventTypeExecutionStopped {
				stoppedEvent = &histOut2.Events[i]
			}
		}

		require.NotNil(t, stoppedEvent, "expected an ExecutionStopped event")
		require.NotNil(t, stoppedEvent.ExecutionStoppedDetails)
		require.NotNil(t, stoppedEvent.ExecutionStoppedDetails.Error)
		require.NotNil(t, stoppedEvent.ExecutionStoppedDetails.Error.Payload)
		// Bug fix (slice 21): the real StopDurableExecution request body IS
		// the ErrorObject (serializers.go: awsRestjson1_serializeDocumentErrorObject
		// writes ErrorType/ErrorMessage/etc. as top-level keys), not
		// {"Error": {...}}. The handler previously unmarshalled into a
		// {Error *ErrorObject} wrapper, so a real client's Error was always
		// silently dropped and every stop fell back to the generic default
		// message -- this assertion fails against the unfixed handler.
		assert.Equal(
			t,
			"CustomStopReason",
			aws.ToString(stoppedEvent.ExecutionStoppedDetails.Error.Payload.ErrorType),
		)
		assert.Equal(t, "stopped by typed slice 21 test",
			aws.ToString(stoppedEvent.ExecutionStoppedDetails.Error.Payload.ErrorMessage))

		// Idempotent second stop: reports the same StopTimestamp, no error.
		stopOut2, err := client.StopDurableExecution(ctx, &lambdasdk.StopDurableExecutionInput{
			DurableExecutionArn: aws.String(arn),
		})
		require.NoError(t, err)
		assert.Equal(
			t,
			aws.ToTime(stopOut.StopTimestamp).Unix(),
			aws.ToTime(stopOut2.StopTimestamp).Unix(),
		)
	})

	t.Run("callbacks", func(t *testing.T) {
		t.Parallel()

		h, _ := newInMemoryHandler(t)
		client := newTestLambdaClient(t, h)
		ctx := t.Context()
		arn := "arn:aws:lambda:us-east-1:000000000000:function:s21-durable-fn:durable-execution:exec-callbacks"

		_, err := client.CheckpointDurableExecution(ctx, &lambdasdk.CheckpointDurableExecutionInput{
			DurableExecutionArn: aws.String(arn),
			CheckpointToken:     aws.String("seed"),
			Updates: []types.OperationUpdate{
				{
					Id:     aws.String("cb-succeed"),
					Type:   types.OperationTypeCallback,
					Action: types.OperationActionStart,
				},
				{
					Id:     aws.String("cb-fail"),
					Type:   types.OperationTypeCallback,
					Action: types.OperationActionStart,
				},
				{
					Id:     aws.String("cb-heartbeat"),
					Type:   types.OperationTypeCallback,
					Action: types.OperationActionStart,
				},
			},
		})
		require.NoError(t, err)

		_, err = client.SendDurableExecutionCallbackSuccess(
			ctx,
			&lambdasdk.SendDurableExecutionCallbackSuccessInput{
				CallbackId: aws.String("cb-succeed"),
				Result:     []byte(`{"done":true}`),
			},
		)
		require.NoError(t, err)

		_, err = client.SendDurableExecutionCallbackFailure(
			ctx,
			&lambdasdk.SendDurableExecutionCallbackFailureInput{
				CallbackId: aws.String("cb-fail"),
				Error: &types.ErrorObject{
					ErrorType:    aws.String("BadCallback"),
					ErrorMessage: aws.String("nope"),
				},
			},
		)
		require.NoError(t, err)

		_, err = client.SendDurableExecutionCallbackHeartbeat(
			ctx,
			&lambdasdk.SendDurableExecutionCallbackHeartbeatInput{
				CallbackId: aws.String("cb-heartbeat"),
			},
		)
		require.NoError(t, err)

		stateOut, err := client.GetDurableExecutionState(
			ctx,
			&lambdasdk.GetDurableExecutionStateInput{
				DurableExecutionArn: aws.String(arn),
				CheckpointToken:     aws.String("ignored-by-this-backend"),
			},
		)
		require.NoError(t, err)

		byID := make(map[string]types.OperationStatus, len(stateOut.Operations))
		for _, op := range stateOut.Operations {
			byID[aws.ToString(op.Id)] = op.Status
		}

		assert.Equal(t, types.OperationStatusSucceeded, byID["cb-succeed"])
		assert.Equal(t, types.OperationStatusFailed, byID["cb-fail"])
		// Heartbeat has no OperationStatus transition on the real API (no
		// "CallbackHeartbeat" EventType exists; real AWS just extends an
		// internal timeout) -- this backend correctly treats it as a no-op,
		// so the callback stays STARTED. sendCallback's own doc comment.
		assert.Equal(t, types.OperationStatusStarted, byID["cb-heartbeat"])

		// Unknown callback ID: real API 404s (ErrDurableExecutionNotFound-
		// class ResourceNotFoundException), confirmed via a real client.
		_, err = client.SendDurableExecutionCallbackSuccess(
			ctx,
			&lambdasdk.SendDurableExecutionCallbackSuccessInput{
				CallbackId: aws.String("cb-does-not-exist"),
			},
		)
		require.Error(t, err)
	})

	t.Run("list by function", func(t *testing.T) {
		t.Parallel()

		h, _ := newInMemoryHandler(t)
		client := newTestLambdaClient(t, h)
		ctx := t.Context()
		arn := "arn:aws:lambda:us-east-1:000000000000:function:s21-list-fn:durable-execution:exec-list"

		_, err := client.CheckpointDurableExecution(ctx, &lambdasdk.CheckpointDurableExecutionInput{
			DurableExecutionArn: aws.String(arn),
			CheckpointToken:     aws.String("seed"),
			Updates: []types.OperationUpdate{
				{
					Id:     aws.String("step1"),
					Type:   types.OperationTypeStep,
					Action: types.OperationActionStart,
				},
			},
		})
		require.NoError(t, err)

		listOut, err := client.ListDurableExecutionsByFunction(
			ctx,
			&lambdasdk.ListDurableExecutionsByFunctionInput{
				FunctionName: aws.String("s21-list-fn"),
			},
		)
		require.NoError(t, err)
		// Real-shape round trip proven (decodes cleanly), but this backend
		// can never return a match: CheckpointDurableExecution is the only
		// creation path and its request carries no function identity at all
		// (DurableExecutionArn is client-opaque, "server-never-parses-
		// structure-from-it" per deriveDurableExecutionName's own doc
		// comment), so DurableExecution.FunctionARN is never assigned
		// anywhere in this package -- confirmed by grep, zero write sites.
		// Same root cause as PARITY.md's documented FunctionArn-always-empty
		// gap (no StartDurableExecution/Invoke entry point), one step
		// further: it also makes this entire op permanently return zero
		// results for any function. Recorded in items_still_open rather
		// than fixed -- fixing it needs the same out-of-scope Invoke
		// rewiring that gap already defers.
		assert.Empty(t, listOut.DurableExecutions)
	})
}
