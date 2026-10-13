package stepfunctions_test

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

type doneECSWaiter struct{}

func (doneECSWaiter) SFNPollSyncTask(context.Context, any) (asl.ECSSyncPoll, error) {
	return asl.ECSSyncPoll{Done: true, Result: map[string]any{"Tasks": []any{"finished"}}}, nil
}

func taskEventTypes(events []stepfunctions.HistoryEvent) []string {
	var out []string
	for _, ev := range events {
		switch ev.Type {
		case "TaskScheduled", "TaskStarted", "TaskSubmitted", "TaskSucceeded", "TaskFailed":
			out = append(out, ev.Type)
		}
	}

	return out
}

func TestExecutionHistory_TaskStartedAndSubmitted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(b *stepfunctions.InMemoryBackend)
		name       string
		definition string
		wantOutput string
		wantTypes  []string
		finish     bool
	}{
		{
			name:       "lambda_request_response",
			definition: taskLambdaDefinition,
			setup:      func(b *stepfunctions.InMemoryBackend) { b.SetLambdaInvoker(&mockLambdaForBackend{}) },
			wantTypes:  []string{"TaskScheduled", "TaskStarted", "TaskSucceeded"},
		},
		{
			name: "wait_for_task_token",
			definition: `{"StartAt":"S","States":{"S":{"Type":"Task",` +
				`"Resource":"arn:aws:states:::sqs:sendMessage.waitForTaskToken",` +
				`"Parameters":{"QueueUrl":"https://sqs.us-east-1.amazonaws.com/123456789012/q","MessageBody":"x"},"End":true}}}`,
			setup:      func(b *stepfunctions.InMemoryBackend) { b.SetSQSIntegration(&mockStepFunctionsSQS{}) },
			finish:     true,
			wantTypes:  []string{"TaskScheduled", "TaskStarted", "TaskSubmitted", "TaskSucceeded"},
			wantOutput: "msg-id",
		},
		{
			name: "ecs_run_task_sync",
			definition: `{"StartAt":"R","States":{"R":{"Type":"Task","Resource":"arn:aws:states:::ecs:runTask.sync",` +
				`"Parameters":{"TaskDefinition":"td","Cluster":"c"},"End":true}}}`,
			setup: func(b *stepfunctions.InMemoryBackend) {
				b.SetECSIntegration(&regionRecordingECS{})
				b.SetECSSyncWaiter(doneECSWaiter{})
			},
			wantTypes:  []string{"TaskScheduled", "TaskStarted", "TaskSubmitted", "TaskSucceeded"},
			wantOutput: "Failures",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
				tc.setup(b)

				sm, err := b.CreateStateMachine(context.Background(), "sm", tc.definition, "arn:role", "STANDARD")
				require.NoError(t, err)
				exec, err := b.StartExecution(sm.StateMachineArn, "e1", "{}")
				require.NoError(t, err)
				synctest.Wait()

				if tc.finish {
					tokens := b.TaskTokensForTest()
					require.NotEmpty(t, tokens)
					require.NoError(t, b.SendTaskSuccess(tokens[0], `{"ok":true}`))
					synctest.Wait()
				}

				events, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 0, false)
				require.NoError(t, err)
				assert.Equal(t, tc.wantTypes, taskEventTypes(events))

				for _, ev := range events {
					switch ev.Type {
					case "TaskStarted":
						require.NotNil(t, ev.TaskStartedEventDetails)
						assert.NotEmpty(t, ev.TaskStartedEventDetails.Resource)
						assert.NotEmpty(t, ev.TaskStartedEventDetails.ResourceType)
					case "TaskSubmitted":
						require.NotNil(t, ev.TaskSubmittedEventDetails)
						assert.NotEmpty(t, ev.TaskSubmittedEventDetails.Resource)
						assert.NotEmpty(t, ev.TaskSubmittedEventDetails.ResourceType)
						assert.Contains(t, ev.TaskSubmittedEventDetails.Output, tc.wantOutput)
					}
				}
			})
		})
	}
}
