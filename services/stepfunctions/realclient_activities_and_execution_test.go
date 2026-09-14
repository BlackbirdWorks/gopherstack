package stepfunctions_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

const testRoleArn = "arn:aws:iam::000000000000:role/sfn-role"

// TestRealClient_ActivitiesAndExecution drives stepfunctions's typed-
// coverage-blind ops (gopherstack-n3zi) through the real aws-sdk-go-v2 sfn
// client.
func TestRealClient_ActivitiesAndExecution(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "activities and list state machines",
			run: func(t *testing.T) {
				t.Helper()

				backend := stepfunctions.NewInMemoryBackend()
				client := newSFNSDKClient(t, stepfunctions.NewHandler(backend))
				ctx := t.Context()

				actName := "s11-activity-" + uuid.NewString()[:8]
				createAct, err := client.CreateActivity(ctx, &sfnsdk.CreateActivityInput{
					Name: aws.String(actName),
				})
				require.NoError(t, err)
				actArn := aws.ToString(createAct.ActivityArn)

				descAct, err := client.DescribeActivity(ctx, &sfnsdk.DescribeActivityInput{
					ActivityArn: aws.String(actArn),
				})
				require.NoError(t, err)
				assert.Equal(t, actName, aws.ToString(descAct.Name))
				assert.Equal(t, actArn, aws.ToString(descAct.ActivityArn))

				listAct, err := client.ListActivities(ctx, &sfnsdk.ListActivitiesInput{})
				require.NoError(t, err)
				var found bool
				for _, a := range listAct.Activities {
					if aws.ToString(a.ActivityArn) == actArn {
						found = true
					}
				}
				assert.True(t, found, "ListActivities must include the newly created activity")

				smName := "s11-sm-" + uuid.NewString()[:8]
				createSM, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
					Name:       aws.String(smName),
					Definition: aws.String(validPassDef),
					RoleArn:    aws.String(testRoleArn),
					Type:       sfntypes.StateMachineTypeStandard,
				})
				require.NoError(t, err)
				smArn := aws.ToString(createSM.StateMachineArn)

				listSM, err := client.ListStateMachines(ctx, &sfnsdk.ListStateMachinesInput{})
				require.NoError(t, err)
				found = false
				for _, sm := range listSM.StateMachines {
					if aws.ToString(sm.StateMachineArn) == smArn {
						found = true
						assert.Equal(t, smName, aws.ToString(sm.Name))
					}
				}
				assert.True(t, found, "ListStateMachines must include the newly created state machine")
			},
		},
		{
			name: "send task failure and heartbeat",
			run: func(t *testing.T) {
				t.Helper()

				backend := stepfunctions.NewInMemoryBackend()
				client := newSFNSDKClient(t, stepfunctions.NewHandler(backend))
				ctx := t.Context()

				actName := "s11-task-activity-" + uuid.NewString()[:8]
				createAct, err := client.CreateActivity(ctx, &sfnsdk.CreateActivityInput{
					Name: aws.String(actName),
				})
				require.NoError(t, err)
				actArn := aws.ToString(createAct.ActivityArn)

				invokeDone := make(chan error, 1)
				go func() {
					_, invokeErr := backend.InvokeActivity(ctx, actArn, `{"in":1}`, 0)
					invokeDone <- invokeErr
				}()

				var taskToken string
				require.Eventually(t, func() bool {
					pollCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
					defer cancel()

					out, pollErr := client.GetActivityTask(pollCtx, &sfnsdk.GetActivityTaskInput{
						ActivityArn: aws.String(actArn),
					})
					if pollErr != nil || out == nil || aws.ToString(out.TaskToken) == "" {
						return false
					}

					taskToken = aws.ToString(out.TaskToken)

					return true
				}, 5*time.Second, 50*time.Millisecond)
				require.NotEmpty(t, taskToken)

				_, err = client.SendTaskHeartbeat(ctx, &sfnsdk.SendTaskHeartbeatInput{
					TaskToken: aws.String(taskToken),
				})
				require.NoError(t, err)

				_, err = client.SendTaskFailure(ctx, &sfnsdk.SendTaskFailureInput{
					TaskToken: aws.String(taskToken),
					Error:     aws.String("S11Error"),
					Cause:     aws.String("s11 failure cause"),
				})
				require.NoError(t, err)

				select {
				case invokeErr := <-invokeDone:
					require.Error(t, invokeErr, "InvokeActivity must surface the SendTaskFailure error")
					assert.Contains(t, invokeErr.Error(), "S11Error")
				case <-time.After(5 * time.Second):
					t.Fatal("timeout waiting for InvokeActivity to observe SendTaskFailure")
				}
			},
		},
		{
			name: "update state machine",
			run: func(t *testing.T) {
				t.Helper()

				backend := stepfunctions.NewInMemoryBackend()
				client := newSFNSDKClient(t, stepfunctions.NewHandler(backend))
				ctx := t.Context()

				smName := "s11-update-sm-" + uuid.NewString()[:8]
				createSM, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
					Name:       aws.String(smName),
					Definition: aws.String(validPassDef),
					RoleArn:    aws.String(testRoleArn),
					Type:       sfntypes.StateMachineTypeStandard,
				})
				require.NoError(t, err)
				smArn := aws.ToString(createSM.StateMachineArn)

				newDef := `{"StartAt":"Q","States":{"Q":{"Type":"Pass","End":true}}}`
				updOut, err := client.UpdateStateMachine(ctx, &sfnsdk.UpdateStateMachineInput{
					StateMachineArn: aws.String(smArn),
					Definition:      aws.String(newDef),
				})
				require.NoError(t, err)
				require.NotNil(t, updOut.UpdateDate)
				assert.NotZero(t, *updOut.UpdateDate)
				assert.NotEmpty(t, aws.ToString(updOut.RevisionId))

				descSM, err := client.DescribeStateMachine(ctx, &sfnsdk.DescribeStateMachineInput{
					StateMachineArn: aws.String(smArn),
				})
				require.NoError(t, err)
				assert.Equal(t, newDef, aws.ToString(descSM.Definition))
			},
		},
		{
			name: "redrive execution",
			run: func(t *testing.T) {
				t.Helper()

				backend := stepfunctions.NewInMemoryBackend()
				client := newSFNSDKClient(t, stepfunctions.NewHandler(backend))
				ctx := t.Context()

				smName := "s11-redrive-sm-" + uuid.NewString()[:8]
				failDef := `{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"ErrFoo","Cause":"test cause","End":true}}}`
				createSM, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
					Name:       aws.String(smName),
					Definition: aws.String(failDef),
					RoleArn:    aws.String(testRoleArn),
					Type:       sfntypes.StateMachineTypeStandard,
				})
				require.NoError(t, err)
				smArn := aws.ToString(createSM.StateMachineArn)

				startOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
					StateMachineArn: aws.String(smArn),
					Input:           aws.String(`{}`),
				})
				require.NoError(t, err)
				execArn := aws.ToString(startOut.ExecutionArn)

				require.Eventually(t, func() bool {
					desc, descErr := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
						ExecutionArn: aws.String(execArn),
					})

					return descErr == nil && desc.Status == sfntypes.ExecutionStatusFailed
				}, 5*time.Second, 20*time.Millisecond, "execution must reach FAILED before it can be redriven")

				redriveOut, err := client.RedriveExecution(ctx, &sfnsdk.RedriveExecutionInput{
					ExecutionArn: aws.String(execArn),
				})
				require.NoError(t, err)
				require.NotNil(t, redriveOut.RedriveDate)
				assert.NotZero(t, *redriveOut.RedriveDate)

				// The Fail-state re-run completes near-instantly and asynchronously, so
				// a status of RUNNING right after RedriveExecution returns is a race,
				// not a guarantee -- RedriveCount is the stable, non-timing-dependent
				// signal that the redrive actually happened.
				afterDesc, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
					ExecutionArn: aws.String(execArn),
				})
				require.NoError(t, err)
				assert.Equal(t, int32(1), aws.ToInt32(afterDesc.RedriveCount),
					"RedriveExecution must increment RedriveCount")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
