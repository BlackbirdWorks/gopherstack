package swf_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	swfsdk "github.com/aws/aws-sdk-go-v2/service/swf"
	swftypes "github.com/aws/aws-sdk-go-v2/service/swf/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

// TestRealClient_WorkflowAndActivity drives every gopherstack-n3zi
// uncovered swf op through the real aws-sdk-go-v2 client.
func TestRealClient_WorkflowAndActivity(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "domain_lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				b := swf.NewInMemoryBackend()
				client := newTestSWFSDKClient(t, swf.NewHandler(b))
				ctx := t.Context()

				require.NoError(t, b.RegisterDomain("dom1", "test domain", "NONE"))

				listOut, err := client.ListDomains(ctx, &swfsdk.ListDomainsInput{
					RegistrationStatus: swftypes.RegistrationStatusRegistered,
				})
				require.NoError(t, err)
				require.Len(t, listOut.DomainInfos, 1)
				assert.Equal(t, "dom1", aws.ToString(listOut.DomainInfos[0].Name))

				descOut, err := client.DescribeDomain(ctx, &swfsdk.DescribeDomainInput{Name: aws.String("dom1")})
				require.NoError(t, err)
				assert.Equal(t, swftypes.RegistrationStatusRegistered, descOut.DomainInfo.Status)

				_, err = client.DeprecateDomain(ctx, &swfsdk.DeprecateDomainInput{Name: aws.String("dom1")})
				require.NoError(t, err)

				_, err = client.UndeprecateDomain(ctx, &swfsdk.UndeprecateDomainInput{Name: aws.String("dom1")})
				require.NoError(t, err)

				descOut2, err := client.DescribeDomain(ctx, &swfsdk.DescribeDomainInput{Name: aws.String("dom1")})
				require.NoError(t, err)
				assert.Equal(t, swftypes.RegistrationStatusRegistered, descOut2.DomainInfo.Status)
			},
		},
		{
			name: "activity_type_lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				b := swf.NewInMemoryBackend()
				client := newTestSWFSDKClient(t, swf.NewHandler(b))
				ctx := t.Context()

				require.NoError(t, b.RegisterDomain("dom-act", "", "NONE"))
				require.NoError(t, b.RegisterActivityType("dom-act", "my-act", "1.0", "", swf.ActivityTypeDefaults{}))

				listOut, err := client.ListActivityTypes(ctx, &swfsdk.ListActivityTypesInput{
					Domain:             aws.String("dom-act"),
					RegistrationStatus: swftypes.RegistrationStatusRegistered,
				})
				require.NoError(t, err)
				require.Len(t, listOut.TypeInfos, 1)
				assert.Equal(t, "my-act", aws.ToString(listOut.TypeInfos[0].ActivityType.Name))

				_, err = client.DeprecateActivityType(ctx, &swfsdk.DeprecateActivityTypeInput{
					Domain:       aws.String("dom-act"),
					ActivityType: &swftypes.ActivityType{Name: aws.String("my-act"), Version: aws.String("1.0")},
				})
				require.NoError(t, err)

				_, err = client.UndeprecateActivityType(ctx, &swfsdk.UndeprecateActivityTypeInput{
					Domain:       aws.String("dom-act"),
					ActivityType: &swftypes.ActivityType{Name: aws.String("my-act"), Version: aws.String("1.0")},
				})
				require.NoError(t, err)

				_, err = client.DeprecateActivityType(ctx, &swfsdk.DeprecateActivityTypeInput{
					Domain:       aws.String("dom-act"),
					ActivityType: &swftypes.ActivityType{Name: aws.String("my-act"), Version: aws.String("1.0")},
				})
				require.NoError(t, err)

				_, err = client.DeleteActivityType(ctx, &swfsdk.DeleteActivityTypeInput{
					Domain:       aws.String("dom-act"),
					ActivityType: &swftypes.ActivityType{Name: aws.String("my-act"), Version: aws.String("1.0")},
				})
				require.NoError(t, err)

				listOut2, err := client.ListActivityTypes(ctx, &swfsdk.ListActivityTypesInput{
					Domain:             aws.String("dom-act"),
					RegistrationStatus: swftypes.RegistrationStatusRegistered,
				})
				require.NoError(t, err)
				assert.Empty(t, listOut2.TypeInfos)
			},
		},
		{
			name: "workflow_type_lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				b := swf.NewInMemoryBackend()
				client := newTestSWFSDKClient(t, swf.NewHandler(b))
				ctx := t.Context()

				require.NoError(t, b.RegisterDomain("dom-wt", "", "NONE"))
				require.NoError(t, b.RegisterWorkflowType("dom-wt", "my-wf", "1.0", "", swf.WorkflowTypeDefaults{}))

				listOut, err := client.ListWorkflowTypes(ctx, &swfsdk.ListWorkflowTypesInput{
					Domain:             aws.String("dom-wt"),
					RegistrationStatus: swftypes.RegistrationStatusRegistered,
				})
				require.NoError(t, err)
				require.Len(t, listOut.TypeInfos, 1)
				assert.Equal(t, "my-wf", aws.ToString(listOut.TypeInfos[0].WorkflowType.Name))

				_, err = client.DeprecateWorkflowType(ctx, &swfsdk.DeprecateWorkflowTypeInput{
					Domain:       aws.String("dom-wt"),
					WorkflowType: &swftypes.WorkflowType{Name: aws.String("my-wf"), Version: aws.String("1.0")},
				})
				require.NoError(t, err)

				_, err = client.UndeprecateWorkflowType(ctx, &swfsdk.UndeprecateWorkflowTypeInput{
					Domain:       aws.String("dom-wt"),
					WorkflowType: &swftypes.WorkflowType{Name: aws.String("my-wf"), Version: aws.String("1.0")},
				})
				require.NoError(t, err)

				_, err = client.DeprecateWorkflowType(ctx, &swfsdk.DeprecateWorkflowTypeInput{
					Domain:       aws.String("dom-wt"),
					WorkflowType: &swftypes.WorkflowType{Name: aws.String("my-wf"), Version: aws.String("1.0")},
				})
				require.NoError(t, err)

				_, err = client.DeleteWorkflowType(ctx, &swfsdk.DeleteWorkflowTypeInput{
					Domain:       aws.String("dom-wt"),
					WorkflowType: &swftypes.WorkflowType{Name: aws.String("my-wf"), Version: aws.String("1.0")},
				})
				require.NoError(t, err)

				listOut2, err := client.ListWorkflowTypes(ctx, &swfsdk.ListWorkflowTypesInput{
					Domain:             aws.String("dom-wt"),
					RegistrationStatus: swftypes.RegistrationStatusRegistered,
				})
				require.NoError(t, err)
				assert.Empty(t, listOut2.TypeInfos)
			},
		},
		{
			name: "workflow_execution_reads_and_signal",
			run: func(t *testing.T) {
				t.Helper()

				b := swf.NewInMemoryBackend()
				client := newTestSWFSDKClient(t, swf.NewHandler(b))
				ctx := t.Context()

				require.NoError(t, b.RegisterDomain("dom-we", "", "NONE"))
				require.NoError(t, b.RegisterWorkflowType("dom-we", "wf-type", "1.0", "", swf.WorkflowTypeDefaults{}))
				_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
					Domain:              "dom-we",
					WorkflowID:          "wf-1",
					WorkflowTypeName:    "wf-type",
					WorkflowTypeVersion: "1.0",
					TaskList:            "default",
				})
				require.NoError(t, err)

				descOut, err := client.DescribeWorkflowExecution(ctx, &swfsdk.DescribeWorkflowExecutionInput{
					Domain: aws.String("dom-we"),
					Execution: &swftypes.WorkflowExecution{
						WorkflowId: aws.String("wf-1"),
						RunId:      aws.String(""),
					},
				})
				require.NoError(t, err)
				assert.Equal(t, swftypes.ExecutionStatusOpen, descOut.ExecutionInfo.ExecutionStatus)

				_, err = client.SignalWorkflowExecution(ctx, &swfsdk.SignalWorkflowExecutionInput{
					Domain:     aws.String("dom-we"),
					WorkflowId: aws.String("wf-1"),
					SignalName: aws.String("my-signal"),
					Input:      aws.String(`{"key":"value"}`),
				})
				require.NoError(t, err)

				histOut, err := client.GetWorkflowExecutionHistory(ctx, &swfsdk.GetWorkflowExecutionHistoryInput{
					Domain: aws.String("dom-we"),
					Execution: &swftypes.WorkflowExecution{
						WorkflowId: aws.String("wf-1"),
						RunId:      aws.String(""),
					},
				})
				require.NoError(t, err)

				found := false
				for _, ev := range histOut.Events {
					if ev.EventType == swftypes.EventTypeWorkflowExecutionSignaled {
						found = true

						require.NotNil(t, ev.WorkflowExecutionSignaledEventAttributes)
						assert.Equal(
							t,
							"my-signal",
							aws.ToString(ev.WorkflowExecutionSignaledEventAttributes.SignalName),
						)
					}
				}
				assert.True(t, found, "expected a WorkflowExecutionSignaled event")

				countOpen, err := client.CountOpenWorkflowExecutions(ctx, &swfsdk.CountOpenWorkflowExecutionsInput{
					Domain: aws.String("dom-we"),
					StartTimeFilter: &swftypes.ExecutionTimeFilter{
						OldestDate: aws.Time(descOut.ExecutionInfo.StartTimestamp.Add(-1)),
					},
				})
				require.NoError(t, err)
				assert.GreaterOrEqual(t, countOpen.Count, int32(1))

				countClosed, err := client.CountClosedWorkflowExecutions(
					ctx,
					&swfsdk.CountClosedWorkflowExecutionsInput{
						Domain: aws.String("dom-we"),
						CloseTimeFilter: &swftypes.ExecutionTimeFilter{
							OldestDate: aws.Time(descOut.ExecutionInfo.StartTimestamp.Add(-1)),
						},
					},
				)
				require.NoError(t, err)
				assert.Equal(t, int32(0), countClosed.Count)
			},
		},
		{
			name: "activity_task_lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				b := swf.NewInMemoryBackend()
				client := newTestSWFSDKClient(t, swf.NewHandler(b))
				ctx := t.Context()

				require.NoError(t, b.RegisterDomain("dom-at", "", "NONE"))
				require.NoError(t, b.RegisterWorkflowType("dom-at", "wf-type", "1.0", "", swf.WorkflowTypeDefaults{}))
				require.NoError(t, b.RegisterActivityType("dom-at", "my-act", "1.0", "", swf.ActivityTypeDefaults{}))

				_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
					Domain:              "dom-at",
					WorkflowID:          "wf-at-1",
					WorkflowTypeName:    "wf-type",
					WorkflowTypeVersion: "1.0",
					TaskList:            "default",
				})
				require.NoError(t, err)

				// StartWorkflowExecution already auto-schedules the workflow's first
				// decision task (createExecutionLocked) -- no separate
				// EnqueueDecisionTaskInternal seed call is needed (or correct) here.
				decisionTask := b.PollForDecisionTask("dom-at", "default", 0, "")
				require.NotNil(t, decisionTask)

				countDecisions, err := client.CountPendingDecisionTasks(ctx, &swfsdk.CountPendingDecisionTasksInput{
					Domain:   aws.String("dom-at"),
					TaskList: &swftypes.TaskList{Name: aws.String("default")},
				})
				require.NoError(t, err)
				assert.Equal(t, int32(0), countDecisions.Count, "the only pending decision task was just polled")

				require.NoError(t, b.RespondDecisionTaskCompleted(decisionTask.TaskToken, "", []swf.Decision{{
					DecisionType: "ScheduleActivityTask",
					ScheduleActivityTaskAttrs: &swf.ScheduleActivityTaskDecisionAttrs{
						ActivityType: swf.ActivityTaskActivityType{Name: "my-act", Version: "1.0"},
						ActivityID:   "act-1",
						Input:        `{"n":1}`,
						TaskList:     "act-list",
					},
				}}))

				countActivities, err := client.CountPendingActivityTasks(ctx, &swfsdk.CountPendingActivityTasksInput{
					Domain:   aws.String("dom-at"),
					TaskList: &swftypes.TaskList{Name: aws.String("act-list")},
				})
				require.NoError(t, err)
				assert.Equal(t, int32(1), countActivities.Count)

				pollOut, err := client.PollForActivityTask(ctx, &swfsdk.PollForActivityTaskInput{
					Domain:   aws.String("dom-at"),
					TaskList: &swftypes.TaskList{Name: aws.String("act-list")},
				})
				require.NoError(t, err)
				require.NotNil(t, pollOut.TaskToken)
				assert.Equal(t, "act-1", aws.ToString(pollOut.ActivityId))
				assert.Equal(t, `{"n":1}`, aws.ToString(pollOut.Input))

				_, err = client.RecordActivityTaskHeartbeat(ctx, &swfsdk.RecordActivityTaskHeartbeatInput{
					TaskToken: pollOut.TaskToken,
					Details:   aws.String("50% done"),
				})
				require.NoError(t, err)

				_, err = client.RespondActivityTaskCompleted(ctx, &swfsdk.RespondActivityTaskCompletedInput{
					TaskToken: pollOut.TaskToken,
					Result:    aws.String(`{"ok":true}`),
				})
				require.NoError(t, err)

				histOut, err := client.GetWorkflowExecutionHistory(ctx, &swfsdk.GetWorkflowExecutionHistoryInput{
					Domain: aws.String("dom-at"),
					Execution: &swftypes.WorkflowExecution{
						WorkflowId: aws.String("wf-at-1"),
						RunId:      aws.String(""),
					},
				})
				require.NoError(t, err)

				var completed *swftypes.HistoryEvent
				for i := range histOut.Events {
					if histOut.Events[i].EventType == swftypes.EventTypeActivityTaskCompleted {
						completed = &histOut.Events[i]
					}
				}
				require.NotNil(t, completed, "expected an ActivityTaskCompleted event")
				require.NotNil(t, completed.ActivityTaskCompletedEventAttributes)
				assert.Equal(t, `{"ok":true}`, aws.ToString(completed.ActivityTaskCompletedEventAttributes.Result))
			},
		},
		{
			name: "activity_task_cancel_and_fail",
			run: func(t *testing.T) {
				t.Helper()

				b := swf.NewInMemoryBackend()
				client := newTestSWFSDKClient(t, swf.NewHandler(b))
				ctx := t.Context()

				require.NoError(t, b.RegisterDomain("dom-atf", "", "NONE"))
				require.NoError(t, b.RegisterWorkflowType("dom-atf", "wf-type", "1.0", "", swf.WorkflowTypeDefaults{}))
				require.NoError(t, b.RegisterActivityType("dom-atf", "my-act", "1.0", "", swf.ActivityTypeDefaults{}))

				_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
					Domain:              "dom-atf",
					WorkflowID:          "wf-atf-1",
					WorkflowTypeName:    "wf-type",
					WorkflowTypeVersion: "1.0",
					TaskList:            "default",
				})
				require.NoError(t, err)
				// StartWorkflowExecution already auto-schedules the first decision
				// task -- no separate EnqueueDecisionTaskInternal seed call needed.
				decisionTask := b.PollForDecisionTask("dom-atf", "default", 0, "")
				require.NotNil(t, decisionTask)
				require.NoError(t, b.RespondDecisionTaskCompleted(decisionTask.TaskToken, "", []swf.Decision{{
					DecisionType: "ScheduleActivityTask",
					ScheduleActivityTaskAttrs: &swf.ScheduleActivityTaskDecisionAttrs{
						ActivityType: swf.ActivityTaskActivityType{Name: "my-act", Version: "1.0"},
						ActivityID:   "act-fail",
						TaskList:     "act-list",
					},
				}}))

				task := b.PollForActivityTask("dom-atf", "act-list")
				require.NotNil(t, task)

				_, err = client.RespondActivityTaskFailed(ctx, &swfsdk.RespondActivityTaskFailedInput{
					TaskToken: aws.String(task.TaskToken),
					Reason:    aws.String("boom"),
					Details:   aws.String("stack trace here"),
				})
				require.NoError(t, err)

				histOut, err := client.GetWorkflowExecutionHistory(ctx, &swfsdk.GetWorkflowExecutionHistoryInput{
					Domain: aws.String("dom-atf"),
					Execution: &swftypes.WorkflowExecution{
						WorkflowId: aws.String("wf-atf-1"),
						RunId:      aws.String(""),
					},
				})
				require.NoError(t, err)

				var failed *swftypes.HistoryEvent
				for i := range histOut.Events {
					if histOut.Events[i].EventType == swftypes.EventTypeActivityTaskFailed {
						failed = &histOut.Events[i]
					}
				}
				require.NotNil(t, failed, "expected an ActivityTaskFailed event")
				assert.Equal(t, "boom", aws.ToString(failed.ActivityTaskFailedEventAttributes.Reason))

				// Second activity task, canceled this time. RespondActivityTaskFailed
				// above already auto-enqueued a fresh decision task for the failure
				// (enqueueDecisionTaskLocked) -- no separate EnqueueDecisionTaskInternal
				// call is needed (or correct) here.
				decisionTask2 := b.PollForDecisionTask("dom-atf", "default", 0, "")
				require.NotNil(t, decisionTask2)
				require.NoError(t, b.RespondDecisionTaskCompleted(decisionTask2.TaskToken, "", []swf.Decision{{
					DecisionType: "ScheduleActivityTask",
					ScheduleActivityTaskAttrs: &swf.ScheduleActivityTaskDecisionAttrs{
						ActivityType: swf.ActivityTaskActivityType{Name: "my-act", Version: "1.0"},
						ActivityID:   "act-cancel",
						TaskList:     "act-list",
					},
				}}))
				task2 := b.PollForActivityTask("dom-atf", "act-list")
				require.NotNil(t, task2)

				_, err = client.RespondActivityTaskCanceled(ctx, &swfsdk.RespondActivityTaskCanceledInput{
					TaskToken: aws.String(task2.TaskToken),
					Details:   aws.String("canceled by test"),
				})
				require.NoError(t, err)

				histOut2, err := client.GetWorkflowExecutionHistory(ctx, &swfsdk.GetWorkflowExecutionHistoryInput{
					Domain: aws.String("dom-atf"),
					Execution: &swftypes.WorkflowExecution{
						WorkflowId: aws.String("wf-atf-1"),
						RunId:      aws.String(""),
					},
				})
				require.NoError(t, err)

				found := false
				for i := range histOut2.Events {
					if histOut2.Events[i].EventType == swftypes.EventTypeActivityTaskCanceled {
						found = true
					}
				}
				assert.True(t, found, "expected an ActivityTaskCanceled event")
			},
		},
		{
			name: "tags",
			run: func(t *testing.T) {
				t.Helper()

				b := swf.NewInMemoryBackend()
				client := newTestSWFSDKClient(t, swf.NewHandler(b))
				ctx := t.Context()

				require.NoError(t, b.RegisterDomain("dom-tags", "", "NONE"))

				domArn := "arn:aws:swf:us-east-1:123456789012:/domain/dom-tags"

				_, err := client.TagResource(ctx, &swfsdk.TagResourceInput{
					ResourceArn: aws.String(domArn),
					Tags:        []swftypes.ResourceTag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				listOut, err := client.ListTagsForResource(ctx, &swfsdk.ListTagsForResourceInput{
					ResourceArn: aws.String(domArn),
				})
				require.NoError(t, err)
				require.Len(t, listOut.Tags, 1)

				_, err = client.UntagResource(ctx, &swfsdk.UntagResourceInput{
					ResourceArn: aws.String(domArn),
					TagKeys:     []string{"env"},
				})
				require.NoError(t, err)

				listOut2, err := client.ListTagsForResource(ctx, &swfsdk.ListTagsForResourceInput{
					ResourceArn: aws.String(domArn),
				})
				require.NoError(t, err)
				assert.Empty(t, listOut2.Tags)
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
