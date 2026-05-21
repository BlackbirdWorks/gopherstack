package scheduler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/scheduler"
)

// --- Audit batch-1: AWS-accuracy parity tests ---

// helpers

func newAuditBackend(t *testing.T) *scheduler.InMemoryBackend {
	t.Helper()

	return scheduler.NewInMemoryBackend("000000000000", "us-east-1")
}

func newAuditHandler(t *testing.T) *scheduler.Handler {
	t.Helper()

	return scheduler.NewHandler(newAuditBackend(t))
}

// createBaseSchedule sends a CreateSchedule request and requires 200 OK.
func createBaseSchedule(t *testing.T, h *scheduler.Handler, name string, extra map[string]any) {
	t.Helper()

	body := map[string]any{
		"Name":               name,
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:sqs:us-east-1:000000000000:q",
			"RoleArn": "arn:aws:iam::000000000000:role/r",
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	}

	for k, v := range extra {
		body[k] = v
	}

	rec := doSchedulerRequest(t, h, "CreateSchedule", body)
	require.Equal(t, http.StatusOK, rec.Code, "create failed: %s", rec.Body.String())
}

// --- Name validation (issue 22) ---

func TestAudit1_NameValidation_InvalidChars(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "bad name!",
		"ScheduleExpression": "rate(1 hour)",
		"Target":             map[string]any{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

func TestAudit1_NameValidation_TooLong(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	longName := "a"

	for i := 0; i < 64; i++ {
		longName += "a"
	}

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               longName,
		"ScheduleExpression": "rate(1 hour)",
		"Target":             map[string]any{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit1_NameValidation_ValidChars(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	for _, name := range []string{"my-schedule", "sched_1.0", "ABC123"} {
		rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
			"Name":               name,
			"ScheduleExpression": "rate(1 hour)",
			"Target": map[string]any{
				"Arn":     "arn:aws:sqs:us-east-1:0:q",
				"RoleArn": "arn:aws:iam::0:role/r",
			},
			"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
		})
		assert.Equal(t, http.StatusOK, rec.Code, "valid name %q should be accepted", name)
	}
}

func TestAudit1_GroupNameValidation_ReservesDefault(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateScheduleGroup", map[string]any{
		"Name": "default",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

func TestAudit1_GroupNameValidation_InvalidChars(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateScheduleGroup", map[string]any{
		"Name": "bad group!",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- RetryPolicy (issue 1) ---

func TestAudit1_RetryPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "sched-retry",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:sqs:us-east-1:0:q",
			"RoleArn": "arn:aws:iam::0:role/r",
			"RetryPolicy": map[string]any{
				"MaximumEventAgeInSeconds": 3600,
				"MaximumRetryAttempts":     10,
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	get := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "sched-retry"})
	require.Equal(t, http.StatusOK, get.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(get.Body.Bytes(), &out))

	var target map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out["Target"], &target))

	var rp map[string]int
	require.NoError(t, json.Unmarshal(target["RetryPolicy"], &rp))
	assert.Equal(t, 3600, rp["MaximumEventAgeInSeconds"])
	assert.Equal(t, 10, rp["MaximumRetryAttempts"])
}

func TestAudit1_RetryPolicy_AgeOutOfRange_Low(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "bad-retry",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:sqs:us-east-1:0:q",
			"RoleArn": "arn:aws:iam::0:role/r",
			"RetryPolicy": map[string]any{
				"MaximumEventAgeInSeconds": 10,
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

func TestAudit1_RetryPolicy_AgeOutOfRange_High(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "bad-retry2",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:sqs:us-east-1:0:q",
			"RoleArn": "arn:aws:iam::0:role/r",
			"RetryPolicy": map[string]any{
				"MaximumEventAgeInSeconds": 99999,
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit1_RetryPolicy_AttemptsOutOfRange(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "bad-retry3",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:sqs:us-east-1:0:q",
			"RoleArn": "arn:aws:iam::0:role/r",
			"RetryPolicy": map[string]any{
				"MaximumRetryAttempts": 200,
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit1_RetryPolicy_ZeroAttemptsAllowed(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "zero-retry",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:sqs:us-east-1:0:q",
			"RoleArn": "arn:aws:iam::0:role/r",
			"RetryPolicy": map[string]any{
				"MaximumRetryAttempts": 0,
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	assert.Equal(t, http.StatusOK, rec.Code, "MaximumRetryAttempts=0 is valid")
}

// --- DeadLetterConfig (issue 2) ---

func TestAudit1_DeadLetterConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	dlqARN := "arn:aws:sqs:us-east-1:000000000000:my-dlq"

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "sched-dlq",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:sqs:us-east-1:0:q",
			"RoleArn": "arn:aws:iam::0:role/r",
			"DeadLetterConfig": map[string]any{
				"Arn": dlqARN,
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	get := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "sched-dlq"})
	require.Equal(t, http.StatusOK, get.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(get.Body.Bytes(), &out))

	var target map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out["Target"], &target))

	var dlc map[string]string
	require.NoError(t, json.Unmarshal(target["DeadLetterConfig"], &dlc))
	assert.Equal(t, dlqARN, dlc["Arn"])
}

func TestAudit1_DeadLetterConfig_NonSQSArn_Rejected(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "bad-dlq",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:sqs:us-east-1:0:q",
			"RoleArn": "arn:aws:iam::0:role/r",
			"DeadLetterConfig": map[string]any{
				"Arn": "arn:aws:sns:us-east-1:000000000000:my-topic",
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

// --- EventBridgeParameters (issue 3) ---

func TestAudit1_EventBridgeParameters_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "sched-eb",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:events:us-east-1:000000000000:event-bus/my-bus",
			"RoleArn": "arn:aws:iam::0:role/r",
			"EventBridgeParameters": map[string]any{
				"DetailType": "MyDetailType",
				"Source":     "com.example.app",
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	get := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "sched-eb"})
	require.Equal(t, http.StatusOK, get.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(get.Body.Bytes(), &out))

	var target map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out["Target"], &target))

	var ebParams map[string]string
	require.NoError(t, json.Unmarshal(target["EventBridgeParameters"], &ebParams))
	assert.Equal(t, "MyDetailType", ebParams["DetailType"])
	assert.Equal(t, "com.example.app", ebParams["Source"])
}

func TestAudit1_EventBridgeParameters_MissingDetailType_Rejected(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "bad-eb",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:events:us-east-1:000000000000:event-bus/my-bus",
			"RoleArn": "arn:aws:iam::0:role/r",
			"EventBridgeParameters": map[string]any{
				"Source": "com.example.app",
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit1_EventBridgeParameters_MissingSource_Rejected(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "bad-eb2",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:events:us-east-1:000000000000:event-bus/my-bus",
			"RoleArn": "arn:aws:iam::0:role/r",
			"EventBridgeParameters": map[string]any{
				"DetailType": "MyType",
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- KinesisParameters (issue 4) ---

func TestAudit1_KinesisParameters_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "sched-kinesis",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
			"RoleArn": "arn:aws:iam::0:role/r",
			"KinesisParameters": map[string]any{
				"PartitionKey": "my-partition-key",
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	get := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "sched-kinesis"})
	require.Equal(t, http.StatusOK, get.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(get.Body.Bytes(), &out))

	var target map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out["Target"], &target))

	var kp map[string]string
	require.NoError(t, json.Unmarshal(target["KinesisParameters"], &kp))
	assert.Equal(t, "my-partition-key", kp["PartitionKey"])
}

func TestAudit1_KinesisParameters_EmptyPartitionKey_Rejected(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "bad-kinesis",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
			"RoleArn": "arn:aws:iam::0:role/r",
			"KinesisParameters": map[string]any{
				"PartitionKey": "",
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

// --- SqsParameters (issue 5) ---

func TestAudit1_SqsParameters_MessageGroupId_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "sched-fifo",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:sqs:us-east-1:000000000000:my-queue.fifo",
			"RoleArn": "arn:aws:iam::0:role/r",
			"SqsParameters": map[string]any{
				"MessageGroupId": "grp-1",
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	get := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "sched-fifo"})
	require.Equal(t, http.StatusOK, get.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(get.Body.Bytes(), &out))

	var target map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out["Target"], &target))

	var sqsP map[string]string
	require.NoError(t, json.Unmarshal(target["SqsParameters"], &sqsP))
	assert.Equal(t, "grp-1", sqsP["MessageGroupId"])
}

// --- SageMakerPipelineParameters (issue 6) ---

func TestAudit1_SageMakerPipelineParameters_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "sched-sm",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:sagemaker:us-east-1:000000000000:pipeline/my-pipeline",
			"RoleArn": "arn:aws:iam::0:role/r",
			"SageMakerPipelineParameters": map[string]any{
				"PipelineParameterList": []map[string]any{
					{"Name": "param1", "Value": "val1"},
					{"Name": "param2", "Value": "val2"},
				},
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	get := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "sched-sm"})
	require.Equal(t, http.StatusOK, get.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(get.Body.Bytes(), &out))

	var target map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out["Target"], &target))

	var smParams map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(target["SageMakerPipelineParameters"], &smParams))

	var paramList []map[string]string
	require.NoError(t, json.Unmarshal(smParams["PipelineParameterList"], &paramList))
	require.Len(t, paramList, 2)
	assert.Equal(t, "param1", paramList[0]["Name"])
	assert.Equal(t, "val1", paramList[0]["Value"])
}

// --- EcsParameters (issue 7) ---

func TestAudit1_EcsParameters_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "sched-ecs",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:ecs:us-east-1:000000000000:cluster/my-cluster",
			"RoleArn": "arn:aws:iam::0:role/r",
			"EcsParameters": map[string]any{
				"TaskDefinitionArn": "arn:aws:ecs:us-east-1:000000000000:task-definition/my-td:1",
				"LaunchType":        "FARGATE",
				"TaskCount":         2,
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	get := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "sched-ecs"})
	require.Equal(t, http.StatusOK, get.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(get.Body.Bytes(), &out))

	var target map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out["Target"], &target))

	var ecsP map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(target["EcsParameters"], &ecsP))

	var tdArn string
	require.NoError(t, json.Unmarshal(ecsP["TaskDefinitionArn"], &tdArn))
	assert.Equal(t, "arn:aws:ecs:us-east-1:000000000000:task-definition/my-td:1", tdArn)

	var launchType string
	require.NoError(t, json.Unmarshal(ecsP["LaunchType"], &launchType))
	assert.Equal(t, "FARGATE", launchType)
}

func TestAudit1_EcsParameters_MissingTaskDef_Rejected(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "bad-ecs",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:ecs:us-east-1:000000000000:cluster/my-cluster",
			"RoleArn": "arn:aws:iam::0:role/r",
			"EcsParameters": map[string]any{
				"LaunchType": "FARGATE",
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- InputTransformer ---

func TestAudit1_InputTransformer_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "sched-transformer",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:lambda:us-east-1:000000000000:function:fn",
			"RoleArn": "arn:aws:iam::0:role/r",
			"InputTransformer": map[string]any{
				"InputPathsMap": map[string]string{
					"body": "$.detail.body",
				},
				"InputTemplate": `{"msg": <body>}`,
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	get := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "sched-transformer"})
	require.Equal(t, http.StatusOK, get.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(get.Body.Bytes(), &out))

	var target map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out["Target"], &target))

	var it map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(target["InputTransformer"], &it))

	var template string
	require.NoError(t, json.Unmarshal(it["InputTemplate"], &template))
	assert.Equal(t, `{"msg": <body>}`, template)
}

// --- Pagination (issue 21) ---

func TestAudit1_ListSchedules_MaxResults_Pagination(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	for _, name := range []string{"sched-a", "sched-b", "sched-c", "sched-d", "sched-e"} {
		createBaseSchedule(t, h, name, nil)
	}

	// Get first page of 2.
	rec1 := doSchedulerRequest(t, h, "ListSchedules", map[string]any{"MaxResults": "2"})
	require.Equal(t, http.StatusOK, rec1.Code)

	var page1 map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &page1))

	var schedules1 []json.RawMessage
	require.NoError(t, json.Unmarshal(page1["Schedules"], &schedules1))
	require.Len(t, schedules1, 2)

	var nextToken string
	require.NoError(t, json.Unmarshal(page1["NextToken"], &nextToken))
	assert.NotEmpty(t, nextToken)

	// Get second page using the token.
	rec2 := doSchedulerRequest(t, h, "ListSchedules", map[string]any{"MaxResults": "2", "NextToken": nextToken})
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))

	var schedules2 []json.RawMessage
	require.NoError(t, json.Unmarshal(page2["Schedules"], &schedules2))
	assert.Len(t, schedules2, 2)
}

func TestAudit1_ListScheduleGroups_MaxResults_Pagination(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	for _, name := range []string{"grp-a", "grp-b", "grp-c"} {
		rec := doSchedulerRequest(t, h, "CreateScheduleGroup", map[string]any{"Name": name})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// "default" + 3 groups = 4 total; page of 2 should yield a token.
	rec := doSchedulerRequest(t, h, "ListScheduleGroups", map[string]any{"MaxResults": "2"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	var groups []json.RawMessage
	require.NoError(t, json.Unmarshal(out["ScheduleGroups"], &groups))
	assert.Len(t, groups, 2)

	var token string
	require.NoError(t, json.Unmarshal(out["NextToken"], &token))
	assert.NotEmpty(t, token)
}

func TestAudit1_ListSchedules_NoToken_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	for _, name := range []string{"all-a", "all-b", "all-c"} {
		createBaseSchedule(t, h, name, nil)
	}

	rec := doSchedulerRequest(t, h, "ListSchedules", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	var schedules []json.RawMessage
	require.NoError(t, json.Unmarshal(out["Schedules"], &schedules))
	assert.Len(t, schedules, 3)
	assert.Nil(t, out["NextToken"])
}

// --- Composite key for lastFiredAt (issue 24) ---

func TestAudit1_CompositeKey_SameNameDifferentGroups_BothFire(t *testing.T) {
	t.Parallel()

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:fn"
	backend := newAuditBackend(t)

	_, err := backend.CreateScheduleGroup("g1", "", nil)
	require.NoError(t, err)

	_, err = backend.CreateScheduleGroup("g2", "", nil)
	require.NoError(t, err)

	_, err = backend.CreateSchedule(
		"same-name", "g1", "rate(1 second)", "", "",
		scheduler.Target{ARN: lambdaARN, RoleARN: "arn:aws:iam::0:role/r"},
		"ENABLED", scheduler.FlexibleTimeWindow{Mode: "OFF"},
	)
	require.NoError(t, err)

	_, err = backend.CreateSchedule(
		"same-name", "g2", "rate(1 second)", "", "",
		scheduler.Target{ARN: lambdaARN, RoleARN: "arn:aws:iam::0:role/r"},
		"ENABLED", scheduler.FlexibleTimeWindow{Mode: "OFF"},
	)
	require.NoError(t, err)

	invoker := &mockLambdaInvoker{}
	runner := scheduler.NewRunner(backend)
	runner.SetLambdaInvoker(invoker)

	scheduler.CheckAndFireSchedules(t.Context(), runner, time.Now())

	// Both schedules should fire (composite key prevents conflict).
	assert.Len(t, invoker.Called(), 2, "both schedules in different groups should fire")
}

// --- UpdateSchedule preserves new target fields (issue 1, 2) ---

func TestAudit1_UpdateSchedule_RetryPolicy_Persisted(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	createBaseSchedule(t, h, "retry-update", nil)

	rec := doSchedulerRequest(t, h, "UpdateSchedule", map[string]any{
		"Name":               "retry-update",
		"ScheduleExpression": "rate(2 hours)",
		"Target": map[string]any{
			"Arn":     "arn:aws:sqs:us-east-1:0:q",
			"RoleArn": "arn:aws:iam::0:role/r",
			"RetryPolicy": map[string]any{
				"MaximumRetryAttempts": 5,
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
		"State":              "ENABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	get := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "retry-update"})
	require.Equal(t, http.StatusOK, get.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(get.Body.Bytes(), &out))

	var target map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out["Target"], &target))

	var rp map[string]int
	require.NoError(t, json.Unmarshal(target["RetryPolicy"], &rp))
	assert.Equal(t, 5, rp["MaximumRetryAttempts"])
}

// --- ActionAfterCompletion in runner (issue 17) ---

func TestAudit1_ActionAfterCompletion_Delete_RemovesSchedule(t *testing.T) {
	t.Parallel()

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:fn"
	backend := newAuditBackend(t)

	_, err := backend.CreateSchedule(
		"one-shot", "", "rate(1 second)", "", "",
		scheduler.Target{ARN: lambdaARN, RoleARN: "arn:aws:iam::0:role/r"},
		"ENABLED", scheduler.FlexibleTimeWindow{Mode: "OFF"},
		scheduler.WithActionAfterCompletion("DELETE"),
	)
	require.NoError(t, err)

	invoker := &mockLambdaInvoker{}
	runner := scheduler.NewRunner(backend)
	runner.SetLambdaInvoker(invoker)

	scheduler.CheckAndFireSchedules(t.Context(), runner, time.Now())

	// Schedule should have fired and been deleted.
	require.Len(t, invoker.Called(), 1)

	_, err = backend.GetSchedule("one-shot", "")
	assert.Error(t, err, "schedule should be deleted after ActionAfterCompletion=DELETE")
}

func TestAudit1_ActionAfterCompletion_None_DoesNotRemove(t *testing.T) {
	t.Parallel()

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:fn"
	backend := newAuditBackend(t)

	_, err := backend.CreateSchedule(
		"keep-me", "", "rate(1 second)", "", "",
		scheduler.Target{ARN: lambdaARN, RoleARN: "arn:aws:iam::0:role/r"},
		"ENABLED", scheduler.FlexibleTimeWindow{Mode: "OFF"},
		scheduler.WithActionAfterCompletion("NONE"),
	)
	require.NoError(t, err)

	invoker := &mockLambdaInvoker{}
	runner := scheduler.NewRunner(backend)
	runner.SetLambdaInvoker(invoker)

	scheduler.CheckAndFireSchedules(t.Context(), runner, time.Now())
	require.Len(t, invoker.Called(), 1)

	_, err = backend.GetSchedule("keep-me", "")
	assert.NoError(t, err, "schedule with ActionAfterCompletion=NONE should remain")
}

// --- Runner new target types ---

type mockEventBusPutter struct {
	calls []struct{ busARN, source, detailType, detail string }
	mu    sync.Mutex
}

func (m *mockEventBusPutter) PutSchedulerEvent(_ context.Context, busARN, source, detailType, detail string) error {
	m.mu.Lock()
	m.calls = append(m.calls, struct{ busARN, source, detailType, detail string }{busARN, source, detailType, detail})
	m.mu.Unlock()

	return nil
}

func (m *mockEventBusPutter) Calls() []struct{ busARN, source, detailType, detail string } {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp := make([]struct{ busARN, source, detailType, detail string }, len(m.calls))
	copy(cp, m.calls)

	return cp
}

func TestAudit1_Runner_EventBridgeTarget_Invoked(t *testing.T) {
	t.Parallel()

	busARN := "arn:aws:events:us-east-1:000000000000:event-bus/my-bus"
	backend := newAuditBackend(t)

	_, err := backend.CreateSchedule(
		"eb-sched", "", "rate(1 second)", "", "",
		scheduler.Target{
			ARN:     busARN,
			RoleARN: "arn:aws:iam::0:role/r",
			EventBridgeParameters: &scheduler.EventBridgeParameters{
				DetailType: "TestType",
				Source:     "com.test",
			},
		},
		"ENABLED", scheduler.FlexibleTimeWindow{Mode: "OFF"},
	)
	require.NoError(t, err)

	putter := &mockEventBusPutter{}
	runner := scheduler.NewRunner(backend)
	runner.SetEventBusPutter(putter)

	scheduler.CheckAndFireSchedules(t.Context(), runner, time.Now())

	calls := putter.Calls()
	require.Len(t, calls, 1)
	assert.Equal(t, busARN, calls[0].busARN)
	assert.Equal(t, "com.test", calls[0].source)
	assert.Equal(t, "TestType", calls[0].detailType)
}

type mockKinesisRecordPutter struct {
	calls []struct{ streamARN, partitionKey string }
	mu    sync.Mutex
}

func (m *mockKinesisRecordPutter) PutSchedulerRecord(
	_ context.Context,
	streamARN, partitionKey string,
	_ []byte,
) error {
	m.mu.Lock()
	m.calls = append(m.calls, struct{ streamARN, partitionKey string }{streamARN, partitionKey})
	m.mu.Unlock()

	return nil
}

func TestAudit1_Runner_KinesisTarget_Invoked(t *testing.T) {
	t.Parallel()

	streamARN := "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream"
	backend := newAuditBackend(t)

	_, err := backend.CreateSchedule(
		"kinesis-sched", "", "rate(1 second)", "", "",
		scheduler.Target{
			ARN:               streamARN,
			RoleARN:           "arn:aws:iam::0:role/r",
			KinesisParameters: &scheduler.KinesisParameters{PartitionKey: "pk-1"},
		},
		"ENABLED", scheduler.FlexibleTimeWindow{Mode: "OFF"},
	)
	require.NoError(t, err)

	putter := &mockKinesisRecordPutter{}
	runner := scheduler.NewRunner(backend)
	runner.SetKinesisRecordPutter(putter)

	scheduler.CheckAndFireSchedules(t.Context(), runner, time.Now())

	putter.mu.Lock()
	calls := putter.calls
	putter.mu.Unlock()

	require.Len(t, calls, 1)
	assert.Equal(t, streamARN, calls[0].streamARN)
	assert.Equal(t, "pk-1", calls[0].partitionKey)
}

type mockSageMakerPipelineStarter struct {
	calls []string
	mu    sync.Mutex
}

func (m *mockSageMakerPipelineStarter) StartPipelineExecution(
	_ context.Context,
	pipelineARN string,
	_ map[string]string,
) error {
	m.mu.Lock()
	m.calls = append(m.calls, pipelineARN)
	m.mu.Unlock()

	return nil
}

func TestAudit1_Runner_SageMakerTarget_Invoked(t *testing.T) {
	t.Parallel()

	pipelineARN := "arn:aws:sagemaker:us-east-1:000000000000:pipeline/my-pipeline"
	backend := newAuditBackend(t)

	_, err := backend.CreateSchedule(
		"sm-sched", "", "rate(1 second)", "", "",
		scheduler.Target{
			ARN:     pipelineARN,
			RoleARN: "arn:aws:iam::0:role/r",
			SageMakerPipelineParameters: &scheduler.SageMakerPipelineParameters{
				PipelineParameterList: []scheduler.SageMakerPipelineParameter{
					{Name: "p1", Value: "v1"},
				},
			},
		},
		"ENABLED", scheduler.FlexibleTimeWindow{Mode: "OFF"},
	)
	require.NoError(t, err)

	starter := &mockSageMakerPipelineStarter{}
	runner := scheduler.NewRunner(backend)
	runner.SetSageMakerPipelineStarter(starter)

	scheduler.CheckAndFireSchedules(t.Context(), runner, time.Now())

	starter.mu.Lock()
	calls := starter.calls
	starter.mu.Unlock()

	require.Len(t, calls, 1)
	assert.Equal(t, pipelineARN, calls[0])
}

type mockECSTaskRunner struct {
	calls []struct {
		taskDefARN, launchType string
		taskCount              int
	}
	mu sync.Mutex
}

func (m *mockECSTaskRunner) RunSchedulerTask(_ context.Context, taskDefARN, launchType string, taskCount int) error {
	m.mu.Lock()
	m.calls = append(m.calls, struct {
		taskDefARN, launchType string
		taskCount              int
	}{taskDefARN, launchType, taskCount})
	m.mu.Unlock()

	return nil
}

func TestAudit1_Runner_ECSTarget_Invoked(t *testing.T) {
	t.Parallel()

	clusterARN := "arn:aws:ecs:us-east-1:000000000000:cluster/my-cluster"
	taskDefARN := "arn:aws:ecs:us-east-1:000000000000:task-definition/my-td:1"
	backend := newAuditBackend(t)

	_, err := backend.CreateSchedule(
		"ecs-sched", "", "rate(1 second)", "", "",
		scheduler.Target{
			ARN:     clusterARN,
			RoleARN: "arn:aws:iam::0:role/r",
			EcsParameters: &scheduler.EcsParameters{
				TaskDefinitionArn: taskDefARN,
				LaunchType:        "FARGATE",
				TaskCount:         3,
			},
		},
		"ENABLED", scheduler.FlexibleTimeWindow{Mode: "OFF"},
	)
	require.NoError(t, err)

	runner_ := &mockECSTaskRunner{}
	runner := scheduler.NewRunner(backend)
	runner.SetECSTaskRunner(runner_)

	scheduler.CheckAndFireSchedules(t.Context(), runner, time.Now())

	runner_.mu.Lock()
	calls := runner_.calls
	runner_.mu.Unlock()

	require.Len(t, calls, 1)
	assert.Equal(t, taskDefARN, calls[0].taskDefARN)
	assert.Equal(t, "FARGATE", calls[0].launchType)
	assert.Equal(t, 3, calls[0].taskCount)
}

// --- Runner DLQ dispatch after retries exhausted ---

type mockFailingLambda struct {
	mu  sync.Mutex
	err error
}

func (m *mockFailingLambda) InvokeFunction(_ context.Context, _, _ string, _ []byte) ([]byte, int, error) {
	return nil, 500, m.err
}

type mockDLQSender struct {
	mu   sync.Mutex
	arns []string
}

func (m *mockDLQSender) SendMessageToQueue(_ context.Context, queueARN, _ string) error {
	m.mu.Lock()
	m.arns = append(m.arns, queueARN)
	m.mu.Unlock()

	return nil
}

func TestAudit1_Runner_DLQ_SentOnExhaustion(t *testing.T) {
	t.Parallel()

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:failing-fn"
	dlqARN := "arn:aws:sqs:us-east-1:000000000000:my-dlq"
	backend := newAuditBackend(t)

	_, err := backend.CreateSchedule(
		"dlq-test", "", "rate(1 second)", "", "",
		scheduler.Target{
			ARN:     lambdaARN,
			RoleARN: "arn:aws:iam::0:role/r",
			RetryPolicy: &scheduler.RetryPolicy{
				MaximumRetryAttempts: 0,
			},
			DeadLetterConfig: &scheduler.DeadLetterConfig{
				Arn: dlqARN,
			},
		},
		"ENABLED", scheduler.FlexibleTimeWindow{Mode: "OFF"},
	)
	require.NoError(t, err)

	failLambda := &mockFailingLambda{err: assert.AnError}
	dlq := &mockDLQSender{}
	runner := scheduler.NewRunner(backend)
	runner.SetLambdaInvoker(failLambda)
	runner.SetSQSSender(dlq)

	scheduler.CheckAndFireSchedules(t.Context(), runner, time.Now())

	dlq.mu.Lock()
	arns := dlq.arns
	dlq.mu.Unlock()

	require.Len(t, arns, 1)
	assert.Equal(t, dlqARN, arns[0])
}

// --- REST path tests for new pagination params ---

func TestAudit1_REST_ListSchedules_MaxResults_QueryParam(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	for _, name := range []string{"r-a", "r-b", "r-c"} {
		createBaseSchedule(t, h, name, nil)
	}

	rec := doRESTRequest(t, h, http.MethodGet, "/schedules", nil, map[string]string{
		"MaxResults": "2",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	var schedules []json.RawMessage
	require.NoError(t, json.Unmarshal(out["Schedules"], &schedules))
	assert.Len(t, schedules, 2)
}

// --- Handler ops count reflects all supported ops ---

func TestAudit1_HandlerOpsLen_Is12(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	assert.Equal(t, 12, scheduler.HandlerOpsLen(h))
}

// --- Target fields survive Update/Get round-trip ---

func TestAudit1_FullTarget_UpdateRoundTrip(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "full-target",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
			"RoleArn": "arn:aws:iam::0:role/r",
			"KinesisParameters": map[string]any{
				"PartitionKey": "initial-key",
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update with different partition key.
	rec2 := doSchedulerRequest(t, h, "UpdateSchedule", map[string]any{
		"Name":               "full-target",
		"ScheduleExpression": "rate(2 hours)",
		"Target": map[string]any{
			"Arn":     "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
			"RoleArn": "arn:aws:iam::0:role/r",
			"KinesisParameters": map[string]any{
				"PartitionKey": "updated-key",
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
		"State":              "ENABLED",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	get := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "full-target"})
	require.Equal(t, http.StatusOK, get.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(get.Body.Bytes(), &out))

	var target map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out["Target"], &target))

	var kp map[string]string
	require.NoError(t, json.Unmarshal(target["KinesisParameters"], &kp))
	assert.Equal(t, "updated-key", kp["PartitionKey"])
}
