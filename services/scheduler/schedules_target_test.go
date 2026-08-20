package scheduler_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateSchedule_RetryPolicyRoundTrip verifies RetryPolicy fields round-trip.
func TestCreateSchedule_RetryPolicyRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

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

// TestCreateSchedule_RetryPolicyValidation covers RetryPolicy field-range validation.
func TestCreateSchedule_RetryPolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		retryPolicy map[string]any
		name        string
		schedName   string
		wantCode    int
	}{
		{
			name:        "age_out_of_range_low",
			schedName:   "bad-retry",
			retryPolicy: map[string]any{"MaximumEventAgeInSeconds": 10},
			wantCode:    http.StatusBadRequest,
		},
		{
			name:        "age_out_of_range_high",
			schedName:   "bad-retry2",
			retryPolicy: map[string]any{"MaximumEventAgeInSeconds": 99999},
			wantCode:    http.StatusBadRequest,
		},
		{
			name:        "attempts_out_of_range",
			schedName:   "bad-retry3",
			retryPolicy: map[string]any{"MaximumRetryAttempts": 200},
			wantCode:    http.StatusBadRequest,
		},
		{
			name:        "zero_attempts_allowed",
			schedName:   "zero-retry",
			retryPolicy: map[string]any{"MaximumRetryAttempts": 0},
			wantCode:    http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)

			rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
				"Name":               tt.schedName,
				"ScheduleExpression": "rate(1 hour)",
				"Target": map[string]any{
					"Arn":         "arn:aws:sqs:us-east-1:0:q",
					"RoleArn":     "arn:aws:iam::0:role/r",
					"RetryPolicy": tt.retryPolicy,
				},
				"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "ValidationException")
			}
		})
	}
}

// TestUpdateSchedule_RetryPolicyPersisted verifies RetryPolicy survives an update.
func TestUpdateSchedule_RetryPolicyPersisted(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	createBaseSchedule(t, h, "retry-update")

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

// TestCreateSchedule_DeadLetterConfigRoundTrip verifies DeadLetterConfig round-trips.
func TestCreateSchedule_DeadLetterConfigRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

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

// TestCreateSchedule_DeadLetterConfigNonSQSArnRejected verifies non-SQS DLQ ARNs are rejected.
func TestCreateSchedule_DeadLetterConfigNonSQSArnRejected(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

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

// TestCreateSchedule_EventBridgeParametersRoundTrip verifies EventBridgeParameters round-trip.
func TestCreateSchedule_EventBridgeParametersRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

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

// TestCreateSchedule_EventBridgeParametersValidation covers required-field
// validation of EventBridgeParameters.
func TestCreateSchedule_EventBridgeParametersValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params map[string]any
		name   string
	}{
		{
			name:   "missing_detail_type",
			params: map[string]any{"Source": "com.example.app"},
		},
		{
			name:   "missing_source",
			params: map[string]any{"DetailType": "MyType"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)

			rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
				"Name":               "bad-eb-" + tt.name,
				"ScheduleExpression": "rate(1 hour)",
				"Target": map[string]any{
					"Arn":                   "arn:aws:events:us-east-1:000000000000:event-bus/my-bus",
					"RoleArn":               "arn:aws:iam::0:role/r",
					"EventBridgeParameters": tt.params,
				},
				"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestCreateSchedule_KinesisParametersRoundTrip verifies KinesisParameters round-trip.
func TestCreateSchedule_KinesisParametersRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

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

// TestCreateSchedule_KinesisParametersEmptyPartitionKeyRejected verifies an
// empty PartitionKey is rejected.
func TestCreateSchedule_KinesisParametersEmptyPartitionKeyRejected(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

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

// TestCreateSchedule_SqsParametersMessageGroupIdRoundTrip verifies FIFO
// SqsParameters.MessageGroupId round-trips.
func TestCreateSchedule_SqsParametersMessageGroupIdRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

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

// TestCreateSchedule_SageMakerPipelineParametersRoundTrip verifies
// SageMakerPipelineParameters round-trip.
func TestCreateSchedule_SageMakerPipelineParametersRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

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

// TestCreateSchedule_EcsParametersRoundTrip verifies basic EcsParameters round-trip.
func TestCreateSchedule_EcsParametersRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

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

// TestCreateSchedule_EcsParametersMissingTaskDefRejected verifies a missing
// TaskDefinitionArn is rejected.
func TestCreateSchedule_EcsParametersMissingTaskDefRejected(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

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

// TestCreateSchedule_EcsParametersNetworkConfigurationRoundtrip verifies that
// NetworkConfiguration (with AwsvpcConfiguration) is persisted and returned.
func TestCreateSchedule_EcsParametersNetworkConfigurationRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "ecs-net-test",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]any{
			"Arn":     "arn:aws:ecs:us-east-1:123:cluster/my-cluster",
			"RoleArn": "arn:aws:iam::123:role/r",
			"EcsParameters": map[string]any{
				"TaskDefinitionArn": "arn:aws:ecs:us-east-1:123:task-definition/my-td:1",
				"NetworkConfiguration": map[string]any{
					"awsvpcConfiguration": map[string]any{
						"Subnets":        []string{"subnet-aaa", "subnet-bbb"},
						"SecurityGroups": []string{"sg-ccc"},
						"AssignPublicIp": "ENABLED",
					},
				},
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})

	getRec := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "ecs-net-test"})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out struct {
		Target struct {
			EcsParameters struct {
				NetworkConfiguration struct {
					AwsvpcConfiguration struct {
						AssignPublicIP string   `json:"AssignPublicIp"`
						SecurityGroups []string `json:"SecurityGroups"`
						Subnets        []string `json:"Subnets"`
					} `json:"awsvpcConfiguration"`
				} `json:"NetworkConfiguration"`
			} `json:"EcsParameters"`
		} `json:"Target"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))

	cfg := out.Target.EcsParameters.NetworkConfiguration.AwsvpcConfiguration
	assert.Equal(t, []string{"subnet-aaa", "subnet-bbb"}, cfg.Subnets)
	assert.Equal(t, []string{"sg-ccc"}, cfg.SecurityGroups)
	assert.Equal(t, "ENABLED", cfg.AssignPublicIP)
}

// TestCreateSchedule_EcsParametersCapacityProviderStrategyRoundtrip verifies that
// CapacityProviderStrategy items are persisted and returned.
func TestCreateSchedule_EcsParametersCapacityProviderStrategyRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "ecs-cap-test",
		"ScheduleExpression": "rate(2 hours)",
		"Target": map[string]any{
			"Arn":     "arn:aws:ecs:us-east-1:123:cluster/c",
			"RoleArn": "arn:aws:iam::123:role/r",
			"EcsParameters": map[string]any{
				"TaskDefinitionArn": "arn:aws:ecs:us-east-1:123:task-definition/td:1",
				"CapacityProviderStrategy": []map[string]any{
					{"capacityProvider": "FARGATE", "weight": 1, "base": 0},
					{"capacityProvider": "FARGATE_SPOT", "weight": 2, "base": 0},
				},
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})

	getRec := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "ecs-cap-test"})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out struct {
		Target struct {
			EcsParameters struct {
				CapacityProviderStrategy []struct {
					CapacityProvider string `json:"capacityProvider"`
					Weight           int    `json:"weight"`
				} `json:"CapacityProviderStrategy"`
			} `json:"EcsParameters"`
		} `json:"Target"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))

	strategy := out.Target.EcsParameters.CapacityProviderStrategy
	require.Len(t, strategy, 2)
	assert.Equal(t, "FARGATE", strategy[0].CapacityProvider)
	assert.Equal(t, 1, strategy[0].Weight)
	assert.Equal(t, "FARGATE_SPOT", strategy[1].CapacityProvider)
	assert.Equal(t, 2, strategy[1].Weight)
}

// TestCreateSchedule_EcsParametersPlacementConstraintsRoundtrip verifies PlacementConstraints roundtrip.
func TestCreateSchedule_EcsParametersPlacementConstraintsRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "ecs-pc-test",
		"ScheduleExpression": "rate(3 hours)",
		"Target": map[string]any{
			"Arn":     "arn:aws:ecs:us-east-1:123:cluster/c",
			"RoleArn": "arn:aws:iam::123:role/r",
			"EcsParameters": map[string]any{
				"TaskDefinitionArn": "arn:aws:ecs:us-east-1:123:task-definition/td:1",
				"PlacementConstraints": []map[string]any{
					{"type": "memberOf", "expression": "attribute:ecs.instance-type =~ g2.*"},
				},
				"PlacementStrategy": []map[string]any{
					{"type": "spread", "field": "attribute:ecs.availability-zone"},
				},
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})

	getRec := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "ecs-pc-test"})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out struct {
		Target struct {
			EcsParameters struct {
				PlacementConstraints []struct {
					Type       string `json:"type"`
					Expression string `json:"expression"`
				} `json:"PlacementConstraints"`
				PlacementStrategy []struct {
					Type  string `json:"type"`
					Field string `json:"field"`
				} `json:"PlacementStrategy"`
			} `json:"EcsParameters"`
		} `json:"Target"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))

	constraints := out.Target.EcsParameters.PlacementConstraints
	require.Len(t, constraints, 1)
	assert.Equal(t, "memberOf", constraints[0].Type)
	assert.Equal(t, "attribute:ecs.instance-type =~ g2.*", constraints[0].Expression)

	strategy := out.Target.EcsParameters.PlacementStrategy
	require.Len(t, strategy, 1)
	assert.Equal(t, "spread", strategy[0].Type)
	assert.Equal(t, "attribute:ecs.availability-zone", strategy[0].Field)
}

// TestCreateSchedule_EcsParametersTaskTagsRoundtrip verifies ECS task-level Tags
// roundtrip. Real SDK's EcsParameters.Tags is []map[string]string -- a list of
// free-form single-entry maps like {"env":"prod"} -- not a list of {Key,Value}
// objects (aws-sdk-go-v2/service/scheduler/types.EcsParameters.Tags, serialized by
// awsRestjson1_(de)serializeDocumentTags/...TagMap).
func TestCreateSchedule_EcsParametersTaskTagsRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "ecs-tag-test",
		"ScheduleExpression": "rate(4 hours)",
		"Target": map[string]any{
			"Arn":     "arn:aws:ecs:us-east-1:123:cluster/c",
			"RoleArn": "arn:aws:iam::123:role/r",
			"EcsParameters": map[string]any{
				"TaskDefinitionArn": "arn:aws:ecs:us-east-1:123:task-definition/td:1",
				"Tags": []map[string]string{
					{"env": "prod"},
					{"team": "data"},
				},
			},
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})

	getRec := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "ecs-tag-test"})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out struct {
		Target struct {
			EcsParameters struct {
				Tags []map[string]string `json:"Tags"`
			} `json:"EcsParameters"`
		} `json:"Target"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))

	ecsTags := out.Target.EcsParameters.Tags
	require.Len(t, ecsTags, 2)
	assert.Equal(t, map[string]string{"env": "prod"}, ecsTags[0])
	assert.Equal(t, map[string]string{"team": "data"}, ecsTags[1])
}

// TestCreateSchedule_InputTransformerNotEchoed verifies that InputTransformer --
// a field that exists on EventBridge Rules targets but not on EventBridge
// Scheduler's Target (aws-sdk-go-v2/service/scheduler/types.Target has no such
// member) -- is silently dropped rather than round-tripped, matching how a real
// client parsing this response would just never see it.
func TestCreateSchedule_InputTransformerNotEchoed(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

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

	_, present := target["InputTransformer"]
	assert.False(t, present, "InputTransformer is not a real Scheduler Target member and must not be echoed back")
}

// TestFullTarget_UpdateRoundTrip verifies target fields survive an Update/Get round-trip.
func TestFullTarget_UpdateRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

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
