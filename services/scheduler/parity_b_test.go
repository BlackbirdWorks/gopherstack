package scheduler_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_ListSchedulesIncludesTargetSummary verifies that each schedule
// returned by ListSchedules includes a Target object with Arn and RoleArn.
// Real AWS ScheduleSummary always includes a Target sub-object.
func TestParity_ListSchedulesIncludesTargetSummary(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "sched-with-target",
		"ScheduleExpression": "rate(5 minutes)",
		"Target": map[string]string{
			"Arn":     "arn:aws:lambda:us-east-1:123:function:fn",
			"RoleArn": "arn:aws:iam::123:role/r",
		},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})

	rec := doSchedulerRequest(t, h, "ListSchedules", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Schedules []map[string]json.RawMessage `json:"Schedules"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Schedules, 1)

	item := out.Schedules[0]
	targetRaw, hasTarget := item["Target"]
	assert.True(t, hasTarget, "ListSchedules item must have a 'Target' field")

	var target struct {
		Arn     string `json:"Arn"`
		RoleArn string `json:"RoleArn"`
	}
	require.NoError(t, json.Unmarshal(targetRaw, &target))
	assert.Equal(t, "arn:aws:lambda:us-east-1:123:function:fn", target.Arn)
	assert.Equal(t, "arn:aws:iam::123:role/r", target.RoleArn)
}

// TestParity_ListSchedulesTargetMatchesGetSchedule verifies that the Target.Arn
// in the list summary matches the Target.Arn in the full GetSchedule response.
func TestParity_ListSchedulesTargetMatchesGetSchedule(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	const targetArn = "arn:aws:sqs:us-east-1:999:my-queue"
	const roleArn = "arn:aws:iam::999:role/scheduler"

	doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "match-sched",
		"ScheduleExpression": "cron(0 12 * * ? *)",
		"Target":             map[string]string{"Arn": targetArn, "RoleArn": roleArn},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})

	listRec := doSchedulerRequest(t, h, "ListSchedules", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		Schedules []struct {
			Target struct {
				Arn     string `json:"Arn"`
				RoleArn string `json:"RoleArn"`
			} `json:"Target"`
		} `json:"Schedules"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.Len(t, listOut.Schedules, 1)
	assert.Equal(t, targetArn, listOut.Schedules[0].Target.Arn)
	assert.Equal(t, roleArn, listOut.Schedules[0].Target.RoleArn)
}

// TestParity_FlexibleModeRequiresMaximumWindowInMinutes verifies that
// FlexibleTimeWindow.Mode=FLEXIBLE without MaximumWindowInMinutes returns 400.
func TestParity_FlexibleModeRequiresMaximumWindowInMinutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ftw      map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "flexible_with_window_ok",
			ftw:      map[string]any{"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 30},
			wantCode: http.StatusOK,
		},
		{
			name:     "flexible_without_window_rejected",
			ftw:      map[string]any{"Mode": "FLEXIBLE"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "flexible_zero_window_rejected",
			ftw:      map[string]any{"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 0},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "off_without_window_ok",
			ftw:      map[string]any{"Mode": "OFF"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)
			rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
				"Name":               "ftw-test-" + tt.name,
				"ScheduleExpression": "rate(1 minute)",
				"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
				"FlexibleTimeWindow": tt.ftw,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusBadRequest {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, "ValidationException", errResp["__type"])
			}
		})
	}
}

// TestParity_FlexibleWindowRoundtrip verifies that MaximumWindowInMinutes is
// persisted and returned in GetSchedule when Mode is FLEXIBLE.
func TestParity_FlexibleWindowRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "ftw-roundtrip",
		"ScheduleExpression": "rate(10 minutes)",
		"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
		"FlexibleTimeWindow": map[string]any{"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 20},
	})

	getRec := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "ftw-roundtrip"})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out struct {
		FlexibleTimeWindow struct {
			Mode                   string `json:"Mode"`
			MaximumWindowInMinutes int    `json:"MaximumWindowInMinutes"`
		} `json:"FlexibleTimeWindow"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	assert.Equal(t, "FLEXIBLE", out.FlexibleTimeWindow.Mode)
	assert.Equal(t, 20, out.FlexibleTimeWindow.MaximumWindowInMinutes)
}

// TestParity_UpdateScheduleFlexibleValidation verifies that UpdateSchedule also
// enforces the MaximumWindowInMinutes requirement for FLEXIBLE mode.
func TestParity_UpdateScheduleFlexibleValidation(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	// Create with OFF mode.
	createRec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "upd-ftw-test",
		"ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Update to FLEXIBLE without MaximumWindowInMinutes — must be rejected.
	rec := doSchedulerRequest(t, h, "UpdateSchedule", map[string]any{
		"Name":               "upd-ftw-test",
		"ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
		"FlexibleTimeWindow": map[string]any{"Mode": "FLEXIBLE"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Update to FLEXIBLE with MaximumWindowInMinutes — must succeed.
	rec2 := doSchedulerRequest(t, h, "UpdateSchedule", map[string]any{
		"Name":               "upd-ftw-test",
		"ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
		"FlexibleTimeWindow": map[string]any{"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 10},
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// TestParity_EcsParametersNetworkConfigurationRoundtrip verifies that
// NetworkConfiguration (with AwsvpcConfiguration) is persisted and returned.
func TestParity_EcsParametersNetworkConfigurationRoundtrip(t *testing.T) {
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
					"AwsvpcConfiguration": map[string]any{
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
					} `json:"AwsvpcConfiguration"`
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

// TestParity_EcsParametersCapacityProviderStrategyRoundtrip verifies that
// CapacityProviderStrategy items are persisted and returned.
func TestParity_EcsParametersCapacityProviderStrategyRoundtrip(t *testing.T) {
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
					{"CapacityProvider": "FARGATE", "Weight": 1, "Base": 0},
					{"CapacityProvider": "FARGATE_SPOT", "Weight": 2, "Base": 0},
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
					CapacityProvider string `json:"CapacityProvider"`
					Weight           int    `json:"Weight"`
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

// TestParity_EcsParametersPlacementConstraintsRoundtrip verifies PlacementConstraints roundtrip.
func TestParity_EcsParametersPlacementConstraintsRoundtrip(t *testing.T) {
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
					{"Type": "memberOf", "Expression": "attribute:ecs.instance-type =~ g2.*"},
				},
				"PlacementStrategy": []map[string]any{
					{"Type": "spread", "Field": "attribute:ecs.availability-zone"},
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
					Type       string `json:"Type"`
					Expression string `json:"Expression"`
				} `json:"PlacementConstraints"`
				PlacementStrategy []struct {
					Type  string `json:"Type"`
					Field string `json:"Field"`
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

// TestParity_EcsParametersTaskTagsRoundtrip verifies ECS task-level Tags roundtrip.
func TestParity_EcsParametersTaskTagsRoundtrip(t *testing.T) {
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
					{"Key": "env", "Value": "prod"},
					{"Key": "team", "Value": "data"},
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
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"Tags"`
			} `json:"EcsParameters"`
		} `json:"Target"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))

	ecsTags := out.Target.EcsParameters.Tags
	require.Len(t, ecsTags, 2)
	assert.Equal(t, "env", ecsTags[0].Key)
	assert.Equal(t, "prod", ecsTags[0].Value)
	assert.Equal(t, "team", ecsTags[1].Key)
	assert.Equal(t, "data", ecsTags[1].Value)
}
