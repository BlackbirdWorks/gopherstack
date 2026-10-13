package dlm_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dlmsdk "github.com/aws/aws-sdk-go-v2/service/dlm"
	dlmtypes "github.com/aws/aws-sdk-go-v2/service/dlm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dlm"
)

func schedule(createRule *dlmtypes.CreateRule, retain *dlmtypes.RetainRule) dlmtypes.Schedule {
	return dlmtypes.Schedule{Name: aws.String("s"), CreateRule: createRule, RetainRule: retain}
}

func hourly(n int32) *dlmtypes.CreateRule {
	return &dlmtypes.CreateRule{Interval: aws.Int32(n), IntervalUnit: dlmtypes.IntervalUnitValuesHours}
}

func count(n int32) *dlmtypes.RetainRule { return &dlmtypes.RetainRule{Count: aws.Int32(n)} }

func TestCreateLifecyclePolicy_Validation(t *testing.T) {
	t.Parallel()

	tags := []dlmtypes.Tag{{Key: aws.String("a"), Value: aws.String("b")}}

	good := schedule(hourly(12), count(5))

	tests := []struct {
		name      string
		state     dlmtypes.SettablePolicyStateValues
		schedules []dlmtypes.Schedule
		resources []dlmtypes.ResourceTypeValues
		wantErr   bool
	}{
		{name: "valid", state: "ENABLED", schedules: []dlmtypes.Schedule{good}},
		{name: "bad state", state: "BOGUS", schedules: []dlmtypes.Schedule{good}, wantErr: true},
		{name: "bad interval", state: "ENABLED", schedules: []dlmtypes.Schedule{schedule(hourly(7), count(5))},
			wantErr: true},
		{name: "count zero", state: "ENABLED", schedules: []dlmtypes.Schedule{schedule(hourly(12), count(0))},
			wantErr: true},
		{name: "count too high", state: "ENABLED", schedules: []dlmtypes.Schedule{schedule(hourly(12), count(1001))},
			wantErr: true},
		{name: "count and interval", state: "ENABLED", schedules: []dlmtypes.Schedule{
			schedule(hourly(12), &dlmtypes.RetainRule{
				Count: aws.Int32(2), Interval: aws.Int32(3), IntervalUnit: "DAYS",
			}),
		}, wantErr: true},
		{name: "five schedules", state: "ENABLED", schedules: []dlmtypes.Schedule{good, good, good, good, good},
			wantErr: true},
		{name: "bad time", state: "ENABLED", schedules: []dlmtypes.Schedule{schedule(&dlmtypes.CreateRule{
			Interval: aws.Int32(12), IntervalUnit: "HOURS", Times: []string{"25:00"},
		}, count(1))}, wantErr: true},
		{name: "bad resource type", state: "ENABLED", resources: []dlmtypes.ResourceTypeValues{"BUCKET"},
			wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestDLMClient(t, dlm.NewHandler(dlm.NewInMemoryBackend("000000000000", "us-east-1")))

			resources := tt.resources
			if resources == nil {
				resources = []dlmtypes.ResourceTypeValues{"VOLUME"}
			}

			out, err := client.CreateLifecyclePolicy(t.Context(), &dlmsdk.CreateLifecyclePolicyInput{
				Description:      aws.String("d"),
				ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
				State:            tt.state,
				PolicyDetails: &dlmtypes.PolicyDetails{
					ResourceTypes: resources, TargetTags: tags, Schedules: tt.schedules,
				},
			})

			if !tt.wantErr {
				require.NoError(t, err)
				assert.Regexp(t, `^policy-[0-9a-f]{17}$`, aws.ToString(out.PolicyId))

				return
			}

			var ir *dlmtypes.InvalidRequestException
			require.ErrorAs(t, err, &ir)
			assert.False(t, strings.HasPrefix(aws.ToString(ir.Message), "InvalidRequestException"), ir.Message)
		})
	}
}
