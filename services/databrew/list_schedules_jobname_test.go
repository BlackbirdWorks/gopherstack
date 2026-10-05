package databrew_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	databrewsdk "github.com/aws/aws-sdk-go-v2/service/databrew"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/databrew"
)

// ListSchedules.JobName: "The name of the job that these schedules apply to."
// (api_op_ListSchedules.go).
func TestListSchedules_JobNameFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		jobName *string
		want    []string
	}{
		{name: "no filter", want: []string{"s-a", "s-b", "s-none"}},
		{name: "job one", jobName: aws.String("job-1"), want: []string{"s-a", "s-b"}},
		{name: "job two", jobName: aws.String("job-2"), want: []string{"s-b"}},
		{name: "unknown job", jobName: aws.String("nope"), want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRoundTripClient(
				t,
				databrew.NewHandler(databrew.NewInMemoryBackend("123456789012", "us-east-1")),
			)
			ctx := t.Context()

			for name, jobs := range map[string][]string{"s-a": {"job-1"}, "s-b": {"job-1", "job-2"}, "s-none": nil} {
				_, err := client.CreateSchedule(ctx, &databrewsdk.CreateScheduleInput{
					Name: aws.String(name), CronExpression: aws.String("cron(0 12 * * ? *)"), JobNames: jobs,
				})
				require.NoError(t, err)
			}

			out, err := client.ListSchedules(ctx, &databrewsdk.ListSchedulesInput{JobName: tt.jobName})
			require.NoError(t, err)

			got := make([]string, 0, len(out.Schedules))
			for _, s := range out.Schedules {
				got = append(got, aws.ToString(s.Name))
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
