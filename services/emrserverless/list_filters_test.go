package emrserverless_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	et "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

func TestListApplications_MultipleStates_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		states []et.ApplicationState
		want   []string
	}{
		{name: "no filter", want: []string{"started", "created"}},
		{
			name:   "single",
			states: []et.ApplicationState{et.ApplicationStateStarted},
			want:   []string{"started"},
		},
		{
			name: "both grouped by state",
			states: []et.ApplicationState{
				et.ApplicationStateStarted,
				et.ApplicationStateCreated,
			},
			want: []string{"created", "started"},
		},
		{
			name: "second state only matches second",
			states: []et.ApplicationState{
				et.ApplicationStateStopped,
				et.ApplicationStateCreated,
			},
			want: []string{"created"},
		},
		{
			name:   "no match",
			states: []et.ApplicationState{et.ApplicationStateStopped},
			want:   []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEMRServerlessSDKClient(
				t,
				emrserverless.NewHandler(emrserverless.NewInMemoryBackend(emrRTAccountID, emrRTRegion)),
			)
			ctx := t.Context()

			for _, n := range []string{"started", "created"} {
				out, err := client.CreateApplication(ctx, &sdk.CreateApplicationInput{
					Name:         aws.String(n),
					ReleaseLabel: aws.String("emr-6.6.0"),
					Type:         aws.String("SPARK"),
				})
				require.NoError(t, err)

				if n == "started" {
					_, err = client.StartApplication(
						ctx,
						&sdk.StartApplicationInput{ApplicationId: out.ApplicationId},
					)
					require.NoError(t, err)
				}
			}

			out, err := client.ListApplications(ctx, &sdk.ListApplicationsInput{States: tt.states})
			require.NoError(t, err)

			got := make([]string, 0)
			for _, a := range out.Applications {
				got = append(got, aws.ToString(a.Name))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestListJobRuns_Filters_RealClient(t *testing.T) {
	t.Parallel()

	hourAgo := time.Now().Add(-time.Hour)
	hourAhead := time.Now().Add(time.Hour)

	tests := []struct {
		name  string
		input sdk.ListJobRunsInput
		want  int
	}{
		{name: "no filter", want: 2},
		{name: "created after past", input: sdk.ListJobRunsInput{CreatedAtAfter: &hourAgo}, want: 2},
		{name: "created after future", input: sdk.ListJobRunsInput{CreatedAtAfter: &hourAhead}, want: 0},
		{name: "created before past", input: sdk.ListJobRunsInput{CreatedAtBefore: &hourAgo}, want: 0},
		{
			name:  "window",
			input: sdk.ListJobRunsInput{CreatedAtAfter: &hourAgo, CreatedAtBefore: &hourAhead},
			want:  2,
		},
		{
			name:  "mode batch includes default",
			input: sdk.ListJobRunsInput{Mode: et.JobRunModeBatch},
			want:  2,
		},
		{
			name:  "mode streaming excludes",
			input: sdk.ListJobRunsInput{Mode: et.JobRunModeStreaming},
			want:  0,
		},
		{
			name: "multiple states",
			input: sdk.ListJobRunsInput{States: []et.JobRunState{
				et.JobRunStateFailed,
				et.JobRunStateCancelled,
				et.JobRunStateSubmitted,
				et.JobRunStateRunning,
				et.JobRunStateSuccess,
				et.JobRunStatePending,
			}},
			want: 2,
		},
		{
			name: "state mismatch",
			input: sdk.ListJobRunsInput{
				States: []et.JobRunState{
					et.JobRunStateFailed,
					et.JobRunStateCancelled,
				},
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEMRServerlessSDKClient(
				t,
				emrserverless.NewHandler(emrserverless.NewInMemoryBackend(emrRTAccountID, emrRTRegion)),
			)
			ctx := t.Context()

			app, err := client.CreateApplication(ctx, &sdk.CreateApplicationInput{
				Name:         aws.String("lf-app"),
				ReleaseLabel: aws.String("emr-6.6.0"),
				Type:         aws.String("SPARK"),
			})
			require.NoError(t, err)

			for range 2 {
				_, err = client.StartJobRun(ctx, &sdk.StartJobRunInput{
					ApplicationId:    app.ApplicationId,
					ExecutionRoleArn: aws.String("arn:aws:iam::" + emrRTAccountID + ":role/test-role"),
					ClientToken:      aws.String(time.Now().String()),
				})
				require.NoError(t, err)
			}

			in := tt.input
			in.ApplicationId = app.ApplicationId

			out, err := client.ListJobRuns(ctx, &in)
			require.NoError(t, err)
			assert.Len(t, out.JobRuns, tt.want)
		})
	}
}
