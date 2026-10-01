package elasticbeanstalk_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ebsdk "github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

func TestDescribeEnvironments_IncludeDeleted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		backTo     time.Time
		name       string
		wantNames  []string
		includeDel bool
	}{
		{name: "default_excludes_deleted", wantNames: []string{"live"}},
		{name: "include_deleted", includeDel: true, wantNames: []string{"gone", "live"}},
		{
			name: "back_to_past", includeDel: true, backTo: time.Now().Add(-time.Hour),
			wantNames: []string{"gone", "live"},
		},
		{name: "back_to_future", includeDel: true, backTo: time.Now().Add(time.Hour), wantNames: []string{"live"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newEBClient31(t)
			ctx := t.Context()

			_, err := client.CreateApplication(ctx, &ebsdk.CreateApplicationInput{ApplicationName: aws.String("app")})
			require.NoError(t, err)

			for _, n := range []string{"gone", "live"} {
				_, err = client.CreateEnvironment(ctx, &ebsdk.CreateEnvironmentInput{
					ApplicationName:   aws.String("app"),
					EnvironmentName:   aws.String(n),
					SolutionStackName: aws.String(testSolutionStack),
				})
				require.NoError(t, err)
			}

			_, err = client.TerminateEnvironment(ctx, &ebsdk.TerminateEnvironmentInput{
				EnvironmentName: aws.String("gone"),
			})
			require.NoError(t, err)

			in := &ebsdk.DescribeEnvironmentsInput{IncludeDeleted: aws.Bool(tt.includeDel)}
			if !tt.backTo.IsZero() {
				in.IncludedDeletedBackTo = aws.Time(tt.backTo)
			}

			out, err := client.DescribeEnvironments(ctx, in)
			require.NoError(t, err)

			got := make([]string, 0, len(out.Environments))
			for _, e := range out.Environments {
				got = append(got, aws.ToString(e.EnvironmentName))

				if aws.ToString(e.EnvironmentName) == "gone" {
					assert.Equal(t, "Terminated", string(e.Status))
				}
			}

			assert.Equal(t, tt.wantNames, got)
		})
	}
}

func TestDeletedEnvironments_BoundedAndPersisted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		terminate int
		wantKept  int
	}{
		{name: "under_cap", terminate: 5, wantKept: 5},
		{name: "over_cap", terminate: 120, wantKept: 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			b := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")
			_, err := b.CreateApplication(ctx, "app", "", nil)
			require.NoError(t, err)

			for range tt.terminate {
				_, err = b.CreateEnvironment(ctx, "app", "env", testSolutionStack, "", nil,
					elasticbeanstalk.CreateEnvironmentParams{})
				require.NoError(t, err)
				_, err = b.TerminateEnvironment(ctx, "app", "env")
				require.NoError(t, err)
			}

			assert.Len(t, b.DescribeDeletedEnvironments(ctx, "", nil, nil, time.Time{}), tt.wantKept)

			restored := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")
			require.NoError(t, restored.Restore(ctx, b.Snapshot(ctx)))
			assert.Len(t, restored.DescribeDeletedEnvironments(ctx, "", nil, nil, time.Time{}), tt.wantKept)
		})
	}
}
