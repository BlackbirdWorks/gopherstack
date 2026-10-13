package elasticbeanstalk_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ebsdk "github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

func TestRealism_CreateErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		create   *ebsdk.CreateEnvironmentInput
		wantCode string
		wantMsg  string
	}{
		{
			name: "missing app",
			create: &ebsdk.CreateEnvironmentInput{
				ApplicationName: aws.String("nope"),
				EnvironmentName: aws.String("env-one"),
			},
			wantCode: "InvalidParameterValue",
			wantMsg:  "No Application named 'nope' found.",
		},
		{
			name: "short env name",
			create: &ebsdk.CreateEnvironmentInput{
				ApplicationName: aws.String("app"),
				EnvironmentName: aws.String("abc"),
			},
			wantCode: "InvalidParameterValue",
			wantMsg:  "Environment name must be between 4 and 40",
		},
		{
			name: "hyphen env name",
			create: &ebsdk.CreateEnvironmentInput{
				ApplicationName: aws.String("app"),
				EnvironmentName: aws.String("env-"),
			},
			wantCode: "InvalidParameterValue",
			wantMsg:  "Environment name must be between 4 and 40",
		},
		{
			name: "bad cname prefix",
			create: &ebsdk.CreateEnvironmentInput{
				ApplicationName: aws.String(
					"app",
				), EnvironmentName: aws.String("env-one"), CNAMEPrefix: aws.String("-bad"),
			},
			wantCode: "InvalidParameterValue",
			wantMsg:  "CNAME prefix must be between 4 and 63",
		},
		{
			name: "cname taken",
			create: &ebsdk.CreateEnvironmentInput{
				ApplicationName: aws.String(
					"app",
				), EnvironmentName: aws.String("env-two"), CNAMEPrefix: aws.String("taken-prefix"),
			},
			wantCode: "InvalidParameterValue",
			wantMsg:  "DNS name (taken-prefix.us-east-1.elasticbeanstalk.com) is not available.",
		},
		{
			name: "duplicate env",
			create: &ebsdk.CreateEnvironmentInput{
				ApplicationName: aws.String("app"),
				EnvironmentName: aws.String("env-first"),
			},
			wantCode: "InvalidParameterValue",
			wantMsg:  "Environment env-first already exists.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newEBClient31(t)
			ctx := t.Context()
			_, err := client.CreateApplication(ctx, &ebsdk.CreateApplicationInput{ApplicationName: aws.String("app")})
			require.NoError(t, err)
			_, err = client.CreateEnvironment(ctx, &ebsdk.CreateEnvironmentInput{
				ApplicationName: aws.String(
					"app",
				), EnvironmentName: aws.String("env-first"), CNAMEPrefix: aws.String("taken-prefix"),
			})
			require.NoError(t, err)

			_, err = client.CreateEnvironment(ctx, tt.create)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.Contains(t, apiErr.ErrorMessage(), tt.wantMsg)
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception:")
		})
	}
}

func TestRealism_ErrorWording(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call    func(*ebsdk.Client) error
		name    string
		wantMsg string
	}{
		{
			name: "duplicate application",
			call: func(c *ebsdk.Client) error {
				_, err := c.CreateApplication(
					t.Context(),
					&ebsdk.CreateApplicationInput{ApplicationName: aws.String("app")},
				)

				return err
			},
			wantMsg: "Application app already exists.",
		},
		{
			name: "terminate missing env",
			call: func(c *ebsdk.Client) error {
				_, err := c.TerminateEnvironment(
					t.Context(),
					&ebsdk.TerminateEnvironmentInput{EnvironmentName: aws.String("ghost-env")},
				)

				return err
			},
			wantMsg: "No Environment found for EnvironmentName = 'ghost-env'.",
		},
		{
			name: "delete missing app",
			call: func(c *ebsdk.Client) error {
				_, err := c.DeleteApplication(
					t.Context(),
					&ebsdk.DeleteApplicationInput{ApplicationName: aws.String("ghost")},
				)

				return err
			},
			wantMsg: "No Application named 'ghost' found.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newEBClient31(t)
			_, err := client.CreateApplication(
				t.Context(),
				&ebsdk.CreateApplicationInput{ApplicationName: aws.String("app")},
			)
			require.NoError(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, tt.call(client), &apiErr)
			assert.Equal(t, "InvalidParameterValue", apiErr.ErrorCode())
			assert.Equal(t, tt.wantMsg, apiErr.ErrorMessage())
		})
	}
}

func TestRealism_EnvironmentLifecycle(t *testing.T) {
	t.Parallel()

	b := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")
	b.SetLifecycleDelay(time.Minute)

	var offset atomic.Int64

	base := time.Now()
	b.SetClock(func() time.Time { return base.Add(time.Duration(offset.Load())) })

	client := newTestEBClient(t, elasticbeanstalk.NewHandler(b))
	ctx := t.Context()

	_, err := client.CreateApplication(ctx, &ebsdk.CreateApplicationInput{ApplicationName: aws.String("app")})
	require.NoError(t, err)

	created, err := client.CreateEnvironment(ctx, &ebsdk.CreateEnvironmentInput{
		ApplicationName: aws.String(
			"app",
		), EnvironmentName: aws.String("env-one"), CNAMEPrefix: aws.String("lifecycle"),
	})
	require.NoError(t, err)
	assert.Equal(t, ebtypes.EnvironmentStatusLaunching, created.Status)
	assert.Equal(t, ebtypes.EnvironmentHealthGrey, created.Health)
	assert.Equal(t, ebtypes.EnvironmentHealthStatusPending, created.HealthStatus)

	events, err := client.DescribeEvents(ctx, &ebsdk.DescribeEventsInput{EnvironmentName: aws.String("env-one")})
	require.NoError(t, err)
	require.Len(t, events.Events, 1)
	assert.Equal(t, "createEnvironment is starting.", aws.ToString(events.Events[0].Message))

	_, err = client.UpdateEnvironment(ctx, &ebsdk.UpdateEnvironmentInput{
		EnvironmentName: aws.String("env-one"), Description: aws.String("x"),
	})
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Contains(t, apiErr.ErrorMessage(), "is in an invalid state for this operation. Must be Ready.")

	offset.Store(int64(2 * time.Minute))

	desc, err := client.DescribeEnvironments(ctx, &ebsdk.DescribeEnvironmentsInput{})
	require.NoError(t, err)
	require.Len(t, desc.Environments, 1)
	assert.Equal(t, ebtypes.EnvironmentStatusReady, desc.Environments[0].Status)
	assert.Equal(t, ebtypes.EnvironmentHealthGreen, desc.Environments[0].Health)

	events, err = client.DescribeEvents(ctx, &ebsdk.DescribeEventsInput{EnvironmentName: aws.String("env-one")})
	require.NoError(t, err)
	assert.Equal(t, "Successfully launched environment: env-one.", aws.ToString(events.Events[0].Message))

	term, err := client.TerminateEnvironment(
		ctx,
		&ebsdk.TerminateEnvironmentInput{EnvironmentName: aws.String("env-one")},
	)
	require.NoError(t, err)
	assert.Equal(t, ebtypes.EnvironmentStatusTerminating, term.Status)

	offset.Store(int64(4 * time.Minute))

	desc, err = client.DescribeEnvironments(ctx, &ebsdk.DescribeEnvironmentsInput{})
	require.NoError(t, err)
	assert.Empty(t, desc.Environments)

	deleted, err := client.DescribeEnvironments(ctx, &ebsdk.DescribeEnvironmentsInput{IncludeDeleted: aws.Bool(true)})
	require.NoError(t, err)
	require.Len(t, deleted.Environments, 1)
	assert.Equal(t, ebtypes.EnvironmentStatusTerminated, deleted.Environments[0].Status)

	_, err = client.CreateEnvironment(ctx, &ebsdk.CreateEnvironmentInput{
		ApplicationName: aws.String(
			"app",
		), EnvironmentName: aws.String("env-one"), CNAMEPrefix: aws.String("lifecycle"),
	})
	require.NoError(t, err)
}
