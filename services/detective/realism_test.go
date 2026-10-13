package detective_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	detectivesdk "github.com/aws/aws-sdk-go-v2/service/detective"
	detectivetypes "github.com/aws/aws-sdk-go-v2/service/detective/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/detective"
)

const missingGraphARN = "arn:aws:detective:us-east-1:000000000000:graph:00000000000000000000000000000000"

func TestRequestValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(c *detectivesdk.Client, graph string) error
		name string
		code string
	}{
		{
			name: "malformed graph arn",
			call: func(c *detectivesdk.Client, _ string) error {
				_, err := c.DeleteGraph(t.Context(), &detectivesdk.DeleteGraphInput{GraphArn: aws.String("bad")})

				return err
			},
			code: "ValidationException",
		},
		{
			name: "unknown graph",
			call: func(c *detectivesdk.Client, _ string) error {
				_, err := c.DeleteGraph(
					t.Context(),
					&detectivesdk.DeleteGraphInput{GraphArn: aws.String(missingGraphARN)},
				)

				return err
			},
			code: "ResourceNotFoundException",
		},
		{
			name: "max results too large",
			call: func(c *detectivesdk.Client, graph string) error {
				_, err := c.ListMembers(t.Context(), &detectivesdk.ListMembersInput{
					GraphArn: aws.String(graph), MaxResults: aws.Int32(201),
				})

				return err
			},
			code: "ValidationException",
		},
		{
			name: "long invitation message",
			call: func(c *detectivesdk.Client, graph string) error {
				_, err := c.CreateMembers(t.Context(), &detectivesdk.CreateMembersInput{
					GraphArn: aws.String(graph),
					Message:  aws.String(strings.Repeat("m", 1001)),
					Accounts: []detectivetypes.Account{
						{AccountId: aws.String("111111111111"), EmailAddress: aws.String("a@example.com")},
					},
				})

				return err
			},
			code: "ValidationException",
		},
		{
			name: "scope end before start",
			call: func(c *detectivesdk.Client, graph string) error {
				now := time.Now()
				_, err := c.StartInvestigation(t.Context(), &detectivesdk.StartInvestigationInput{
					GraphArn:       aws.String(graph),
					EntityArn:      aws.String("arn:aws:iam::000000000000:user/bob"),
					ScopeStartTime: aws.Time(now),
					ScopeEndTime:   aws.Time(now.Add(-time.Hour)),
				})

				return err
			},
			code: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestDetectiveSDKClient(t, newTestHandler(t))
			g, err := client.CreateGraph(t.Context(), &detectivesdk.CreateGraphInput{})
			require.NoError(t, err)

			err = tt.call(client, aws.ToString(g.GraphArn))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.code)
			assert.NotContains(t, err.Error(), tt.code+": "+tt.code)
		})
	}
}

func TestNewGraphHasDetectiveCore(t *testing.T) {
	t.Parallel()

	client := newTestDetectiveSDKClient(t, newTestHandler(t))
	g, err := client.CreateGraph(t.Context(), &detectivesdk.CreateGraphInput{})
	require.NoError(t, err)

	out, err := client.ListDatasourcePackages(
		t.Context(),
		&detectivesdk.ListDatasourcePackagesInput{GraphArn: g.GraphArn},
	)
	require.NoError(t, err)
	assert.Equal(t, detectivetypes.DatasourcePackageIngestStateStarted,
		out.DatasourcePackages[string(detectivetypes.DatasourcePackageDetectiveCore)].DatasourcePackageIngestState)
}

func TestInvestigationRunsUntilDelayElapses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status detectivetypes.Status
		delay  time.Duration
	}{
		{name: "default", status: detectivetypes.StatusSuccessful},
		{name: "elapsed", delay: time.Nanosecond, status: detectivetypes.StatusSuccessful},
		{name: "running", delay: time.Hour, status: detectivetypes.StatusRunning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := detective.NewInMemoryBackend("000000000000", "us-east-1")
			backend.SetLifecycleDelay(tt.delay)
			client := newTestDetectiveSDKClient(t, detective.NewHandler(backend))

			g, err := client.CreateGraph(t.Context(), &detectivesdk.CreateGraphInput{})
			require.NoError(t, err)

			now := time.Now()
			inv, err := client.StartInvestigation(t.Context(), &detectivesdk.StartInvestigationInput{
				GraphArn:       g.GraphArn,
				EntityArn:      aws.String("arn:aws:iam::000000000000:user/bob"),
				ScopeStartTime: aws.Time(now.Add(-time.Hour)),
				ScopeEndTime:   aws.Time(now),
			})
			require.NoError(t, err)

			got, err := client.GetInvestigation(t.Context(), &detectivesdk.GetInvestigationInput{
				GraphArn: g.GraphArn, InvestigationId: inv.InvestigationId,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.status, got.Status)
		})
	}
}
