package eventbridge_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestManagedRule_SDKForce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		force bool
	}{
		{name: "without_force_rejected", force: false},
		{name: "force_allowed", force: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			client := newTestEventBridgeClient(t, eventbridge.NewHandler(b))
			ctx := t.Context()

			_, err := b.PutRule(ctx, eventbridge.PutRuleInput{Name: "r", EventPattern: `{"source":["x"]}`})
			require.NoError(t, err)

			_, err = client.PutTargets(ctx, &eventbridgesdk.PutTargetsInput{
				Rule: aws.String("r"),
				Targets: []types.Target{
					{Id: aws.String("t1"), Arn: aws.String("arn:aws:sqs:us-east-1:000000000000:q")},
				},
			})
			require.NoError(t, err)

			_, err = b.PutRule(ctx, eventbridge.PutRuleInput{
				Name: "r", EventPattern: `{"source":["x"]}`, ManagedBy: "scheduler.amazonaws.com",
			})
			require.NoError(t, err)

			_, err = client.RemoveTargets(ctx, &eventbridgesdk.RemoveTargetsInput{
				Rule: aws.String("r"), Ids: []string{"t1"}, Force: tt.force,
			})
			assertManaged(t, err, tt.force)

			_, err = client.DeleteRule(ctx, &eventbridgesdk.DeleteRuleInput{Name: aws.String("r"), Force: tt.force})
			assertManaged(t, err, tt.force)

			_, descErr := client.DescribeRule(ctx, &eventbridgesdk.DescribeRuleInput{Name: aws.String("r")})
			if tt.force {
				require.Error(t, descErr)
			} else {
				require.NoError(t, descErr)
			}
		})
	}
}

func assertManaged(t *testing.T, err error, force bool) {
	t.Helper()

	if force {
		require.NoError(t, err)

		return
	}

	var managed *types.ManagedRuleException

	require.ErrorAs(t, err, &managed)
	assert.NotNil(t, managed)
}

func TestCreateEventBus_SDKPartnerEventSource(t *testing.T) {
	t.Parallel()

	const source = "aws.partner/example.com/acct/src"

	tests := []struct {
		name        string
		busName     string
		sourceName  string
		wantErrType string
		seedSource  bool
	}{
		{name: "matches_source", busName: source, sourceName: source, seedSource: true},
		{
			name:        "name_mismatch",
			busName:     "other",
			sourceName:  source,
			seedSource:  true,
			wantErrType: "ValidationException",
		},
		{name: "source_missing", busName: source, sourceName: source, wantErrType: "ResourceNotFoundException"},
		{name: "reserved_prefix_without_source", busName: source, wantErrType: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEventBridgeClient(t, eventbridge.NewHandler(newBackend()))
			ctx := t.Context()

			if tt.seedSource {
				_, err := client.CreatePartnerEventSource(ctx, &eventbridgesdk.CreatePartnerEventSourceInput{
					Name: aws.String(source), Account: aws.String("123456789012"),
				})
				require.NoError(t, err)
			}

			in := &eventbridgesdk.CreateEventBusInput{Name: aws.String(tt.busName)}
			if tt.sourceName != "" {
				in.EventSourceName = aws.String(tt.sourceName)
			}

			_, err := client.CreateEventBus(ctx, in)
			if tt.wantErrType != "" {
				var apiErr smithy.APIError

				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantErrType, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)

			desc, err := client.DescribeEventBus(
				ctx,
				&eventbridgesdk.DescribeEventBusInput{Name: aws.String(tt.busName)},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.busName, aws.ToString(desc.Name))
		})
	}
}
