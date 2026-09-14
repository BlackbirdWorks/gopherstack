package eventbridge_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// TestRealClient_RuleEndpointAndAPIDestination drives eventbridge's typed-coverage-blind ops
// (gopherstack-n3zi) through the real aws-sdk-go-v2 client.
func TestRealClient_RuleEndpointAndAPIDestination(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "tags", run: func(t *testing.T) {
			t.Helper()

			h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
			client := newTestEventBridgeClient(t, h)
			ctx := t.Context()

			busOut, err := client.CreateEventBus(ctx, &eventbridgesdk.CreateEventBusInput{
				Name: aws.String("s11-tags-bus"),
			})
			require.NoError(t, err)
			busArn := aws.ToString(busOut.EventBusArn)

			_, err = client.TagResource(ctx, &eventbridgesdk.TagResourceInput{
				ResourceARN: aws.String(busArn),
				Tags: []ebtypes.Tag{
					{Key: aws.String("env"), Value: aws.String("test")},
				},
			})
			require.NoError(t, err)

			listOut, err := client.ListTagsForResource(ctx, &eventbridgesdk.ListTagsForResourceInput{
				ResourceARN: aws.String(busArn),
			})
			require.NoError(t, err)
			tagMap := map[string]string{}
			for _, tg := range listOut.Tags {
				tagMap[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
			}
			assert.Equal(t, "test", tagMap["env"])

			_, err = client.UntagResource(ctx, &eventbridgesdk.UntagResourceInput{
				ResourceARN: aws.String(busArn),
				TagKeys:     []string{"env"},
			})
			require.NoError(t, err)

			afterOut, err := client.ListTagsForResource(ctx, &eventbridgesdk.ListTagsForResourceInput{
				ResourceARN: aws.String(busArn),
			})
			require.NoError(t, err)
			assert.Empty(t, afterOut.Tags)
		}},
		{name: "api destination lifecycle", run: func(t *testing.T) {
			t.Helper()

			h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
			client := newTestEventBridgeClient(t, h)
			ctx := t.Context()

			connOut, err := client.CreateConnection(ctx, &eventbridgesdk.CreateConnectionInput{
				Name:              aws.String("s11-conn"),
				AuthorizationType: ebtypes.ConnectionAuthorizationTypeApiKey,
				AuthParameters: &ebtypes.CreateConnectionAuthRequestParameters{
					ApiKeyAuthParameters: &ebtypes.CreateConnectionApiKeyAuthRequestParameters{
						ApiKeyName:  aws.String("x-api-key"),
						ApiKeyValue: aws.String("secret"),
					},
				},
			})
			require.NoError(t, err)
			connArn := aws.ToString(connOut.ConnectionArn)

			_, err = client.CreateApiDestination(ctx, &eventbridgesdk.CreateApiDestinationInput{
				Name:               aws.String("s11-api-dest"),
				ConnectionArn:      aws.String(connArn),
				InvocationEndpoint: aws.String("https://example.com/webhook"),
				HttpMethod:         ebtypes.ApiDestinationHttpMethodPost,
			})
			require.NoError(t, err)

			updOut, err := client.UpdateApiDestination(ctx, &eventbridgesdk.UpdateApiDestinationInput{
				Name:        aws.String("s11-api-dest"),
				Description: aws.String("s11 updated description"),
			})
			require.NoError(t, err)
			assert.NotZero(t, aws.ToTime(updOut.LastModifiedTime))

			descOut, err := client.DescribeApiDestination(ctx, &eventbridgesdk.DescribeApiDestinationInput{
				Name: aws.String("s11-api-dest"),
			})
			require.NoError(t, err)
			assert.Equal(t, "s11 updated description", aws.ToString(descOut.Description))

			_, err = client.DeleteApiDestination(ctx, &eventbridgesdk.DeleteApiDestinationInput{
				Name: aws.String("s11-api-dest"),
			})
			require.NoError(t, err)

			_, err = client.DescribeApiDestination(ctx, &eventbridgesdk.DescribeApiDestinationInput{
				Name: aws.String("s11-api-dest"),
			})
			require.Error(t, err, "DescribeApiDestination on a deleted destination must fail")

			_, err = client.DeleteConnection(ctx, &eventbridgesdk.DeleteConnectionInput{
				Name: aws.String("s11-conn"),
			})
			require.NoError(t, err)
		}},
		{name: "archive delete and update", run: func(t *testing.T) {
			t.Helper()

			h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
			client := newTestEventBridgeClient(t, h)
			ctx := t.Context()

			busOut, err := client.CreateEventBus(ctx, &eventbridgesdk.CreateEventBusInput{
				Name: aws.String("s11-archive-bus"),
			})
			require.NoError(t, err)
			busArn := busOut.EventBusArn

			_, err = client.CreateArchive(ctx, &eventbridgesdk.CreateArchiveInput{
				ArchiveName:    aws.String("s11-archive"),
				EventSourceArn: busArn,
			})
			require.NoError(t, err)

			updOut, err := client.UpdateArchive(ctx, &eventbridgesdk.UpdateArchiveInput{
				ArchiveName: aws.String("s11-archive"),
				Description: aws.String("s11 archive description"),
			})
			require.NoError(t, err)
			assert.Equal(t, ebtypes.ArchiveStateEnabled, updOut.State)

			_, err = client.DeleteArchive(ctx, &eventbridgesdk.DeleteArchiveInput{
				ArchiveName: aws.String("s11-archive"),
			})
			require.NoError(t, err)

			_, err = client.DescribeArchive(ctx, &eventbridgesdk.DescribeArchiveInput{
				ArchiveName: aws.String("s11-archive"),
			})
			require.Error(t, err, "DescribeArchive on a deleted archive must fail")
		}},
		{name: "endpoint delete and describe", run: func(t *testing.T) {
			t.Helper()

			h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
			client := newTestEventBridgeClient(t, h)
			ctx := t.Context()

			_, err := client.CreateEventBus(
				ctx,
				&eventbridgesdk.CreateEventBusInput{Name: aws.String("s11-ep-primary")},
			)
			require.NoError(t, err)
			_, err = client.CreateEventBus(
				ctx,
				&eventbridgesdk.CreateEventBusInput{Name: aws.String("s11-ep-secondary")},
			)
			require.NoError(t, err)

			primaryArn := "arn:aws:events:us-east-1:000000000000:event-bus/s11-ep-primary"
			secondaryArn := "arn:aws:events:us-west-2:000000000000:event-bus/s11-ep-secondary"

			_, err = client.CreateEndpoint(ctx, &eventbridgesdk.CreateEndpointInput{
				Name: aws.String("s11-endpoint"),
				RoutingConfig: &ebtypes.RoutingConfig{
					FailoverConfig: &ebtypes.FailoverConfig{
						Primary:   &ebtypes.Primary{HealthCheck: aws.String("arn:aws:route53:::healthcheck/abc")},
						Secondary: &ebtypes.Secondary{Route: aws.String("us-west-2")},
					},
				},
				EventBuses: []ebtypes.EndpointEventBus{
					{EventBusArn: aws.String(primaryArn)},
					{EventBusArn: aws.String(secondaryArn)},
				},
			})
			require.NoError(t, err)

			descOut, err := client.DescribeEndpoint(ctx, &eventbridgesdk.DescribeEndpointInput{
				Name: aws.String("s11-endpoint"),
			})
			require.NoError(t, err)
			assert.Equal(t, "s11-endpoint", aws.ToString(descOut.Name))
			require.Len(t, descOut.EventBuses, 2)

			_, err = client.DeleteEndpoint(ctx, &eventbridgesdk.DeleteEndpointInput{
				Name: aws.String("s11-endpoint"),
			})
			require.NoError(t, err)

			_, err = client.DescribeEndpoint(ctx, &eventbridgesdk.DescribeEndpointInput{
				Name: aws.String("s11-endpoint"),
			})
			require.Error(t, err, "DescribeEndpoint on a deleted endpoint must fail")
		}},
		{name: "rule enable disable and test event pattern", run: func(t *testing.T) {
			t.Helper()

			h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
			client := newTestEventBridgeClient(t, h)
			ctx := t.Context()

			pattern := `{"source":["s11.source"]}`
			_, err := client.PutRule(ctx, &eventbridgesdk.PutRuleInput{
				Name:         aws.String("s11-rule"),
				EventPattern: aws.String(pattern),
			})
			require.NoError(t, err)

			_, err = client.DisableRule(ctx, &eventbridgesdk.DisableRuleInput{
				Name: aws.String("s11-rule"),
			})
			require.NoError(t, err)

			disabledOut, err := client.DescribeRule(ctx, &eventbridgesdk.DescribeRuleInput{
				Name: aws.String("s11-rule"),
			})
			require.NoError(t, err)
			assert.Equal(t, ebtypes.RuleStateDisabled, disabledOut.State)

			_, err = client.EnableRule(ctx, &eventbridgesdk.EnableRuleInput{
				Name: aws.String("s11-rule"),
			})
			require.NoError(t, err)

			enabledOut, err := client.DescribeRule(ctx, &eventbridgesdk.DescribeRuleInput{
				Name: aws.String("s11-rule"),
			})
			require.NoError(t, err)
			assert.Equal(t, ebtypes.RuleStateEnabled, enabledOut.State)

			matchOut, err := client.TestEventPattern(ctx, &eventbridgesdk.TestEventPatternInput{
				EventPattern: aws.String(pattern),
				Event:        aws.String(`{"source":"s11.source","detail-type":"x","detail":{}}`),
			})
			require.NoError(t, err)
			assert.True(t, matchOut.Result)

			noMatchOut, err := client.TestEventPattern(ctx, &eventbridgesdk.TestEventPatternInput{
				EventPattern: aws.String(pattern),
				Event:        aws.String(`{"source":"other.source","detail-type":"x","detail":{}}`),
			})
			require.NoError(t, err)
			assert.False(t, noMatchOut.Result)
		}},
		{name: "remove permission", run: func(t *testing.T) {
			t.Helper()

			h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
			client := newTestEventBridgeClient(t, h)
			ctx := t.Context()

			_, err := client.PutPermission(ctx, &eventbridgesdk.PutPermissionInput{
				Action:      aws.String("events:PutEvents"),
				Principal:   aws.String("111111111111"),
				StatementId: aws.String("S11Statement"),
			})
			require.NoError(t, err)

			before, err := client.DescribeEventBus(ctx, &eventbridgesdk.DescribeEventBusInput{})
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(before.Policy), "S11Statement")

			_, err = client.RemovePermission(ctx, &eventbridgesdk.RemovePermissionInput{
				StatementId: aws.String("S11Statement"),
			})
			require.NoError(t, err)

			after, err := client.DescribeEventBus(ctx, &eventbridgesdk.DescribeEventBusInput{})
			require.NoError(t, err)
			assert.NotContains(t, aws.ToString(after.Policy), "S11Statement")
		}},
		{name: "partner event source lifecycle", run: func(t *testing.T) {
			t.Helper()

			h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
			client := newTestEventBridgeClient(t, h)
			ctx := t.Context()

			sourceName := "aws.partner/s11partner.com/s11-source"
			_, err := client.CreatePartnerEventSource(ctx, &eventbridgesdk.CreatePartnerEventSourceInput{
				Name:    aws.String(sourceName),
				Account: aws.String("111111111111"),
			})
			require.NoError(t, err)

			descOut, err := client.DescribePartnerEventSource(ctx, &eventbridgesdk.DescribePartnerEventSourceInput{
				Name: aws.String(sourceName),
			})
			require.NoError(t, err)
			assert.Equal(t, sourceName, aws.ToString(descOut.Name))

			putOut, err := client.PutPartnerEvents(ctx, &eventbridgesdk.PutPartnerEventsInput{
				Entries: []ebtypes.PutPartnerEventsRequestEntry{
					{
						Source:     aws.String(sourceName),
						DetailType: aws.String("s11-event"),
						Detail:     aws.String(`{"key":"value"}`),
					},
				},
			})
			require.NoError(t, err)
			assert.Equal(t, int32(0), putOut.FailedEntryCount)

			describeSrcOut, err := client.DescribeEventSource(ctx, &eventbridgesdk.DescribeEventSourceInput{
				Name: aws.String(sourceName),
			})
			require.NoError(t, err)
			assert.Equal(t, ebtypes.EventSourceStatePending, describeSrcOut.State)

			_, err = client.DeactivateEventSource(ctx, &eventbridgesdk.DeactivateEventSourceInput{
				Name: aws.String(sourceName),
			})
			require.NoError(t, err)

			afterDeactivate, err := client.DescribeEventSource(ctx, &eventbridgesdk.DescribeEventSourceInput{
				Name: aws.String(sourceName),
			})
			require.NoError(t, err)
			assert.Equal(
				t,
				ebtypes.EventSourceStatePending,
				afterDeactivate.State,
				"real AWS: DeactivateEventSource transitions the source back to PENDING, not a fabricated INACTIVE value",
			)

			_, err = client.DeletePartnerEventSource(ctx, &eventbridgesdk.DeletePartnerEventSourceInput{
				Name:    aws.String(sourceName),
				Account: aws.String("111111111111"),
			})
			require.NoError(t, err)

			_, err = client.DescribePartnerEventSource(ctx, &eventbridgesdk.DescribePartnerEventSourceInput{
				Name: aws.String(sourceName),
			})
			require.Error(t, err, "DescribePartnerEventSource on a deleted source must fail")
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
