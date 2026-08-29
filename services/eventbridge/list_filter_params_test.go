package eventbridge_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func newTestConnection(t *testing.T, client *eventbridgesdk.Client, name string) {
	t.Helper()

	_, err := client.CreateConnection(t.Context(), &eventbridgesdk.CreateConnectionInput{
		Name:              aws.String(name),
		AuthorizationType: ebtypes.ConnectionAuthorizationTypeApiKey,
		AuthParameters: &ebtypes.CreateConnectionAuthRequestParameters{
			ApiKeyAuthParameters: &ebtypes.CreateConnectionApiKeyAuthRequestParameters{
				ApiKeyName:  aws.String("x-api-key"),
				ApiKeyValue: aws.String("v"),
			},
		},
	})
	require.NoError(t, err)
}

// TestListConnections_ConnectionStateFilter asserts ListConnectionsInput.
// ConnectionState (api_op_ListConnections.go) narrows the result to
// connections in that state, instead of being silently ignored.
func TestListConnections_ConnectionStateFilter(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	newTestConnection(t, client, "conn-authorized")
	newTestConnection(t, client, "conn-deauthorized")

	_, err := client.DeauthorizeConnection(
		t.Context(), &eventbridgesdk.DeauthorizeConnectionInput{Name: aws.String("conn-deauthorized")},
	)
	require.NoError(t, err)

	out, err := client.ListConnections(t.Context(), &eventbridgesdk.ListConnectionsInput{
		ConnectionState: ebtypes.ConnectionStateAuthorized,
	})
	require.NoError(t, err)

	names := make([]string, 0, len(out.Connections))
	for _, c := range out.Connections {
		names = append(names, aws.ToString(c.Name))
	}

	require.Equal(t, []string{"conn-authorized"}, names)
}

// TestListConnections_Limit asserts ListConnectionsInput.Limit is honoured
// instead of always returning the default page size.
func TestListConnections_Limit(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	for _, name := range []string{"a", "b", "c"} {
		newTestConnection(t, client, name)
	}

	out, err := client.ListConnections(t.Context(), &eventbridgesdk.ListConnectionsInput{Limit: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, out.Connections, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestListApiDestinations_ConnectionArnFilter asserts ListApiDestinationsInput.
// ConnectionArn (api_op_ListApiDestinations.go) narrows the result to API
// destinations using that connection.
func TestListApiDestinations_ConnectionArnFilter(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	newTestConnection(t, client, "conn-a")
	newTestConnection(t, client, "conn-b")

	connA, err := client.DescribeConnection(t.Context(), &eventbridgesdk.DescribeConnectionInput{
		Name: aws.String("conn-a"),
	})
	require.NoError(t, err)

	connB, err := client.DescribeConnection(t.Context(), &eventbridgesdk.DescribeConnectionInput{
		Name: aws.String("conn-b"),
	})
	require.NoError(t, err)

	_, err = client.CreateApiDestination(t.Context(), &eventbridgesdk.CreateApiDestinationInput{
		Name:               aws.String("dst-a"),
		ConnectionArn:      connA.ConnectionArn,
		HttpMethod:         ebtypes.ApiDestinationHttpMethodGet,
		InvocationEndpoint: aws.String("https://example.com/a"),
	})
	require.NoError(t, err)

	_, err = client.CreateApiDestination(t.Context(), &eventbridgesdk.CreateApiDestinationInput{
		Name:               aws.String("dst-b"),
		ConnectionArn:      connB.ConnectionArn,
		HttpMethod:         ebtypes.ApiDestinationHttpMethodGet,
		InvocationEndpoint: aws.String("https://example.com/b"),
	})
	require.NoError(t, err)

	out, err := client.ListApiDestinations(t.Context(), &eventbridgesdk.ListApiDestinationsInput{
		ConnectionArn: connA.ConnectionArn,
	})
	require.NoError(t, err)

	names := make([]string, 0, len(out.ApiDestinations))
	for _, d := range out.ApiDestinations {
		names = append(names, aws.ToString(d.Name))
	}

	require.Equal(t, []string{"dst-a"}, names)
}

// TestListEndpoints_MaxResults asserts ListEndpointsInput.MaxResults is
// honoured instead of always returning the default page size.
func TestListEndpoints_MaxResults(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	for _, name := range []string{"ep-a", "ep-b", "ep-c"} {
		_, err := client.CreateEndpoint(t.Context(), &eventbridgesdk.CreateEndpointInput{
			Name: aws.String(name),
			RoutingConfig: &ebtypes.RoutingConfig{
				FailoverConfig: &ebtypes.FailoverConfig{
					Primary:   &ebtypes.Primary{HealthCheck: aws.String("arn:aws:route53:::healthcheck/abc")},
					Secondary: &ebtypes.Secondary{Route: aws.String("us-west-2")},
				},
			},
			EventBuses: []ebtypes.EndpointEventBus{
				{EventBusArn: aws.String("arn:aws:events:us-east-1:123456789012:event-bus/default")},
				{EventBusArn: aws.String("arn:aws:events:us-west-2:123456789012:event-bus/default")},
			},
		})
		require.NoError(t, err)
	}

	out, err := client.ListEndpoints(t.Context(), &eventbridgesdk.ListEndpointsInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, out.Endpoints, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}
