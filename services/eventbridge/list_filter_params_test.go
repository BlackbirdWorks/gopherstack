package eventbridge_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/schemas"
	schemastypes "github.com/aws/aws-sdk-go-v2/service/schemas/types"
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

// TestListEventSources_Limit asserts ListEventSourcesInput.Limit is honoured
// instead of always returning the default page size.
func TestListEventSources_Limit(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	for _, name := range []string{"acme/orders/a", "acme/orders/b", "acme/orders/c"} {
		_, err := client.CreatePartnerEventSource(t.Context(), &eventbridgesdk.CreatePartnerEventSourceInput{
			Name:    aws.String(name),
			Account: aws.String("111122223333"),
		})
		require.NoError(t, err)
	}

	out, err := client.ListEventSources(t.Context(), &eventbridgesdk.ListEventSourcesInput{Limit: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, out.EventSources, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestListPartnerEventSources_Limit asserts ListPartnerEventSourcesInput.Limit
// is honoured instead of always returning the default page size.
func TestListPartnerEventSources_Limit(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	for _, name := range []string{"acme/orders/a", "acme/orders/b", "acme/orders/c"} {
		_, err := client.CreatePartnerEventSource(t.Context(), &eventbridgesdk.CreatePartnerEventSourceInput{
			Name:    aws.String(name),
			Account: aws.String("111122223333"),
		})
		require.NoError(t, err)
	}

	out, err := client.ListPartnerEventSources(t.Context(), &eventbridgesdk.ListPartnerEventSourcesInput{
		NamePrefix: aws.String("acme/"),
		Limit:      aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, out.PartnerEventSources, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestListRuleNamesByTarget_Limit asserts ListRuleNamesByTargetInput.Limit is
// honoured instead of always returning the default page size.
func TestListRuleNamesByTarget_Limit(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	const targetARN = "arn:aws:sqs:us-east-1:123456789012:q"

	for _, name := range []string{"rule-a", "rule-b", "rule-c"} {
		_, err := client.PutRule(t.Context(), &eventbridgesdk.PutRuleInput{
			Name:         aws.String(name),
			EventPattern: aws.String(`{"source":["x"]}`),
		})
		require.NoError(t, err)

		_, err = client.PutTargets(t.Context(), &eventbridgesdk.PutTargetsInput{
			Rule:    aws.String(name),
			Targets: []ebtypes.Target{{Id: aws.String("t1"), Arn: aws.String(targetARN)}},
		})
		require.NoError(t, err)
	}

	out, err := client.ListRuleNamesByTarget(t.Context(), &eventbridgesdk.ListRuleNamesByTargetInput{
		TargetArn: aws.String(targetARN),
		Limit:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, out.RuleNames, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestListRegistries_Limit asserts ListRegistriesInput.Limit (schemas
// service, REST-JSON) is honoured instead of always returning the default
// page size.
func TestListRegistries_Limit(t *testing.T) {
	t.Parallel()

	h := newTestSchemasHandler(t)
	client := newTestSchemasClient(t, h)

	for _, name := range []string{"reg-a", "reg-b", "reg-c"} {
		_, err := client.CreateRegistry(t.Context(), &schemas.CreateRegistryInput{RegistryName: aws.String(name)})
		require.NoError(t, err)
	}

	out, err := client.ListRegistries(t.Context(), &schemas.ListRegistriesInput{Limit: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, out.Registries, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestListSchemas_Limit asserts ListSchemasInput.Limit (schemas service,
// REST-JSON) is honoured instead of always returning the default page size.
func TestListSchemas_Limit(t *testing.T) {
	t.Parallel()

	h := newTestSchemasHandler(t)
	client := newTestSchemasClient(t, h)

	_, err := client.CreateRegistry(t.Context(), &schemas.CreateRegistryInput{RegistryName: aws.String("reg")})
	require.NoError(t, err)

	for _, name := range []string{"schema-a", "schema-b", "schema-c"} {
		_, err = client.CreateSchema(t.Context(), &schemas.CreateSchemaInput{
			RegistryName: aws.String("reg"),
			SchemaName:   aws.String(name),
			Type:         schemastypes.TypeOpenApi3,
			Content:      aws.String(`{"openapi":"3.0.0","info":{"title":"t","version":"1"},"paths":{}}`),
		})
		require.NoError(t, err)
	}

	out, err := client.ListSchemas(t.Context(), &schemas.ListSchemasInput{
		RegistryName: aws.String("reg"),
		Limit:        aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, out.Schemas, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestSearchSchemas_Limit asserts SearchSchemasInput.Limit (schemas service,
// REST-JSON) is honoured instead of always returning the default page size.
func TestSearchSchemas_Limit(t *testing.T) {
	t.Parallel()

	h := newTestSchemasHandler(t)
	client := newTestSchemasClient(t, h)

	_, err := client.CreateRegistry(t.Context(), &schemas.CreateRegistryInput{RegistryName: aws.String("reg")})
	require.NoError(t, err)

	for _, name := range []string{"order-a", "order-b", "order-c"} {
		_, err = client.CreateSchema(t.Context(), &schemas.CreateSchemaInput{
			RegistryName: aws.String("reg"),
			SchemaName:   aws.String(name),
			Type:         schemastypes.TypeOpenApi3,
			Content:      aws.String(`{"openapi":"3.0.0","info":{"title":"t","version":"1"},"paths":{}}`),
		})
		require.NoError(t, err)
	}

	out, err := client.SearchSchemas(t.Context(), &schemas.SearchSchemasInput{
		RegistryName: aws.String("reg"),
		Keywords:     aws.String("order"),
		Limit:        aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, out.Schemas, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestListSchemaVersions_Limit asserts ListSchemaVersionsInput.Limit
// (schemas service, REST-JSON) is honoured instead of always returning the
// default page size.
func TestListSchemaVersions_Limit(t *testing.T) {
	t.Parallel()

	h := newTestSchemasHandler(t)
	client := newTestSchemasClient(t, h)

	_, err := client.CreateRegistry(t.Context(), &schemas.CreateRegistryInput{RegistryName: aws.String("reg")})
	require.NoError(t, err)

	_, err = client.CreateSchema(t.Context(), &schemas.CreateSchemaInput{
		RegistryName: aws.String("reg"),
		SchemaName:   aws.String("versioned"),
		Type:         schemastypes.TypeOpenApi3,
		Content:      aws.String(`{"openapi":"3.0.0","info":{"title":"t","version":"0"},"paths":{}}`),
	})
	require.NoError(t, err)

	for i := 1; i < 3; i++ {
		_, err = client.UpdateSchema(t.Context(), &schemas.UpdateSchemaInput{
			RegistryName: aws.String("reg"),
			SchemaName:   aws.String("versioned"),
			Content: aws.String(fmt.Sprintf(
				`{"openapi":"3.0.0","info":{"title":"t","version":"%d"},"paths":{}}`, i,
			)),
		})
		require.NoError(t, err)
	}

	out, err := client.ListSchemaVersions(t.Context(), &schemas.ListSchemaVersionsInput{
		RegistryName: aws.String("reg"),
		SchemaName:   aws.String("versioned"),
		Limit:        aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, out.SchemaVersions, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestListArchives_Limit asserts ListArchivesInput.Limit is honoured instead
// of always returning the default page size.
func TestListArchives_Limit(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	bus, err := client.CreateEventBus(t.Context(), &eventbridgesdk.CreateEventBusInput{Name: aws.String("bus")})
	require.NoError(t, err)

	for _, name := range []string{"arch-a", "arch-b", "arch-c"} {
		_, err = client.CreateArchive(t.Context(), &eventbridgesdk.CreateArchiveInput{
			ArchiveName:    aws.String(name),
			EventSourceArn: bus.EventBusArn,
		})
		require.NoError(t, err)
	}

	out, err := client.ListArchives(t.Context(), &eventbridgesdk.ListArchivesInput{Limit: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, out.Archives, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestListReplays_Limit asserts ListReplaysInput.Limit is honoured instead
// of always returning the default page size.
func TestListReplays_Limit(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	bus, err := client.CreateEventBus(t.Context(), &eventbridgesdk.CreateEventBusInput{Name: aws.String("bus")})
	require.NoError(t, err)

	archive, err := client.CreateArchive(t.Context(), &eventbridgesdk.CreateArchiveInput{
		ArchiveName:    aws.String("arch"),
		EventSourceArn: bus.EventBusArn,
	})
	require.NoError(t, err)

	now := time.Now()

	for _, name := range []string{"replay-a", "replay-b", "replay-c"} {
		_, err = client.StartReplay(t.Context(), &eventbridgesdk.StartReplayInput{
			ReplayName:     aws.String(name),
			EventSourceArn: archive.ArchiveArn,
			Destination:    &ebtypes.ReplayDestination{Arn: bus.EventBusArn},
			EventStartTime: aws.Time(now.Add(-time.Hour)),
			EventEndTime:   aws.Time(now),
		})
		require.NoError(t, err)
	}

	out, err := client.ListReplays(t.Context(), &eventbridgesdk.ListReplaysInput{Limit: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, out.Replays, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}
