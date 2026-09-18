package eventbridge_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/schemas"
	schemastypes "github.com/aws/aws-sdk-go-v2/service/schemas/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// TestUpdate_OmittedMembersPreserveState locks in the zeroguard census fix
// (gopherstack-101r follow-up): an Update/Put op's optional *string member
// must be applied only when the caller sends it. Before the fix these
// fields decoded as plain strings, so a second update that simply omitted a
// field silently blanked it instead of leaving the stored value alone. Each
// case creates a resource, sets a field, sends a second update that omits
// it and asserts the earlier value survived, then sends an explicit empty
// string and asserts it is applied (not treated as omitted).
func TestUpdate_OmittedMembersPreserveState(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "archive_description_pattern_kms", run: testArchiveFieldsPreserved},
		{name: "connection_description", run: testConnectionDescriptionPreserved},
		{name: "endpoint_description_and_role_arn", run: testEndpointFieldsPreserved},
		{name: "event_bus_description_and_kms", run: testEventBusFieldsPreserved},
		{name: "registry_description", run: testRegistryDescriptionPreserved},
		{name: "schema_content_and_description", run: testSchemaFieldsPreserved},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testArchiveFieldsPreserved(t *testing.T) {
	t.Helper()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)
	ctx := t.Context()

	bus, err := client.CreateEventBus(ctx, &eventbridgesdk.CreateEventBusInput{
		Name: aws.String("omit-archive-bus"),
	})
	require.NoError(t, err)

	_, err = client.CreateArchive(ctx, &eventbridgesdk.CreateArchiveInput{
		ArchiveName:    aws.String("omit-archive"),
		EventSourceArn: bus.EventBusArn,
	})
	require.NoError(t, err)

	_, err = client.UpdateArchive(ctx, &eventbridgesdk.UpdateArchiveInput{
		ArchiveName:      aws.String("omit-archive"),
		Description:      aws.String("first description"),
		EventPattern:     aws.String(`{"source":["a"]}`),
		KmsKeyIdentifier: aws.String("alias/first-key"),
	})
	require.NoError(t, err)

	described, err := client.DescribeArchive(ctx, &eventbridgesdk.DescribeArchiveInput{
		ArchiveName: aws.String("omit-archive"),
	})
	require.NoError(t, err)
	assert.Equal(t, "first description", aws.ToString(described.Description))

	// Omits every optional field -- all three must survive.
	_, err = client.UpdateArchive(ctx, &eventbridgesdk.UpdateArchiveInput{
		ArchiveName: aws.String("omit-archive"),
	})
	require.NoError(t, err)

	afterOmit, err := client.DescribeArchive(ctx, &eventbridgesdk.DescribeArchiveInput{
		ArchiveName: aws.String("omit-archive"),
	})
	require.NoError(t, err)
	assert.Equal(t, "first description", aws.ToString(afterOmit.Description),
		"Description must survive an update that omits it")
	assert.JSONEq(t, `{"source":["a"]}`, aws.ToString(afterOmit.EventPattern),
		"EventPattern must survive an update that omits it")

	// An explicit empty Description is a meaningful clear and must be applied.
	_, err = client.UpdateArchive(ctx, &eventbridgesdk.UpdateArchiveInput{
		ArchiveName: aws.String("omit-archive"),
		Description: aws.String(""),
	})
	require.NoError(t, err)

	cleared, err := client.DescribeArchive(ctx, &eventbridgesdk.DescribeArchiveInput{
		ArchiveName: aws.String("omit-archive"),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Description))
	assert.JSONEq(t, `{"source":["a"]}`, aws.ToString(cleared.EventPattern),
		"fields not sent in this call must remain untouched")
}

func testConnectionDescriptionPreserved(t *testing.T) {
	t.Helper()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)
	ctx := t.Context()

	_, err := client.CreateConnection(ctx, &eventbridgesdk.CreateConnectionInput{
		Name:              aws.String("omit-conn"),
		AuthorizationType: ebtypes.ConnectionAuthorizationTypeApiKey,
		AuthParameters: &ebtypes.CreateConnectionAuthRequestParameters{
			ApiKeyAuthParameters: &ebtypes.CreateConnectionApiKeyAuthRequestParameters{
				ApiKeyName:  aws.String("x-api-key"),
				ApiKeyValue: aws.String("secret"),
			},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateConnection(ctx, &eventbridgesdk.UpdateConnectionInput{
		Name:        aws.String("omit-conn"),
		Description: aws.String("first description"),
	})
	require.NoError(t, err)

	// Omits Description -- must survive.
	_, err = client.UpdateConnection(ctx, &eventbridgesdk.UpdateConnectionInput{
		Name:              aws.String("omit-conn"),
		AuthorizationType: ebtypes.ConnectionAuthorizationTypeApiKey,
		AuthParameters: &ebtypes.UpdateConnectionAuthRequestParameters{
			ApiKeyAuthParameters: &ebtypes.UpdateConnectionApiKeyAuthRequestParameters{
				ApiKeyName:  aws.String("x-api-key"),
				ApiKeyValue: aws.String("secret2"),
			},
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeConnection(ctx, &eventbridgesdk.DescribeConnectionInput{
		Name: aws.String("omit-conn"),
	})
	require.NoError(t, err)
	assert.Equal(t, "first description", aws.ToString(described.Description),
		"Description must survive an update that omits it")

	// Explicit empty Description clears it.
	_, err = client.UpdateConnection(ctx, &eventbridgesdk.UpdateConnectionInput{
		Name:        aws.String("omit-conn"),
		Description: aws.String(""),
	})
	require.NoError(t, err)

	cleared, err := client.DescribeConnection(ctx, &eventbridgesdk.DescribeConnectionInput{
		Name: aws.String("omit-conn"),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Description))
}

func testEndpointFieldsPreserved(t *testing.T) {
	t.Helper()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)
	ctx := t.Context()

	_, err := client.CreateEndpoint(ctx, &eventbridgesdk.CreateEndpointInput{
		Name: aws.String("omit-endpoint"),
		EventBuses: []ebtypes.EndpointEventBus{
			{EventBusArn: aws.String("arn:aws:events:us-east-1:123456789012:event-bus/primary")},
			{EventBusArn: aws.String("arn:aws:events:us-west-2:123456789012:event-bus/secondary")},
		},
		RoutingConfig: &ebtypes.RoutingConfig{
			FailoverConfig: &ebtypes.FailoverConfig{
				Primary:   &ebtypes.Primary{HealthCheck: aws.String("arn:aws:route53:::healthcheck/abc")},
				Secondary: &ebtypes.Secondary{Route: aws.String("us-west-2")},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateEndpoint(ctx, &eventbridgesdk.UpdateEndpointInput{
		Name:        aws.String("omit-endpoint"),
		Description: aws.String("first description"),
		RoleArn:     aws.String("arn:aws:iam::123456789012:role/first-role"),
	})
	require.NoError(t, err)

	// Omits both -- must survive.
	_, err = client.UpdateEndpoint(ctx, &eventbridgesdk.UpdateEndpointInput{
		Name: aws.String("omit-endpoint"),
	})
	require.NoError(t, err)

	described, err := client.DescribeEndpoint(ctx, &eventbridgesdk.DescribeEndpointInput{
		Name: aws.String("omit-endpoint"),
	})
	require.NoError(t, err)
	assert.Equal(t, "first description", aws.ToString(described.Description),
		"Description must survive an update that omits it")
	assert.Equal(t, "arn:aws:iam::123456789012:role/first-role", aws.ToString(described.RoleArn),
		"RoleArn must survive an update that omits it")

	// Explicit empty Description clears it, RoleArn still untouched.
	_, err = client.UpdateEndpoint(ctx, &eventbridgesdk.UpdateEndpointInput{
		Name:        aws.String("omit-endpoint"),
		Description: aws.String(""),
	})
	require.NoError(t, err)

	cleared, err := client.DescribeEndpoint(ctx, &eventbridgesdk.DescribeEndpointInput{
		Name: aws.String("omit-endpoint"),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Description))
	assert.Equal(t, "arn:aws:iam::123456789012:role/first-role", aws.ToString(cleared.RoleArn))
}

func testEventBusFieldsPreserved(t *testing.T) {
	t.Helper()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)
	ctx := t.Context()

	_, err := client.CreateEventBus(ctx, &eventbridgesdk.CreateEventBusInput{
		Name: aws.String("omit-eventbus"),
	})
	require.NoError(t, err)

	_, err = client.UpdateEventBus(ctx, &eventbridgesdk.UpdateEventBusInput{
		Name:             aws.String("omit-eventbus"),
		Description:      aws.String("first description"),
		KmsKeyIdentifier: aws.String("alias/first-key"),
	})
	require.NoError(t, err)

	// Omits both -- must survive.
	withoutFields, err := client.UpdateEventBus(ctx, &eventbridgesdk.UpdateEventBusInput{
		Name: aws.String("omit-eventbus"),
	})
	require.NoError(t, err)
	assert.Equal(t, "first description", aws.ToString(withoutFields.Description),
		"Description must survive an update that omits it")
	assert.Equal(t, "alias/first-key", aws.ToString(withoutFields.KmsKeyIdentifier),
		"KmsKeyIdentifier must survive an update that omits it")

	// Explicit empty Description clears it.
	cleared, err := client.UpdateEventBus(ctx, &eventbridgesdk.UpdateEventBusInput{
		Name:        aws.String("omit-eventbus"),
		Description: aws.String(""),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Description))
	assert.Equal(t, "alias/first-key", aws.ToString(cleared.KmsKeyIdentifier),
		"fields not sent in this call must remain untouched")
}

func testRegistryDescriptionPreserved(t *testing.T) {
	t.Helper()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestSchemasClient(t, h)
	ctx := t.Context()

	_, err := client.CreateRegistry(ctx, &schemas.CreateRegistryInput{
		RegistryName: aws.String("omit-registry"),
	})
	require.NoError(t, err)

	_, err = client.UpdateRegistry(ctx, &schemas.UpdateRegistryInput{
		RegistryName: aws.String("omit-registry"),
		Description:  aws.String("first description"),
	})
	require.NoError(t, err)

	got, err := client.DescribeRegistry(ctx, &schemas.DescribeRegistryInput{
		RegistryName: aws.String("omit-registry"),
	})
	require.NoError(t, err)
	assert.Equal(t, "first description", aws.ToString(got.Description))

	// Explicit empty Description clears it.
	cleared, err := client.UpdateRegistry(ctx, &schemas.UpdateRegistryInput{
		RegistryName: aws.String("omit-registry"),
		Description:  aws.String(""),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Description))
}

func testSchemaFieldsPreserved(t *testing.T) {
	t.Helper()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestSchemasClient(t, h)
	ctx := t.Context()

	_, err := client.CreateRegistry(ctx, &schemas.CreateRegistryInput{
		RegistryName: aws.String("omit-schema-reg"),
	})
	require.NoError(t, err)

	const firstContent = `{"openapi":"3.0.0","info":{"title":"S","version":"1.0"},"paths":{}}`

	_, err = client.CreateSchema(ctx, &schemas.CreateSchemaInput{
		RegistryName: aws.String("omit-schema-reg"),
		SchemaName:   aws.String("omit-schema"),
		Type:         schemastypes.TypeOpenApi3,
		Content:      aws.String(firstContent),
	})
	require.NoError(t, err)

	_, err = client.UpdateSchema(ctx, &schemas.UpdateSchemaInput{
		RegistryName: aws.String("omit-schema-reg"),
		SchemaName:   aws.String("omit-schema"),
		Description:  aws.String("first description"),
	})
	require.NoError(t, err)

	// Omits both Content and Description -- both must survive.
	_, err = client.UpdateSchema(ctx, &schemas.UpdateSchemaInput{
		RegistryName: aws.String("omit-schema-reg"),
		SchemaName:   aws.String("omit-schema"),
	})
	require.NoError(t, err)

	got, err := client.DescribeSchema(ctx, &schemas.DescribeSchemaInput{
		RegistryName: aws.String("omit-schema-reg"),
		SchemaName:   aws.String("omit-schema"),
	})
	require.NoError(t, err)
	assert.JSONEq(t, firstContent, aws.ToString(got.Content),
		"Content must survive an update that omits it")
	assert.Equal(t, "first description", aws.ToString(got.Description),
		"Description must survive an update that omits it")

	// Explicit empty Description clears it, Content still untouched.
	_, err = client.UpdateSchema(ctx, &schemas.UpdateSchemaInput{
		RegistryName: aws.String("omit-schema-reg"),
		SchemaName:   aws.String("omit-schema"),
		Description:  aws.String(""),
	})
	require.NoError(t, err)

	cleared, err := client.DescribeSchema(ctx, &schemas.DescribeSchemaInput{
		RegistryName: aws.String("omit-schema-reg"),
		SchemaName:   aws.String("omit-schema"),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Description))
	assert.JSONEq(t, firstContent, aws.ToString(cleared.Content))
}
