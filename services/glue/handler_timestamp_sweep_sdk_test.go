package glue_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestSDKRoundTrip_DescribeInboundIntegrationsPagination proves
// DescribeInboundIntegrations now honors MaxRecords/Marker like its sibling
// DescribeIntegrations (gopherstack-7f5k): before the fix, MaxRecords/Marker
// were declared on the input struct but never read by the handler, so every
// call returned every stored integration in one unbounded response and
// Marker was always empty.
func TestSDKRoundTrip_DescribeInboundIntegrationsPagination(t *testing.T) {
	t.Parallel()

	tc := paginationCase{
		name: "describe inbound integrations",
		want: 3,
		seed: func(t *testing.T, b *glue.InMemoryBackend) {
			t.Helper()
			for _, n := range []string{"ig1", "ig2", "ig3"} {
				_, err := b.CreateIntegration(n, "arn:aws:rds:us-east-1:000000000000:db/"+n,
					"arn:aws:redshift:us-east-1:000000000000:cluster/"+n, nil)
				require.NoError(t, err)
			}
		},
		list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
			t.Helper()
			out, err := c.DescribeInboundIntegrations(
				ctx, &gluesdk.DescribeInboundIntegrationsInput{MaxRecords: aws.Int32(pageSize), Marker: token},
			)
			require.NoError(t, err)

			return len(out.InboundIntegrations), out.Marker
		},
	}

	runPaginationCase(t, tc)
}

// TestSDKRoundTrip_DescribeInboundIntegrationsFilters locks in the
// IntegrationArn and TargetArn filters, neither of which the original
// handler applied: IntegrationArn was compared but TargetArn was declared on
// the input struct and never read at all.
func TestSDKRoundTrip_DescribeInboundIntegrationsFilters(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	ig1, err := backend.CreateIntegration("ig1", "arn:aws:rds:us-east-1:000000000000:db/ig1",
		"arn:aws:redshift:us-east-1:000000000000:cluster/shared", nil)
	require.NoError(t, err)
	_, err = backend.CreateIntegration("ig2", "arn:aws:rds:us-east-1:000000000000:db/ig2",
		"arn:aws:redshift:us-east-1:000000000000:cluster/other", nil)
	require.NoError(t, err)

	client := newTestGlueClient(t, glue.NewHandler(backend))
	ctx := t.Context()

	t.Run("filters by IntegrationArn", func(t *testing.T) {
		t.Parallel()

		out, callErr := client.DescribeInboundIntegrations(ctx, &gluesdk.DescribeInboundIntegrationsInput{
			IntegrationArn: aws.String(ig1.IntegrationArn),
		})
		require.NoError(t, callErr)
		require.Len(t, out.InboundIntegrations, 1)
		assert.Equal(t, ig1.IntegrationArn, *out.InboundIntegrations[0].IntegrationArn)
	})

	t.Run("filters by TargetArn", func(t *testing.T) {
		t.Parallel()

		out, callErr := client.DescribeInboundIntegrations(ctx, &gluesdk.DescribeInboundIntegrationsInput{
			TargetArn: aws.String("arn:aws:redshift:us-east-1:000000000000:cluster/shared"),
		})
		require.NoError(t, callErr)
		require.Len(t, out.InboundIntegrations, 1)
		assert.Equal(t, ig1.IntegrationArn, *out.InboundIntegrations[0].IntegrationArn)
	})
}

// TestSDKRoundTrip_DescribeInboundIntegrationsCreateTime proves
// InboundIntegration.CreateTime decodes as the real unixTimestamp JSON
// Number (glue@v1.152.0 deserializers.go's deserializeDocumentInboundIntegration:
// "expected IntegrationTimestamp to be a JSON Number, got %T instead")
// rather than the RFC3339 string the raw backend Integration.CreatedAt
// (time.Time, plain json tag) marshaled before the fix -- a raw-body/200
// test cannot catch this, only a typed client decode can.
func TestSDKRoundTrip_DescribeInboundIntegrationsCreateTime(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	before := time.Now().Add(-time.Minute)

	_, err := backend.CreateIntegration("ig1", "arn:aws:rds:us-east-1:000000000000:db/ig1",
		"arn:aws:redshift:us-east-1:000000000000:cluster/ig1", nil)
	require.NoError(t, err)

	client := newTestGlueClient(t, glue.NewHandler(backend))

	out, err := client.DescribeInboundIntegrations(t.Context(), &gluesdk.DescribeInboundIntegrationsInput{})
	require.NoError(t, err)
	require.Len(t, out.InboundIntegrations, 1)

	got := out.InboundIntegrations[0]
	require.NotNil(t, got.IntegrationArn)
	assert.Contains(t, *got.IntegrationArn, "ig1")

	require.NotNil(t, got.CreateTime)
	assert.True(t, got.CreateTime.After(before), "CreateTime must decode to a recent, real time.Time")
	assert.Equal(t, types.IntegrationStatus("CREATING"), got.Status)
}

// timestampDecodeCase seeds a fresh backend and drives one real SDK op,
// asserting the typed response decoded successfully and carries the members
// the pre-fix handler either mistyped (CreatedTime/UpdatedTime as JSON
// numbers where the Schema Registry wire shape requires strings) or dropped
// entirely (GetSchema's LatestSchemaVersion/NextSchemaVersion/SchemaCheckpoint).
type timestampDecodeCase struct {
	run  func(t *testing.T, ctx context.Context, c *gluesdk.Client, b *glue.InMemoryBackend)
	name string
}

// TestSDKRoundTrip_SchemaRegistryTimestamps covers the five handler_schemas.go
// ops fixed under gopherstack-7f5k: GetRegistry, GetSchema, ListSchemas,
// ListSchemaVersions and GetSchemaVersion all emitted CreatedTime/UpdatedTime
// as numeric epoch floats before the fix. Glue's Schema Registry declares
// these fields as *string (glue@v1.152.0 deserializers.go, every one of these
// ops' own switch: "expected CreatedTimestamp to be of type string"), so a
// real client rejected the whole response outright -- the same class of bug
// ListRegistries was fixed for in the prior pass.
func TestSDKRoundTrip_SchemaRegistryTimestamps(t *testing.T) {
	t.Parallel()

	cases := []timestampDecodeCase{
		{
			name: "get registry",
			run: func(t *testing.T, ctx context.Context, c *gluesdk.Client, b *glue.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateRegistry("reg1", "a registry", nil)
				require.NoError(t, err)

				out, err := c.GetRegistry(ctx, &gluesdk.GetRegistryInput{
					RegistryId: &types.RegistryId{RegistryName: aws.String("reg1")},
				})
				require.NoError(t, err)

				require.NotNil(t, out.CreatedTime)
				require.NotNil(t, out.UpdatedTime)
				assertRFC3339(t, *out.CreatedTime)
				assertRFC3339(t, *out.UpdatedTime)
				assert.Equal(t, "reg1", *out.RegistryName)
			},
		},
		{
			name: "get schema",
			run: func(t *testing.T, ctx context.Context, c *gluesdk.Client, b *glue.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateRegistry("reg1", "", nil)
				require.NoError(t, err)
				_, _, err = b.CreateSchema("reg1", "sch1", "AVRO", "NONE", "",
					`{"type":"record","name":"User","fields":[]}`, nil)
				require.NoError(t, err)

				out, err := c.GetSchema(ctx, &gluesdk.GetSchemaInput{
					SchemaId: &types.SchemaId{RegistryName: aws.String("reg1"), SchemaName: aws.String("sch1")},
				})
				require.NoError(t, err)

				require.NotNil(t, out.CreatedTime)
				require.NotNil(t, out.UpdatedTime)
				assertRFC3339(t, *out.CreatedTime)
				assertRFC3339(t, *out.UpdatedTime)

				require.NotNil(t, out.LatestSchemaVersion)
				require.NotNil(t, out.NextSchemaVersion)
				require.NotNil(t, out.SchemaCheckpoint)
				assert.Equal(t, int64(1), *out.LatestSchemaVersion)
				assert.Equal(t, int64(2), *out.NextSchemaVersion)
				assert.Equal(t, int64(1), *out.SchemaCheckpoint)
			},
		},
		{
			name: "list schemas",
			run: func(t *testing.T, ctx context.Context, c *gluesdk.Client, b *glue.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateRegistry("reg1", "", nil)
				require.NoError(t, err)
				_, _, err = b.CreateSchema("reg1", "sch1", "AVRO", "NONE", "", "", nil)
				require.NoError(t, err)
				_, _, err = b.CreateSchema("reg1", "sch2", "AVRO", "NONE", "", "", nil)
				require.NoError(t, err)

				out, err := c.ListSchemas(ctx, &gluesdk.ListSchemasInput{
					RegistryId: &types.RegistryId{RegistryName: aws.String("reg1")},
				})
				require.NoError(t, err)
				require.Len(t, out.Schemas, 2)

				for _, s := range out.Schemas {
					require.NotNil(t, s.CreatedTime)
					assertRFC3339(t, *s.CreatedTime)
				}
			},
		},
		{
			name: "list schema versions",
			run: func(t *testing.T, ctx context.Context, c *gluesdk.Client, b *glue.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateRegistry("reg1", "", nil)
				require.NoError(t, err)
				_, _, err = b.CreateSchema("reg1", "sch1", "AVRO", "NONE", "",
					`{"type":"record","name":"User","fields":[]}`, nil)
				require.NoError(t, err)
				_, err = b.RegisterSchemaVersion("reg1", "sch1", `{"type":"record","name":"User2","fields":[]}`)
				require.NoError(t, err)

				out, err := c.ListSchemaVersions(ctx, &gluesdk.ListSchemaVersionsInput{
					SchemaId: &types.SchemaId{RegistryName: aws.String("reg1"), SchemaName: aws.String("sch1")},
				})
				require.NoError(t, err)
				require.Len(t, out.Schemas, 2)

				for _, v := range out.Schemas {
					require.NotNil(t, v.CreatedTime)
					assertRFC3339(t, *v.CreatedTime)
				}
			},
		},
		{
			name: "get schema version",
			run: func(t *testing.T, ctx context.Context, c *gluesdk.Client, b *glue.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateRegistry("reg1", "", nil)
				require.NoError(t, err)
				_, _, err = b.CreateSchema("reg1", "sch1", "AVRO", "NONE", "",
					`{"type":"record","name":"User","fields":[]}`, nil)
				require.NoError(t, err)

				out, err := c.GetSchemaVersion(ctx, &gluesdk.GetSchemaVersionInput{
					SchemaId: &types.SchemaId{RegistryName: aws.String("reg1"), SchemaName: aws.String("sch1")},
					SchemaVersionNumber: &types.SchemaVersionNumber{
						VersionNumber: aws.Int64(1),
					},
				})
				require.NoError(t, err)

				require.NotNil(t, out.CreatedTime)
				assertRFC3339(t, *out.CreatedTime)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := glue.NewInMemoryBackend(testAccountID, testRegion)
			client := newTestGlueClient(t, glue.NewHandler(backend))

			tc.run(t, t.Context(), client, backend)
		})
	}
}

// assertRFC3339 fails the test unless s parses as the RFC3339 timestamp
// string the real Schema Registry wire shape requires.
func assertRFC3339(t *testing.T, s string) {
	t.Helper()

	_, err := time.Parse(time.RFC3339, s)
	assert.NoError(t, err, "CreatedTime/UpdatedTime must be an RFC3339 string, got %q", s)
}
