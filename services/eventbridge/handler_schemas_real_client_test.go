package eventbridge_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/schemas"
	schemastypes "github.com/aws/aws-sdk-go-v2/service/schemas/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// newTestSchemasClient stands up the real aws-sdk-go-v2 schemas client
// against an httptest server running eventbridge's Handler, wired through
// the same pkgs/service registry/router used in production. schemas is a
// separate, REST-JSON1 SDK client from EventBridge's own JSON-RPC 1.1 one:
// it sends no X-Amz-Target header at all and instead POSTs/GETs/PUTs/DELETEs
// literal paths like "/v1/registries/name/{RegistryName}" (see
// handler_schemas_rest.go's schemasRESTContentType doc comment for the
// serializers.go citation). Routing this through RouteMatcher, rather than
// calling h.Handler()(c) directly, is the point -- RouteMatcher is what a
// real client's request has to pass before dispatch is even reached
// (gopherstack-92ft).
func newTestSchemasClient(t *testing.T, h *eventbridge.Handler) *schemas.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(config.DefaultRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return schemas.NewFromConfig(cfg, func(o *schemas.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func newTestSchemasHandler(t *testing.T) *eventbridge.Handler {
	t.Helper()

	return eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
}

// TestSchemasRegistry_RealSDKClient drives the full Registry CRUD family
// through the real schemas client. Before gopherstack-92ft's fix,
// RouteMatcher required an X-Amz-Target header under one of three fixed
// prefixes; the real REST-JSON1 client sends no such header at all, so
// every real call 404'd (fell through to standard Echo routing) before ever
// reaching this handler.
func TestSchemasRegistry_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := newTestSchemasHandler(t)
	client := newTestSchemasClient(t, h)

	created, err := client.CreateRegistry(t.Context(), &schemas.CreateRegistryInput{
		RegistryName: aws.String("sdk-registry"),
		Description:  aws.String("created via real sdk"),
		Tags:         map[string]string{"env": "test"},
	})
	require.NoError(t, err)
	assert.Equal(t, "sdk-registry", aws.ToString(created.RegistryName))
	assert.Equal(t, "created via real sdk", aws.ToString(created.Description))
	assert.Equal(t, "test", created.Tags["env"])
	assert.NotEmpty(t, aws.ToString(created.RegistryArn))

	got, err := client.DescribeRegistry(t.Context(), &schemas.DescribeRegistryInput{
		RegistryName: aws.String("sdk-registry"),
	})
	require.NoError(t, err)
	assert.Equal(t, "sdk-registry", aws.ToString(got.RegistryName))

	listed, err := client.ListRegistries(t.Context(), &schemas.ListRegistriesInput{
		RegistryNamePrefix: aws.String("sdk-"),
	})
	require.NoError(t, err)
	require.Len(t, listed.Registries, 1)
	assert.Equal(t, "sdk-registry", aws.ToString(listed.Registries[0].RegistryName))

	updated, err := client.UpdateRegistry(t.Context(), &schemas.UpdateRegistryInput{
		RegistryName: aws.String("sdk-registry"),
		Description:  aws.String("updated via real sdk"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated via real sdk", aws.ToString(updated.Description))

	_, err = client.DeleteRegistry(t.Context(), &schemas.DeleteRegistryInput{
		RegistryName: aws.String("sdk-registry"),
	})
	require.NoError(t, err)

	_, err = client.DescribeRegistry(t.Context(), &schemas.DescribeRegistryInput{
		RegistryName: aws.String("sdk-registry"),
	})
	require.Error(t, err)

	var nf *schemastypes.NotFoundException
	assert.ErrorAs(t, err, &nf)
}

// TestSchemasSchema_RealSDKClient drives the full Schema CRUD + version
// family through the real schemas client, including SearchSchemas' nested
// per-version response shape (searchSchemaSummaryRESTOutput).
func TestSchemasSchema_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := newTestSchemasHandler(t)
	client := newTestSchemasClient(t, h)

	_, err := client.CreateRegistry(t.Context(), &schemas.CreateRegistryInput{
		RegistryName: aws.String("sdk-schema-registry"),
	})
	require.NoError(t, err)

	created, err := client.CreateSchema(t.Context(), &schemas.CreateSchemaInput{
		RegistryName: aws.String("sdk-schema-registry"),
		SchemaName:   aws.String("sdk-schema"),
		Type:         schemastypes.TypeOpenApi3,
		Content:      aws.String(`{"openapi":"3.0.0"}`),
		Tags:         map[string]string{"team": "core"},
	})
	require.NoError(t, err)
	assert.Equal(t, "sdk-schema", aws.ToString(created.SchemaName))
	assert.Equal(t, "1", aws.ToString(created.SchemaVersion))
	assert.Equal(t, "core", created.Tags["team"])

	described, err := client.DescribeSchema(t.Context(), &schemas.DescribeSchemaInput{
		RegistryName: aws.String("sdk-schema-registry"),
		SchemaName:   aws.String("sdk-schema"),
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"openapi":"3.0.0"}`, aws.ToString(described.Content))

	listed, err := client.ListSchemas(t.Context(), &schemas.ListSchemasInput{
		RegistryName: aws.String("sdk-schema-registry"),
	})
	require.NoError(t, err)
	require.Len(t, listed.Schemas, 1)
	assert.Equal(t, "sdk-schema", aws.ToString(listed.Schemas[0].SchemaName))
	assert.Equal(t, int64(1), aws.ToInt64(listed.Schemas[0].VersionCount))

	updated, err := client.UpdateSchema(t.Context(), &schemas.UpdateSchemaInput{
		RegistryName: aws.String("sdk-schema-registry"),
		SchemaName:   aws.String("sdk-schema"),
		Content:      aws.String(`{"openapi":"3.0.1"}`),
	})
	require.NoError(t, err)
	assert.Equal(t, "2", aws.ToString(updated.SchemaVersion))

	versions, err := client.ListSchemaVersions(t.Context(), &schemas.ListSchemaVersionsInput{
		RegistryName: aws.String("sdk-schema-registry"),
		SchemaName:   aws.String("sdk-schema"),
	})
	require.NoError(t, err)
	require.Len(t, versions.SchemaVersions, 2)

	searched, err := client.SearchSchemas(t.Context(), &schemas.SearchSchemasInput{
		RegistryName: aws.String("sdk-schema-registry"),
		Keywords:     aws.String("sdk-schema"),
	})
	require.NoError(t, err)
	require.Len(t, searched.Schemas, 1)
	assert.Equal(t, "sdk-schema", aws.ToString(searched.Schemas[0].SchemaName))
	assert.Equal(t, "sdk-schema-registry", aws.ToString(searched.Schemas[0].RegistryName))
	require.Len(t, searched.Schemas[0].SchemaVersions, 2)

	_, err = client.DeleteSchemaVersion(t.Context(), &schemas.DeleteSchemaVersionInput{
		RegistryName:  aws.String("sdk-schema-registry"),
		SchemaName:    aws.String("sdk-schema"),
		SchemaVersion: aws.String("1"),
	})
	require.NoError(t, err)

	_, err = client.DeleteSchema(t.Context(), &schemas.DeleteSchemaInput{
		RegistryName: aws.String("sdk-schema-registry"),
		SchemaName:   aws.String("sdk-schema"),
	})
	require.NoError(t, err)
}

// TestSchemasCodeBinding_RealSDKClient drives PutCodeBinding,
// DescribeCodeBinding, and GetCodeBindingSource through the real client.
// GetCodeBindingSource is the case that most directly exercises the
// raw-bytes-not-JSON wire fix: GetCodeBindingSourceOutput.Body is populated
// straight from the HTTP response body by the real deserializer.
func TestSchemasCodeBinding_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := newTestSchemasHandler(t)
	client := newTestSchemasClient(t, h)

	_, err := client.CreateRegistry(t.Context(), &schemas.CreateRegistryInput{
		RegistryName: aws.String("sdk-cb-registry"),
	})
	require.NoError(t, err)

	_, err = client.CreateSchema(t.Context(), &schemas.CreateSchemaInput{
		RegistryName: aws.String("sdk-cb-registry"),
		SchemaName:   aws.String("sdk-cb-schema"),
		Type:         schemastypes.TypeOpenApi3,
		Content:      aws.String(`{"openapi":"3.0.0"}`),
	})
	require.NoError(t, err)

	put, err := client.PutCodeBinding(t.Context(), &schemas.PutCodeBindingInput{
		RegistryName: aws.String("sdk-cb-registry"),
		SchemaName:   aws.String("sdk-cb-schema"),
		Language:     aws.String("Go"),
	})
	require.NoError(t, err)
	assert.Equal(t, schemastypes.CodeGenerationStatusCreateComplete, put.Status)

	described, err := client.DescribeCodeBinding(t.Context(), &schemas.DescribeCodeBindingInput{
		RegistryName: aws.String("sdk-cb-registry"),
		SchemaName:   aws.String("sdk-cb-schema"),
		Language:     aws.String("Go"),
	})
	require.NoError(t, err)
	assert.Equal(t, schemastypes.CodeGenerationStatusCreateComplete, described.Status)

	source, err := client.GetCodeBindingSource(t.Context(), &schemas.GetCodeBindingSourceInput{
		RegistryName: aws.String("sdk-cb-registry"),
		SchemaName:   aws.String("sdk-cb-schema"),
		Language:     aws.String("Go"),
	})
	require.NoError(t, err)
	assert.Contains(t, string(source.Body), "sdk-cb-registry")
}

// TestSchemasGetDiscoveredSchema_RealSDKClient drives GetDiscoveredSchema,
// the one op that carries no path parameters at all (POST /v1/discover).
func TestSchemasGetDiscoveredSchema_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := newTestSchemasHandler(t)
	client := newTestSchemasClient(t, h)

	out, err := client.GetDiscoveredSchema(t.Context(), &schemas.GetDiscoveredSchemaInput{
		Events: []string{`{"foo":"bar"}`},
		Type:   schemastypes.TypeOpenApi3,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.Content))
}
