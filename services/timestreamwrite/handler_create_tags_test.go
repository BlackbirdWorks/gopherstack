package timestreamwrite_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	twsdk "github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

const rtTestRegion = "us-east-1"

// newTestHandlerAndClient stands up a fresh in-memory timestreamwrite
// backend and a real aws-sdk-go-v2 client against an httptest server running
// its Handler, wired through the same pkgs/service registry/router used in
// production.
func newTestHandlerAndClient(t *testing.T) *twsdk.Client {
	t.Helper()

	backend := timestreamwrite.NewInMemoryBackend()
	h := timestreamwrite.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return twsdk.NewFromConfig(cfg, func(o *twsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every timestreamwrite Create* op
// whose real Input struct accepts Tags (timestreamwrite@v1.38.4:
// api_op_CreateDatabase.go, api_op_CreateTable.go, both `Tags []types.Tag`)
// through the real SDK client and asserts ListTagsForResource sees what was
// supplied at creation (gopherstack-2mwl).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	wantTags := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}

	t.Run("database", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		out, err := client.CreateDatabase(t.Context(), &twsdk.CreateDatabaseInput{
			DatabaseName: aws.String("tagged-db"),
			Tags:         wantTags,
		})
		require.NoError(t, err)

		got, err := client.ListTagsForResource(t.Context(), &twsdk.ListTagsForResourceInput{
			ResourceARN: out.Database.Arn,
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.Tags)
	})

	t.Run("table", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		_, err := client.CreateDatabase(t.Context(), &twsdk.CreateDatabaseInput{
			DatabaseName: aws.String("tagged-tbl-db"),
		})
		require.NoError(t, err)

		out, err := client.CreateTable(t.Context(), &twsdk.CreateTableInput{
			DatabaseName: aws.String("tagged-tbl-db"),
			TableName:    aws.String("tagged-table"),
			Tags:         wantTags,
		})
		require.NoError(t, err)

		got, err := client.ListTagsForResource(t.Context(), &twsdk.ListTagsForResourceInput{
			ResourceARN: out.Table.Arn,
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.Tags)
	})
}

// TestListTables_RealSDKClient_DatabaseNameOptional proves a real-SDK-client
// request omitting DatabaseName entirely succeeds, listing tables across
// every database. ListTablesInput marks no member required, DatabaseName
// included (api_op_ListTables.go, timestreamwrite@v1.38.4) -- the handler
// previously rejected this with a 400 (gopherstack-4ly2).
func TestListTables_RealSDKClient_DatabaseNameOptional(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	for _, db := range []string{"rt-db-a", "rt-db-b"} {
		_, err := client.CreateDatabase(t.Context(), &twsdk.CreateDatabaseInput{
			DatabaseName: aws.String(db),
		})
		require.NoError(t, err)
	}

	_, err := client.CreateTable(t.Context(), &twsdk.CreateTableInput{
		DatabaseName: aws.String("rt-db-a"),
		TableName:    aws.String("t1"),
	})
	require.NoError(t, err)

	_, err = client.CreateTable(t.Context(), &twsdk.CreateTableInput{
		DatabaseName: aws.String("rt-db-b"),
		TableName:    aws.String("t2"),
	})
	require.NoError(t, err)

	out, err := client.ListTables(t.Context(), &twsdk.ListTablesInput{})
	require.NoError(t, err)
	assert.Len(t, out.Tables, 2)
}
