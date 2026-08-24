package s3tables_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3tablessdk "github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/s3tables"
)

const rtTestRegion = "us-east-1"

// newTestS3TablesClient stands up the real aws-sdk-go-v2 S3 Tables client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production. Round-tripping
// through the genuine SDK deserializer (rather than string-matching the raw
// JSON body) is what proves a response is wire-compatible: unrecognized
// keys are skipped silently rather than erroring, so a plausible-looking
// response can still decode to an empty/nil field.
func newTestS3TablesClient(t *testing.T, h *s3tables.Handler) *s3tablessdk.Client {
	t.Helper()

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

	return s3tablessdk.NewFromConfig(cfg, func(o *s3tablessdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestSDKRoundTrip_NamespaceIDFix covers a bug found by diffing
// gopherstack's s3tables JSON keys against the pinned SDK's deserializer
// (s3tables@v1.18.4): NamespaceSummary/GetNamespaceOutput's real
// "namespaceId" member was never emitted at all, even though the backend
// computes and stores a NamespaceID at creation time. A real client always
// decoded NamespaceId as nil.
func TestSDKRoundTrip_NamespaceIDFix(t *testing.T) {
	t.Parallel()

	backend := s3tables.NewInMemoryBackend("000000000000", rtTestRegion)
	h := s3tables.NewHandler(backend)
	client := newTestS3TablesClient(t, h)
	ctx := t.Context()

	bucket, err := backend.CreateTableBucket("rt-bucket", s3tables.CreateTableBucketOptions{})
	require.NoError(t, err)

	_, err = client.CreateNamespace(ctx, &s3tablessdk.CreateNamespaceInput{
		TableBucketARN: aws.String(bucket.ARN),
		Namespace:      []string{"rt_namespace"},
	})
	require.NoError(t, err)

	getOut, err := client.GetNamespace(ctx, &s3tablessdk.GetNamespaceInput{
		TableBucketARN: aws.String(bucket.ARN),
		Namespace:      aws.String("rt_namespace"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(getOut.NamespaceId))

	listOut, err := client.ListNamespaces(ctx, &s3tablessdk.ListNamespacesInput{
		TableBucketARN: aws.String(bucket.ARN),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Namespaces, 1)
	assert.NotEmpty(t, aws.ToString(listOut.Namespaces[0].NamespaceId))
	assert.Equal(t, aws.ToString(getOut.NamespaceId), aws.ToString(listOut.Namespaces[0].NamespaceId))
}

// TestSDKRoundTrip_TableBucketIDFix covers gopherstack-wla0: GetTable and
// ListTables wrote the wire key "tableBucketARN", but the real
// GetTableOutput/TableSummary deserializers
// (awsRestjson1_deserializeOpDocumentGetTableOutput/
// ...DocumentTableSummary in s3tables@v1.18.4's deserializers.go) have no
// such member -- only "tableBucketId", a genuinely different,
// system-assigned value. GetNamespace/ListNamespaces had the identical bug
// (GetNamespaceOutput/NamespaceSummary also lack tableBucketARN). Before the
// fix, every one of these four real client calls decoded TableBucketId as
// nil; this test fails against the unfixed handler with exactly that
// symptom (asserted via hand-revert, see PARITY.md).
func TestSDKRoundTrip_TableBucketIDFix(t *testing.T) {
	t.Parallel()

	backend := s3tables.NewInMemoryBackend("000000000000", rtTestRegion)
	h := s3tables.NewHandler(backend)
	client := newTestS3TablesClient(t, h)
	ctx := t.Context()

	bucketOut, err := client.CreateTableBucket(ctx, &s3tablessdk.CreateTableBucketInput{
		Name: aws.String("wla0-bucket"),
	})
	require.NoError(t, err)

	getBucketOut, err := client.GetTableBucket(ctx, &s3tablessdk.GetTableBucketInput{
		TableBucketARN: bucketOut.Arn,
	})
	require.NoError(t, err)
	wantBucketID := aws.ToString(getBucketOut.TableBucketId)
	assert.NotEmpty(t, wantBucketID)

	listBucketsOut, err := client.ListTableBuckets(ctx, &s3tablessdk.ListTableBucketsInput{})
	require.NoError(t, err)
	require.Len(t, listBucketsOut.TableBuckets, 1)
	assert.Equal(t, wantBucketID, aws.ToString(listBucketsOut.TableBuckets[0].TableBucketId))

	_, err = client.CreateNamespace(ctx, &s3tablessdk.CreateNamespaceInput{
		TableBucketARN: bucketOut.Arn,
		Namespace:      []string{"wla0_namespace"},
	})
	require.NoError(t, err)

	getNsOut, err := client.GetNamespace(ctx, &s3tablessdk.GetNamespaceInput{
		TableBucketARN: bucketOut.Arn,
		Namespace:      aws.String("wla0_namespace"),
	})
	require.NoError(t, err)
	assert.Equal(t, wantBucketID, aws.ToString(getNsOut.TableBucketId))

	listNsOut, err := client.ListNamespaces(ctx, &s3tablessdk.ListNamespacesInput{
		TableBucketARN: bucketOut.Arn,
	})
	require.NoError(t, err)
	require.Len(t, listNsOut.Namespaces, 1)
	assert.Equal(t, wantBucketID, aws.ToString(listNsOut.Namespaces[0].TableBucketId))

	_, err = client.CreateTable(ctx, &s3tablessdk.CreateTableInput{
		TableBucketARN: bucketOut.Arn,
		Namespace:      aws.String("wla0_namespace"),
		Name:           aws.String("wla0_table"),
		Format:         "ICEBERG",
	})
	require.NoError(t, err)

	getTableOut, err := client.GetTable(ctx, &s3tablessdk.GetTableInput{
		TableBucketARN: bucketOut.Arn,
		Namespace:      aws.String("wla0_namespace"),
		Name:           aws.String("wla0_table"),
	})
	require.NoError(t, err)
	assert.Equal(t, wantBucketID, aws.ToString(getTableOut.TableBucketId))

	listTablesOut, err := client.ListTables(ctx, &s3tablessdk.ListTablesInput{
		TableBucketARN: bucketOut.Arn,
	})
	require.NoError(t, err)
	require.Len(t, listTablesOut.Tables, 1)
	assert.Equal(t, wantBucketID, aws.ToString(listTablesOut.Tables[0].TableBucketId))
}
