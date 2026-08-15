package accessanalyzer_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	aasdk "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	aatypes "github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// newTestAccessAnalyzerClient stands up the real aws-sdk-go-v2 accessanalyzer
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
func newTestAccessAnalyzerClient(t *testing.T, h *accessanalyzer.Handler) *aasdk.Client {
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

	return aasdk.NewFromConfig(cfg, func(o *aasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateAccessPreview_RealSDKClient drives CreateAccessPreview and
// GetAccessPreview through the real aws-sdk-go-v2 client, whose REST-JSON
// serializer binds the required Configurations member to the lowercase
// "configurations" key with each entry's union type keyed by member name
// (e.g. "s3Bucket"; awsRestjson1_serializeDocumentConfiguration in
// aws-sdk-go-v2/service/accessanalyzer@v1.51.4/serializers.go). The handler
// previously never decoded "configurations" from the request body at all --
// a hand-built map[string]any request asserting only on a 200 status would
// have passed against that bug exactly as it did before this fix.
func TestCreateAccessPreview_RealSDKClient(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
	h := accessanalyzer.NewHandler(b)
	client := newTestAccessAnalyzerClient(t, h)

	analyzer, err := client.CreateAnalyzer(t.Context(), &aasdk.CreateAnalyzerInput{
		AnalyzerName: aws.String("sdk-preview-analyzer"),
		Type:         aatypes.TypeAccount,
	})
	require.NoError(t, err)

	created, err := client.CreateAccessPreview(t.Context(), &aasdk.CreateAccessPreviewInput{
		AnalyzerArn: analyzer.Arn,
		Configurations: map[string]aatypes.Configuration{
			"arn:aws:s3:::sdk-preview-bucket": &aatypes.ConfigurationMemberS3Bucket{
				Value: aatypes.S3BucketConfiguration{
					BucketPolicy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.Id))

	got, err := client.GetAccessPreview(t.Context(), &aasdk.GetAccessPreviewInput{
		AccessPreviewId: created.Id,
		AnalyzerArn:     analyzer.Arn,
	})
	require.NoError(t, err)
	require.NotNil(t, got.AccessPreview)
	require.Contains(t, got.AccessPreview.Configurations, "arn:aws:s3:::sdk-preview-bucket")

	cfg, ok := got.AccessPreview.Configurations["arn:aws:s3:::sdk-preview-bucket"].(*aatypes.ConfigurationMemberS3Bucket)
	require.True(t, ok, "Configurations must round-trip as the submitted s3Bucket union member, not be dropped")
	assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[]}`, aws.ToString(cfg.Value.BucketPolicy))
}

// TestCreateAccessPreview_RealSDKClient_RejectsMultipleConfigurations
// verifies the server-side "exactly one element" rule from
// CreateAccessPreviewInput's Configurations doc comment
// (api_op_CreateAccessPreview.go:39-43); the SDK's own client-side validator
// only requires the map be non-nil, so this path IS reachable through the
// real client.
func TestCreateAccessPreview_RealSDKClient_RejectsMultipleConfigurations(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
	h := accessanalyzer.NewHandler(b)
	client := newTestAccessAnalyzerClient(t, h)

	analyzer, err := client.CreateAnalyzer(t.Context(), &aasdk.CreateAnalyzerInput{
		AnalyzerName: aws.String("sdk-preview-analyzer-multi"),
		Type:         aatypes.TypeAccount,
	})
	require.NoError(t, err)

	_, err = client.CreateAccessPreview(t.Context(), &aasdk.CreateAccessPreviewInput{
		AnalyzerArn: analyzer.Arn,
		Configurations: map[string]aatypes.Configuration{
			"arn:aws:s3:::bucket-a": &aatypes.ConfigurationMemberS3Bucket{Value: aatypes.S3BucketConfiguration{}},
			"arn:aws:s3:::bucket-b": &aatypes.ConfigurationMemberS3Bucket{Value: aatypes.S3BucketConfiguration{}},
		},
	})
	require.Error(t, err)
}

// TestListAccessPreviewFindings_RealClient_FilterByResourceType is the same
// discarded-filter bug as ListFindings/ListFindingsV2
// (TestListFindings_RealClient_FilterByResourceType,
// TestListFindingsV2_RealClient_FilterByResourceType in
// handler_findings_test.go): ListAccessPreviewFindingsInput.Filter
// (map[string]types.Criterion, the real wire "filter" key) was parsed from
// the request body but never passed to the backend at all -- the backend
// method took no filter parameter.
func TestListAccessPreviewFindings_RealClient_FilterByResourceType(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
	h := accessanalyzer.NewHandler(b)
	client := newTestAccessAnalyzerClient(t, h)

	analyzer, err := client.CreateAnalyzer(t.Context(), &aasdk.CreateAnalyzerInput{
		AnalyzerName: aws.String("sdk-preview-filter-analyzer"),
		Type:         aatypes.TypeAccount,
	})
	require.NoError(t, err)

	created, err := client.CreateAccessPreview(t.Context(), &aasdk.CreateAccessPreviewInput{
		AnalyzerArn: analyzer.Arn,
		Configurations: map[string]aatypes.Configuration{
			"arn:aws:s3:::sdk-preview-filter-bucket": &aatypes.ConfigurationMemberS3Bucket{
				Value: aatypes.S3BucketConfiguration{
					BucketPolicy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = b.AddFinding(
		"sdk-preview-filter-analyzer", "AWS::S3::Bucket", "arn:aws:s3:::bucket", nil, nil, nil,
	)
	require.NoError(t, err)
	_, err = b.AddFinding(
		"sdk-preview-filter-analyzer", "AWS::IAM::Role", "arn:aws:iam::000000000000:role/r", nil, nil, nil,
	)
	require.NoError(t, err)

	out, err := client.ListAccessPreviewFindings(t.Context(), &aasdk.ListAccessPreviewFindingsInput{
		AccessPreviewId: created.Id,
		AnalyzerArn:     analyzer.Arn,
		Filter: map[string]aatypes.Criterion{
			"resourceType": {Eq: []string{"AWS::IAM::Role"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Findings, 1)
	assert.Equal(t, "AWS::IAM::Role", string(out.Findings[0].ResourceType))
}
