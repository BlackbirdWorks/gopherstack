package integration_test

import (
	"bytes"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	lwtypes "github.com/aws/aws-sdk-go-v2/service/lambdaweb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func lambdaWebSuffix() string { return strconv.FormatInt(time.Now().UnixNano(), 36) }

func createLambdaWebClient(t *testing.T) *lambdaweb.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err, "unable to load SDK config")

	return lambdaweb.NewFromConfig(cfg, func(o *lambdaweb.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func lambdaWebRevisionConfig(bucket string) *lwtypes.RevisionConfig {
	return &lwtypes.RevisionConfig{
		BuildConfig: &lwtypes.BuildConfig{
			CodeConfig: &lwtypes.CodeConfig{
				S3Object: &lwtypes.S3Object{Bucket: aws.String(bucket), Key: aws.String("app.zip")},
			},
			RuntimeConfig: &lwtypes.RuntimeConfig{Runtime: aws.String("nodejs24.x")},
		},
		ServiceConfig: &lwtypes.ServiceConfig{ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/web-role")},
	}
}

// TestIntegration_LambdaWeb_Lifecycle drives the Lambda Web Functions control plane through the real SDK client
// against a versioned S3 bucket and checks the endpoint domain is routed by Host header.
func TestIntegration_LambdaWeb_Lifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createLambdaWebClient(t)
	s3c := createS3Client(t)

	bucket := "lambdaweb-it-" + lambdaWebSuffix()
	_, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = client.CreateWebFunction(ctx, &lambdaweb.CreateWebFunctionInput{
		FunctionName: aws.String("it-unversioned"), RevisionConfig: lambdaWebRevisionConfig(bucket),
	})
	require.Error(t, err, "an unversioned bucket is rejected")
	assert.True(t, hasAPIErrorCode(err, "ValidationException"))

	_, err = s3c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled},
	})
	require.NoError(t, err)

	_, err = s3c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("app.zip"), Body: bytes.NewReader([]byte("PK")),
	})
	require.NoError(t, err)

	name := "it-" + lambdaWebSuffix()
	created, err := client.CreateWebFunction(ctx, &lambdaweb.CreateWebFunctionInput{
		FunctionName: aws.String(name), RevisionConfig: lambdaWebRevisionConfig(bucket),
		EndpointConfig: &lwtypes.EndpointConfig{
			EndpointName: aws.String("prod"), EndpointType: lwtypes.EndpointTypeHomeRegion,
			AuthType: lwtypes.AuthTypeApplicationManaged,
		},
		Tags: map[string]string{"env": "it"},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteWebFunction(cleanupCtx, &lambdaweb.DeleteWebFunctionInput{FunctionName: aws.String(name)})
	})

	assert.Equal(t, lwtypes.EndpointStatePending, created.Endpoint.State)
	domain := aws.ToString(created.Endpoint.DomainName)
	assert.Contains(t, domain, ".lambda-web.us-east-1.on.aws")

	require.Eventually(t, func() bool {
		ep, getErr := client.GetWebFunctionEndpoint(ctx, &lambdaweb.GetWebFunctionEndpointInput{
			FunctionName: aws.String(name), EndpointName: aws.String("prod"),
		})
		rev, revErr := client.GetWebFunctionRevision(ctx, &lambdaweb.GetWebFunctionRevisionInput{
			FunctionName: aws.String(name), RevisionId: created.Revision.RevisionId,
		})

		return getErr == nil && revErr == nil && ep.State == lwtypes.EndpointStateActive &&
			rev.State == lwtypes.RevisionStateActive
	}, 20*time.Second, 100*time.Millisecond)

	rev, err := client.GetWebFunctionRevision(ctx, &lambdaweb.GetWebFunctionRevisionInput{
		FunctionName: aws.String(name), RevisionId: created.Revision.RevisionId,
	})
	require.NoError(t, err)
	assert.NotEmpty(
		t,
		aws.ToString(rev.BuildConfig.CodeConfig.S3Object.VersionId),
		"the revision pins the object version",
	)

	second, err := client.CreateWebFunctionRevision(ctx, &lambdaweb.CreateWebFunctionRevisionInput{
		FunctionName: aws.String(name), BuildConfig: lambdaWebRevisionConfig(bucket).BuildConfig,
		ServiceConfig: lambdaWebRevisionConfig(bucket).ServiceConfig,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		r, getErr := client.GetWebFunctionRevision(ctx, &lambdaweb.GetWebFunctionRevisionInput{
			FunctionName: aws.String(name), RevisionId: second.RevisionId,
		})

		return getErr == nil && r.State == lwtypes.RevisionStateActive
	}, 20*time.Second, 100*time.Millisecond)

	updated, err := client.UpdateWebFunctionEndpoint(ctx, &lambdaweb.UpdateWebFunctionEndpointInput{
		FunctionName: aws.String(name), EndpointName: aws.String("prod"),
		RevisionWeights: []lwtypes.RevisionWeight{
			{RevisionId: created.Revision.RevisionId, Weight: aws.Int32(90)},
			{RevisionId: second.RevisionId, Weight: aws.Int32(10)},
		},
	})
	require.NoError(t, err)
	assert.Len(t, updated.RevisionWeights, 2)

	_, err = client.UpdateWebFunctionEndpoint(ctx, &lambdaweb.UpdateWebFunctionEndpointInput{
		FunctionName: aws.String(name), EndpointName: aws.String("prod"),
		RevisionWeights: []lwtypes.RevisionWeight{{RevisionId: created.Revision.RevisionId, Weight: aws.Int32(60)}},
	})
	require.Error(t, err, "weights must sum to 100")

	tags, err := client.ListTags(ctx, &lambdaweb.ListTagsInput{Resource: created.FunctionArn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "it"}, tags.Tags)

	listed, err := client.ListWebFunctions(ctx, &lambdaweb.ListWebFunctionsInput{})
	require.NoError(t, err)

	var found bool

	for _, f := range listed.Functions {
		found = found || aws.ToString(f.FunctionName) == name
	}

	assert.True(t, found)

	// Data plane: the endpoint domain is routed by Host header; the default stub engine answers 503, an unknown
	// endpoint domain 404 with the platform error type.
	for host, want := range map[string]int{
		domain: http.StatusServiceUnavailable,
		"aaaaaaaaaaaaaaaa.zzz.lambda-web.us-east-1.on.aws": http.StatusNotFound,
	} {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/", nil)
		require.NoError(t, reqErr)

		req.Host = host

		resp, doErr := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		require.NoError(t, doErr)
		_ = resp.Body.Close()

		assert.Equal(t, want, resp.StatusCode, host)
		assert.NotEmpty(t, resp.Header.Get("Lambda-Web-Request-Id"))
	}
}
