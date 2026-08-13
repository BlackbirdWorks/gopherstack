package omics_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"
	"github.com/aws/aws-sdk-go-v2/service/omics/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/omics"
)

const wireTestRegion = "us-east-1"

// testReferenceArn is a placeholder genome reference ARN for CreateVariantStore,
// whose Reference field is required on the real wire (CreateVariantStoreInput.
// Reference) but unvalidated against any real reference store by this backend.
const testReferenceArn = "arn:aws:omics:us-east-1:000000000000:referencestore/rs-1/reference/ref-1"

// newTestOmicsClient stands up the real aws-sdk-go-v2 omics client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production. Round-tripping through
// the genuine SDK serializer/deserializer -- rather than decoding the raw
// JSON body with ad-hoc structs -- is what actually proves a response is
// wire-compatible: the SDK deserializer silently ignores any key it doesn't
// recognize, so a raw-JSON assertion against the wrong key can pass even
// though no real client would ever see the value.
func newTestOmicsClient(t *testing.T, h *omics.Handler) *omicssdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(wireTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return omicssdk.NewFromConfig(cfg, func(o *omicssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.APIOptions = append(o.APIOptions, disableAnalyticsHostPrefix)
	})
}

// disableAnalyticsHostPrefix stops the annotation/variant-store family of
// operations (real AWS routes these to an "analytics-omics.<region>..."
// subdomain, see e.g. api_op_GetAnnotationStore.go's
// endpointPrefix_opGetAnnotationStoreMiddleware) from prepending
// "analytics-" onto this test's httptest server host, which has no such DNS
// entry. Real clients pointed at the real AWS endpoint want this prefix;
// this test only cares about the request/response body shape.
func disableAnalyticsHostPrefix(stack *middleware.Stack) error {
	return stack.Initialize.Add(
		middleware.InitializeMiddlewareFunc(
			"DisableAnalyticsHostPrefix",
			func(
				ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler,
			) (middleware.InitializeOutput, middleware.Metadata, error) {
				ctx = smithyhttp.DisableEndpointHostPrefix(ctx, true)

				return next.HandleInitialize(ctx, in)
			},
		),
		middleware.Before,
	)
}

// Test_SDKRoundTrip_GetAnnotationStore_StoreArn proves GetAnnotationStore's
// ARN decodes through the real SDK client. Before the fix the backend tagged
// the field "arn"; the real wire key is "storeArn" (omics@v1.49.5
// deserializers.go:6266), a key the deserializer's switch never matched, so
// the SDK silently left StoreArn nil despite the handler writing a non-empty
// value into the response body.
func Test_SDKRoundTrip_GetAnnotationStore_StoreArn(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateAnnotationStore(t.Context(), &omicssdk.CreateAnnotationStoreInput{
		Name:        aws.String("store-arn-test"),
		StoreFormat: types.StoreFormatVcf,
	})
	require.NoError(t, err)

	got, err := client.GetAnnotationStore(t.Context(), &omicssdk.GetAnnotationStoreInput{
		Name: aws.String("store-arn-test"),
	})
	require.NoError(t, err)
	require.NotNil(t, got.StoreArn, "StoreArn must decode from the real \"storeArn\" wire key")
	require.Contains(t, *got.StoreArn, "annotationStore/store-arn-test")
}

// Test_SDKRoundTrip_GetVariantStore_StoreArn is the VariantStore analogue of
// the AnnotationStore fix above (real wire key "storeArn",
// deserializers.go:11673).
func Test_SDKRoundTrip_GetVariantStore_StoreArn(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateVariantStore(t.Context(), &omicssdk.CreateVariantStoreInput{
		Name:      aws.String("var-store-arn-test"),
		Reference: &types.ReferenceItemMemberReferenceArn{Value: testReferenceArn},
	})
	require.NoError(t, err)

	got, err := client.GetVariantStore(t.Context(), &omicssdk.GetVariantStoreInput{
		Name: aws.String("var-store-arn-test"),
	})
	require.NoError(t, err)
	require.NotNil(t, got.StoreArn, "StoreArn must decode from the real \"storeArn\" wire key")
	require.Contains(t, *got.StoreArn, "variantStore/var-store-arn-test")
}

// Test_SDKRoundTrip_GetAnnotationStoreVersion_VersionArn proves
// GetAnnotationStoreVersion's ARN decodes through the real SDK client (real
// wire key "versionArn", deserializers.go:6564).
func Test_SDKRoundTrip_GetAnnotationStoreVersion_VersionArn(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateAnnotationStore(t.Context(), &omicssdk.CreateAnnotationStoreInput{
		Name:        aws.String("ver-arn-test"),
		StoreFormat: types.StoreFormatVcf,
	})
	require.NoError(t, err)

	_, err = client.CreateAnnotationStoreVersion(t.Context(), &omicssdk.CreateAnnotationStoreVersionInput{
		Name:        aws.String("ver-arn-test"),
		VersionName: aws.String("v1"),
	})
	require.NoError(t, err)

	got, err := client.GetAnnotationStoreVersion(t.Context(), &omicssdk.GetAnnotationStoreVersionInput{
		Name:        aws.String("ver-arn-test"),
		VersionName: aws.String("v1"),
	})
	require.NoError(t, err)
	require.NotNil(t, got.VersionArn, "VersionArn must decode from the real \"versionArn\" wire key")
	require.Contains(t, *got.VersionArn, "ver-arn-test/version/v1")
}

// Test_SDKRoundTrip_StartAnnotationImportJob_JobId proves
// StartAnnotationImportJobOutput.JobId decodes through the real SDK client.
// Before the fix the handler marshaled the domain AnnotationImportJob struct
// directly (tagged "id"); real StartAnnotationImportJobOutput's only member
// is "jobId" (deserializers.go:17434) -- a completely different key from
// GetAnnotationImportJobOutput's "id" (deserializers.go:5954), so no rename
// could have fixed both operations at once.
func Test_SDKRoundTrip_StartAnnotationImportJob_JobId(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateAnnotationStore(t.Context(), &omicssdk.CreateAnnotationStoreInput{
		Name:        aws.String("job-id-test"),
		StoreFormat: types.StoreFormatVcf,
	})
	require.NoError(t, err)

	started, err := client.StartAnnotationImportJob(t.Context(), &omicssdk.StartAnnotationImportJobInput{
		DestinationName: aws.String("job-id-test"),
		RoleArn:         aws.String("arn:aws:iam::000000000000:role/role"),
		Items:           []types.AnnotationImportItemSource{{Source: aws.String("s3://bucket/ann.vcf")}},
	})
	require.NoError(t, err)
	require.NotNil(t, started.JobId, "JobId must decode from the real \"jobId\" wire key")
	require.NotEmpty(t, *started.JobId)

	got, err := client.GetAnnotationImportJob(t.Context(), &omicssdk.GetAnnotationImportJobInput{
		JobId: started.JobId,
	})
	require.NoError(t, err)
	require.NotNil(t, got.Id, "Id must decode from the real \"id\" wire key")
	require.Equal(t, *started.JobId, *got.Id)
}

// Test_SDKRoundTrip_StartVariantImportJob_JobId is the VariantImportJob
// analogue of the AnnotationImportJob fix above -- same split-response root
// cause (StartVariantImportJobOutput "jobId" vs GetVariantImportJobOutput
// "id"), found while reading the whole Start/Get operation pair rather than
// only the two ops the originating bd issue named.
func Test_SDKRoundTrip_StartVariantImportJob_JobId(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateVariantStore(t.Context(), &omicssdk.CreateVariantStoreInput{
		Name:      aws.String("var-job-id-test"),
		Reference: &types.ReferenceItemMemberReferenceArn{Value: testReferenceArn},
	})
	require.NoError(t, err)

	started, err := client.StartVariantImportJob(t.Context(), &omicssdk.StartVariantImportJobInput{
		DestinationName: aws.String("var-job-id-test"),
		RoleArn:         aws.String("arn:aws:iam::000000000000:role/role"),
		Items:           []types.VariantImportItemSource{{Source: aws.String("s3://bucket/var.vcf")}},
	})
	require.NoError(t, err)
	require.NotNil(t, started.JobId, "JobId must decode from the real \"jobId\" wire key")
	require.NotEmpty(t, *started.JobId)

	got, err := client.GetVariantImportJob(t.Context(), &omicssdk.GetVariantImportJobInput{
		JobId: started.JobId,
	})
	require.NoError(t, err)
	require.NotNil(t, got.Id, "Id must decode from the real \"id\" wire key")
	require.Equal(t, *started.JobId, *got.Id)
}

// Test_SDKRoundTrip_GetAnnotationStore_NumVersions proves NumVersions is
// computed live from the store's actual version count rather than left at
// its zero value, real GetAnnotationStoreOutput required field
// (deserializers.go:6225) that this struct previously had no field for at
// all.
func Test_SDKRoundTrip_GetAnnotationStore_NumVersions(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateAnnotationStore(t.Context(), &omicssdk.CreateAnnotationStoreInput{
		Name:        aws.String("num-versions-test"),
		StoreFormat: types.StoreFormatVcf,
	})
	require.NoError(t, err)

	before, err := client.GetAnnotationStore(t.Context(), &omicssdk.GetAnnotationStoreInput{
		Name: aws.String("num-versions-test"),
	})
	require.NoError(t, err)
	require.NotNil(t, before.NumVersions)
	require.Equal(t, int32(0), *before.NumVersions)

	_, err = client.CreateAnnotationStoreVersion(t.Context(), &omicssdk.CreateAnnotationStoreVersionInput{
		Name:        aws.String("num-versions-test"),
		VersionName: aws.String("v1"),
	})
	require.NoError(t, err)

	after, err := client.GetAnnotationStore(t.Context(), &omicssdk.GetAnnotationStoreInput{
		Name: aws.String("num-versions-test"),
	})
	require.NoError(t, err)
	require.NotNil(t, after.NumVersions)
	require.Equal(t, int32(1), *after.NumVersions, "NumVersions must reflect the store's real version count")
}

// Test_SDKRoundTrip_AnnotationImportJob_FormatOptionsAndRunLeftNormalization
// proves FormatOptions/RunLeftNormalization/VersionName round-trip through
// StartAnnotationImportJob -> GetAnnotationImportJob via the real SDK client.
// These were previously entirely absent from the domain model -- a schema
// gap on both the request and response sides, not a dropped wire key.
func Test_SDKRoundTrip_AnnotationImportJob_FormatOptionsAndRunLeftNormalization(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateAnnotationStore(t.Context(), &omicssdk.CreateAnnotationStoreInput{
		Name:        aws.String("format-options-test"),
		StoreFormat: types.StoreFormatVcf,
	})
	require.NoError(t, err)

	started, err := client.StartAnnotationImportJob(t.Context(), &omicssdk.StartAnnotationImportJobInput{
		DestinationName: aws.String("format-options-test"),
		RoleArn:         aws.String("arn:aws:iam::000000000000:role/role"),
		Items:           []types.AnnotationImportItemSource{{Source: aws.String("s3://bucket/ann.vcf")}},
		FormatOptions: &types.FormatOptionsMemberVcfOptions{
			Value: types.VcfOptions{IgnoreFilterField: aws.Bool(true)},
		},
		RunLeftNormalization: true,
		VersionName:          aws.String("v1"),
	})
	require.NoError(t, err)

	got, err := client.GetAnnotationImportJob(t.Context(), &omicssdk.GetAnnotationImportJobInput{
		JobId: started.JobId,
	})
	require.NoError(t, err)

	require.True(t, got.RunLeftNormalization, "RunLeftNormalization must round-trip")
	require.Equal(t, "v1", aws.ToString(got.VersionName), "VersionName must round-trip")

	require.NotNil(t, got.FormatOptions, "FormatOptions must round-trip")
	vcfOpts, ok := got.FormatOptions.(*types.FormatOptionsMemberVcfOptions)
	require.True(t, ok, "FormatOptions must decode as the vcfOptions union member")
	require.NotNil(t, vcfOpts.Value.IgnoreFilterField)
	require.True(t, *vcfOpts.Value.IgnoreFilterField)
}

// Test_SDKRoundTrip_VariantImportJob_RunLeftNormalization is the
// VariantImportJob analogue: RunLeftNormalization round-trips, and unlike
// annotation import jobs there is no FormatOptions or VersionName field
// anywhere in the real API for variant import jobs (StartVariantImportJobInput/
// GetVariantImportJobOutput both lack them) -- verified against the pinned
// SDK, not assumed from the annotation sibling.
func Test_SDKRoundTrip_VariantImportJob_RunLeftNormalization(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateVariantStore(t.Context(), &omicssdk.CreateVariantStoreInput{
		Name:      aws.String("var-run-left-norm-test"),
		Reference: &types.ReferenceItemMemberReferenceArn{Value: testReferenceArn},
	})
	require.NoError(t, err)

	started, err := client.StartVariantImportJob(t.Context(), &omicssdk.StartVariantImportJobInput{
		DestinationName:      aws.String("var-run-left-norm-test"),
		RoleArn:              aws.String("arn:aws:iam::000000000000:role/role"),
		Items:                []types.VariantImportItemSource{{Source: aws.String("s3://bucket/var.vcf")}},
		RunLeftNormalization: true,
	})
	require.NoError(t, err)

	got, err := client.GetVariantImportJob(t.Context(), &omicssdk.GetVariantImportJobInput{
		JobId: started.JobId,
	})
	require.NoError(t, err)
	require.True(t, got.RunLeftNormalization, "RunLeftNormalization must round-trip")
}
