package omics_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"
	"github.com/aws/aws-sdk-go-v2/service/omics/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
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

	_, err = client.CreateAnnotationStoreVersion(
		t.Context(),
		&omicssdk.CreateAnnotationStoreVersionInput{
			Name:        aws.String("ver-arn-test"),
			VersionName: aws.String("v1"),
		},
	)
	require.NoError(t, err)

	got, err := client.GetAnnotationStoreVersion(
		t.Context(),
		&omicssdk.GetAnnotationStoreVersionInput{
			Name:        aws.String("ver-arn-test"),
			VersionName: aws.String("v1"),
		},
	)
	require.NoError(t, err)
	require.NotNil(
		t,
		got.VersionArn,
		"VersionArn must decode from the real \"versionArn\" wire key",
	)
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

	started, err := client.StartAnnotationImportJob(
		t.Context(),
		&omicssdk.StartAnnotationImportJobInput{
			DestinationName: aws.String("job-id-test"),
			RoleArn:         aws.String("arn:aws:iam::000000000000:role/role"),
			Items: []types.AnnotationImportItemSource{
				{Source: aws.String("s3://bucket/ann.vcf")},
			},
		},
	)
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
		Items: []types.VariantImportItemSource{
			{Source: aws.String("s3://bucket/var.vcf")},
		},
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

	_, err = client.CreateAnnotationStoreVersion(
		t.Context(),
		&omicssdk.CreateAnnotationStoreVersionInput{
			Name:        aws.String("num-versions-test"),
			VersionName: aws.String("v1"),
		},
	)
	require.NoError(t, err)

	after, err := client.GetAnnotationStore(t.Context(), &omicssdk.GetAnnotationStoreInput{
		Name: aws.String("num-versions-test"),
	})
	require.NoError(t, err)
	require.NotNil(t, after.NumVersions)
	require.Equal(
		t,
		int32(1),
		*after.NumVersions,
		"NumVersions must reflect the store's real version count",
	)
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

	started, err := client.StartAnnotationImportJob(
		t.Context(),
		&omicssdk.StartAnnotationImportJobInput{
			DestinationName: aws.String("format-options-test"),
			RoleArn:         aws.String("arn:aws:iam::000000000000:role/role"),
			Items: []types.AnnotationImportItemSource{
				{Source: aws.String("s3://bucket/ann.vcf")},
			},
			FormatOptions: &types.FormatOptionsMemberVcfOptions{
				Value: types.VcfOptions{IgnoreFilterField: aws.Bool(true)},
			},
			RunLeftNormalization: true,
			VersionName:          aws.String("v1"),
		},
	)
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
		DestinationName: aws.String("var-run-left-norm-test"),
		RoleArn:         aws.String("arn:aws:iam::000000000000:role/role"),
		Items: []types.VariantImportItemSource{
			{Source: aws.String("s3://bucket/var.vcf")},
		},
		RunLeftNormalization: true,
	})
	require.NoError(t, err)

	got, err := client.GetVariantImportJob(t.Context(), &omicssdk.GetVariantImportJobInput{
		JobId: started.JobId,
	})
	require.NoError(t, err)
	require.True(t, got.RunLeftNormalization, "RunLeftNormalization must round-trip")
}

// Test_SDKRoundTrip_StatusMessage proves StatusMessage decodes through the
// real SDK client on all three store Get outputs where it is a required
// member (GetAnnotationStoreOutput deserializers.go:6280,
// GetVariantStoreOutput deserializers.go, GetAnnotationStoreVersionOutput
// deserializers.go) -- gopherstack-7s8r: the field was entirely absent from
// this backend's response, so before the fix the SDK left it nil rather
// than a zero-value pointer, since the key was never on the wire at all.
func Test_SDKRoundTrip_StatusMessage(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateAnnotationStore(t.Context(), &omicssdk.CreateAnnotationStoreInput{
		Name:        aws.String("status-message-test"),
		StoreFormat: types.StoreFormatVcf,
	})
	require.NoError(t, err)

	annStore, err := client.GetAnnotationStore(t.Context(), &omicssdk.GetAnnotationStoreInput{
		Name: aws.String("status-message-test"),
	})
	require.NoError(t, err)
	require.NotNil(
		t,
		annStore.StatusMessage,
		"GetAnnotationStoreOutput.StatusMessage must decode (real required member)",
	)

	_, err = client.CreateAnnotationStoreVersion(
		t.Context(),
		&omicssdk.CreateAnnotationStoreVersionInput{
			Name:        aws.String("status-message-test"),
			VersionName: aws.String("v1"),
		},
	)
	require.NoError(t, err)

	version, err := client.GetAnnotationStoreVersion(
		t.Context(),
		&omicssdk.GetAnnotationStoreVersionInput{
			Name:        aws.String("status-message-test"),
			VersionName: aws.String("v1"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, version.StatusMessage,
		"GetAnnotationStoreVersionOutput.StatusMessage must decode (real required member)")

	_, err = client.CreateVariantStore(t.Context(), &omicssdk.CreateVariantStoreInput{
		Name:      aws.String("var-status-message-test"),
		Reference: &types.ReferenceItemMemberReferenceArn{Value: testReferenceArn},
	})
	require.NoError(t, err)

	varStore, err := client.GetVariantStore(t.Context(), &omicssdk.GetVariantStoreInput{
		Name: aws.String("var-status-message-test"),
	})
	require.NoError(t, err)
	require.NotNil(
		t,
		varStore.StatusMessage,
		"GetVariantStoreOutput.StatusMessage must decode (real required member)",
	)
}

// Test_SDKRoundTrip_AnnotationImportJob_ItemDetail proves
// GetAnnotationImportJobOutput.Items decodes as the real
// AnnotationImportItemDetail shape (JobStatus + Source, types.go:75-89),
// not the ItemSource shape (Source only) StartAnnotationImportJobInput.Items
// uses. gopherstack-7s8r: this backend previously used one shared Go type
// for both, so JobStatus -- required on every item -- was absent from
// every Get/List response.
func Test_SDKRoundTrip_AnnotationImportJob_ItemDetail(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateAnnotationStore(t.Context(), &omicssdk.CreateAnnotationStoreInput{
		Name:        aws.String("item-detail-test"),
		StoreFormat: types.StoreFormatVcf,
	})
	require.NoError(t, err)

	started, err := client.StartAnnotationImportJob(
		t.Context(),
		&omicssdk.StartAnnotationImportJobInput{
			DestinationName: aws.String("item-detail-test"),
			RoleArn:         aws.String("arn:aws:iam::000000000000:role/role"),
			Items: []types.AnnotationImportItemSource{
				{Source: aws.String("s3://bucket/ann.vcf")},
			},
		},
	)
	require.NoError(t, err)

	got, err := client.GetAnnotationImportJob(t.Context(), &omicssdk.GetAnnotationImportJobInput{
		JobId: started.JobId,
	})
	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	assert.Equal(t, "s3://bucket/ann.vcf", aws.ToString(got.Items[0].Source))
	assert.Equal(
		t,
		types.JobStatusCompleted,
		got.Items[0].JobStatus,
		"Items[].JobStatus must decode (real required member absent from the old shared-with-Start shape)",
	)
}

// Test_SDKRoundTrip_VariantImportJob_ItemDetail is the VariantImportJob
// analogue of the annotation test above (VariantImportItemDetail,
// types.go:2060-2071).
func Test_SDKRoundTrip_VariantImportJob_ItemDetail(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateVariantStore(t.Context(), &omicssdk.CreateVariantStoreInput{
		Name:      aws.String("var-item-detail-test"),
		Reference: &types.ReferenceItemMemberReferenceArn{Value: testReferenceArn},
	})
	require.NoError(t, err)

	started, err := client.StartVariantImportJob(t.Context(), &omicssdk.StartVariantImportJobInput{
		DestinationName: aws.String("var-item-detail-test"),
		RoleArn:         aws.String("arn:aws:iam::000000000000:role/role"),
		Items: []types.VariantImportItemSource{
			{Source: aws.String("s3://bucket/var.vcf")},
		},
	})
	require.NoError(t, err)

	got, err := client.GetVariantImportJob(t.Context(), &omicssdk.GetVariantImportJobInput{
		JobId: started.JobId,
	})
	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	assert.Equal(t, "s3://bucket/var.vcf", aws.ToString(got.Items[0].Source))
	assert.Equal(
		t,
		types.JobStatusCompleted,
		got.Items[0].JobStatus,
		"Items[].JobStatus must decode (real required member absent from the old shared-with-Start shape)",
	)
}

// TestListAnnotationImportJobs_OmitsGetOnlyFields proves the raw wire body
// of a List response has no items/formatOptions/statusMessage keys.
// gopherstack-7s8r assumed List, like Get, returns ItemDetail-shaped items;
// it does not -- the real List element (AnnotationImportJobItem,
// types.go:102-146) has none of those three members at all, so this
// backend's previous habit of marshaling the same Go struct for both Get
// and List leaked GetAnnotationImportJobOutput-only fields into every List
// response. A real client's ListAnnotationImportJobsOutput deserializer
// would silently ignore the extra keys rather than error, so only a raw
// body inspection -- not an SDK round trip -- can catch this class of bug.
func TestListAnnotationImportJobs_OmitsGetOnlyFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	storeRec := doRequest(t, h, http.MethodPost, "/annotationStore", map[string]any{
		"name": "list-omit-test", "storeFormat": "VCF",
	})
	require.Equal(t, http.StatusCreated, storeRec.Code)

	jobRec := doRequest(t, h, http.MethodPost, "/import/annotation", map[string]any{
		"destinationName": "list-omit-test",
		"roleArn":         "arn:aws:iam::000000000000:role/role",
		"items":           []map[string]any{{"source": "s3://bucket/ann.vcf"}},
		"formatOptions":   map[string]any{"vcfOptions": map[string]any{}},
	})
	require.Equal(t, http.StatusCreated, jobRec.Code)

	listRec := doRequest(t, h, http.MethodPost, "/import/annotations", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp struct {
		ImportJobs []map[string]any `json:"annotationImportJobs"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
	require.Len(t, resp.ImportJobs, 1)

	job := resp.ImportJobs[0]
	assert.NotContains(t, job, "items", "real AnnotationImportJobItem has no items member")
	assert.NotContains(
		t,
		job,
		"formatOptions",
		"real AnnotationImportJobItem has no formatOptions member",
	)
	assert.NotContains(
		t,
		job,
		"statusMessage",
		"real AnnotationImportJobItem has no statusMessage member",
	)
	assert.Contains(t, job, "status")
	assert.Contains(t, job, "destinationName")
}

// TestListVariantImportJobs_OmitsGetOnlyFields is the VariantImportJob
// analogue (real List element VariantImportJobItem, types.go:2090-2132, has
// no items or statusMessage member).
func TestListVariantImportJobs_OmitsGetOnlyFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	storeRec := doRequest(t, h, http.MethodPost, "/variantStore", map[string]any{
		"name":      "var-list-omit-test",
		"reference": map[string]any{"referenceArn": testReferenceArn},
	})
	require.Equal(t, http.StatusCreated, storeRec.Code)

	jobRec := doRequest(t, h, http.MethodPost, "/import/variant", map[string]any{
		"destinationName": "var-list-omit-test",
		"roleArn":         "arn:aws:iam::000000000000:role/role",
		"items":           []map[string]any{{"source": "s3://bucket/var.vcf"}},
	})
	require.Equal(t, http.StatusCreated, jobRec.Code)

	listRec := doRequest(t, h, http.MethodPost, "/import/variants", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp struct {
		ImportJobs []map[string]any `json:"variantImportJobs"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
	require.Len(t, resp.ImportJobs, 1)

	job := resp.ImportJobs[0]
	assert.NotContains(t, job, "items", "real VariantImportJobItem has no items member")
	assert.NotContains(
		t,
		job,
		"statusMessage",
		"real VariantImportJobItem has no statusMessage member",
	)
	assert.Contains(t, job, "status")
	assert.Contains(t, job, "destinationName")
}

// TestOmicsStoreLists_OmitGetOnlyFields covers the three ListAnnotationStores/
// ListVariantStores/ListAnnotationStoreVersions leaks named but not fixed in
// e68817984 (gopherstack-dv4s): each backend marshaled its full Get-shaped
// domain struct for List, which is narrower on the real wire. Fixtures set
// every candidate leaked field to a nonempty value so a regression back to
// marshaling the domain struct directly would actually be caught, not pass
// vacuously against an empty fixture.
func TestOmicsStoreLists_OmitGetOnlyFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// item creates the fixtures and returns the single list-response
		// element as a raw decoded map.
		item      func(t *testing.T, h *omics.Handler) map[string]any
		forbidden []string
		required  []string
	}{
		{
			name: "annotation stores omit numVersions storeOptions and tags",
			item: func(t *testing.T, h *omics.Handler) map[string]any {
				t.Helper()

				storeRec := doRequest(t, h, http.MethodPost, "/annotationStore", map[string]any{
					"name":         "as-omit-test",
					"storeFormat":  "VCF",
					"tags":         map[string]any{"env": "test"},
					"storeOptions": map[string]any{"tsvStoreOptions": map[string]any{}},
				})
				require.Equal(t, http.StatusCreated, storeRec.Code)

				versionRec := doRequest(t, h, http.MethodPost, "/annotationStore/as-omit-test/version",
					map[string]any{"versionName": "v1"})
				require.Equal(t, http.StatusCreated, versionRec.Code)

				listRec := doRequest(t, h, http.MethodPost, "/annotationStores", map[string]any{})
				require.Equal(t, http.StatusOK, listRec.Code)

				var resp struct {
					Stores []map[string]any `json:"annotationStores"`
				}
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
				require.Len(t, resp.Stores, 1)

				return resp.Stores[0]
			},
			forbidden: []string{"numVersions", "storeOptions", "tags"},
			required:  []string{"status", "storeArn", "storeFormat"},
		},
		{
			name: "variant stores omit tags",
			item: func(t *testing.T, h *omics.Handler) map[string]any {
				t.Helper()

				storeRec := doRequest(t, h, http.MethodPost, "/variantStore", map[string]any{
					"name":      "vs-omit-test",
					"reference": map[string]any{"referenceArn": testReferenceArn},
					"tags":      map[string]any{"env": "test"},
				})
				require.Equal(t, http.StatusCreated, storeRec.Code)

				listRec := doRequest(t, h, http.MethodPost, "/variantStores", map[string]any{})
				require.Equal(t, http.StatusOK, listRec.Code)

				var resp struct {
					Stores []map[string]any `json:"variantStores"`
				}
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
				require.Len(t, resp.Stores, 1)

				return resp.Stores[0]
			},
			forbidden: []string{"tags"},
			required:  []string{"status", "storeArn"},
		},
		{
			name: "annotation store versions omit tags and storeName",
			item: func(t *testing.T, h *omics.Handler) map[string]any {
				t.Helper()

				storeRec := doRequest(t, h, http.MethodPost, "/annotationStore", map[string]any{
					"name": "asv-omit-test", "storeFormat": "VCF",
				})
				require.Equal(t, http.StatusCreated, storeRec.Code)

				versionRec := doRequest(t, h, http.MethodPost, "/annotationStore/asv-omit-test/version",
					map[string]any{"versionName": "v1", "tags": map[string]any{"env": "test"}})
				require.Equal(t, http.StatusCreated, versionRec.Code)

				listRec := doRequest(t, h, http.MethodPost, "/annotationStore/asv-omit-test/versions",
					map[string]any{})
				require.Equal(t, http.StatusOK, listRec.Code)

				var resp struct {
					Versions []map[string]any `json:"annotationStoreVersions"`
				}
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
				require.Len(t, resp.Versions, 1)

				return resp.Versions[0]
			},
			// storeName is a phantom field present on Get too (not fixed
			// here, see AnnotationStoreVersionSummary's doc comment), but
			// the real List element never carries it regardless, so it
			// still belongs on this List-specific forbidden list.
			forbidden: []string{"tags", "storeName"},
			required:  []string{"status", "versionArn", "versionName"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			item := tc.item(t, h)

			for _, key := range tc.forbidden {
				assert.NotContains(t, item, key)
			}

			for _, key := range tc.required {
				assert.Contains(t, item, key)
			}
		})
	}
}

// Test_SDKRoundTrip_CreateRunCache_CacheS3Uri proves RunCache's S3 location
// decodes through the real SDK client's CacheS3Uri field. GetRunCacheOutput's
// (and CreateRunCacheOutput's shared model's) real wire key is "cacheS3Uri"
// (deserializers.go:9853) -- the request body key for the same value is the
// unrelated "cacheS3Location" (serializers.go:1334). Before the fix the
// backend tagged its RunCache.CacheS3Location Go field "cacheS3Location" on
// the wire too, so a real client's CacheS3Uri was always nil.
func Test_SDKRoundTrip_CreateRunCache_CacheS3Uri(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	created, err := client.CreateRunCache(t.Context(), &omicssdk.CreateRunCacheInput{
		Name:            aws.String("cache-uri-test"),
		CacheS3Location: aws.String("s3://my-bucket/cache"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.Id)

	got, err := client.GetRunCache(t.Context(), &omicssdk.GetRunCacheInput{Id: created.Id})
	require.NoError(t, err)
	require.NotNil(t, got.CacheS3Uri, "CacheS3Uri must decode from the real \"cacheS3Uri\" wire key")
	assert.Equal(t, "s3://my-bucket/cache", *got.CacheS3Uri)
}

// Test_SDKRoundTrip_CreateShare_ShareName proves Share's name decodes through
// the real SDK client's ShareName field. Real ShareDetails/CreateShareOutput
// wire key is "shareName" (deserializers.go:3062, deserializers.go:26670) --
// before the fix the backend's shared Share model tagged this field "name",
// so a real client's ShareName was always nil on GetShare/ListShares, and
// CreateShareOutput's own ShareName was likewise always empty.
func Test_SDKRoundTrip_CreateShare_ShareName(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	created, err := client.CreateShare(t.Context(), &omicssdk.CreateShareInput{
		ResourceArn:         aws.String("arn:aws:omics:us-east-1:000000000000:annotationStore/share-name-test"),
		PrincipalSubscriber: aws.String("123456789012"),
		ShareName:           aws.String("my-share"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.ShareId)
	require.NotNil(t, created.ShareName, "CreateShareOutput.ShareName must decode from the real \"shareName\" wire key")
	assert.Equal(t, "my-share", *created.ShareName)

	got, err := client.GetShare(t.Context(), &omicssdk.GetShareInput{ShareId: created.ShareId})
	require.NoError(t, err)
	require.NotNil(t, got.Share)
	require.NotNil(t, got.Share.ShareName, "ShareDetails.ShareName must decode from the real \"shareName\" wire key")
	assert.Equal(t, "my-share", *got.Share.ShareName)
}

// Test_SDKRoundTrip_CompleteMultipartReadSetUpload_ReadSetId proves the
// created read set's ID decodes through the real SDK client's ReadSetId
// field, and chains it into a real GetReadSetMetadata call -- exactly the
// create-then-reference pattern a dropped identifier silently breaks. Real
// CompleteMultipartReadSetUploadOutput's only member is "readSetId"
// (deserializers.go's awsRestjson1_deserializeOpDocumentCompleteMultipartReadSetUploadOutput),
// a different key from GetReadSetMetadataOutput's "id" for the same
// resource. Before the fix the backend marshaled the shared ReadSetMetadata
// struct (tagged "id") directly as the Complete response, so a real
// client's ReadSetId was always nil.
func Test_SDKRoundTrip_CompleteMultipartReadSetUpload_ReadSetId(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	store, err := client.CreateSequenceStore(t.Context(), &omicssdk.CreateSequenceStoreInput{
		Name: aws.String("seq-store-complete-test"),
	})
	require.NoError(t, err)

	upload, err := client.CreateMultipartReadSetUpload(t.Context(), &omicssdk.CreateMultipartReadSetUploadInput{
		SequenceStoreId: store.Id,
		Name:            aws.String("rs-complete-test"),
		SourceFileType:  types.FileTypeFastq,
		SubjectId:       aws.String("subject-1"),
		SampleId:        aws.String("sample-1"),
	})
	require.NoError(t, err)

	part, err := client.UploadReadSetPart(t.Context(), &omicssdk.UploadReadSetPartInput{
		SequenceStoreId: store.Id,
		UploadId:        upload.UploadId,
		PartNumber:      aws.Int32(1),
		PartSource:      types.ReadSetPartSourceSource1,
		Payload:         strings.NewReader("hello omics"),
	})
	require.NoError(t, err)

	completed, err := client.CompleteMultipartReadSetUpload(t.Context(), &omicssdk.CompleteMultipartReadSetUploadInput{
		SequenceStoreId: store.Id,
		UploadId:        upload.UploadId,
		Parts: []types.CompleteReadSetUploadPartListItem{
			{PartNumber: aws.Int32(1), PartSource: types.ReadSetPartSourceSource1, Checksum: part.Checksum},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, completed.ReadSetId, "ReadSetId must decode from the real \"readSetId\" wire key")
	assert.NotEmpty(t, *completed.ReadSetId)

	got, err := client.GetReadSetMetadata(t.Context(), &omicssdk.GetReadSetMetadataInput{
		SequenceStoreId: store.Id,
		Id:              completed.ReadSetId,
	})
	require.NoError(t, err, "the id returned by Complete must resolve via a real GetReadSetMetadata call")
	require.NotNil(t, got.Id)
	assert.Equal(t, *completed.ReadSetId, *got.Id)
}

// Test_SDKRoundTrip_VariantStore_SseConfig proves GetVariantStoreOutput/
// ListVariantStoresOutput's required "sseConfig" (types.go, VariantStoreItem
// and GetVariantStoreOutput) decodes through the real SDK client.
// VariantStore previously had no SseConfig field at all -- the "member with
// no struct field" class (like iam's JobCompletionDate) -- and
// CreateVariantStore's handler didn't even read the real (optional)
// CreateVariantStoreInput.SseConfig (gopherstack-r80d batch 7).
func Test_SDKRoundTrip_VariantStore_SseConfig(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateVariantStore(t.Context(), &omicssdk.CreateVariantStoreInput{
		Name:      aws.String("var-store-sseconfig-test"),
		Reference: &types.ReferenceItemMemberReferenceArn{Value: testReferenceArn},
		SseConfig: &types.SseConfig{
			Type:   types.EncryptionTypeKms,
			KeyArn: aws.String("arn:aws:kms:us-east-1:000000000000:key/test"),
		},
	})
	require.NoError(t, err)

	got, err := client.GetVariantStore(t.Context(), &omicssdk.GetVariantStoreInput{
		Name: aws.String("var-store-sseconfig-test"),
	})
	require.NoError(t, err)
	require.NotNil(t, got.SseConfig, "SseConfig must decode from the real \"sseConfig\" wire key")
	assert.Equal(t, types.EncryptionTypeKms, got.SseConfig.Type)
	require.NotNil(t, got.SseConfig.KeyArn)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/test", *got.SseConfig.KeyArn)

	listed, err := client.ListVariantStores(t.Context(), &omicssdk.ListVariantStoresInput{})
	require.NoError(t, err)
	require.Len(t, listed.VariantStores, 1)
	require.NotNil(
		t, listed.VariantStores[0].SseConfig,
		"SseConfig must decode from the real \"sseConfig\" wire key on the List element too",
	)
	assert.Equal(t, types.EncryptionTypeKms, listed.VariantStores[0].SseConfig.Type)
}

// Test_SDKRoundTrip_CreateMultipartReadSetUpload_ReferenceArn proves
// CreateMultipartReadSetUploadOutput.ReferenceArn (real required
// "referenceArn", api_op_CreateMultipartReadSetUpload.go:82-85) decodes
// through the real SDK client even when the caller omits the (optional on
// the input side) ReferenceArn field. Before the fix the backend tagged the
// field "referenceArn,omitempty", so an empty ReferenceArn dropped the key
// from the response entirely instead of emitting it as an empty string --
// a required field a real client decodes as missing, not as "" (gopherstack-
// r80d batch 7).
func Test_SDKRoundTrip_CreateMultipartReadSetUpload_ReferenceArn(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	store, err := client.CreateSequenceStore(t.Context(), &omicssdk.CreateSequenceStoreInput{
		Name: aws.String("seq-store-refarn-test"),
	})
	require.NoError(t, err)

	upload, err := client.CreateMultipartReadSetUpload(t.Context(), &omicssdk.CreateMultipartReadSetUploadInput{
		SequenceStoreId: store.Id,
		Name:            aws.String("rs-refarn-test"),
		SourceFileType:  types.FileTypeFastq,
		SubjectId:       aws.String("subject-1"),
		SampleId:        aws.String("sample-1"),
		// ReferenceArn deliberately omitted -- optional on the input, but
		// required on the output.
	})
	require.NoError(t, err)
	require.NotNil(
		t, upload.ReferenceArn,
		"ReferenceArn must decode from the real \"referenceArn\" wire key even when empty",
	)
	assert.Empty(t, *upload.ReferenceArn)
}

// Test_SDKRoundTrip_CreateAnnotationStore_VersionName proves
// CreateAnnotationStoreOutput.VersionName (real required "versionName",
// deserializers.go:1290 -- this backend previously had no field for it at
// all, gopherstack-r80d batch 7) decodes through the real SDK client and
// echoes the caller-supplied CreateAnnotationStoreInput.VersionName.
func Test_SDKRoundTrip_CreateAnnotationStore_VersionName(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	got, err := client.CreateAnnotationStore(t.Context(), &omicssdk.CreateAnnotationStoreInput{
		Name:        aws.String("store-versionname-test"),
		StoreFormat: types.StoreFormatVcf,
		VersionName: aws.String("v1"),
	})
	require.NoError(t, err)
	require.NotNil(t, got.VersionName, "VersionName must decode from the real \"versionName\" wire key")
	assert.Equal(t, "v1", *got.VersionName)
}

// Test_SDKRoundTrip_AnnotationStoreVersion_IdAndName proves Get/Update/
// CreateAnnotationStoreVersionOutput's required "id" and "name" members
// (deserializers.go:6501/6510 for Get; the same two keys on Create/Update)
// decode through the real SDK client. Both were entirely absent from this
// backend's AnnotationStoreVersion struct: no field at all backed "id", and
// "name" was present under the struct but tagged the invented key
// "storeName", which no real deserializer for this shape reads. Flagged but
// deliberately left unfixed by two prior passes (gopherstack-lx5h/kb66,
// gopherstack-dv4s) as out of their scope; closed here (gopherstack-r80d
// batch 7).
func Test_SDKRoundTrip_AnnotationStoreVersion_IdAndName(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("000000000000", wireTestRegion)
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	_, err := client.CreateAnnotationStore(t.Context(), &omicssdk.CreateAnnotationStoreInput{
		Name:        aws.String("store-version-idname-test"),
		StoreFormat: types.StoreFormatVcf,
	})
	require.NoError(t, err)

	created, err := client.CreateAnnotationStoreVersion(t.Context(), &omicssdk.CreateAnnotationStoreVersionInput{
		Name:        aws.String("store-version-idname-test"),
		VersionName: aws.String("v1"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.Id, "Id must decode from the real \"id\" wire key")
	assert.NotEmpty(t, *created.Id)
	require.NotNil(t, created.Name, "Name must decode from the real \"name\" wire key")
	assert.Equal(t, "store-version-idname-test", *created.Name)

	got, err := client.GetAnnotationStoreVersion(t.Context(), &omicssdk.GetAnnotationStoreVersionInput{
		Name:        aws.String("store-version-idname-test"),
		VersionName: aws.String("v1"),
	})
	require.NoError(t, err)
	require.NotNil(t, got.Id, "Id must decode from the real \"id\" wire key")
	assert.Equal(t, *created.Id, *got.Id)
	require.NotNil(t, got.Name, "Name must decode from the real \"name\" wire key")
	assert.Equal(t, "store-version-idname-test", *got.Name)

	updated, err := client.UpdateAnnotationStoreVersion(t.Context(), &omicssdk.UpdateAnnotationStoreVersionInput{
		Name:        aws.String("store-version-idname-test"),
		VersionName: aws.String("v1"),
		Description: aws.String("updated"),
	})
	require.NoError(t, err)
	require.NotNil(t, updated.Id, "Id must decode from the real \"id\" wire key")
	assert.Equal(t, *created.Id, *updated.Id)
	require.NotNil(t, updated.Name, "Name must decode from the real \"name\" wire key")
	assert.Equal(t, "store-version-idname-test", *updated.Name)

	listed, err := client.ListAnnotationStoreVersions(t.Context(), &omicssdk.ListAnnotationStoreVersionsInput{
		Name: aws.String("store-version-idname-test"),
	})
	require.NoError(t, err)
	require.Len(t, listed.AnnotationStoreVersions, 1)
	require.NotNil(
		t, listed.AnnotationStoreVersions[0].Id,
		"Id must decode from the real \"id\" wire key on the List element too",
	)
	assert.Equal(t, *created.Id, *listed.AnnotationStoreVersions[0].Id)
	require.NotNil(t, listed.AnnotationStoreVersions[0].Name)
	assert.Equal(t, "store-version-idname-test", *listed.AnnotationStoreVersions[0].Name)
}
