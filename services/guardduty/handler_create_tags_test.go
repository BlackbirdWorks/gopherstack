package guardduty_test

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
	"github.com/aws/aws-sdk-go-v2/service/guardduty/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

const tagsRTRegion = "us-east-1"

const tagsRTAccountID = "000000000000"

// newTestGuardDutyClient stands up the real aws-sdk-go-v2 GuardDuty client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production. Round-tripping
// through the genuine SDK serializer/deserializer is what actually proves a
// response is wire-compatible, rather than a raw-JSON string match.
func newTestGuardDutyClient(t *testing.T, h *guardduty.Handler) *guarddutysdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(tagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return guarddutysdk.NewFromConfig(cfg, func(o *guarddutysdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every guardduty Create* op whose
// real Input struct accepts Tags (guardduty@v1.85.4: api_op_CreateDetector.go:79,
// CreateFilter.go:1454, CreateIPSet.go:77, CreateThreatIntelSet.go:75,
// CreateMalwareProtectionPlan.go:57, CreatePublishingDestination.go:58,
// CreateThreatEntitySet.go:84, CreateTrustedEntitySet.go:87) through the real
// SDK client and asserts ListTagsForResource sees what was supplied at
// creation (gopherstack-2mwl). CreateMembers/CreateSampleFindings/
// CreateInvestigation take no Tags field in the real SDK and are excluded.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *guarddutysdk.Client) string
		name  string
	}{
		{
			name: "detector",
			setup: func(t *testing.T, client *guarddutysdk.Client) string {
				t.Helper()
				out, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{
					Enable: aws.Bool(true),
					Tags:   map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return fmt.Sprintf(
					"arn:aws:guardduty:%s:%s:detector/%s",
					tagsRTRegion, tagsRTAccountID, aws.ToString(out.DetectorId),
				)
			},
		},
		{
			name: "filter",
			setup: func(t *testing.T, client *guarddutysdk.Client) string {
				t.Helper()
				det, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{
					Enable: aws.Bool(true),
				})
				require.NoError(t, err)
				detectorID := aws.ToString(det.DetectorId)

				_, err = client.CreateFilter(t.Context(), &guarddutysdk.CreateFilterInput{
					DetectorId: aws.String(detectorID),
					Name:       aws.String("tagged-filter"),
					Action:     types.FilterActionNoop,
					FindingCriteria: &types.FindingCriteria{
						Criterion: map[string]types.Condition{
							"severity": {Gte: aws.Int32(4)},
						},
					},
					Tags: map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return fmt.Sprintf(
					"arn:aws:guardduty:%s:%s:detector/%s/filter/tagged-filter",
					tagsRTRegion, tagsRTAccountID, detectorID,
				)
			},
		},
		{
			name: "ip set",
			setup: func(t *testing.T, client *guarddutysdk.Client) string {
				t.Helper()
				det, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{
					Enable: aws.Bool(true),
				})
				require.NoError(t, err)
				detectorID := aws.ToString(det.DetectorId)

				out, err := client.CreateIPSet(t.Context(), &guarddutysdk.CreateIPSetInput{
					DetectorId: aws.String(detectorID),
					Name:       aws.String("tagged-ipset"),
					Format:     types.IpSetFormatTxt,
					Location:   aws.String("s3://bucket/ipset.txt"),
					Activate:   aws.Bool(false),
					Tags:       map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return fmt.Sprintf(
					"arn:aws:guardduty:%s:%s:detector/%s/ipset/%s",
					tagsRTRegion, tagsRTAccountID, detectorID, aws.ToString(out.IpSetId),
				)
			},
		},
		{
			name: "threat intel set",
			setup: func(t *testing.T, client *guarddutysdk.Client) string {
				t.Helper()
				det, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{
					Enable: aws.Bool(true),
				})
				require.NoError(t, err)
				detectorID := aws.ToString(det.DetectorId)

				out, err := client.CreateThreatIntelSet(t.Context(), &guarddutysdk.CreateThreatIntelSetInput{
					DetectorId: aws.String(detectorID),
					Name:       aws.String("tagged-tiset"),
					Format:     types.ThreatIntelSetFormatTxt,
					Location:   aws.String("s3://bucket/tiset.txt"),
					Activate:   aws.Bool(false),
					Tags:       map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return fmt.Sprintf(
					"arn:aws:guardduty:%s:%s:detector/%s/threatintelset/%s",
					tagsRTRegion, tagsRTAccountID, detectorID, aws.ToString(out.ThreatIntelSetId),
				)
			},
		},
		{
			name: "malware protection plan",
			setup: func(t *testing.T, client *guarddutysdk.Client) string {
				t.Helper()
				out, err := client.CreateMalwareProtectionPlan(
					t.Context(),
					&guarddutysdk.CreateMalwareProtectionPlanInput{
						Role: aws.String("arn:aws:iam::000000000000:role/malware-role"),
						ProtectedResource: &types.CreateProtectedResource{
							S3Bucket: &types.CreateS3BucketResource{
								BucketName: aws.String("tagged-bucket"),
							},
						},
						Tags: map[string]string{"env": "prod"},
					},
				)
				require.NoError(t, err)

				return fmt.Sprintf(
					"arn:aws:guardduty:%s:%s:malware-protection-plan/%s",
					tagsRTRegion, tagsRTAccountID, aws.ToString(out.MalwareProtectionPlanId),
				)
			},
		},
		{
			name: "publishing destination",
			setup: func(t *testing.T, client *guarddutysdk.Client) string {
				t.Helper()
				det, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{
					Enable: aws.Bool(true),
				})
				require.NoError(t, err)
				detectorID := aws.ToString(det.DetectorId)

				out, err := client.CreatePublishingDestination(
					t.Context(),
					&guarddutysdk.CreatePublishingDestinationInput{
						DetectorId:      aws.String(detectorID),
						DestinationType: types.DestinationTypeS3,
						DestinationProperties: &types.DestinationProperties{
							DestinationArn: aws.String("arn:aws:s3:::tagged-bucket"),
							KmsKeyArn:      aws.String("arn:aws:kms:us-east-1:000000000000:key/tagged-key"),
						},
						Tags: map[string]string{"env": "prod"},
					},
				)
				require.NoError(t, err)

				return fmt.Sprintf(
					"arn:aws:guardduty:%s:%s:detector/%s/publishingDestination/%s",
					tagsRTRegion, tagsRTAccountID, detectorID, aws.ToString(out.DestinationId),
				)
			},
		},
		{
			name: "threat entity set",
			setup: func(t *testing.T, client *guarddutysdk.Client) string {
				t.Helper()
				det, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{
					Enable: aws.Bool(true),
				})
				require.NoError(t, err)
				detectorID := aws.ToString(det.DetectorId)

				out, err := client.CreateThreatEntitySet(t.Context(), &guarddutysdk.CreateThreatEntitySetInput{
					DetectorId: aws.String(detectorID),
					Name:       aws.String("tagged-teset"),
					Format:     types.ThreatEntitySetFormatTxt,
					Location:   aws.String("s3://bucket/teset.txt"),
					Activate:   aws.Bool(false),
					Tags:       map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return fmt.Sprintf(
					"arn:aws:guardduty:%s:%s:detector/%s/threatentityset/%s",
					tagsRTRegion, tagsRTAccountID, detectorID, aws.ToString(out.ThreatEntitySetId),
				)
			},
		},
		{
			name: "trusted entity set",
			setup: func(t *testing.T, client *guarddutysdk.Client) string {
				t.Helper()
				det, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{
					Enable: aws.Bool(true),
				})
				require.NoError(t, err)
				detectorID := aws.ToString(det.DetectorId)

				out, err := client.CreateTrustedEntitySet(t.Context(), &guarddutysdk.CreateTrustedEntitySetInput{
					DetectorId: aws.String(detectorID),
					Name:       aws.String("tagged-tseset"),
					Format:     types.TrustedEntitySetFormatTxt,
					Location:   aws.String("s3://bucket/tseset.txt"),
					Activate:   aws.Bool(false),
					Tags:       map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return fmt.Sprintf(
					"arn:aws:guardduty:%s:%s:detector/%s/trustedentityset/%s",
					tagsRTRegion, tagsRTAccountID, detectorID, aws.ToString(out.TrustedEntitySetId),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := guardduty.NewInMemoryBackend(tagsRTAccountID, tagsRTRegion)
			h := guardduty.NewHandler(backend)
			client := newTestGuardDutyClient(t, h)

			resourceARN := tt.setup(t, client)
			require.NotEmpty(t, resourceARN)

			out, err := client.ListTagsForResource(t.Context(), &guarddutysdk.ListTagsForResourceInput{
				ResourceArn: aws.String(resourceARN),
			})
			require.NoError(t, err)

			require.Len(t, out.Tags, 1)
			assert.Equal(t, "prod", out.Tags["env"])
		})
	}
}
