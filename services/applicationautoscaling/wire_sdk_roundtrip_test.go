package applicationautoscaling_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	aassdk "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"
	aastypes "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
)

// newTestAASSDKClient stands up the real aws-sdk-go-v2 applicationautoscaling
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production -- so a
// shape is verified by the real client's own deserializer, not gopherstack's
// own JSON tags.
func newTestAASSDKClient(t *testing.T, h *applicationautoscaling.Handler) *aassdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return aassdk.NewFromConfig(cfg, func(o *aassdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestGetPredictiveScalingForecast_SDKRoundTrip proves the response the real
// aws-sdk-go-v2 client can actually decode. LoadForecast[].MetricSpecification
// is *types.PredictiveScalingMetricSpecification (an object) on the wire
// (types/types.go: LoadForecast.MetricSpecification, "This member is
// required"), not a string -- a JSON string body for that key fails the
// client's own JSON unmarshal into that struct field, breaking the entire
// call for every caller, not just a cosmetic mismatch.
func TestGetPredictiveScalingForecast_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1")
	h := applicationautoscaling.NewHandler(backend)
	client := newTestAASSDKClient(t, h)

	const (
		resourceID = "service/my-cluster/my-service"
		dimension  = "ecs:service:DesiredCount"
		namespace  = "ecs"
		policyName = "predictive-forecast-test"
	)

	_, err := client.RegisterScalableTarget(t.Context(), &aassdk.RegisterScalableTargetInput{
		ServiceNamespace:  aastypes.ServiceNamespaceEcs,
		ResourceId:        aws.String(resourceID),
		ScalableDimension: aastypes.ScalableDimensionECSServiceDesiredCount,
		MinCapacity:       aws.Int32(1),
		MaxCapacity:       aws.Int32(10),
	})
	require.NoError(t, err)

	_, err = client.PutScalingPolicy(t.Context(), &aassdk.PutScalingPolicyInput{
		ServiceNamespace:  aastypes.ServiceNamespaceEcs,
		ResourceId:        aws.String(resourceID),
		ScalableDimension: aastypes.ScalableDimensionECSServiceDesiredCount,
		PolicyName:        aws.String(policyName),
		PolicyType:        aastypes.PolicyTypePredictiveScaling,
		PredictiveScalingPolicyConfiguration: &aastypes.PredictiveScalingPolicyConfiguration{
			MetricSpecifications: []aastypes.PredictiveScalingMetricSpecification{
				{
					TargetValue: aws.Float64(50),
					PredefinedMetricPairSpecification: &aastypes.PredictiveScalingPredefinedMetricPairSpecification{
						PredefinedMetricType: aws.String("ECSServiceAverageCPUUtilization"),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.GetPredictiveScalingForecast(t.Context(), &aassdk.GetPredictiveScalingForecastInput{
		ServiceNamespace:  aastypes.ServiceNamespaceEcs,
		ResourceId:        aws.String(resourceID),
		ScalableDimension: aastypes.ScalableDimensionECSServiceDesiredCount,
		PolicyName:        aws.String(policyName),
		StartTime:         aws.Time(mustParseTime(t, "2026-08-19T00:00:00Z")),
		EndTime:           aws.Time(mustParseTime(t, "2026-08-20T00:00:00Z")),
	})
	require.NoError(t, err, "real SDK client must be able to decode GetPredictiveScalingForecast's response body")
	require.NotNil(t, out.CapacityForecast)

	for _, lf := range out.LoadForecast {
		if lf.MetricSpecification != nil {
			require.NotEmpty(
				t, lf.MetricSpecification,
				"LoadForecast MetricSpecification, if present, must be a real object, not a fabricated placeholder",
			)
		}
	}
}

// TestListTagsForResource_EmptyARN_SDKRoundTrip proves ListTagsForResource
// with an empty ResourceARN types as ResourceNotFoundException through the
// real client, not as an untyped GenericAPIError. ListTagsForResource's own
// deserializeOpErrorListTagsForResource switch in the vendored SDK has no
// ValidationException case -- only ResourceNotFoundException -- so a
// ValidationException body would fail errors.As here.
func TestListTagsForResource_EmptyARN_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1")
	h := applicationautoscaling.NewHandler(backend)
	client := newTestAASSDKClient(t, h)

	_, err := client.ListTagsForResource(t.Context(), &aassdk.ListTagsForResourceInput{
		ResourceARN: aws.String(""),
	})
	require.Error(t, err)

	var notFound *aastypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound,
		"empty ResourceARN must type as ResourceNotFoundException, "+
			"the only exception ListTagsForResource's own switch can type")
}

func mustParseTime(t *testing.T, s string) time.Time {
	t.Helper()

	parsed, err := time.Parse(time.RFC3339, s)
	require.NoError(t, err)

	return parsed
}
