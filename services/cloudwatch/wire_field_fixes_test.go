package cloudwatch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeAnomalyDetectors_Dimensions_RealClient covers a layer-3 bug
// (gopherstack-g8k9): the backend's AnomalyDetector.Dimensions field is real,
// settable state (used to key detectors so different dimension sets are
// distinct, per anomalyDetectorKey in anomaly_detectors.go), but neither
// cborPutAnomalyDetector nor cborDescribeAnomalyDetectors ever touched it --
// Put silently dropped Dimensions from the request, and Describe never
// emitted them in the response even when present. Both are fixed together
// since fixing only the response side has no observable effect for a real
// client (Put never stored anything to show). Real member names confirmed
// against cloudwatch@v1.66.3 schemas/schemas.go: SingleMetricAnomalyDetector
// Dimensions at line 3319, and the deprecated-but-still-real top-level
// AnomalyDetector.Dimensions at line 3415.
func TestDescribeAnomalyDetectors_Dimensions_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.PutAnomalyDetector(ctx, &cwsdk.PutAnomalyDetectorInput{
		SingleMetricAnomalyDetector: &cwtypes.SingleMetricAnomalyDetector{
			Namespace:  aws.String("Custom/Test"),
			MetricName: aws.String("Latency"),
			Stat:       aws.String("Average"),
			Dimensions: []cwtypes.Dimension{
				{Name: aws.String("InstanceId"), Value: aws.String("i-0123456789abcdef0")},
			},
		},
	})
	require.NoError(t, err, "PutAnomalyDetector should succeed against the real wire shape")

	out, err := client.DescribeAnomalyDetectors(ctx, &cwsdk.DescribeAnomalyDetectorsInput{
		Namespace:  aws.String("Custom/Test"),
		MetricName: aws.String("Latency"),
	})
	require.NoError(t, err)
	require.Len(t, out.AnomalyDetectors, 1)

	got := out.AnomalyDetectors[0]
	require.NotNil(t, got.SingleMetricAnomalyDetector)
	require.Len(t, got.SingleMetricAnomalyDetector.Dimensions, 1,
		"Dimensions must round-trip through Put -> Describe; pre-fix this was always empty")
	assert.Equal(t, "InstanceId", aws.ToString(got.SingleMetricAnomalyDetector.Dimensions[0].Name))
	assert.Equal(t, "i-0123456789abcdef0", aws.ToString(got.SingleMetricAnomalyDetector.Dimensions[0].Value))

	// The deprecated top-level field is still real on the wire and should
	// carry the same data for callers that still read it.
	//nolint:staticcheck // SA1019: deliberately exercising the deprecated-but-real wire field
	require.Len(t, got.Dimensions, 1)
	assert.Equal(t, "InstanceId", aws.ToString(got.Dimensions[0].Name)) //nolint:staticcheck // SA1019: same
}

// TestDeleteInsightRules_FailureResource_RealClient covers a layer-2 bug: the
// wire member for a failed batch entry is FailureResource, not RuleName
// (cloudwatch@v1.66.3 schemas/schemas.go:3271, PartialFailure, shared by
// DeleteInsightRules/DisableInsightRules/EnableInsightRules/
// PutManagedInsightRules via the BatchFailures list). gopherstack emitted
// "RuleName" instead, so a real client's PartialFailure.FailureResource was
// always nil even though the backend knew exactly which rule failed.
func TestDeleteInsightRules_FailureResource_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	out, err := client.DeleteInsightRules(ctx, &cwsdk.DeleteInsightRulesInput{
		RuleNames: []string{"does-not-exist"},
	})
	require.NoError(t, err)
	require.Len(t, out.Failures, 1)

	f := out.Failures[0]
	require.NotNil(t, f.FailureResource,
		"FailureResource must be populated; pre-fix the real deserializer never matched the wrong RuleName key")
	assert.Equal(t, "does-not-exist", aws.ToString(f.FailureResource))
	assert.NotEmpty(t, aws.ToString(f.FailureCode))
}

// TestListManagedInsightRules_TemplateName_RealClient covers a layer-2 bug:
// the ManagedRules>member.TemplateName field was populated from the wrong
// domain field (rule.Name, which a real PutManagedInsightRules client call
// never populates -- the real ManagedRule input has no RuleName member at
// all, only ResourceARN/TemplateName/Tags) instead of rule.Definition, which
// is where PutManagedInsightRules actually stores the TemplateName value.
// Pre-fix, TemplateName on every managed rule returned by a real client was
// always empty.
func TestListManagedInsightRules_TemplateName_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	const resourceARN = "arn:aws:events:us-east-1:000000000000:rule/my-rule"
	const templateName = "AWSEventsRuleMonitor"

	putOut, err := client.PutManagedInsightRules(ctx, &cwsdk.PutManagedInsightRulesInput{
		ManagedRules: []cwtypes.ManagedRule{
			{
				ResourceARN:  aws.String(resourceARN),
				TemplateName: aws.String(templateName),
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, putOut.Failures)

	listOut, err := client.ListManagedInsightRules(ctx, &cwsdk.ListManagedInsightRulesInput{
		ResourceARN: aws.String(resourceARN),
	})
	require.NoError(t, err)
	require.Len(t, listOut.ManagedRules, 1)

	got := listOut.ManagedRules[0]
	assert.Equal(t, templateName, aws.ToString(got.TemplateName),
		"TemplateName must reflect what PutManagedInsightRules stored; pre-fix this always came back empty")
	assert.Equal(t, resourceARN, aws.ToString(got.ResourceARN))
}

// TestGetMetricStream_Filters_RealClient covers a layer-3 bug
// (gopherstack-g8k9): MetricStream.IncludeFilters/ExcludeFilters are real,
// settable state (correctly parsed by PutMetricStream) but
// GetMetricStreamOutput never emitted either field, despite both being real
// GetMetricStreamOutput members (cloudwatch@v1.66.3 schemas/schemas.go:4253
// and 4255).
func TestGetMetricStream_Filters_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		build func(in *cwsdk.PutMetricStreamInput)
		check func(t *testing.T, out *cwsdk.GetMetricStreamOutput)
		name  string
	}{
		{
			name: "include filters",
			build: func(in *cwsdk.PutMetricStreamInput) {
				in.IncludeFilters = []cwtypes.MetricStreamFilter{
					{Namespace: aws.String("AWS/EC2"), MetricNames: []string{"CPUUtilization"}},
				}
			},
			check: func(t *testing.T, out *cwsdk.GetMetricStreamOutput) {
				t.Helper()
				require.Len(t, out.IncludeFilters, 1,
					"IncludeFilters must round-trip; pre-fix GetMetricStream never emitted it")
				assert.Equal(t, "AWS/EC2", aws.ToString(out.IncludeFilters[0].Namespace))
				assert.Equal(t, []string{"CPUUtilization"}, out.IncludeFilters[0].MetricNames)
				assert.Empty(t, out.ExcludeFilters)
			},
		},
		{
			name: "exclude filters",
			build: func(in *cwsdk.PutMetricStreamInput) {
				in.ExcludeFilters = []cwtypes.MetricStreamFilter{
					{Namespace: aws.String("AWS/RDS")},
				}
			},
			check: func(t *testing.T, out *cwsdk.GetMetricStreamOutput) {
				t.Helper()
				require.Len(t, out.ExcludeFilters, 1,
					"ExcludeFilters must round-trip; pre-fix GetMetricStream never emitted it")
				assert.Equal(t, "AWS/RDS", aws.ToString(out.ExcludeFilters[0].Namespace))
				assert.Empty(t, out.IncludeFilters)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			in := &cwsdk.PutMetricStreamInput{
				Name:         aws.String("stream-" + tc.name),
				FirehoseArn:  aws.String("arn:aws:firehose:us-east-1:000000000000:deliverystream/test"),
				RoleArn:      aws.String("arn:aws:iam::000000000000:role/test"),
				OutputFormat: cwtypes.MetricStreamOutputFormatJson,
			}
			tc.build(in)

			_, err := client.PutMetricStream(ctx, in)
			require.NoError(t, err)

			out, err := client.GetMetricStream(ctx, &cwsdk.GetMetricStreamInput{
				Name: aws.String("stream-" + tc.name),
			})
			require.NoError(t, err)

			tc.check(t, out)
		})
	}
}
