package cloudwatch_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypedSlice11RealClient drives cloudwatch's typed-coverage-blind ops
// (gopherstack-n3zi slice 11) through the real aws-sdk-go-v2 client.
func TestTypedSlice11RealClient(t *testing.T) {
	t.Parallel()

	t.Run("anomaly detector delete", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		_, err := client.PutAnomalyDetector(ctx, &cwsdk.PutAnomalyDetectorInput{
			SingleMetricAnomalyDetector: &types.SingleMetricAnomalyDetector{
				Namespace:  aws.String("S11/Namespace"),
				MetricName: aws.String("S11Metric"),
				Stat:       aws.String("Average"),
			},
		})
		require.NoError(t, err)

		_, err = client.DeleteAnomalyDetector(ctx, &cwsdk.DeleteAnomalyDetectorInput{
			SingleMetricAnomalyDetector: &types.SingleMetricAnomalyDetector{
				Namespace:  aws.String("S11/Namespace"),
				MetricName: aws.String("S11Metric"),
				Stat:       aws.String("Average"),
			},
		})
		require.NoError(t, err)

		descOut, err := client.DescribeAnomalyDetectors(ctx, &cwsdk.DescribeAnomalyDetectorsInput{
			Namespace:  aws.String("S11/Namespace"),
			MetricName: aws.String("S11Metric"),
		})
		require.NoError(t, err)
		assert.Empty(t, descOut.AnomalyDetectors, "DeleteAnomalyDetector must remove the detector")
	})

	t.Run("metric stream delete", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		_, err := client.PutMetricStream(ctx, &cwsdk.PutMetricStreamInput{
			Name:         aws.String("s11-metric-stream"),
			FirehoseArn:  aws.String("arn:aws:firehose:us-east-1:000000000000:deliverystream/s11-stream"),
			RoleArn:      aws.String("arn:aws:iam::000000000000:role/s11-role"),
			OutputFormat: types.MetricStreamOutputFormatJson,
		})
		require.NoError(t, err)

		_, err = client.DeleteMetricStream(ctx, &cwsdk.DeleteMetricStreamInput{
			Name: aws.String("s11-metric-stream"),
		})
		require.NoError(t, err)

		_, err = client.GetMetricStream(ctx, &cwsdk.GetMetricStreamInput{
			Name: aws.String("s11-metric-stream"),
		})
		assert.Error(t, err, "GetMetricStream must fail for a deleted stream")
	})

	t.Run("insight rules enable disable describe", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		_, err := client.PutInsightRule(ctx, &cwsdk.PutInsightRuleInput{
			RuleName:       aws.String("s11-insight-rule"),
			RuleDefinition: aws.String(validInsightRuleDefinition),
		})
		require.NoError(t, err)

		descOut, err := client.DescribeInsightRules(ctx, &cwsdk.DescribeInsightRulesInput{})
		require.NoError(t, err)
		var found bool
		for _, r := range descOut.InsightRules {
			if aws.ToString(r.Name) == "s11-insight-rule" {
				found = true
				assert.Equal(t, "ENABLED", aws.ToString(r.State))
			}
		}
		assert.True(t, found, "DescribeInsightRules must include the newly created rule")

		disableOut, err := client.DisableInsightRules(ctx, &cwsdk.DisableInsightRulesInput{
			RuleNames: []string{"s11-insight-rule", "s11-missing-rule"},
		})
		require.NoError(t, err)
		require.Len(t, disableOut.Failures, 1)
		assert.Equal(t, "s11-missing-rule", aws.ToString(disableOut.Failures[0].FailureResource))

		enableOut, err := client.EnableInsightRules(ctx, &cwsdk.EnableInsightRulesInput{
			RuleNames: []string{"s11-insight-rule"},
		})
		require.NoError(t, err)
		assert.Empty(t, enableOut.Failures)
	})

	t.Run("dataset kms key", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		validKeyArn := "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-1234567890ab"
		_, err := client.AssociateDatasetKmsKey(ctx, &cwsdk.AssociateDatasetKmsKeyInput{
			DatasetIdentifier: aws.String("default"),
			KmsKeyArn:         aws.String(validKeyArn),
		})
		require.NoError(t, err)

		getOut, err := client.GetDataset(ctx, &cwsdk.GetDatasetInput{
			DatasetIdentifier: aws.String("default"),
		})
		require.NoError(t, err)
		assert.Equal(t, validKeyArn, aws.ToString(getOut.KmsKeyArn))

		_, err = client.DisassociateDatasetKmsKey(ctx, &cwsdk.DisassociateDatasetKmsKeyInput{
			DatasetIdentifier: aws.String("default"),
		})
		require.NoError(t, err)

		afterOut, err := client.GetDataset(ctx, &cwsdk.GetDatasetInput{
			DatasetIdentifier: aws.String("default"),
		})
		require.NoError(t, err)
		assert.Empty(t, aws.ToString(afterOut.KmsKeyArn))
	})

	t.Run("otel enrichment lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		getBefore, err := client.GetOTelEnrichment(ctx, &cwsdk.GetOTelEnrichmentInput{})
		require.NoError(t, err)
		assert.Equal(t, types.OTelEnrichmentStatusStopped, getBefore.Status)

		_, err = client.StartOTelEnrichment(ctx, &cwsdk.StartOTelEnrichmentInput{})
		require.NoError(t, err)

		getAfterStart, err := client.GetOTelEnrichment(ctx, &cwsdk.GetOTelEnrichmentInput{})
		require.NoError(t, err)
		assert.Equal(t, types.OTelEnrichmentStatusRunning, getAfterStart.Status)

		_, err = client.StopOTelEnrichment(ctx, &cwsdk.StopOTelEnrichmentInput{})
		require.NoError(t, err)

		getAfterStop, err := client.GetOTelEnrichment(ctx, &cwsdk.GetOTelEnrichmentInput{})
		require.NoError(t, err)
		assert.Equal(t, types.OTelEnrichmentStatusStopped, getAfterStop.Status)
	})

	t.Run("get metric data", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		now := time.Now().UTC().Truncate(time.Minute)

		_, err := client.PutMetricData(ctx, &cwsdk.PutMetricDataInput{
			Namespace: aws.String("S11/GetMetricData"),
			MetricData: []types.MetricDatum{
				{
					MetricName: aws.String("S11Value"),
					Value:      aws.Float64(42),
					Timestamp:  aws.Time(now),
				},
			},
		})
		require.NoError(t, err)

		out, err := client.GetMetricData(ctx, &cwsdk.GetMetricDataInput{
			StartTime: aws.Time(now.Add(-10 * time.Minute)),
			EndTime:   aws.Time(now.Add(10 * time.Minute)),
			MetricDataQueries: []types.MetricDataQuery{
				{
					Id: aws.String("m1"),
					MetricStat: &types.MetricStat{
						Metric: &types.Metric{
							Namespace:  aws.String("S11/GetMetricData"),
							MetricName: aws.String("S11Value"),
						},
						Period: aws.Int32(60),
						Stat:   aws.String("Average"),
					},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.MetricDataResults, 1)
		require.Len(t, out.MetricDataResults[0].Values, 1)
		assert.InDelta(t, 42.0, out.MetricDataResults[0].Values[0], 0.001)
	})
}
