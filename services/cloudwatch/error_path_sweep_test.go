package cloudwatch_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/require"
)

// These tests drive cloudwatch's rpc-v2-cbor path (the only protocol the real
// aws-sdk-go-v2 cloudwatch client speaks) and assert the specific typed
// exception the SDK's error-type registry resolves by exact shape name
// (smithy-go type_registry.go: lookup by the __type body field, matched
// case-sensitively against the shape's short name). CloudWatch's schema
// embeds an AWSQueryError compatibility alias for each exception
// (schemas.go), e.g. InvalidParameterValueException's alias is
// "InvalidParameterValue" -- that bare alias is only resolved when the
// client negotiates query-compat mode (X-Amzn-Query-Error), which this
// client does not. A handler that writes the bare alias as the CBOR __type
// produces a code the real client's TypeRegistry never matches, so
// errors.As into the typed exception fails.

// TestSDK_PutMetricData_ErrorCodes does not cover the Value+StatisticValues
// combination (InvalidParameterCombinationException): cborDecodeDatum
// short-circuits on the first shape it finds (Values, then StatisticValues,
// then Value) and never records that another shape was also present, so
// datumShapeCount in metrics.go can never observe more than one shape
// through the CBOR path -- ErrValueAndStatisticSet is unreachable from the
// real client via this handler, independent of the error-code fix below.
func TestSDK_PutMetricData_ErrorCodes(t *testing.T) {
	t.Parallel()

	t.Run("too many values", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		values := make([]float64, 151)
		counts := make([]float64, 151)
		for i := range values {
			values[i] = float64(i)
			counts[i] = 1
		}

		_, err := client.PutMetricData(t.Context(), &cwsdk.PutMetricDataInput{
			Namespace: aws.String("errsweep/toomany"),
			MetricData: []cwtypes.MetricDatum{
				{MetricName: aws.String("TooMany"), Values: values, Counts: counts},
			},
		})
		require.Error(t, err)

		var target *cwtypes.InvalidParameterValueException
		require.ErrorAs(t, err, &target,
			"expected a real InvalidParameterValueException from the SDK deserializer")
	})

	t.Run("metric series limit exceeded", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		data := make([]cwtypes.MetricDatum, 501)
		for i := range data {
			data[i] = cwtypes.MetricDatum{
				MetricName: aws.String(fmt.Sprintf("Series%d", i)),
				Value:      aws.Float64(1),
			}
		}

		_, err := client.PutMetricData(t.Context(), &cwsdk.PutMetricDataInput{
			Namespace:  aws.String("errsweep/limit"),
			MetricData: data,
		}, func(o *cwsdk.Options) { o.DisableRequestCompression = true })
		require.Error(t, err)

		var target *cwtypes.LimitExceededFault
		require.ErrorAs(t, err, &target,
			"expected a real LimitExceededFault from the SDK deserializer")
	})
}

func TestSDK_PutMetricAlarm_StatisticAndExtendedStatistic(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.PutMetricAlarm(t.Context(), &cwsdk.PutMetricAlarmInput{
		AlarmName:          aws.String("errsweep-alarm"),
		Namespace:          aws.String("errsweep"),
		MetricName:         aws.String("Metric"),
		Statistic:          cwtypes.StatisticAverage,
		ExtendedStatistic:  aws.String("p99"),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		EvaluationPeriods:  aws.Int32(1),
		Threshold:          aws.Float64(1),
		Period:             aws.Int32(60),
	})
	require.Error(t, err)

	var target *cwtypes.InvalidParameterValueException
	require.ErrorAs(t, err, &target, "expected a real InvalidParameterValueException from the SDK deserializer")
}

func TestSDK_PutAlarmMuteRule_InvalidDuration(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.PutAlarmMuteRule(t.Context(), &cwsdk.PutAlarmMuteRuleInput{
		Name: aws.String("errsweep-mute"),
		Rule: &cwtypes.Rule{
			Schedule: &cwtypes.Schedule{
				Expression: aws.String("cron(0 2 * * *)"),
				Duration:   aws.String("not-a-duration"),
			},
		},
	})
	require.Error(t, err)

	var target *cwtypes.InvalidParameterValueException
	require.ErrorAs(t, err, &target, "expected a real InvalidParameterValueException from the SDK deserializer")
}

func TestSDK_ListMetrics_InvalidRecentlyActive(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.ListMetrics(t.Context(), &cwsdk.ListMetricsInput{
		RecentlyActive: cwtypes.RecentlyActive("Bogus"),
	})
	require.Error(t, err)

	var target *cwtypes.InvalidParameterValueException
	require.ErrorAs(t, err, &target, "expected a real InvalidParameterValueException from the SDK deserializer")
}

func TestSDK_AssociateDatasetKmsKey_InvalidArn(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.AssociateDatasetKmsKey(t.Context(), &cwsdk.AssociateDatasetKmsKeyInput{
		DatasetIdentifier: aws.String("default"),
		KmsKeyArn:         aws.String("not-an-arn"),
	})
	require.Error(t, err)

	var target *cwtypes.InvalidParameterValueException
	require.ErrorAs(t, err, &target, "expected a real InvalidParameterValueException from the SDK deserializer")
}

func TestSDK_PutInsightRule_InvalidDefinition(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.PutInsightRule(t.Context(), &cwsdk.PutInsightRuleInput{
		RuleName:       aws.String("errsweep-rule"),
		RuleDefinition: aws.String("not json"),
	})
	require.Error(t, err)

	var target *cwtypes.InvalidParameterValueException
	require.ErrorAs(t, err, &target, "expected a real InvalidParameterValueException from the SDK deserializer")
}

func TestSDK_PutMetricStream_InvalidOutputFormat(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.PutMetricStream(t.Context(), &cwsdk.PutMetricStreamInput{
		Name:         aws.String("errsweep-stream"),
		FirehoseArn:  aws.String("arn:aws:firehose:us-east-1:111122223333:deliverystream/errsweep"),
		RoleArn:      aws.String("arn:aws:iam::111122223333:role/errsweep"),
		OutputFormat: cwtypes.MetricStreamOutputFormat("bogus"),
	})
	require.Error(t, err)

	var target *cwtypes.InvalidParameterValueException
	require.ErrorAs(t, err, &target, "expected a real InvalidParameterValueException from the SDK deserializer")
}

func TestSDK_PutLogAlarm_InvalidComparisonOperator(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.PutLogAlarm(t.Context(), &cwsdk.PutLogAlarmInput{
		AlarmName:              aws.String("errsweep-log-alarm"),
		ComparisonOperator:     cwtypes.ComparisonOperator("BogusOperator"),
		QueryResultsToEvaluate: aws.Int32(1),
		QueryResultsToAlarm:    aws.Int32(1),
		Threshold:              aws.Float64(1),
		ScheduledQueryConfiguration: &cwtypes.ScheduledQueryConfiguration{
			QueryString:           aws.String("fields @timestamp"),
			AggregationExpression: aws.String("count(*)"),
			ScheduledQueryRoleARN: aws.String("arn:aws:iam::111122223333:role/errsweep"),
			ScheduleConfiguration: &cwtypes.ScheduleConfiguration{
				ScheduleExpression: aws.String("rate(5 minutes)"),
				StartTimeOffset:    aws.Int64(360),
			},
		},
	})
	require.Error(t, err)

	var target *cwtypes.InvalidParameterValueException
	require.ErrorAs(t, err, &target, "expected a real InvalidParameterValueException from the SDK deserializer")
}
