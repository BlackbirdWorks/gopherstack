package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_CloudWatchMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testCWMetricStream, "metric_stream"},
		{testCWAnomalyDetector, "anomaly_detector"},
		{testCWInsightRule, "insight_rule"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testCWMetricStream(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Stream": {
    "Type": "AWS::CloudWatch::MetricStream",
    "Properties": {
      "Name": "my-metric-stream",
      "FirehoseArn": "arn:aws:firehose:us-east-1:000000000000:deliverystream/my-stream",
      "RoleArn": "arn:aws:iam::000000000000:role/metric-stream-role",
      "OutputFormat": "json"
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Stream"}},
  "Arn": {"Value": {"Fn::GetAtt": ["Stream", "Arn"]}},
  "State": {"Value": {"Fn::GetAtt": ["Stream", "State"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ms-stack", tmpl)
	assert.Equal(t, "my-metric-stream", outputs["Ref"])
	assert.NotEmpty(t, outputs["Arn"])
	assert.NotEmpty(t, outputs["State"])

	stream, err := backends.CloudWatch.Backend.GetMetricStream("my-metric-stream")
	require.NoError(t, err)
	assert.Equal(t, "json", stream.OutputFormat)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ms-stack")})
	require.NoError(t, err)

	_, err = backends.CloudWatch.Backend.GetMetricStream("my-metric-stream")
	require.Error(t, err)
}

func testCWAnomalyDetector(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "AD": {
    "Type": "AWS::CloudWatch::AnomalyDetector",
    "Properties": {
      "Namespace": "AWS/EC2",
      "MetricName": "CPUUtilization",
      "Stat": "Average",
      "Dimensions": [{"Name": "InstanceId", "Value": "i-1234567890abcdef0"}]
    }
  }
}
}`

	_ = createStackAndGetOutputs(t, client, "ad-stack", tmpl)

	page, err := backends.CloudWatch.Backend.DescribeAnomalyDetectors("AWS/EC2", "CPUUtilization", "", 0)
	require.NoError(t, err)
	require.Len(t, page.Data, 1)
	assert.Equal(t, "Average", page.Data[0].Stat)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ad-stack")})
	require.NoError(t, err)

	page, err = backends.CloudWatch.Backend.DescribeAnomalyDetectors("AWS/EC2", "CPUUtilization", "", 0)
	require.NoError(t, err)
	assert.Empty(t, page.Data)
}

func testCWInsightRule(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	const ruleBodyEscaped = `{\"Schema\":{\"Name\":\"CloudWatchLogRule\",\"Version\":1},` +
		`\"LogGroupNames\":[\"my-log-group\"],\"LogFormat\":\"JSON\",` +
		`\"Contribution\":{\"Keys\":[\"$.ip\"]},\"AggregateOn\":\"Count\"}`

	tmpl := `{
"Resources": {
  "IR": {
    "Type": "AWS::CloudWatch::InsightRule",
    "Properties": {
      "RuleName": "my-insight-rule",
      "RuleState": "ENABLED",
      "RuleBody": "` + ruleBodyEscaped + `"
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "IR"}},
  "RuleName": {"Value": {"Fn::GetAtt": ["IR", "RuleName"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ir-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "insight-rule/my-insight-rule")
	assert.Equal(t, "my-insight-rule", outputs["RuleName"])

	rule, err := backends.CloudWatch.Backend.GetInsightRule("my-insight-rule")
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", rule.State)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ir-stack")})
	require.NoError(t, err)

	_, err = backends.CloudWatch.Backend.GetInsightRule("my-insight-rule")
	require.Error(t, err)
}
