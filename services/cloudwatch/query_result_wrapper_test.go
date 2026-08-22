package cloudwatch_test

import (
	"encoding/xml"
	"net/url"
	"strings"
	"testing"

	smithyxml "github.com/aws/smithy-go/encoding/xml"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// decodeQueryResultWrapper replicates the exact lookup aws-sdk-go-v2's
// generated awsAwsquery deserializers perform (e.g.
// cloudformation@v1.76.1/deserializers.go:1548-1557): fetch the root
// <XxxResponse> element, wrap it, then GetElement(wrapper). Real terraform
// destroy runs hit this codepath and fail with "<wrapper> node not found"
// when gopherstack omits the Result element -- a raw substring match on the
// response body cannot catch that, because the broken output still contains
// the substring "XxxResponse".
// putMetricStreamForm builds a minimal valid PutMetricStream query body for
// the given stream name.
func putMetricStreamForm(name string) string {
	return "Action=PutMetricStream&Name=" + name +
		"&FirehoseArn=arn:aws:firehose:us-east-1:123456789012:deliverystream/x" +
		"&RoleArn=arn:aws:iam::123456789012:role/x&OutputFormat=json"
}

func decodeQueryResultWrapper(t *testing.T, body []byte, wrapper string) error {
	t.Helper()

	decoder := xml.NewDecoder(strings.NewReader(string(body)))
	root, err := smithyxml.FetchRootElement(decoder)
	require.NoError(t, err, "response must have a root element")

	node := smithyxml.WrapNodeDecoder(decoder, root)
	_, err = node.GetElement(wrapper)

	return err
}

// TestCloudWatchQueryProtocol_ResultWrapperPresent covers gopherstack-jodk
// bug 2's family: every cloudwatch query/XML op whose AWS output shape is
// declared (verified against botocore's cloudwatch service-2.json
// resultWrapper/output keys) must wrap its data -- even when the shape has
// zero members -- in a <XxxResult> element, because AWS's query protocol
// server always emits it whenever an output shape exists. Ops with NO output
// shape at all (e.g. DeleteAlarms, SetAlarmState, PutMetricData) correctly
// have no wrapper and are not covered here.
func TestCloudWatchQueryProtocol_ResultWrapperPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, h *cloudwatch.Handler)
		name    string
		body    string
		wrapper string
	}{
		{
			name: "deletedashboards",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, `Action=PutDashboard&DashboardName=jodk-board&DashboardBody={}`)
			},
			body:    "Action=DeleteDashboards&DashboardNames.member.1=jodk-board",
			wrapper: "DeleteDashboardsResult",
		},
		{
			name: "putinsightrule",
			body: "Action=PutInsightRule&RuleName=jodk-rule&RuleDefinition=" +
				url.QueryEscape(validInsightRuleDefinition),
			wrapper: "PutInsightRuleResult",
		},
		{
			name: "deletemetricstream",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, putMetricStreamForm("jodk-stream"))
			},
			body:    "Action=DeleteMetricStream&Name=jodk-stream",
			wrapper: "DeleteMetricStreamResult",
		},
		{
			name:    "putmetricstream",
			body:    putMetricStreamForm("jodk-stream-put"),
			wrapper: "PutMetricStreamResult",
		},
		{
			name: "startmetricstreams",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, putMetricStreamForm("jodk-stream2"))
			},
			body:    "Action=StartMetricStreams&Names.member.1=jodk-stream2",
			wrapper: "StartMetricStreamsResult",
		},
		{
			name: "stopmetricstreams",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, putMetricStreamForm("jodk-stream3"))
			},
			body:    "Action=StopMetricStreams&Names.member.1=jodk-stream3",
			wrapper: "StopMetricStreamsResult",
		},
		{
			name: "deleteanomalydetector",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, "Action=PutAnomalyDetector&Namespace=AWS/EC2&MetricName=CPUUtilization&Stat=Average")
			},
			body:    "Action=DeleteAnomalyDetector&Namespace=AWS/EC2&MetricName=CPUUtilization&Stat=Average",
			wrapper: "DeleteAnomalyDetectorResult",
		},
		{
			name:    "putanomalydetector",
			body:    "Action=PutAnomalyDetector&Namespace=AWS/EC2&MetricName=DiskReadBytes&Stat=Average",
			wrapper: "PutAnomalyDetectorResult",
		},
		{
			name: "tagresource",
			body: "Action=TagResource&ResourceARN=arn:aws:cloudwatch:us-east-1:123456789012:alarm:jodk" +
				"&Tags.member.1.Key=k&Tags.member.1.Value=v",
			wrapper: "TagResourceResult",
		},
		{
			name: "untagresource",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, "Action=TagResource&ResourceARN=arn:aws:cloudwatch:us-east-1:123456789012:alarm:jodk2"+
					"&Tags.member.1.Key=k&Tags.member.1.Value=v")
			},
			body: "Action=UntagResource&ResourceARN=arn:aws:cloudwatch:us-east-1:123456789012:alarm:jodk2" +
				"&TagKeys.member.1=k",
			wrapper: "UntagResourceResult",
		},
		{
			name:    "startotelenrichment",
			body:    "Action=StartOTelEnrichment",
			wrapper: "StartOTelEnrichmentResult",
		},
		{
			name:    "stopotelenrichment",
			body:    "Action=StopOTelEnrichment",
			wrapper: "StopOTelEnrichmentResult",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newCWHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.body)
			require.Equal(t, 200, rec.Code, rec.Body.String())

			err := decodeQueryResultWrapper(t, rec.Body.Bytes(), tt.wrapper)
			require.NoError(t, err, "real query/XML deserializer must find <%s>", tt.wrapper)
		})
	}
}
