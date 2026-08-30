package cloudformation_test

import (
	"encoding/xml"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeEvents_FailedEventsFilter locks in DescribeEventsInput's
// Filters.FailedEvents member (cloudformation@v1.76.1 api_op_DescribeEvents.go,
// types.EventFilter) -- handleDescribeEvents previously read only StackName
// and NextToken, so Filters.FailedEvents=true silently returned every event
// (successes included) instead of only the failed ones.
func TestDescribeEvents_FailedEventsFilter(t *testing.T) {
	t.Parallel()

	h := newHandler()
	failTemplate := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"Bucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {"BucketName": {"Fn::ImportValue": "nonexistent-export"}}
			}
		}
	}`
	postFormValues(t, h, url.Values{
		"Action":       {"CreateStack"},
		"StackName":    {"failed-events-stack"},
		"TemplateBody": {failTemplate},
		"OnFailure":    {"DO_NOTHING"},
	}).mustOK(t)

	type eventXML struct {
		Status string `xml:"ResourceStatus"`
	}
	type describeResponse struct {
		XMLName xml.Name `xml:"DescribeEventsResponse"`
		Result  struct {
			StackEvents []eventXML `xml:"StackEvents>member"`
		} `xml:"DescribeEventsResult"`
	}

	resp := postFormValues(t, h, url.Values{
		"Action":               {"DescribeEvents"},
		"StackName":            {"failed-events-stack"},
		"Filters.FailedEvents": {"true"},
	})
	resp.mustOK(t)

	var out describeResponse
	require.NoError(t, xml.Unmarshal([]byte(resp.Body), &out))
	require.NotEmpty(t, out.Result.StackEvents, "the fixture must produce at least one failed event")
	for _, e := range out.Result.StackEvents {
		assert.Contains(t, e.Status, "FAILED")
	}
}
