package ec2_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNetworkPerformance_HTTP_Lifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	enableResp, err := dispatchHandler(h, url.Values{
		"Action":      []string{"EnableAwsNetworkPerformanceMetricSubscription"},
		"Source":      []string{"us-east-1"},
		"Destination": []string{"us-east-2"},
	})
	require.NoError(t, err)
	assert.Contains(t, enableResp, "<EnableAwsNetworkPerformanceMetricSubscriptionResponse")
	assert.NotContains(t, enableResp, "StubResponse")
	assert.Contains(t, enableResp, "<output>true</output>")

	describeResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeAwsNetworkPerformanceMetricSubscriptions"},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<source>us-east-1</source>")
	assert.Contains(t, describeResp, "<destination>us-east-2</destination>")

	dataResp, err := dispatchHandler(h, url.Values{
		"Action":                  []string{"GetAwsNetworkPerformanceData"},
		"DataQuery.1.Source":      []string{"us-east-1"},
		"DataQuery.1.Destination": []string{"us-east-2"},
	})
	require.NoError(t, err)
	assert.Contains(t, dataResp, "<GetAwsNetworkPerformanceDataResponse")
	assert.Contains(t, dataResp, "<metricPointSet>")

	disableResp, err := dispatchHandler(h, url.Values{
		"Action":      []string{"DisableAwsNetworkPerformanceMetricSubscription"},
		"Source":      []string{"us-east-1"},
		"Destination": []string{"us-east-2"},
	})
	require.NoError(t, err)
	assert.Contains(t, disableResp, "<DisableAwsNetworkPerformanceMetricSubscriptionResponse")
	assert.Contains(t, disableResp, "<output>true</output>")

	describeAfterResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeAwsNetworkPerformanceMetricSubscriptions"},
	})
	require.NoError(t, err)
	assert.NotContains(t, describeAfterResp, "<source>us-east-1</source>")
}
