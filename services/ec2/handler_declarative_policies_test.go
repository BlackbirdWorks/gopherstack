package ec2_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeclarativePoliciesReport_HTTP_Lifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	startResp, err := dispatchHandler(h, url.Values{
		"Action":   []string{"StartDeclarativePoliciesReport"},
		"S3Bucket": []string{"my-bucket"},
		"TargetId": []string{"r-ab12"},
	})
	require.NoError(t, err)
	assert.Contains(t, startResp, "<StartDeclarativePoliciesReportResponse")
	assert.NotContains(t, startResp, "StubResponse")
	reportID := accuracyExtractXMLValue(startResp, "reportId")
	require.NotEmpty(t, reportID)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":     []string{"DescribeDeclarativePoliciesReports"},
		"ReportId.1": []string{reportID},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<status>complete</status>")

	summaryResp, err := dispatchHandler(h, url.Values{
		"Action":   []string{"GetDeclarativePoliciesReportSummary"},
		"ReportId": []string{reportID},
	})
	require.NoError(t, err)
	assert.Contains(t, summaryResp, "<GetDeclarativePoliciesReportSummaryResponse")
	assert.Contains(t, summaryResp, reportID)

	// Cancel after settling to complete should fail, not silently succeed as a stub would.
	_, err = dispatchHandler(h, url.Values{
		"Action":   []string{"CancelDeclarativePoliciesReport"},
		"ReportId": []string{reportID},
	})
	require.Error(t, err)

	start2Resp, err := dispatchHandler(h, url.Values{
		"Action":   []string{"StartDeclarativePoliciesReport"},
		"S3Bucket": []string{"my-bucket"},
		"TargetId": []string{"111122223333"},
	})
	require.NoError(t, err)
	reportID2 := accuracyExtractXMLValue(start2Resp, "reportId")

	cancelResp, err := dispatchHandler(h, url.Values{
		"Action":   []string{"CancelDeclarativePoliciesReport"},
		"ReportId": []string{reportID2},
	})
	require.NoError(t, err)
	assert.Contains(t, cancelResp, "<CancelDeclarativePoliciesReportResponse")
	assert.Contains(t, cancelResp, "<return>true</return>")
}
