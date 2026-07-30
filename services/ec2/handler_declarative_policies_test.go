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

// TestDeclarativePoliciesReport_TagDualWritePathVisibility proves that
// declarative_policies.go's DeclarativePoliciesReport consolidated onto the
// shared tag store: a tag supplied at create time (TagSpecification) and a
// tag added afterwards via CreateTags are BOTH visible through
// DescribeDeclarativePoliciesReports AND through the generic DescribeTags
// call. Before the fix, the report carried its own embedded Tags field
// populated only at create time, invisible to a post-creation CreateTags
// call. (StartDeclarativePoliciesReportResponse itself only echoes the
// ReportId, not TagSet, matching the real API -- so the create-time tag is
// checked via Describe rather than on the create response.)
func TestDeclarativePoliciesReport_TagDualWritePathVisibility(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	startResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"StartDeclarativePoliciesReport"},
		"S3Bucket":                        []string{"my-bucket"},
		"TargetId":                        []string{"r-ab12"},
		"TagSpecification.1.ResourceType": []string{"declarative-policies-report"},
		"TagSpecification.1.Tag.1.Key":    []string{"CreateTime"},
		"TagSpecification.1.Tag.1.Value":  []string{"yes"},
	})
	require.NoError(t, err)
	reportID := accuracyExtractXMLValue(startResp, "reportId")
	require.NotEmpty(t, reportID)

	_, err = dispatchHandler(h, url.Values{
		"Action":       []string{"CreateTags"},
		"ResourceId.1": []string{reportID},
		"Tag.1.Key":    []string{"AddedLater"},
		"Tag.1.Value":  []string{"yes"},
	})
	require.NoError(t, err)

	describeResp, err := dispatchHandler(h, url.Values{"Action": []string{"DescribeDeclarativePoliciesReports"}})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "CreateTime")
	assert.Contains(t, describeResp, "AddedLater")

	tagsResp, err := dispatchHandler(h, url.Values{
		"Action":           []string{"DescribeTags"},
		"Filter.1.Name":    []string{"resource-id"},
		"Filter.1.Value.1": []string{reportID},
	})
	require.NoError(t, err)
	assert.Contains(t, tagsResp, "CreateTime")
	assert.Contains(t, tagsResp, "AddedLater")
}
