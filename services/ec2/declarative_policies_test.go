package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestDeclarativePoliciesReport_Lifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	t.Run("start requires bucket and target", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.StartDeclarativePoliciesReport("", "r-ab12", "", nil)
		require.Error(t, err)

		_, err = b.StartDeclarativePoliciesReport("my-bucket", "", "", nil)
		require.Error(t, err)
	})

	var reportID string

	t.Run("start creates a running report", func(t *testing.T) { //nolint:paralleltest // existing issue.
		report, err := b.StartDeclarativePoliciesReport(
			"my-bucket",
			"r-ab12",
			"prefix/",
			map[string]string{"Name": "test"},
		)
		require.NoError(t, err)
		assert.NotEmpty(t, report.ReportID)
		assert.Equal(t, "running", report.Status)
		assert.Equal(t, "my-bucket", report.S3Bucket)
		assert.Equal(t, "test", report.Tags["Name"])
		reportID = report.ReportID
	})

	t.Run("describe settles running report to complete", func(t *testing.T) { //nolint:paralleltest // existing issue.
		reports := b.DescribeDeclarativePoliciesReports([]string{reportID})
		require.Len(t, reports, 1)
		assert.Equal(t, "complete", reports[0].Status)
		assert.False(t, reports[0].EndTime.IsZero())
	})

	t.Run("get summary reports account counts", func(t *testing.T) { //nolint:paralleltest // existing issue.
		summary, err := b.GetDeclarativePoliciesReportSummary(reportID)
		require.NoError(t, err)
		assert.Equal(t, reportID, summary.ReportID)
		assert.Equal(t, "r-ab12", summary.TargetID)
		assert.Equal(t, int32(1), summary.NumberOfAccounts)
	})

	t.Run("get summary unknown report errors", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.GetDeclarativePoliciesReportSummary("report-doesnotexist")
		require.Error(t, err)
	})

	t.Run("cancel a completed report errors", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CancelDeclarativePoliciesReport(reportID)
		require.Error(t, err)
	})

	t.Run("cancel a running report succeeds", func(t *testing.T) { //nolint:paralleltest // existing issue.
		report, err := b.StartDeclarativePoliciesReport("my-bucket", "111122223333", "", nil)
		require.NoError(t, err)

		ok, err := b.CancelDeclarativePoliciesReport(report.ReportID)
		require.NoError(t, err)
		assert.True(t, ok)

		reports := b.DescribeDeclarativePoliciesReports([]string{report.ReportID})
		require.Len(t, reports, 1)
		assert.Equal(t, "cancelled", reports[0].Status)
	})

	t.Run("cancel unknown report errors", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CancelDeclarativePoliciesReport("report-doesnotexist")
		require.Error(t, err)
	})
}
