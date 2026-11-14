package codebuild

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

func TestReportTruncatedAtTestCaseLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cases     int
		wantCases int
		truncated bool
	}{
		{name: "at limit", cases: 500, wantCases: 500},
		{name: "over limit", cases: 501, wantCases: 500, truncated: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-east-1")
			content := reportContent{}

			for i := range tt.cases {
				content.tests = append(content.tests, TestCase{Name: fmt.Sprintf("t%d", i), Status: "SUCCEEDED"})
			}

			require.NoError(t, b.recordBuildReport(Build{ProjectName: "p"}, "g", reportTypeTest, content))

			arns := b.ListReports("")
			require.Len(t, arns, 1)

			reps, _ := b.BatchGetReports(arns)
			require.Len(t, reps, 1)
			assert.Equal(t, tt.truncated, reps[0].Truncated)
			assert.InDelta(t, reps[0].Created+30*24*3600, reps[0].Expired, 1)
			assert.EqualValues(t, tt.cases, reps[0].TestSummary.Total)

			cases, err := b.DescribeTestCases(arns[0], TestCaseQuery{})
			require.NoError(t, err)
			assert.Len(t, cases, tt.wantCases)
			assert.InDelta(t, reps[0].Expired, cases[0].Expired, 1)
		})
	}
}

func TestExpiredReportIsUnavailable(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1")
	rep := &Report{Arn: "arn:aws:codebuild:us-east-1:123456789012:report/g:1", Status: "SUCCEEDED",
		Expired: awstime.Epoch(time.Now().Add(-time.Minute))}
	b.AddReportInternal(rep)

	assert.Empty(t, b.ListReports(""))

	found, missing := b.BatchGetReports([]string{rep.Arn})
	assert.Empty(t, found)
	assert.Equal(t, []string{rep.Arn}, missing)

	_, err := b.DescribeTestCases(rep.Arn, TestCaseQuery{})
	require.ErrorIs(t, err, ErrNotFound)
}

func TestGetReportGroupTrend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		field    string
		wantAvg  string
		wantMax  string
		wantMin  string
		wantData []string
		num      int32
	}{
		{name: "total", field: "TOTAL", wantData: []string{"4", "10"}, wantAvg: "7", wantMax: "10", wantMin: "4"},
		{
			name: "pass rate", field: "PASS_RATE", wantData: []string{"50", "100"},
			wantAvg: "75", wantMax: "100", wantMin: "50",
		},
		{
			name: "duration", field: "DURATION", wantData: []string{"200", "100"},
			wantAvg: "150", wantMax: "200", wantMin: "100",
		},
		{
			name: "newest only", field: "TOTAL", num: 1, wantData: []string{"4"},
			wantAvg: "4", wantMax: "4", wantMin: "4",
		},
		{name: "coverage field on test reports", field: "LINE_COVERAGE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-east-1")
			rg, err := b.CreateReportGroup("g", reportTypeTest, ReportExportConfig{ExportConfigType: "NO_EXPORT"}, nil)
			require.NoError(t, err)

			now := awstime.Epoch(time.Now())
			b.AddReportInternal(&Report{
				Arn: "arn:aws:codebuild:us-east-1:123456789012:report/g:old", ReportGroupArn: rg.Arn, Created: now - 10,
				TestSummary: &TestReportSummary{
					Total: 10, DurationInNanoSeconds: 100, StatusCounts: map[string]int32{"SUCCEEDED": 10},
				},
			})
			b.AddReportInternal(&Report{
				Arn: "arn:aws:codebuild:us-east-1:123456789012:report/g:new", ReportGroupArn: rg.Arn, Created: now,
				TestSummary: &TestReportSummary{
					Total: 4, DurationInNanoSeconds: 200, StatusCounts: map[string]int32{"SUCCEEDED": 2, "FAILED": 2},
				},
			})

			res, err := b.GetReportGroupTrend(rg.Arn, tt.field, tt.num)
			require.NoError(t, err)

			var got []string
			for _, p := range res.RawData {
				got = append(got, p.Data)
			}

			assert.Equal(t, tt.wantData, got)
			assert.Equal(t, tt.wantAvg, res.Stats["average"])
			assert.Equal(t, tt.wantMax, res.Stats["max"])
			assert.Equal(t, tt.wantMin, res.Stats["min"])
		})
	}
}
