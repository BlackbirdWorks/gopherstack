package resourcegroupstaggingapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// TestDescribeReportCreation_NoReport verifies that, per real AWS behavior (see the
// aws-sdk-go-v2 DescribeReportCreationOutput.Status doc comment: "NO REPORT - No report
// was generated in the last 90 days"), calling DescribeReportCreation before any report
// has ever been generated in the region returns the literal "NO REPORT" status string --
// a real, documented AWS value, not an absent/nil field.
func TestDescribeReportCreation_NoReport(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	out := b.DescribeReportCreation(context.Background())

	require.NotNil(t, out)
	require.NotNil(t, out.Status, "Status must be \"NO REPORT\", not nil, when no report has ever been generated")
	assert.Equal(t, "NO REPORT", *out.Status)
	assert.Nil(t, out.S3Location)
	assert.Nil(t, out.StartDate)
}

func TestDescribeReportCreation_NoReportResponseBody(t *testing.T) {
	t.Parallel()

	// At the HTTP level the response must contain the real "NO REPORT" status string.
	h := resourcegroupstaggingapi.NewHandler(newBackend(t))
	rec := doTaggingRequest(t, h, "DescribeReportCreation", map[string]any{})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "NO REPORT",
		"\"NO REPORT\" is a real, documented AWS status value and must appear in the response")
}

// TestDescribeReportCreation_StaleReportBecomesNoReport verifies that a report older
// than reportStaleAfter (90 days) reports "NO REPORT" instead of its stale terminal
// status, matching real AWS's documented "no report generated in the last 90 days"
// behavior.
func TestDescribeReportCreation_StaleReportBecomesNoReport(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	start := time.Now()
	resourcegroupstaggingapi.SetClockFunc(b, func() time.Time { return start })

	_, err := b.StartReportCreation(context.Background(), &resourcegroupstaggingapi.StartReportCreationInput{
		S3Bucket: "bkt",
	})
	require.NoError(t, err)

	// Past the running window: report is SUCCEEDED.
	succeeded := start.Add(resourcegroupstaggingapi.ReportRunningDuration() + time.Second)
	resourcegroupstaggingapi.SetClockFunc(b, func() time.Time { return succeeded })

	out := b.DescribeReportCreation(context.Background())
	require.NotNil(t, out.Status)
	assert.Equal(t, "SUCCEEDED", *out.Status)

	// Past the 90-day staleness window: report reverts to NO REPORT.
	stale := start.Add(91 * 24 * time.Hour)
	resourcegroupstaggingapi.SetClockFunc(b, func() time.Time { return stale })

	out = b.DescribeReportCreation(context.Background())
	require.NotNil(t, out.Status)
	assert.Equal(t, "NO REPORT", *out.Status)
	assert.Nil(t, out.S3Location)
	assert.Nil(t, out.StartDate)
}

func TestStartReportCreationInput_HasNoS3BucketRegionField(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.StartReportCreation(context.Background(), &resourcegroupstaggingapi.StartReportCreationInput{
		S3Bucket: "cross-region-bucket",
	})

	require.NoError(t, err)
	assert.Equal(t, "RUNNING", resourcegroupstaggingapi.ReportStatus(b))
	assert.Contains(t, resourcegroupstaggingapi.ReportS3Location(b), "cross-region-bucket")

	// A request body containing S3BucketRegion (as some non-AWS client might send) must
	// simply be ignored as an unknown field, not silently accepted as a modeled member.
	marshaled, err := json.Marshal(resourcegroupstaggingapi.StartReportCreationInput{S3Bucket: "b"})
	require.NoError(t, err)
	assert.NotContains(t, string(marshaled), "S3BucketRegion")
}

// TestReportCreation_FullLifecycle exercises StartReportCreation/DescribeReportCreation
// end to end: an empty S3Bucket is rejected, and a valid one transitions RUNNING →
// SUCCEEDED once reportRunningDuration has elapsed.
func TestReportCreation_FullLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bucket      string
		wantS3Parts []string
		wantErr     bool
	}{
		{
			name:        "valid_bucket",
			bucket:      "my-report-bucket",
			wantS3Parts: []string{"my-report-bucket", "AwsTagPolicies", "report.csv"},
		},
		{
			name:    "empty_bucket",
			bucket:  "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.StartReportCreation(context.Background(), &resourcegroupstaggingapi.StartReportCreationInput{
				S3Bucket: tt.bucket,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			// Immediately after Start the report is RUNNING.
			assert.Equal(t, "RUNNING", resourcegroupstaggingapi.ReportStatus(b))

			// Advance clock past the running window so DescribeReportCreation returns SUCCEEDED.
			fastForward := time.Now().Add(resourcegroupstaggingapi.ReportRunningDuration() + time.Second)
			resourcegroupstaggingapi.SetClockFunc(b, func() time.Time { return fastForward })

			desc := b.DescribeReportCreation(context.Background())
			require.NotNil(t, desc.Status)
			assert.Equal(t, "SUCCEEDED", *desc.Status)
			require.NotNil(t, desc.S3Location)

			for _, part := range tt.wantS3Parts {
				assert.Contains(t, *desc.S3Location, part)
			}

			require.NotNil(t, desc.StartDate)
			assert.NotEmpty(t, *desc.StartDate)
		})
	}
}

func TestStartReportCreation_SetsRunningState(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.StartReportCreation(context.Background(), &resourcegroupstaggingapi.StartReportCreationInput{
		S3Bucket: "my-bucket",
	})

	require.NoError(t, err)
	assert.Equal(t, "RUNNING", resourcegroupstaggingapi.ReportStatus(b))
}

func TestStartReportCreation_SetsS3Location(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.StartReportCreation(
		context.Background(),
		&resourcegroupstaggingapi.StartReportCreationInput{S3Bucket: "report-bucket"},
	)

	require.NoError(t, err)
	assert.Equal(t, "s3://report-bucket/AwsTagPolicies/report.csv", resourcegroupstaggingapi.ReportS3Location(b))
}

func TestStartReportCreation_TimestampFromNowFunc(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	resourcegroupstaggingapi.SetNowFunc(b, func() string { return "2025-12-31T23:59:59Z" })

	_, err := b.StartReportCreation(
		context.Background(),
		&resourcegroupstaggingapi.StartReportCreationInput{S3Bucket: "bkt"},
	)
	require.NoError(t, err)

	h := resourcegroupstaggingapi.NewHandler(b)
	rec := doTaggingRequest(t, h, "DescribeReportCreation", map[string]any{})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "2025-12-31T23:59:59Z")
}

func TestStartReportCreation_ConcurrentModification(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// First report starts successfully.
	_, err := b.StartReportCreation(context.Background(), &resourcegroupstaggingapi.StartReportCreationInput{
		S3Bucket: "bucket-one",
	})
	require.NoError(t, err)
	require.Equal(t, "RUNNING", resourcegroupstaggingapi.ReportStatus(b))

	// Second request while RUNNING (clock not advanced) must fail.
	_, err = b.StartReportCreation(context.Background(), &resourcegroupstaggingapi.StartReportCreationInput{
		S3Bucket: "bucket-two",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, resourcegroupstaggingapi.ErrConcurrentModification)

	// S3 location unchanged — first report state still active.
	assert.Contains(t, resourcegroupstaggingapi.ReportS3Location(b), "bucket-one")
}

// TestStartReportCreation_SucceedsAfterPreviousCompletes verifies that once a RUNNING
// report's window elapses, a new StartReportCreation call succeeds, replacing the
// completed report's state with the new one.
func TestStartReportCreation_SucceedsAfterPreviousCompletes(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.StartReportCreation(context.Background(), &resourcegroupstaggingapi.StartReportCreationInput{
		S3Bucket: "first-bucket",
	})
	require.NoError(t, err)

	// Advance clock past running window — first report is now SUCCEEDED.
	done := time.Now().Add(resourcegroupstaggingapi.ReportRunningDuration() + time.Second)
	resourcegroupstaggingapi.SetClockFunc(b, func() time.Time { return done })

	// Second report can now be started.
	_, err = b.StartReportCreation(context.Background(), &resourcegroupstaggingapi.StartReportCreationInput{
		S3Bucket: "second-bucket",
	})
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", resourcegroupstaggingapi.ReportStatus(b))
	assert.Contains(t, resourcegroupstaggingapi.ReportS3Location(b), "second-bucket")
}

func TestDescribeReportCreation_RunningState(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.StartReportCreation(context.Background(), &resourcegroupstaggingapi.StartReportCreationInput{
		S3Bucket: "bkt",
	})
	require.NoError(t, err)

	// Without clock advance, report stays RUNNING.
	out := b.DescribeReportCreation(context.Background())

	require.NotNil(t, out)
	require.NotNil(t, out.Status)
	assert.Equal(t, "RUNNING", *out.Status)
	assert.NotNil(t, out.S3Location)
	assert.NotNil(t, out.StartDate)
}

// TestDescribeReportCreation_TransitionsToSucceeded verifies the RUNNING→SUCCEEDED
// transition and that the SUCCEEDED status persists across a second describe call.
func TestDescribeReportCreation_TransitionsToSucceeded(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.StartReportCreation(context.Background(), &resourcegroupstaggingapi.StartReportCreationInput{
		S3Bucket: "my-bucket",
	})
	require.NoError(t, err)

	// Advance clock past running duration.
	fastForward := time.Now().Add(resourcegroupstaggingapi.ReportRunningDuration() + time.Second)
	resourcegroupstaggingapi.SetClockFunc(b, func() time.Time { return fastForward })

	out := b.DescribeReportCreation(context.Background())
	require.NotNil(t, out)
	require.NotNil(t, out.Status)
	assert.Equal(t, "SUCCEEDED", *out.Status)

	// Second call also returns SUCCEEDED (state persists).
	out2 := b.DescribeReportCreation(context.Background())
	require.NotNil(t, out2.Status)
	assert.Equal(t, "SUCCEEDED", *out2.Status)
}

func TestDescribeReportCreation_ExactBoundary(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	start := time.Now()
	resourcegroupstaggingapi.SetClockFunc(b, func() time.Time { return start })

	_, err := b.StartReportCreation(context.Background(), &resourcegroupstaggingapi.StartReportCreationInput{
		S3Bucket: "bkt",
	})
	require.NoError(t, err)

	// At exactly startedAt + duration, the report transitions to SUCCEEDED.
	atBoundary := start.Add(resourcegroupstaggingapi.ReportRunningDuration())
	resourcegroupstaggingapi.SetClockFunc(b, func() time.Time { return atBoundary })

	out := b.DescribeReportCreation(context.Background())
	require.NotNil(t, out.Status)
	assert.Equal(t, "SUCCEEDED", *out.Status)
}

func TestDescribeReportCreationJSONShape(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	resourcegroupstaggingapi.AddReportStateInternal(b, "SUCCEEDED", "s3://bkt/path.csv", "2025-01-01T00:00:00Z")

	h := resourcegroupstaggingapi.NewHandler(b)
	rec := doTaggingRequest(t, h, "DescribeReportCreation", map[string]any{})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Equal(t, "SUCCEEDED", out["Status"])
	assert.Equal(t, "s3://bkt/path.csv", out["S3Location"])
	assert.Equal(t, "2025-01-01T00:00:00Z", out["StartDate"])
}

func TestErrConcurrentModification_IsDistinct(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t, resourcegroupstaggingapi.ErrConcurrentModification, resourcegroupstaggingapi.ErrValidation)
	assert.NotEqual(t, resourcegroupstaggingapi.ErrConcurrentModification, resourcegroupstaggingapi.ErrMissingS3Bucket)
	assert.Contains(t, resourcegroupstaggingapi.ErrConcurrentModification.Error(), "ConcurrentModificationException")
}
