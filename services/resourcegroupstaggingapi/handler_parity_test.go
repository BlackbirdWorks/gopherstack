package resourcegroupstaggingapi_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// ======================================================================
// GetTagValues — handler-level Key validation
// ======================================================================

func TestHandler_GetTagValues_NilKey_Returns400(t *testing.T) {
	t.Parallel()

	h := resourcegroupstaggingapi.NewHandler(newBackend(t))
	rec := doTaggingRequest(t, h, "GetTagValues", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
	assert.Contains(t, rec.Body.String(), "Key is required")
}

func TestHandler_GetTagValues_EmptyKey_Returns400(t *testing.T) {
	t.Parallel()

	h := resourcegroupstaggingapi.NewHandler(newBackend(t))
	rec := doTaggingRequest(t, h, "GetTagValues", map[string]any{"Key": ""})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

func TestHandler_GetTagValues_ValidKey_Returns200(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "prod"},
		},
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q2",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "dev"},
		},
	})
	h := resourcegroupstaggingapi.NewHandler(b)
	rec := doTaggingRequest(t, h, "GetTagValues", map[string]any{"Key": "env"})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "dev")
	assert.Contains(t, rec.Body.String(), "prod")
}

// ======================================================================
// GetComplianceSummary — GroupBy validation
// ======================================================================

func TestHandler_GetComplianceSummary_InvalidGroupBy_Returns400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		groupBy []string
	}{
		{name: "unknown_value", groupBy: []string{"INVALID"}},
		{name: "lowercase_region", groupBy: []string{"region"}},
		{name: "mixed_valid_invalid", groupBy: []string{"REGION", "INVALID_VALUE"}},
		{name: "empty_string", groupBy: []string{""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := resourcegroupstaggingapi.NewHandler(newBackend(t))
			rec := doTaggingRequest(t, h, "GetComplianceSummary", map[string]any{"GroupBy": tt.groupBy})

			assert.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
			assert.Contains(t, rec.Body.String(), "ValidationException")
		})
	}
}

func TestHandler_GetComplianceSummary_ValidGroupBy_Returns200(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		groupBy []string
	}{
		{name: "TARGET_ID", groupBy: []string{"TARGET_ID"}},
		{name: "REGION", groupBy: []string{"REGION"}},
		{name: "RESOURCE_TYPE", groupBy: []string{"RESOURCE_TYPE"}},
		{name: "multi", groupBy: []string{"REGION", "RESOURCE_TYPE"}},
		{name: "empty", groupBy: []string{}},
		{name: "nil", groupBy: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := resourcegroupstaggingapi.NewHandler(newBackend(t))
			body := map[string]any{}
			if tt.groupBy != nil {
				body["GroupBy"] = tt.groupBy
			}

			rec := doTaggingRequest(t, h, "GetComplianceSummary", body)

			assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
			assert.Contains(t, rec.Body.String(), "SummaryList")
		})
	}
}

// ======================================================================
// TagResources — aws: reserved prefix validation
// ======================================================================

func TestBackend_TagResources_AwsReservedPrefix_Returns400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{name: "aws_colon_prefix", tags: map[string]string{"aws:reserved": "value"}},
		{name: "aws_colon_prefix_long", tags: map[string]string{"aws:ec2:autoscaling": "yes"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
				ResourceARNList: []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
				Tags:            tt.tags,
			})

			require.Error(t, err)
			require.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
			assert.Contains(t, err.Error(), "reserved prefix")
		})
	}
}

func TestBackend_TagResources_NormalTagKey_OK(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	arn := "arn:aws:sqs:us-east-1:000000000000:q1"
	handled := false
	b.RegisterARNTagger(func(_ context.Context, a string, _ map[string]string) (bool, error) {
		if a == arn {
			handled = true

			return true, nil
		}

		return false, nil
	})

	_, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{arn},
		Tags:            map[string]string{"env": "prod", "team": "platform"},
	})

	require.NoError(t, err)
	assert.True(t, handled)
}

// ======================================================================
// StartReportCreation — RUNNING state + ConcurrentModificationException
// ======================================================================

func TestBackend_StartReportCreation_SetsRunningState(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.StartReportCreation(context.Background(), &resourcegroupstaggingapi.StartReportCreationInput{
		S3Bucket: "my-bucket",
	})

	require.NoError(t, err)
	assert.Equal(t, "RUNNING", resourcegroupstaggingapi.ReportStatus(b))
}

func TestBackend_StartReportCreation_ConcurrentModification(t *testing.T) {
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

func TestHandler_StartReportCreation_ConcurrentModification_Returns409(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	h := resourcegroupstaggingapi.NewHandler(b)

	// Start first report successfully.
	rec := doTaggingRequest(t, h, "StartReportCreation", map[string]any{"S3Bucket": "first-bucket"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Concurrent attempt returns 409.
	rec = doTaggingRequest(t, h, "StartReportCreation", map[string]any{"S3Bucket": "second-bucket"})
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "ConcurrentModificationException")
}

func TestBackend_StartReportCreation_SucceedsAfterPreviousCompletes(t *testing.T) {
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

// ======================================================================
// DescribeReportCreation — RUNNING→SUCCEEDED lifecycle
// ======================================================================

func TestBackend_DescribeReportCreation_RunningState(t *testing.T) {
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

func TestBackend_DescribeReportCreation_TransitionsToSucceeded(t *testing.T) {
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

func TestBackend_DescribeReportCreation_ExactBoundary(t *testing.T) {
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

// ======================================================================
// StartReportCreation — S3BucketRegion field
// ======================================================================

func TestBackend_StartReportCreation_S3BucketRegion_Accepted(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	region := "eu-west-1"
	_, err := b.StartReportCreation(context.Background(), &resourcegroupstaggingapi.StartReportCreationInput{
		S3Bucket:       "cross-region-bucket",
		S3BucketRegion: &region,
	})

	require.NoError(t, err)
	assert.Equal(t, "RUNNING", resourcegroupstaggingapi.ReportStatus(b))
	assert.Contains(t, resourcegroupstaggingapi.ReportS3Location(b), "cross-region-bucket")
}

// ======================================================================
// TagResources / UntagResources — HTTP status codes in FailedResourcesMap
// ======================================================================

func TestBackend_TagResources_UnhandledARN_Returns400InMap(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	out, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:000000000000:unregistered-queue"},
		Tags:            map[string]string{"key": "val"},
	})

	require.NoError(t, err)
	require.NotNil(t, out.FailedResourcesMap)
	entry := out.FailedResourcesMap["arn:aws:sqs:us-east-1:000000000000:unregistered-queue"]
	assert.Equal(t, http.StatusBadRequest, entry.StatusCode)
	assert.Equal(t, "InvalidParameterException", entry.ErrorCode)
}

func TestBackend_UntagResources_UnhandledARN_Returns400InMap(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	out, err := b.UntagResources(context.Background(), &resourcegroupstaggingapi.UntagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:000000000000:unregistered-queue"},
		TagKeys:         []string{"env"},
	})

	require.NoError(t, err)
	require.NotNil(t, out.FailedResourcesMap)
	entry := out.FailedResourcesMap["arn:aws:sqs:us-east-1:000000000000:unregistered-queue"]
	assert.Equal(t, http.StatusBadRequest, entry.StatusCode)
}

func TestBackend_TagResources_TaggerInternalError_Returns500InMap(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	arn := "arn:aws:sqs:us-east-1:000000000000:q1"
	b.RegisterARNTagger(func(_ context.Context, a string, _ map[string]string) (bool, error) {
		if a == arn {
			return true, assert.AnError
		}

		return false, nil
	})

	out, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{arn},
		Tags:            map[string]string{"key": "val"},
	})

	require.NoError(t, err)
	require.NotNil(t, out.FailedResourcesMap)
	entry := out.FailedResourcesMap[arn]
	assert.Equal(t, http.StatusInternalServerError, entry.StatusCode)
	assert.Equal(t, "InternalServiceException", entry.ErrorCode)
}

// ======================================================================
// ErrConcurrentModification — error identity
// ======================================================================

func TestErrConcurrentModification_IsDistinct(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t, resourcegroupstaggingapi.ErrConcurrentModification, resourcegroupstaggingapi.ErrValidation)
	assert.NotEqual(t, resourcegroupstaggingapi.ErrConcurrentModification, resourcegroupstaggingapi.ErrMissingS3Bucket)
	assert.Contains(t, resourcegroupstaggingapi.ErrConcurrentModification.Error(), "ConcurrentModificationException")
}
