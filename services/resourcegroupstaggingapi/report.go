package resourcegroupstaggingapi

import (
	"context"
	"errors"
	"time"
)

// ErrMissingS3Bucket is returned when StartReportCreation is called without an S3 bucket.
var ErrMissingS3Bucket = errors.New("S3Bucket is required")

// ErrConcurrentModification is returned when StartReportCreation is called while a report
// is still running. AWS requires waiting for the current report to finish.
var ErrConcurrentModification = errors.New("ConcurrentModificationException")

// reportStatusRunning is the status for a report job that is currently running.
const reportStatusRunning = "RUNNING"

// reportStatusSucceeded is the status for a successfully created report.
const reportStatusSucceeded = "SUCCEEDED"

// reportStatusNoReport is the status DescribeReportCreation returns when no report has
// ever been generated for the region, or none in the last reportStaleAfter window. This
// is a real, documented AWS status value -- not a gopherstack invention -- per the
// aws-sdk-go-v2 DescribeReportCreationOutput.Status doc comment: "NO REPORT - No report
// was generated in the last 90 days".
const reportStatusNoReport = "NO REPORT"

// reportStaleAfter is how long a non-RUNNING report state remains reportable before
// DescribeReportCreation reports reportStatusNoReport instead, matching the real API's
// documented "no report in the last 90 days" behavior.
const reportStaleAfter = 90 * 24 * time.Hour

// reportS3PathTemplate is the S3 path template for generated reports.
const reportS3PathTemplate = "AwsTagPolicies/report.csv"

// reportRunningDuration is the simulated time a report stays in RUNNING state before
// automatically transitioning to SUCCEEDED. AWS reports typically complete in 5-15 minutes;
// the in-memory backend uses a 30-second window to keep tests fast.
const reportRunningDuration = 30 * time.Second

// reportCreationState holds the state of a StartReportCreation job.
// Region is tagged json:"-" -- it exists solely so store.Table's keyFn (see
// store_setup.go) has something to key on, since nothing in the AWS wire
// shape needs a report state to carry its own region (report state is
// always looked up by request-scoped region, never serialized directly).
type reportCreationState struct {
	startedAt  time.Time
	Region     string `json:"-"`
	S3Location string `json:"s3Location"`
	StartDate  string `json:"startDate"`
	Status     string `json:"status"`
}

// StartReportCreationInput is the request payload for StartReportCreation.
// The real AWS API (aws-sdk-go-v2/service/resourcegroupstaggingapi StartReportCreationInput)
// has no S3BucketRegion member -- only S3Bucket -- so no field for it is modeled here either.
type StartReportCreationInput struct {
	// S3Bucket is the Amazon S3 bucket to store the report in.
	S3Bucket string `json:"S3Bucket"`
}

// StartReportCreationOutput is the response payload for StartReportCreation.
type StartReportCreationOutput struct{}

// StartReportCreation records a new report creation request.
// The report begins in RUNNING state and transitions to SUCCEEDED after reportRunningDuration
// as observed through DescribeReportCreation. AWS rejects a new request when a report is
// currently RUNNING (ConcurrentModificationException).
func (b *InMemoryBackend) StartReportCreation(
	ctx context.Context,
	input *StartReportCreationInput,
) (*StartReportCreationOutput, error) {
	if input.S3Bucket == "" {
		return nil, ErrMissingS3Bucket
	}

	b.mu.Lock("StartReportCreation")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)
	now := b.clockFunc()

	// Reject concurrent report creation while a previous report is still running.
	if state, ok := b.reportStates.Get(region); ok &&
		state.Status == reportStatusRunning &&
		now.Before(state.startedAt.Add(reportRunningDuration)) {
		return nil, ErrConcurrentModification
	}

	b.reportStates.Put(&reportCreationState{
		Region:     region,
		S3Location: "s3://" + input.S3Bucket + "/" + reportS3PathTemplate,
		StartDate:  b.now(),
		Status:     reportStatusRunning,
		startedAt:  now,
	})

	return &StartReportCreationOutput{}, nil
}

// DescribeReportCreationInput is the request payload for DescribeReportCreation.
type DescribeReportCreationInput struct{}

// DescribeReportCreationOutput is the response payload for DescribeReportCreation.
type DescribeReportCreationOutput struct {
	// ErrorMessage is set when Status is FAILED.
	ErrorMessage *string `json:"ErrorMessage,omitempty"`
	// S3Location is the path to the report in the S3 bucket.
	S3Location *string `json:"S3Location,omitempty"`
	// StartDate is the date and time that the report was started.
	StartDate *string `json:"StartDate,omitempty"`
	// Status is the current status of the report (RUNNING, SUCCEEDED, FAILED). Nil when no report exists.
	Status *string `json:"Status"`
}

// DescribeReportCreation returns the status of the most recent StartReportCreation operation.
// A RUNNING report transitions to SUCCEEDED once reportRunningDuration has elapsed.
// When no report has ever been started for the region, or the most recent non-RUNNING
// report is older than reportStaleAfter, Status is reportStatusNoReport ("NO REPORT") --
// a real, documented AWS status value, not an absent/nil field.
func (b *InMemoryBackend) DescribeReportCreation(ctx context.Context) *DescribeReportCreationOutput {
	b.mu.Lock("DescribeReportCreation")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	state, ok := b.reportStates.Get(region)
	if !ok {
		noReport := reportStatusNoReport

		return &DescribeReportCreationOutput{Status: &noReport}
	}

	now := b.clockFunc()

	// Transition RUNNING → SUCCEEDED once the simulated run duration has elapsed.
	if state.Status == reportStatusRunning && !now.Before(state.startedAt.Add(reportRunningDuration)) {
		state.Status = reportStatusSucceeded
	}

	// A non-RUNNING report older than reportStaleAfter is reported as NO REPORT, per
	// real AWS's documented "no report generated in the last 90 days" behavior.
	if state.Status != reportStatusRunning && now.After(state.startedAt.Add(reportStaleAfter)) {
		noReport := reportStatusNoReport

		return &DescribeReportCreationOutput{Status: &noReport}
	}

	s3Loc := state.S3Location
	startDate := state.StartDate
	status := state.Status

	return &DescribeReportCreationOutput{
		S3Location: &s3Loc,
		StartDate:  &startDate,
		Status:     &status,
	}
}
