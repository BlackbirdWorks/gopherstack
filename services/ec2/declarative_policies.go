package ec2

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// ErrDeclarativePoliciesReportNotFound is returned when a Declarative Policies report
// is not found.
var ErrDeclarativePoliciesReportNotFound = errors.New("InvalidReportID.NotFound")

// ErrDeclarativePoliciesReportNotCancellable is returned when
// CancelDeclarativePoliciesReport is called on a report that has already reached a
// terminal state.
var ErrDeclarativePoliciesReportNotCancellable = errors.New("IncorrectState")

// ---- constants ----

const (
	declarativePoliciesReportStateRunning   = "running"
	declarativePoliciesReportStateCancelled = "cancelled"
	declarativePoliciesReportStateComplete  = "complete"

	declarativePoliciesReportIDPrefixLength = 17

	// declarativePoliciesReportDefaultAccountCount is the account count reported for
	// every generated report; this mock has no organizational account data to derive
	// a real count from, so it reports the single account under which it runs.
	declarativePoliciesReportDefaultAccountCount = 1
)

// ---- models ----

// DeclarativePoliciesReport represents a Declarative Policies report generation task.
type DeclarativePoliciesReport struct {
	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`
	ReportID  string    `json:"reportId,omitempty"`
	TargetID  string    `json:"targetId,omitempty"`
	S3Bucket  string    `json:"s3Bucket,omitempty"`
	S3Prefix  string    `json:"s3Prefix,omitempty"`
	Status    string    `json:"status,omitempty"`
}

func cloneDeclarativePoliciesReport(r *DeclarativePoliciesReport) *DeclarativePoliciesReport {
	cp := *r

	return &cp
}

// settleDeclarativePoliciesReport transitions a running report to "complete" as a
// side effect of being described, mirroring settleConversionTask. This mock has no
// real report generation pipeline, so reports settle immediately on first observation.
func settleDeclarativePoliciesReport(r *DeclarativePoliciesReport) {
	if r.Status == declarativePoliciesReportStateRunning {
		r.Status = declarativePoliciesReportStateComplete
		r.EndTime = time.Now().UTC()
	}
}

// StartDeclarativePoliciesReport starts generating a Declarative Policies report for
// the given target (root, organizational unit, or account ID), to be written to S3.
func (b *InMemoryBackend) StartDeclarativePoliciesReport(
	s3Bucket, targetID, s3Prefix string,
	tags map[string]string,
) (*DeclarativePoliciesReport, error) {
	if s3Bucket == "" {
		return nil, fmt.Errorf("%w: S3Bucket is required", ErrInvalidParameter)
	}

	if targetID == "" {
		return nil, fmt.Errorf("%w: TargetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("StartDeclarativePoliciesReport")
	defer b.mu.Unlock()

	report := &DeclarativePoliciesReport{
		ReportID:  newDeclarativePoliciesReportID(),
		TargetID:  targetID,
		S3Bucket:  s3Bucket,
		S3Prefix:  s3Prefix,
		Status:    declarativePoliciesReportStateRunning,
		StartTime: time.Now().UTC(),
	}
	b.declarativePoliciesReports.Put(report)
	b.setTagsLocked(report.ReportID, tags)

	return cloneDeclarativePoliciesReport(report), nil
}

// CancelDeclarativePoliciesReport cancels a report that has not yet completed.
func (b *InMemoryBackend) CancelDeclarativePoliciesReport(reportID string) (bool, error) {
	if reportID == "" {
		return false, fmt.Errorf("%w: ReportId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelDeclarativePoliciesReport")
	defer b.mu.Unlock()

	report, ok := b.declarativePoliciesReports.Get(reportID)
	if !ok {
		return false, fmt.Errorf("%w: %s", ErrDeclarativePoliciesReportNotFound, reportID)
	}

	if report.Status != declarativePoliciesReportStateRunning {
		return false, fmt.Errorf(
			"%w: report %s is already %s", ErrDeclarativePoliciesReportNotCancellable, reportID, report.Status,
		)
	}

	report.Status = declarativePoliciesReportStateCancelled
	report.EndTime = time.Now().UTC()

	return true, nil
}

// DescribeDeclarativePoliciesReports returns Declarative Policies reports, optionally
// filtered by report ID. Running reports settle to "complete" as a side effect of
// being described.
func (b *InMemoryBackend) DescribeDeclarativePoliciesReports(ids []string) []*DeclarativePoliciesReport {
	b.mu.Lock("DescribeDeclarativePoliciesReports")
	defer b.mu.Unlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*DeclarativePoliciesReport, 0, b.declarativePoliciesReports.Len())

	for _, r := range b.declarativePoliciesReports.All() {
		if len(idSet) > 0 && !idSet[r.ReportID] {
			continue
		}

		settleDeclarativePoliciesReport(r)
		out = append(out, cloneDeclarativePoliciesReport(r))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ReportID < out[j].ReportID })

	return out
}

// DeclarativePoliciesReportSummary is the result of GetDeclarativePoliciesReportSummary.
type DeclarativePoliciesReportSummary struct {
	StartTime              time.Time
	EndTime                time.Time
	ReportID               string
	TargetID               string
	S3Bucket               string
	S3Prefix               string
	NumberOfAccounts       int32
	NumberOfFailedAccounts int32
}

// GetDeclarativePoliciesReportSummary returns the summary of a Declarative Policies
// report, settling it to "complete" first if it was still running.
func (b *InMemoryBackend) GetDeclarativePoliciesReportSummary(
	reportID string,
) (*DeclarativePoliciesReportSummary, error) {
	if reportID == "" {
		return nil, fmt.Errorf("%w: ReportId is required", ErrInvalidParameter)
	}

	b.mu.Lock("GetDeclarativePoliciesReportSummary")
	defer b.mu.Unlock()

	report, ok := b.declarativePoliciesReports.Get(reportID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrDeclarativePoliciesReportNotFound, reportID)
	}

	settleDeclarativePoliciesReport(report)

	return &DeclarativePoliciesReportSummary{
		ReportID:               report.ReportID,
		TargetID:               report.TargetID,
		S3Bucket:               report.S3Bucket,
		S3Prefix:               report.S3Prefix,
		StartTime:              report.StartTime,
		EndTime:                report.EndTime,
		NumberOfAccounts:       declarativePoliciesReportDefaultAccountCount,
		NumberOfFailedAccounts: 0,
	}, nil
}
