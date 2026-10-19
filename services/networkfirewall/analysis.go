package networkfirewall

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

// AnalysisReport is a ListAnalysisReports entry.
type AnalysisReport struct {
	ReportTime       *float64 `json:"ReportTime,omitempty"`
	AnalysisReportID string   `json:"AnalysisReportId"`
	AnalysisType     string   `json:"AnalysisType"`
	Status           string   `json:"Status"`
}

// AnalysisTypeReportResult is one domain row of a report.
type AnalysisTypeReportResult struct {
	Domain string `json:"Domain,omitempty"`
}

// StartAnalysisReportInput is the StartAnalysisReport request.
type StartAnalysisReportInput struct {
	firewallTarget
	AnalysisType string `json:"AnalysisType"`
}

// StartAnalysisReportOutput is the StartAnalysisReport response.
type StartAnalysisReportOutput struct {
	AnalysisReportID string `json:"AnalysisReportId"`
}

// ListAnalysisReportsInput is the ListAnalysisReports request.
type ListAnalysisReportsInput struct {
	MaxResults   *int32 `json:"MaxResults"`
	FirewallArn  string `json:"FirewallArn"`
	FirewallName string `json:"FirewallName"`
	NextToken    string `json:"NextToken"`
}

// ListAnalysisReportsOutput is the ListAnalysisReports response.
type ListAnalysisReportsOutput struct {
	NextToken       string           `json:"NextToken,omitempty"`
	AnalysisReports []AnalysisReport `json:"AnalysisReports"`
}

// GetAnalysisReportResultsInput is the GetAnalysisReportResults request.
type GetAnalysisReportResultsInput struct {
	MaxResults       *int32 `json:"MaxResults"`
	AnalysisReportID string `json:"AnalysisReportId"`
	FirewallArn      string `json:"FirewallArn"`
	FirewallName     string `json:"FirewallName"`
	NextToken        string `json:"NextToken"`
}

// GetAnalysisReportResultsOutput is the GetAnalysisReportResults response.
type GetAnalysisReportResultsOutput struct {
	StartTime             *float64                   `json:"StartTime,omitempty"`
	EndTime               *float64                   `json:"EndTime,omitempty"`
	ReportTime            *float64                   `json:"ReportTime,omitempty"`
	AnalysisType          string                     `json:"AnalysisType"`
	NextToken             string                     `json:"NextToken,omitempty"`
	Status                string                     `json:"Status"`
	AnalysisReportResults []AnalysisTypeReportResult `json:"AnalysisReportResults"`
}

func (r *analysisReportRecord) status(now time.Time) string {
	if now.Before(r.DoneAt) {
		return flowInProgress
	}

	return flowCompleted
}

func (r *analysisReportRecord) view(now time.Time) AnalysisReport {
	return AnalysisReport{
		ReportTime: epoch(r.ReportTime), AnalysisReportID: r.ID, AnalysisType: r.Type, Status: r.status(now),
	}
}

// StartAnalysisReport starts a report for an analysis type the firewall has enabled.
func (b *InMemoryBackend) StartAnalysisReport(
	_ context.Context, in *StartAnalysisReportInput,
) (*StartAnalysisReportOutput, error) {
	if err := validateAnalysisTypes([]string{in.AnalysisType}); err != nil {
		return nil, err
	}

	b.mu.Lock("StartAnalysisReport")
	defer b.mu.Unlock()

	now := time.Now()
	b.sweepLocked(now)

	fw, err := b.findFirewallLocked(in.FirewallArn, in.FirewallName, now)
	if err != nil {
		return nil, err
	}

	if !slices.Contains(fw.AnalysisTypes, in.AnalysisType) {
		return nil, invalidOp("Analysis type %s is not enabled on firewall %s", in.AnalysisType, fw.Name)
	}

	r := &analysisReportRecord{
		FirewallARN: fw.ARN, ID: awsid.New(""), Type: in.AnalysisType, ReportTime: now, DoneAt: b.deadline(now),
	}
	r.Key = flowKey(fw.ARN, r.ID)

	b.analysisReports.Put(r)

	return &StartAnalysisReportOutput{AnalysisReportID: r.ID}, nil
}

// ListAnalysisReports lists a firewall's reports, newest first.
func (b *InMemoryBackend) ListAnalysisReports(
	_ context.Context, in *ListAnalysisReportsInput,
) (*ListAnalysisReportsOutput, error) {
	b.mu.RLock("ListAnalysisReports")
	defer b.mu.RUnlock()

	now := time.Now()

	fw, err := b.findFirewallLocked(in.FirewallArn, in.FirewallName, now)
	if err != nil {
		return nil, err
	}

	reports := slices.DeleteFunc(b.analysisReports.All(), func(r *analysisReportRecord) bool {
		return r.FirewallARN != fw.ARN
	})
	slices.SortFunc(reports, func(a, c *analysisReportRecord) int {
		if n := c.ReportTime.Compare(a.ReportTime); n != 0 {
			return n
		}

		return strings.Compare(c.ID, a.ID)
	})

	all := make([]AnalysisReport, 0, len(reports))
	for _, r := range reports {
		all = append(all, r.view(now))
	}

	pg, err := paginate(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	if pg.Data == nil {
		pg.Data = []AnalysisReport{}
	}

	return &ListAnalysisReportsOutput{AnalysisReports: pg.Data, NextToken: pg.Next}, nil
}

// GetAnalysisReportResults returns a report's rows; with no traffic through the emulator there are none.
func (b *InMemoryBackend) GetAnalysisReportResults(
	_ context.Context, in *GetAnalysisReportResultsInput,
) (*GetAnalysisReportResultsOutput, error) {
	if in.AnalysisReportID == "" {
		return nil, invalid("AnalysisReportId is required")
	}

	b.mu.RLock("GetAnalysisReportResults")
	defer b.mu.RUnlock()

	now := time.Now()

	fw, err := b.findFirewallLocked(in.FirewallArn, in.FirewallName, now)
	if err != nil {
		return nil, err
	}

	r, err := getOrNotFound(b.analysisReports, "analysis report", flowKey(fw.ARN, in.AnalysisReportID))
	if err != nil {
		return nil, err
	}

	pg, err := paginate([]AnalysisTypeReportResult{}, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &GetAnalysisReportResultsOutput{
		StartTime:             epoch(r.ReportTime),
		EndTime:               epoch(r.DoneAt),
		ReportTime:            epoch(r.ReportTime),
		AnalysisType:          r.Type,
		Status:                r.status(now),
		NextToken:             pg.Next,
		AnalysisReportResults: []AnalysisTypeReportResult{},
	}, nil
}
