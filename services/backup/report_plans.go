package backup

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateReportPlan creates a report plan.
func (b *InMemoryBackend) CreateReportPlan(
	name, description string,
	deliveryChannel *ReportDeliveryChannel,
	setting *ReportSetting,
) (*ReportPlan, error) {
	return b.CreateReportPlanWithOptions(name, description, deliveryChannel, setting, CreateOptions{})
}

// CreateReportPlanWithOptions is CreateReportPlan with ReportPlanTags and
// IdempotencyToken; a retry with the same token returns the existing plan.
func (b *InMemoryBackend) CreateReportPlanWithOptions(
	name, description string,
	deliveryChannel *ReportDeliveryChannel,
	setting *ReportSetting,
	opts CreateOptions,
) (*ReportPlan, error) {
	b.mu.Lock("CreateReportPlan")
	defer b.mu.Unlock()

	if existing, ok := b.reportPlans.Get(name); ok {
		if opts.IdempotencyToken != "" && existing.IdempotencyToken == opts.IdempotencyToken {
			cp := *existing

			return &cp, nil
		}

		return nil, fmt.Errorf("%w: report plan %s already exists", ErrAlreadyExists, name)
	}

	planARN := arn.Build("backup", b.region, b.accountID, "report-plan:"+name)
	t := tags.New("backup.report-plan." + name + ".tags")
	t.Merge(opts.Tags)
	rp := &ReportPlan{
		IdempotencyToken:      opts.IdempotencyToken,
		ReportPlanName:        name,
		ReportPlanArn:         planARN,
		ReportPlanDescription: description,
		ReportDeliveryChannel: deliveryChannel,
		ReportSetting:         setting,
		CreationTime:          time.Now().UTC(),
		Tags:                  t,
		DeploymentStatus:      "COMPLETED",
	}
	b.reportPlans.Put(rp)
	b.reportPlanARNIndex[planARN] = name
	cp := *rp

	return &cp, nil
}

// ListReportPlans returns report plans, paginated by MaxResults/NextToken
// (real query params, ListReportPlans serializers.go:6633-6639 -- capitalized
// "MaxResults"/"NextToken" on the wire).
func (b *InMemoryBackend) ListReportPlans(maxResults int, nextToken string) ([]*ReportPlan, string) {
	b.mu.RLock("ListReportPlans")
	defer b.mu.RUnlock()

	all := b.reportPlans.All()
	list := make([]*ReportPlan, 0, len(all))
	for _, rp := range all {
		cp := *rp
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *ReportPlan) int {
		if a.ReportPlanName < b.ReportPlanName {
			return -1
		}
		if a.ReportPlanName > b.ReportPlanName {
			return 1
		}

		return 0
	})

	return paginateByID(list, func(rp *ReportPlan) string { return rp.ReportPlanName }, maxResults, nextToken)
}

// DescribeReportPlan returns a report plan by name.
func (b *InMemoryBackend) DescribeReportPlan(name string) (*ReportPlan, error) {
	b.mu.RLock("DescribeReportPlan")
	defer b.mu.RUnlock()

	rp, ok := b.reportPlans.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: report plan %s not found", ErrNotFound, name)
	}

	cp := *rp

	return &cp, nil
}

// UpdateReportPlan updates a report plan's description and, when non-nil,
// its ReportDeliveryChannel/ReportSetting (both optional on the real
// UpdateReportPlanInput -- an omitted field leaves the existing value
// unchanged, matching UpdateFramework's FrameworkControls semantics).
func (b *InMemoryBackend) UpdateReportPlan(
	name, description string,
	deliveryChannel *ReportDeliveryChannel,
	setting *ReportSetting,
) (*ReportPlan, error) {
	b.mu.Lock("UpdateReportPlan")
	defer b.mu.Unlock()

	rp, ok := b.reportPlans.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: report plan %s not found", ErrNotFound, name)
	}

	rp.ReportPlanDescription = description
	if deliveryChannel != nil {
		rp.ReportDeliveryChannel = deliveryChannel
	}
	if setting != nil {
		rp.ReportSetting = setting
	}
	cp := *rp

	return &cp, nil
}

// DeleteReportPlan deletes a report plan.
func (b *InMemoryBackend) DeleteReportPlan(name string) error {
	b.mu.Lock("DeleteReportPlan")
	defer b.mu.Unlock()

	rp, ok := b.reportPlans.Get(name)
	if !ok {
		return fmt.Errorf("%w: report plan %s not found", ErrNotFound, name)
	}

	delete(b.reportPlanARNIndex, rp.ReportPlanArn)
	b.reportPlans.Delete(name)
	rp.Tags.Close()

	return nil
}

// StartReportJob creates a new report job for a report plan.
func (b *InMemoryBackend) StartReportJob(reportPlanName string) *ReportJob {
	b.mu.Lock("StartReportJob")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	done := now
	job := &ReportJob{
		ReportJobID:    "report-job-" + uuid.NewString()[:8],
		ReportPlanArn:  "arn:aws:backup:" + b.region + ":" + b.accountID + ":report-plan:" + reportPlanName,
		Status:         statusCompleted,
		CreationTime:   now,
		CompletionTime: &done,
	}
	b.reportJobs.Put(job)

	return job
}

// DescribeReportJob returns a report job by ID.
func (b *InMemoryBackend) DescribeReportJob(reportJobID string) (*ReportJob, error) {
	b.mu.RLock("DescribeReportJob")
	defer b.mu.RUnlock()

	job, ok := b.reportJobs.Get(reportJobID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errReportJobNotFound, reportJobID)
	}

	return job, nil
}

// ReportJobsFilter holds ListReportJobs' ByReportPlanName/ByStatus/ByCreation* filters.
type ReportJobsFilter struct {
	CreatedAfter   *time.Time
	CreatedBefore  *time.Time
	ReportPlanName string
	Status         string
}

// ListReportJobs returns all report jobs, optionally filtered by report plan name.
func (b *InMemoryBackend) ListReportJobs(reportPlanName string) []*ReportJob {
	return b.ListReportJobsFiltered(ReportJobsFilter{ReportPlanName: reportPlanName})
}

// ListReportJobsFiltered returns the report jobs matching f.
func (b *InMemoryBackend) ListReportJobsFiltered(f ReportJobsFilter) []*ReportJob {
	b.mu.RLock("ListReportJobs")
	defer b.mu.RUnlock()

	planArn := ""
	if f.ReportPlanName != "" {
		planArn = arn.Build("backup", b.region, b.accountID, "report-plan:"+f.ReportPlanName)
	}

	var out []*ReportJob
	for _, j := range b.reportJobs.All() {
		if planArn != "" && j.ReportPlanArn != planArn {
			continue
		}
		if f.Status != "" && j.Status != f.Status {
			continue
		}
		if !inTimeRange(j.CreationTime, f.CreatedAfter, f.CreatedBefore) {
			continue
		}
		cp := *j
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ReportJobID < out[j].ReportJobID })

	return out
}

// ---- Scan Jobs ----
