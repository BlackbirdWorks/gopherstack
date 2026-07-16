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
	b.mu.Lock("CreateReportPlan")
	defer b.mu.Unlock()

	if b.reportPlans.Has(name) {
		return nil, fmt.Errorf("%w: report plan %s already exists", ErrAlreadyExists, name)
	}

	planARN := arn.Build("backup", b.region, b.accountID, "report-plan:"+name)
	t := tags.New("backup.report-plan." + name + ".tags")
	rp := &ReportPlan{
		ReportPlanName:        name,
		ReportPlanArn:         planARN,
		ReportPlanDescription: description,
		ReportDeliveryChannel: deliveryChannel,
		ReportSetting:         setting,
		CreationTime:          time.Now().UTC(),
		Tags:                  t,
	}
	b.reportPlans.Put(rp)
	b.reportPlanARNIndex[planARN] = name
	cp := *rp

	return &cp, nil
}

// ListReportPlans returns all report plans.
func (b *InMemoryBackend) ListReportPlans() []*ReportPlan {
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

	return list
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

// UpdateReportPlan updates a report plan's description.
func (b *InMemoryBackend) UpdateReportPlan(name, description string) (*ReportPlan, error) {
	b.mu.Lock("UpdateReportPlan")
	defer b.mu.Unlock()

	rp, ok := b.reportPlans.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: report plan %s not found", ErrNotFound, name)
	}

	rp.ReportPlanDescription = description
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
		ReportJobID:    "report-job-" + uuid.New().String()[:8],
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

// ListReportJobs returns all report jobs, optionally filtered by report plan name.
func (b *InMemoryBackend) ListReportJobs(reportPlanName string) []*ReportJob {
	b.mu.RLock("ListReportJobs")
	defer b.mu.RUnlock()

	var out []*ReportJob
	for _, j := range b.reportJobs.All() {
		if reportPlanName != "" &&
			j.ReportPlanArn != "arn:aws:backup:"+b.region+":"+b.accountID+":report-plan:"+reportPlanName {
			continue
		}
		cp := *j
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ReportJobID < out[j].ReportJobID })

	return out
}

// ---- Scan Jobs ----
