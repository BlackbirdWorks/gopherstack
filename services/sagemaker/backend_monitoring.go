package sagemaker

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// ---------------------------------------------------------------------------
// MonitoringAlert
// ---------------------------------------------------------------------------

// MonitoringAlert is the read/update state of a model monitor alert tied to a
// monitoring schedule. AWS has no CreateMonitoringAlert API — alert records
// are provisioned from a schedule's monitoring statistics/constraints config,
// so UpdateMonitoringAlert creates the record on first use and updates it
// thereafter.
type MonitoringAlert struct {
	CreationTime              time.Time `json:"CreationTime"`
	LastModifiedTime          time.Time `json:"LastModifiedTime"`
	MonitoringScheduleName    string    `json:"MonitoringScheduleName"`
	MonitoringAlertName       string    `json:"MonitoringAlertName"`
	AlertStatus               string    `json:"AlertStatus"`
	DatapointsToAlert         int32     `json:"DatapointsToAlert"`
	EvaluationPeriod          int32     `json:"EvaluationPeriod"`
	DashboardIndicatorEnabled bool      `json:"DashboardIndicatorEnabled"`
}

func cloneMonitoringAlert(a *MonitoringAlert) *MonitoringAlert {
	cp := *a

	return &cp
}

// monitoringAlertKey builds the store.Table primary key for a monitoring
// alert: its schedule name and alert name are already unique together within
// a region (an alert belongs to exactly one schedule).
func monitoringAlertKey(scheduleName, alertName string) string {
	return scheduleName + "/" + alertName
}

// monitoringAlertsStore returns (registering if necessary) the per-region
// store.Table of MonitoringAlert, keyed by monitoringAlertKey(scheduleName,
// alertName). Callers must hold b.mu.
func (b *InMemoryBackend) monitoringAlertsStore(r string) *store.Table[MonitoringAlert] {
	if b.monitoringAlerts[r] == nil {
		b.monitoringAlerts[r] = store.Register(
			b.registry,
			"monitoringAlerts:"+r,
			store.New(func(v *MonitoringAlert) string {
				return monitoringAlertKey(v.MonitoringScheduleName, v.MonitoringAlertName)
			}),
		)
	}

	return b.monitoringAlerts[r]
}

// monitoringAlertsStoreRO returns the region-scoped monitoringAlerts table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) monitoringAlertsStoreRO(r string) *store.Table[MonitoringAlert] {
	if v := b.monitoringAlerts[r]; v != nil {
		return v
	}

	return store.New(func(v *MonitoringAlert) string {
		return monitoringAlertKey(v.MonitoringScheduleName, v.MonitoringAlertName)
	})
}

// UpdateMonitoringAlert updates the datapoints/evaluation-period configuration
// of a monitoring alert, creating the alert record on first use. It returns
// the alert's schedule's ARN alongside the updated alert.
func (b *InMemoryBackend) UpdateMonitoringAlert(
	ctx context.Context,
	scheduleName, alertName string,
	datapointsToAlert, evaluationPeriod int32,
) (*MonitoringAlert, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateMonitoringAlert")
	defer b.mu.Unlock()

	sched, ok := b.monitoringSchedulesStore(region).Get(scheduleName)
	if !ok {
		return nil, "", fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, scheduleName)
	}

	tbl := b.monitoringAlertsStore(region)
	now := time.Now()

	a, ok := tbl.Get(monitoringAlertKey(scheduleName, alertName))
	if !ok {
		a = &MonitoringAlert{
			MonitoringScheduleName: scheduleName,
			MonitoringAlertName:    alertName,
			AlertStatus:            "OK",
			CreationTime:           now,
		}
		tbl.Put(a)
	}

	a.DatapointsToAlert = datapointsToAlert
	a.EvaluationPeriod = evaluationPeriod
	a.LastModifiedTime = now

	return cloneMonitoringAlert(a), sched.MonitoringScheduleArn, nil
}

// ListMonitoringAlerts returns the alerts configured for a monitoring schedule.
func (b *InMemoryBackend) ListMonitoringAlerts(
	ctx context.Context,
	scheduleName, nextToken string,
) ([]*MonitoringAlert, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListMonitoringAlerts")
	defer b.mu.RUnlock()

	if _, ok := b.monitoringSchedulesStoreRO(region).Get(scheduleName); !ok {
		return nil, "", fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, scheduleName)
	}

	// Rebuild a by-alert-name map scoped to this schedule, matching the
	// pre-conversion per-schedule map's exact key shape, so pagination tokens
	// (alert names) behave identically.
	alerts := make(map[string]*MonitoringAlert)

	for _, a := range b.monitoringAlertsStoreRO(region).All() {
		if a.MonitoringScheduleName == scheduleName {
			alerts[a.MonitoringAlertName] = a
		}
	}

	items, next := sagemakerListKeyPagedMap(alerts, nextToken, cloneMonitoringAlert)

	return items, next, nil
}

// ---------------------------------------------------------------------------
// MonitoringAlertHistory
// ---------------------------------------------------------------------------

// MonitoringAlertHistoryEntry records a single point-in-time alert status
// observation for a monitoring schedule's alert.
type MonitoringAlertHistoryEntry struct {
	CreationTime           time.Time `json:"CreationTime"`
	MonitoringScheduleName string    `json:"MonitoringScheduleName"`
	MonitoringAlertName    string    `json:"MonitoringAlertName"`
	AlertStatus            string    `json:"AlertStatus"`
}

func cloneMonitoringAlertHistoryEntry(e *MonitoringAlertHistoryEntry) *MonitoringAlertHistoryEntry {
	cp := *e

	return &cp
}

// MonitoringAlertHistoryFilter narrows ListMonitoringAlertHistory results.
type MonitoringAlertHistoryFilter struct {
	CreationTimeAfter      *time.Time
	CreationTimeBefore     *time.Time
	MonitoringScheduleName string
	MonitoringAlertName    string
	StatusEquals           string
	SortOrder              string // "Ascending" | "Descending" (default); sort key is always CreationTime
	MaxResults             int32
}

func matchesAlertHistoryFilter(e *MonitoringAlertHistoryEntry, f MonitoringAlertHistoryFilter) bool {
	if f.MonitoringScheduleName != "" && e.MonitoringScheduleName != f.MonitoringScheduleName {
		return false
	}
	if f.MonitoringAlertName != "" && e.MonitoringAlertName != f.MonitoringAlertName {
		return false
	}
	if f.StatusEquals != "" && !strings.EqualFold(e.AlertStatus, f.StatusEquals) {
		return false
	}
	if f.CreationTimeAfter != nil && !e.CreationTime.After(*f.CreationTimeAfter) {
		return false
	}
	if f.CreationTimeBefore != nil && !e.CreationTime.Before(*f.CreationTimeBefore) {
		return false
	}

	return true
}

// ListMonitoringAlertHistory returns alert status history entries, optionally
// filtered by schedule/alert name, status, and creation-time window. AWS
// provides no API to record a transition directly — entries only appear here
// if seeded (e.g. via the test helper SeedMonitoringAlertHistory), matching
// real AWS accounts where a schedule with no completed monitoring runs has an
// empty history.
func (b *InMemoryBackend) ListMonitoringAlertHistory(
	ctx context.Context,
	nextToken string,
	f MonitoringAlertHistoryFilter,
) ([]*MonitoringAlertHistoryEntry, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListMonitoringAlertHistory")
	defer b.mu.RUnlock()

	list := make([]*MonitoringAlertHistoryEntry, 0, len(b.monitoringAlertHistory[region]))

	for _, e := range b.monitoringAlertHistory[region] {
		if matchesAlertHistoryFilter(e, f) {
			list = append(list, cloneMonitoringAlertHistoryEntry(e))
		}
	}

	descending := !strings.EqualFold(f.SortOrder, "Ascending")
	sort.SliceStable(list, func(i, k int) bool {
		cmp := compareTimes(list[i].CreationTime, list[k].CreationTime)
		if descending {
			return cmp > 0
		}

		return cmp < 0
	})

	return paginateSlice(list, nextToken, f.MaxResults)
}

// ---------------------------------------------------------------------------
// MonitoringExecution
// ---------------------------------------------------------------------------

// MonitoringExecution represents a single run of a monitoring schedule.
type MonitoringExecution struct {
	CreationTime                time.Time `json:"CreationTime"`
	LastModifiedTime            time.Time `json:"LastModifiedTime"`
	ScheduledTime               time.Time `json:"ScheduledTime"`
	MonitoringScheduleName      string    `json:"MonitoringScheduleName"`
	MonitoringExecutionStatus   string    `json:"MonitoringExecutionStatus"`
	EndpointName                string    `json:"EndpointName,omitempty"`
	MonitoringJobDefinitionName string    `json:"MonitoringJobDefinitionName,omitempty"`
	MonitoringType              string    `json:"MonitoringType,omitempty"`
	ProcessingJobArn            string    `json:"ProcessingJobArn,omitempty"`
	FailureReason               string    `json:"FailureReason,omitempty"`
}

func cloneMonitoringExecution(e *MonitoringExecution) *MonitoringExecution {
	cp := *e

	return &cp
}

// MonitoringExecutionFilter narrows ListMonitoringExecutions results.
type MonitoringExecutionFilter struct {
	CreationTimeAfter           *time.Time
	CreationTimeBefore          *time.Time
	LastModifiedTimeAfter       *time.Time
	LastModifiedTimeBefore      *time.Time
	ScheduledTimeAfter          *time.Time
	ScheduledTimeBefore         *time.Time
	MonitoringScheduleName      string
	MonitoringJobDefinitionName string
	EndpointName                string
	MonitoringTypeEquals        string
	StatusEquals                string
	SortBy                      string // "CreationTime" (default) | "ScheduledTime" | "Status"
	SortOrder                   string
	MaxResults                  int32
}

// matchesMonitoringExecutionFields checks the equality-style filters
// (schedule/job-definition/endpoint name, type, status).
func matchesMonitoringExecutionFields(e *MonitoringExecution, f MonitoringExecutionFilter) bool {
	if f.MonitoringScheduleName != "" && e.MonitoringScheduleName != f.MonitoringScheduleName {
		return false
	}
	if f.MonitoringJobDefinitionName != "" && e.MonitoringJobDefinitionName != f.MonitoringJobDefinitionName {
		return false
	}
	if f.EndpointName != "" && e.EndpointName != f.EndpointName {
		return false
	}
	if f.MonitoringTypeEquals != "" && !strings.EqualFold(e.MonitoringType, f.MonitoringTypeEquals) {
		return false
	}
	if f.StatusEquals != "" && !strings.EqualFold(e.MonitoringExecutionStatus, f.StatusEquals) {
		return false
	}

	return true
}

// matchesMonitoringExecutionWindows checks the CreationTime/LastModifiedTime/
// ScheduledTime before/after window filters.
func matchesMonitoringExecutionWindows(e *MonitoringExecution, f MonitoringExecutionFilter) bool {
	if f.CreationTimeAfter != nil && !e.CreationTime.After(*f.CreationTimeAfter) {
		return false
	}
	if f.CreationTimeBefore != nil && !e.CreationTime.Before(*f.CreationTimeBefore) {
		return false
	}
	if f.LastModifiedTimeAfter != nil && !e.LastModifiedTime.After(*f.LastModifiedTimeAfter) {
		return false
	}
	if f.LastModifiedTimeBefore != nil && !e.LastModifiedTime.Before(*f.LastModifiedTimeBefore) {
		return false
	}
	if f.ScheduledTimeAfter != nil && !e.ScheduledTime.After(*f.ScheduledTimeAfter) {
		return false
	}
	if f.ScheduledTimeBefore != nil && !e.ScheduledTime.Before(*f.ScheduledTimeBefore) {
		return false
	}

	return true
}

func matchesMonitoringExecutionFilter(e *MonitoringExecution, f MonitoringExecutionFilter) bool {
	return matchesMonitoringExecutionFields(e, f) && matchesMonitoringExecutionWindows(e, f)
}

func compareMonitoringExecutions(a, b *MonitoringExecution, sortBy string) int {
	switch {
	case strings.EqualFold(sortBy, "ScheduledTime"):
		return compareTimes(a.ScheduledTime, b.ScheduledTime)
	case strings.EqualFold(sortBy, "Status"):
		return strings.Compare(a.MonitoringExecutionStatus, b.MonitoringExecutionStatus)
	default:
		return compareTimes(a.CreationTime, b.CreationTime)
	}
}

// ListMonitoringExecutions returns monitoring schedule execution runs. AWS
// creates these automatically when a schedule's periodic run completes; this
// emulator has no scheduler driving that, so entries only appear here if
// seeded (e.g. via the test helper SeedMonitoringExecution).
func (b *InMemoryBackend) ListMonitoringExecutions(
	ctx context.Context,
	nextToken string,
	f MonitoringExecutionFilter,
) ([]*MonitoringExecution, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListMonitoringExecutions")
	defer b.mu.RUnlock()

	list := make([]*MonitoringExecution, 0, b.monitoringExecutionsStoreRO(region).Len())

	for _, e := range b.monitoringExecutionsStoreRO(region).All() {
		if matchesMonitoringExecutionFilter(e, f) {
			list = append(list, cloneMonitoringExecution(e))
		}
	}

	descending := !strings.EqualFold(f.SortOrder, "Ascending")
	sort.SliceStable(list, func(i, k int) bool {
		cmp := compareMonitoringExecutions(list[i], list[k], f.SortBy)
		if descending {
			return cmp > 0
		}

		return cmp < 0
	})

	return paginateSlice(list, nextToken, f.MaxResults)
}
