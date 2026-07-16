package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrMonitoringScheduleNotFound is returned when a monitoring schedule does not exist.
	ErrMonitoringScheduleNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrMonitoringScheduleAlreadyStopped is returned when stopping an already-stopped schedule.
	ErrMonitoringScheduleAlreadyStopped = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrMonitoringScheduleNotStopped is returned when starting a non-stopped schedule.
	ErrMonitoringScheduleNotStopped = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// ---------------------------------------------------------------------------
// MonitoringSchedule
// ---------------------------------------------------------------------------

// MonitoringSchedule represents a SageMaker monitoring schedule.
type MonitoringSchedule struct {
	CreationTime             time.Time         `json:"CreationTime"`
	LastModifiedTime         time.Time         `json:"LastModifiedTime"`
	Tags                     map[string]string `json:"Tags,omitempty"`
	MonitoringScheduleName   string            `json:"MonitoringScheduleName"`
	MonitoringScheduleArn    string            `json:"MonitoringScheduleArn"`
	MonitoringScheduleStatus string            `json:"MonitoringScheduleStatus"`
}

func cloneMonitoringSchedule(ms *MonitoringSchedule) *MonitoringSchedule {
	cp := *ms
	cp.Tags = maps.Clone(ms.Tags)

	return &cp
}

// CreateMonitoringSchedule creates a monitoring schedule.
func (b *InMemoryBackend) CreateMonitoringSchedule(
	ctx context.Context,
	name string,
	tags map[string]string,
) (*MonitoringSchedule, error) {
	b.mu.Lock("CreateMonitoringSchedule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", ErrValidation)
	}

	if _, ok := b.monitoringSchedulesStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: monitoring schedule %q already exists", ErrValidation, name)
	}

	schedARN := arn.Build("sagemaker", region, b.accountID, "monitoring-schedule/"+name)
	now := time.Now()

	ms := &MonitoringSchedule{
		MonitoringScheduleName:   name,
		MonitoringScheduleArn:    schedARN,
		MonitoringScheduleStatus: "Scheduled",
		Tags:                     mergeTags(nil, tags),
		CreationTime:             now,
		LastModifiedTime:         now,
	}
	b.monitoringSchedulesStore(region).Put(ms)

	return cloneMonitoringSchedule(ms), nil
}

// DescribeMonitoringSchedule returns a monitoring schedule by name.
func (b *InMemoryBackend) DescribeMonitoringSchedule(ctx context.Context, name string) (*MonitoringSchedule, error) {
	b.mu.RLock("DescribeMonitoringSchedule")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	ms, ok := b.monitoringSchedulesStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, name)
	}

	return cloneMonitoringSchedule(ms), nil
}

// DeleteMonitoringSchedule removes a monitoring schedule.
func (b *InMemoryBackend) DeleteMonitoringSchedule(ctx context.Context, name string) error {
	b.mu.Lock("DeleteMonitoringSchedule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.monitoringSchedulesStore(region).Get(name); !ok {
		return fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, name)
	}

	store := b.monitoringSchedulesStore(region)
	store.Delete(name)

	return nil
}

// StopMonitoringSchedule sets a monitoring schedule status to "Stopped".
func (b *InMemoryBackend) StopMonitoringSchedule(ctx context.Context, name string) error {
	b.mu.Lock("StopMonitoringSchedule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ms, ok := b.monitoringSchedulesStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, name)
	}

	// AWS rejects stopping an already-stopped schedule.
	if ms.MonitoringScheduleStatus == pipelineStatusStopped {
		return fmt.Errorf("%w: monitoring schedule %q is already stopped", ErrMonitoringScheduleAlreadyStopped, name)
	}

	ms.MonitoringScheduleStatus = pipelineStatusStopped
	ms.LastModifiedTime = time.Now()

	return nil
}

// StartMonitoringSchedule sets a monitoring schedule status to "Scheduled".
func (b *InMemoryBackend) StartMonitoringSchedule(ctx context.Context, name string) error {
	b.mu.Lock("StartMonitoringSchedule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ms, ok := b.monitoringSchedulesStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, name)
	}

	// AWS rejects starting a schedule that is not in Stopped state.
	if ms.MonitoringScheduleStatus != pipelineStatusStopped {
		return fmt.Errorf("%w: monitoring schedule %q is not stopped (status: %s)",
			ErrMonitoringScheduleNotStopped, name, ms.MonitoringScheduleStatus)
	}

	ms.MonitoringScheduleStatus = "Scheduled"
	ms.LastModifiedTime = time.Now()

	return nil
}

// UpdateMonitoringSchedule updates a monitoring schedule (marks it modified).
func (b *InMemoryBackend) UpdateMonitoringSchedule(ctx context.Context, name string) (*MonitoringSchedule, error) {
	b.mu.Lock("UpdateMonitoringSchedule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ms, ok := b.monitoringSchedulesStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, name)
	}

	ms.LastModifiedTime = time.Now()

	return cloneMonitoringSchedule(ms), nil
}

// ListMonitoringSchedules returns all monitoring schedules sorted by name.
func (b *InMemoryBackend) ListMonitoringSchedules(
	ctx context.Context,
	nextToken string,
) ([]*MonitoringSchedule, string) {
	b.mu.RLock("ListMonitoringSchedules")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.monitoringSchedulesStoreRO(region),
		nextToken,
		cloneMonitoringSchedule,
		func(v *MonitoringSchedule) string { return v.MonitoringScheduleName },
	)
}
