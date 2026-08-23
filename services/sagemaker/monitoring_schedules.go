package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
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

// MonitoringScheduleSchedule mirrors types.ScheduleConfig
// (types/types.go:15980-16037, three string members).
type MonitoringScheduleSchedule struct {
	ScheduleExpression    string `json:"ScheduleExpression,omitempty"`
	DataAnalysisStartTime string `json:"DataAnalysisStartTime,omitempty"`
	DataAnalysisEndTime   string `json:"DataAnalysisEndTime,omitempty"`
}

// MonitoringScheduleConfig mirrors types.MonitoringScheduleConfig
// (types/types.go:15917-15932). MonitoringJobDefinition is deeply nested
// (AppSpecification/Inputs/OutputConfig/Resources/NetworkConfig/...) and is
// carried as opaque json.RawMessage passthrough per this campaign's
// established convention — stored and echoed back verbatim, never
// simulated, except for a best-effort EndpointName extraction used only for
// filtering/display (see monitoringEndpointNameFromJobDefinition).
type MonitoringScheduleConfig struct {
	ScheduleConfig              *MonitoringScheduleSchedule `json:"ScheduleConfig,omitempty"`
	MonitoringJobDefinitionName string                      `json:"MonitoringJobDefinitionName,omitempty"`
	MonitoringType              string                      `json:"MonitoringType,omitempty"`
	MonitoringJobDefinition     json.RawMessage             `json:"MonitoringJobDefinition,omitempty"`
}

// MonitoringSchedule represents a SageMaker monitoring schedule.
type MonitoringSchedule struct {
	CreationTime             time.Time                 `json:"CreationTime"`
	LastModifiedTime         time.Time                 `json:"LastModifiedTime"`
	MonitoringScheduleConfig *MonitoringScheduleConfig `json:"MonitoringScheduleConfig,omitempty"`
	Tags                     map[string]string         `json:"Tags,omitempty"`
	MonitoringScheduleName   string                    `json:"MonitoringScheduleName"`
	MonitoringScheduleArn    string                    `json:"MonitoringScheduleArn"`
	MonitoringScheduleStatus string                    `json:"MonitoringScheduleStatus"`
	EndpointName             string                    `json:"EndpointName,omitempty"`
}

func cloneMonitoringSchedule(ms *MonitoringSchedule) *MonitoringSchedule {
	cp := *ms
	cp.Tags = maps.Clone(ms.Tags)

	if ms.MonitoringScheduleConfig != nil {
		cfg := *ms.MonitoringScheduleConfig
		cfg.MonitoringJobDefinition = append(
			json.RawMessage(nil),
			ms.MonitoringScheduleConfig.MonitoringJobDefinition...)

		if ms.MonitoringScheduleConfig.ScheduleConfig != nil {
			sc := *ms.MonitoringScheduleConfig.ScheduleConfig
			cfg.ScheduleConfig = &sc
		}

		cp.MonitoringScheduleConfig = &cfg
	}

	return &cp
}

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeMonitoringSchedule.
func (ms *MonitoringSchedule) MarshalJSON() ([]byte, error) {
	type alias MonitoringSchedule

	// MonitoringType is also a top-level, optional member of
	// DescribeMonitoringScheduleOutput (api_op_DescribeMonitoringSchedule.go:96-104),
	// separate from the required, nested MonitoringScheduleConfig.MonitoringType
	// this struct already carries — mirrored here for Describe's response.
	var monitoringType string
	if ms.MonitoringScheduleConfig != nil {
		monitoringType = ms.MonitoringScheduleConfig.MonitoringType
	}

	return json.Marshal(struct {
		*alias
		MonitoringType   string  `json:"MonitoringType,omitempty"`
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(ms),
		CreationTime:     epochSeconds(ms.CreationTime),
		LastModifiedTime: epochSeconds(ms.LastModifiedTime),
		MonitoringType:   monitoringType,
	})
}

// UnmarshalJSON is the inverse of [MonitoringSchedule.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (ms *MonitoringSchedule) UnmarshalJSON(data []byte) error {
	type alias MonitoringSchedule

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(ms)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	ms.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	ms.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

// monitoringEndpointNameFromJobDefinition best-effort extracts
// MonitoringInputs[0].EndpointInput.EndpointName from a raw
// MonitoringJobDefinition document (types/types.go: MonitoringInput has an
// EndpointInput *or* BatchTransformInput; EndpointInput.EndpointName is
// required when present). Used only to populate
// DescribeMonitoringScheduleOutput.EndpointName and
// ListMonitoringSchedulesInput's EndpointName filter — this backend never
// parses MonitoringJobDefinition for any other purpose. Returns "" if
// raw is empty, unparseable, or has no endpoint input (e.g. a batch
// transform monitoring job, or a schedule referencing a job definition by
// name only).
func monitoringEndpointNameFromJobDefinition(raw json.RawMessage) string {
	var doc struct {
		MonitoringInputs []struct {
			EndpointInput *struct {
				EndpointName string `json:"EndpointName"`
			} `json:"EndpointInput"`
		} `json:"MonitoringInputs"`
	}

	if len(raw) == 0 || json.Unmarshal(raw, &doc) != nil {
		return ""
	}

	for _, in := range doc.MonitoringInputs {
		if in.EndpointInput != nil && in.EndpointInput.EndpointName != "" {
			return in.EndpointInput.EndpointName
		}
	}

	return ""
}

// CreateMonitoringSchedule creates a monitoring schedule. config is
// required by the real API (api_op_CreateMonitoringSchedule.go:29-50) — a
// previous version of this backend never read it at all, so no real
// client's monitoring job definition, schedule expression, or endpoint
// association was ever stored.
func (b *InMemoryBackend) CreateMonitoringSchedule(
	ctx context.Context,
	name string,
	config *MonitoringScheduleConfig,
	tags map[string]string,
) (*MonitoringSchedule, error) {
	b.mu.Lock("CreateMonitoringSchedule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", ErrValidation)
	}

	if config == nil {
		return nil, fmt.Errorf("%w: MonitoringScheduleConfig is required", ErrValidation)
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
		MonitoringScheduleConfig: config,
		EndpointName:             monitoringEndpointNameFromJobDefinition(config.MonitoringJobDefinition),
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

// UpdateMonitoringSchedule updates a monitoring schedule's config. config is
// required by the real API (api_op_UpdateMonitoringSchedule.go:28-45), same
// as CreateMonitoringSchedule's.
func (b *InMemoryBackend) UpdateMonitoringSchedule(
	ctx context.Context,
	name string,
	config *MonitoringScheduleConfig,
) (*MonitoringSchedule, error) {
	b.mu.Lock("UpdateMonitoringSchedule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if config == nil {
		return nil, fmt.Errorf("%w: MonitoringScheduleConfig is required", ErrValidation)
	}

	ms, ok := b.monitoringSchedulesStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, name)
	}

	ms.MonitoringScheduleConfig = config
	ms.EndpointName = monitoringEndpointNameFromJobDefinition(config.MonitoringJobDefinition)
	ms.LastModifiedTime = time.Now()

	return cloneMonitoringSchedule(ms), nil
}

// ListMonitoringSchedulesParams bundles the filter/sort criteria for
// ListMonitoringSchedules, mirroring ListMonitoringSchedulesInput
// (api_op_ListMonitoringSchedules.go:29-72).
type ListMonitoringSchedulesParams struct {
	CreationTimeAfter           *time.Time
	CreationTimeBefore          *time.Time
	LastModifiedTimeAfter       *time.Time
	LastModifiedTimeBefore      *time.Time
	EndpointName                string
	MonitoringJobDefinitionName string
	MonitoringTypeEquals        string
	NameContains                string
	SortBy                      string
	SortOrder                   string
	StatusEquals                string
	NextToken                   string
	MaxResults                  int32
}

// ListMonitoringSchedules returns monitoring schedules, optionally filtered
// and sorted per params. Both SortBy ("CreationTime") and SortOrder
// ("Descending") have documented defaults
// (api_op_ListMonitoringSchedules.go:59-66), implemented as documented. The
// doc comment's SortBy prose also mentions a "ScheduledTime" field that
// does not exist in the real MonitoringScheduleSortKey enum
// (Name/CreationTime/Status, types/enums.go:6363-6365) — a doc/enum
// mismatch, so SortBy is read from the enum, not the prose.
func (b *InMemoryBackend) ListMonitoringSchedules(
	ctx context.Context,
	params ListMonitoringSchedulesParams,
) ([]*MonitoringSchedule, string) {
	b.mu.RLock("ListMonitoringSchedules")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*MonitoringSchedule, 0)

	for _, ms := range b.monitoringSchedulesStoreRO(region).All() {
		if !matchesMonitoringScheduleListParams(ms, params) {
			continue
		}

		list = append(list, cloneMonitoringSchedule(ms))
	}

	sortMonitoringSchedulesByParams(list, params)

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

func matchesMonitoringScheduleListParams(ms *MonitoringSchedule, params ListMonitoringSchedulesParams) bool {
	return matchesMonitoringScheduleNameParams(ms, params) && matchesMonitoringScheduleTimeParams(ms, params)
}

func matchesMonitoringScheduleNameParams(ms *MonitoringSchedule, params ListMonitoringSchedulesParams) bool {
	if params.StatusEquals != "" && ms.MonitoringScheduleStatus != params.StatusEquals {
		return false
	}

	if params.EndpointName != "" && ms.EndpointName != params.EndpointName {
		return false
	}

	if params.NameContains != "" &&
		!strings.Contains(strings.ToLower(ms.MonitoringScheduleName), strings.ToLower(params.NameContains)) {
		return false
	}

	cfg := ms.MonitoringScheduleConfig
	if params.MonitoringJobDefinitionName != "" &&
		(cfg == nil || cfg.MonitoringJobDefinitionName != params.MonitoringJobDefinitionName) {
		return false
	}

	if params.MonitoringTypeEquals != "" && (cfg == nil || cfg.MonitoringType != params.MonitoringTypeEquals) {
		return false
	}

	return true
}

func matchesMonitoringScheduleTimeParams(ms *MonitoringSchedule, params ListMonitoringSchedulesParams) bool {
	if params.CreationTimeAfter != nil && !ms.CreationTime.After(*params.CreationTimeAfter) {
		return false
	}

	if params.CreationTimeBefore != nil && !ms.CreationTime.Before(*params.CreationTimeBefore) {
		return false
	}

	if params.LastModifiedTimeAfter != nil && !ms.LastModifiedTime.After(*params.LastModifiedTimeAfter) {
		return false
	}

	if params.LastModifiedTimeBefore != nil && !ms.LastModifiedTime.Before(*params.LastModifiedTimeBefore) {
		return false
	}

	return true
}

func sortMonitoringSchedulesByParams(list []*MonitoringSchedule, params ListMonitoringSchedulesParams) {
	desc := !strings.EqualFold(params.SortOrder, "Ascending")

	sort.Slice(list, func(i, j int) bool {
		var less bool

		switch params.SortBy {
		case keyGenericName:
			less = list[i].MonitoringScheduleName < list[j].MonitoringScheduleName
		case keyStatus:
			less = list[i].MonitoringScheduleStatus < list[j].MonitoringScheduleStatus
		default:
			less = list[i].CreationTime.Before(list[j].CreationTime)
		}

		if desc {
			return !less
		}

		return less
	})
}
