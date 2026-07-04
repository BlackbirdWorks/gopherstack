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

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

var (
	// ErrDataQualityJobDefNotFound is returned when a data quality job definition does not exist.
	ErrDataQualityJobDefNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrDataQualityJobDefExists is returned when creating a data quality job definition whose name is taken.
	ErrDataQualityJobDefExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrModelBiasJobDefNotFound is returned when a model bias job definition does not exist.
	ErrModelBiasJobDefNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrModelBiasJobDefExists is returned when creating a model bias job definition whose name is taken.
	ErrModelBiasJobDefExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrModelQualityJobDefNotFound is returned when a model quality job definition does not exist.
	ErrModelQualityJobDefNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrModelQualityJobDefExists is returned when creating a model quality job definition whose name is taken.
	ErrModelQualityJobDefExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrModelExplainJobDefNotFound is returned when a model explainability job definition does not exist.
	ErrModelExplainJobDefNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrModelExplainJobDefExists is returned when creating a model explainability job definition whose name is taken.
	ErrModelExplainJobDefExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrHumanTaskUINotFound is returned when a human task UI does not exist.
	ErrHumanTaskUINotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrWorkforceNotFound is returned when a workforce does not exist.
	ErrWorkforceNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrFlowDefinitionNotFound is returned when a flow definition does not exist.
	ErrFlowDefinitionNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrAppImageConfigNotFound is returned when an app image config does not exist.
	ErrAppImageConfigNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrInferenceExperimentNotFound is returned when an inference experiment does not exist.
	ErrInferenceExperimentNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrMlflowTrackingServerNotFound is returned when an MLflow tracking server does not exist.
	ErrMlflowTrackingServerNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrModelCardNotFound is returned when a model card does not exist.
	ErrModelCardNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrOptimizationJobNotFound is returned when an optimization job does not exist.
	ErrOptimizationJobNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrStudioLifecycleConfigNotFound is returned when a studio lifecycle config does not exist.
	ErrStudioLifecycleConfigNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrPartnerAppNotFound is returned when a partner app does not exist.
	ErrPartnerAppNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrTrainingPlanNotFound is returned when a training plan does not exist.
	ErrTrainingPlanNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
)

// ---------------------------------------------------------------------------
// JobDefinition — shared struct for Data Quality / Model Bias / Quality / Explainability
// ---------------------------------------------------------------------------

// JobDefinition is the shared backend representation for the four SageMaker
// Model Monitor job definition types (DataQuality, ModelBias, ModelQuality,
// ModelExplainability). Each type sends its AppSpecification/JobInput/
// JobOutputConfig blocks under differently-named wire fields (e.g.
// "DataQualityAppSpecification" vs "ModelBiasAppSpecification"); Config
// captures those verbatim, plus the shared JobResources/NetworkConfig/
// StoppingCondition/BaselineConfig blocks, so Describe echoes back exactly
// what Create received.
type JobDefinition struct {
	CreationTime      time.Time                  `json:"CreationTime"`
	Tags              map[string]string          `json:"Tags,omitempty"`
	Config            map[string]json.RawMessage `json:"Config,omitempty"`
	JobDefinitionName string                     `json:"JobDefinitionName"`
	JobDefinitionArn  string                     `json:"JobDefinitionArn"`
	JobDefinitionType string                     `json:"JobDefinitionType"`
	RoleArn           string                     `json:"RoleArn,omitempty"`
	EndpointName      string                     `json:"EndpointName,omitempty"`
}

func cloneJobDefinition(j *JobDefinition) *JobDefinition {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)
	cp.Config = maps.Clone(j.Config)

	return &cp
}

// createJobDefinition is the shared Create implementation for all four job
// definition types. storeFn is called only while b.mu is held, so the
// per-region map's lazy initialisation stays race-free.
func (b *InMemoryBackend) createJobDefinition(
	ctx context.Context,
	storeFn func(string) map[string]*JobDefinition,
	defType, name, roleArn, endpointName string,
	config map[string]json.RawMessage,
	tags map[string]string,
	resourceType string,
	alreadyExists error,
) (*JobDefinition, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("createJobDefinition")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: %sJobDefinitionName is required", ErrValidation, defType)
	}

	store := storeFn(region)
	if _, ok := store[name]; ok {
		return nil, fmt.Errorf("%w: %s job definition %q already exists", alreadyExists, defType, name)
	}

	defARN := arn.Build("sagemaker", region, b.accountID, resourceType+"/"+name)

	j := &JobDefinition{
		JobDefinitionName: name,
		JobDefinitionArn:  defARN,
		JobDefinitionType: defType,
		RoleArn:           roleArn,
		EndpointName:      endpointName,
		Tags:              mergeTags(nil, tags),
		Config:            config,
		CreationTime:      time.Now(),
	}
	store[name] = j

	return cloneJobDefinition(j), nil
}

func (b *InMemoryBackend) describeJobDefinition(
	ctx context.Context,
	storeFn func(string) map[string]*JobDefinition,
	name string,
	notFound error,
) (*JobDefinition, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("describeJobDefinition")
	defer b.mu.RUnlock()

	j, ok := storeFn(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: job definition %q not found", notFound, name)
	}

	return cloneJobDefinition(j), nil
}

func (b *InMemoryBackend) deleteJobDefinition(
	ctx context.Context,
	storeFn func(string) map[string]*JobDefinition,
	name string,
	notFound error,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("deleteJobDefinition")
	defer b.mu.Unlock()

	store := storeFn(region)
	if _, ok := store[name]; !ok {
		return fmt.Errorf("%w: job definition %q not found", notFound, name)
	}

	delete(store, name)

	return nil
}

// JobDefinitionFilter narrows the four List*JobDefinitions operations.
type JobDefinitionFilter struct {
	CreationTimeAfter  *time.Time
	CreationTimeBefore *time.Time
	EndpointName       string
	NameContains       string
	SortBy             string // "Name" | "CreationTime" (default)
	SortOrder          string // "Ascending" | "Descending" (default)
	MaxResults         int32
}

func matchesJobDefinitionFilter(j *JobDefinition, f JobDefinitionFilter) bool {
	if f.EndpointName != "" && j.EndpointName != f.EndpointName {
		return false
	}
	if f.NameContains != "" &&
		!strings.Contains(strings.ToLower(j.JobDefinitionName), strings.ToLower(f.NameContains)) {
		return false
	}
	if f.CreationTimeAfter != nil && !j.CreationTime.After(*f.CreationTimeAfter) {
		return false
	}
	if f.CreationTimeBefore != nil && !j.CreationTime.Before(*f.CreationTimeBefore) {
		return false
	}

	return true
}

func sortJobDefinitions(list []*JobDefinition, sortBy, sortOrder string) {
	byName := strings.EqualFold(sortBy, "Name")
	descending := !strings.EqualFold(sortOrder, "Ascending") // AWS default sort order is Descending

	sort.SliceStable(list, func(i, k int) bool {
		var cmp int
		if byName {
			cmp = strings.Compare(list[i].JobDefinitionName, list[k].JobDefinitionName)
		} else {
			cmp = compareTimes(list[i].CreationTime, list[k].CreationTime)
		}

		if descending {
			return cmp > 0
		}

		return cmp < 0
	})
}

// listJobDefinitions is the shared List implementation for all four job
// definition types. storeFn is called only while b.mu is held (by the
// per-type List* wrapper).
func (b *InMemoryBackend) listJobDefinitions(
	storeFn func(string) map[string]*JobDefinition,
	region, nextToken string,
	f JobDefinitionFilter,
) ([]*JobDefinition, string) {
	store := storeFn(region)

	list := make([]*JobDefinition, 0, len(store))

	for _, j := range store {
		if matchesJobDefinitionFilter(j, f) {
			list = append(list, cloneJobDefinition(j))
		}
	}

	sortJobDefinitions(list, f.SortBy, f.SortOrder)

	return paginateSlice(list, nextToken, f.MaxResults)
}

// ---------------------------------------------------------------------------
// DataQualityJobDefinition
// ---------------------------------------------------------------------------

// CreateDataQualityJobDefinition creates a data quality job definition.
func (b *InMemoryBackend) CreateDataQualityJobDefinition(
	ctx context.Context,
	name, roleArn, endpointName string,
	config map[string]json.RawMessage,
	tags map[string]string,
) (*JobDefinition, error) {
	return b.createJobDefinition(
		ctx, b.dataQualityJobDefsStore, "DataQuality", name, roleArn, endpointName, config, tags,
		"data-quality-job-definition", ErrDataQualityJobDefExists,
	)
}

// DescribeDataQualityJobDefinition returns a data quality job definition by name.
func (b *InMemoryBackend) DescribeDataQualityJobDefinition(ctx context.Context, name string) (*JobDefinition, error) {
	return b.describeJobDefinition(ctx, b.dataQualityJobDefsStore, name, ErrDataQualityJobDefNotFound)
}

// DeleteDataQualityJobDefinition removes a data quality job definition by name.
func (b *InMemoryBackend) DeleteDataQualityJobDefinition(ctx context.Context, name string) error {
	return b.deleteJobDefinition(ctx, b.dataQualityJobDefsStore, name, ErrDataQualityJobDefNotFound)
}

// ListDataQualityJobDefinitions returns data quality job definitions matching f.
func (b *InMemoryBackend) ListDataQualityJobDefinitions(
	ctx context.Context, nextToken string, f JobDefinitionFilter,
) ([]*JobDefinition, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListDataQualityJobDefinitions")
	defer b.mu.RUnlock()

	return b.listJobDefinitions(b.dataQualityJobDefsStore, region, nextToken, f)
}

// ---------------------------------------------------------------------------
// ModelBiasJobDefinition
// ---------------------------------------------------------------------------

// CreateModelBiasJobDefinition creates a model bias job definition.
func (b *InMemoryBackend) CreateModelBiasJobDefinition(
	ctx context.Context,
	name, roleArn, endpointName string,
	config map[string]json.RawMessage,
	tags map[string]string,
) (*JobDefinition, error) {
	return b.createJobDefinition(
		ctx, b.modelBiasJobDefsStore, "ModelBias", name, roleArn, endpointName, config, tags,
		"model-bias-job-definition", ErrModelBiasJobDefExists,
	)
}

// DescribeModelBiasJobDefinition returns a model bias job definition by name.
func (b *InMemoryBackend) DescribeModelBiasJobDefinition(ctx context.Context, name string) (*JobDefinition, error) {
	return b.describeJobDefinition(ctx, b.modelBiasJobDefsStore, name, ErrModelBiasJobDefNotFound)
}

// DeleteModelBiasJobDefinition removes a model bias job definition by name.
func (b *InMemoryBackend) DeleteModelBiasJobDefinition(ctx context.Context, name string) error {
	return b.deleteJobDefinition(ctx, b.modelBiasJobDefsStore, name, ErrModelBiasJobDefNotFound)
}

// ListModelBiasJobDefinitions returns model bias job definitions matching f.
func (b *InMemoryBackend) ListModelBiasJobDefinitions(
	ctx context.Context, nextToken string, f JobDefinitionFilter,
) ([]*JobDefinition, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListModelBiasJobDefinitions")
	defer b.mu.RUnlock()

	return b.listJobDefinitions(b.modelBiasJobDefsStore, region, nextToken, f)
}

// ---------------------------------------------------------------------------
// ModelQualityJobDefinition
// ---------------------------------------------------------------------------

// CreateModelQualityJobDefinition creates a model quality job definition.
func (b *InMemoryBackend) CreateModelQualityJobDefinition(
	ctx context.Context,
	name, roleArn, endpointName string,
	config map[string]json.RawMessage,
	tags map[string]string,
) (*JobDefinition, error) {
	return b.createJobDefinition(
		ctx, b.modelQualityJobDefsStore, "ModelQuality", name, roleArn, endpointName, config, tags,
		"model-quality-job-definition", ErrModelQualityJobDefExists,
	)
}

// DescribeModelQualityJobDefinition returns a model quality job definition by name.
func (b *InMemoryBackend) DescribeModelQualityJobDefinition(ctx context.Context, name string) (*JobDefinition, error) {
	return b.describeJobDefinition(ctx, b.modelQualityJobDefsStore, name, ErrModelQualityJobDefNotFound)
}

// DeleteModelQualityJobDefinition removes a model quality job definition by name.
func (b *InMemoryBackend) DeleteModelQualityJobDefinition(ctx context.Context, name string) error {
	return b.deleteJobDefinition(ctx, b.modelQualityJobDefsStore, name, ErrModelQualityJobDefNotFound)
}

// ListModelQualityJobDefinitions returns model quality job definitions matching f.
func (b *InMemoryBackend) ListModelQualityJobDefinitions(
	ctx context.Context, nextToken string, f JobDefinitionFilter,
) ([]*JobDefinition, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListModelQualityJobDefinitions")
	defer b.mu.RUnlock()

	return b.listJobDefinitions(b.modelQualityJobDefsStore, region, nextToken, f)
}

// ---------------------------------------------------------------------------
// ModelExplainabilityJobDefinition
// ---------------------------------------------------------------------------

// CreateModelExplainabilityJobDefinition creates a model explainability job definition.
func (b *InMemoryBackend) CreateModelExplainabilityJobDefinition(
	ctx context.Context,
	name, roleArn, endpointName string,
	config map[string]json.RawMessage,
	tags map[string]string,
) (*JobDefinition, error) {
	return b.createJobDefinition(
		ctx, b.modelExplainJobDefsStore, "ModelExplainability", name, roleArn, endpointName, config, tags,
		"model-explainability-job-definition", ErrModelExplainJobDefExists,
	)
}

// DescribeModelExplainabilityJobDefinition returns a model explainability job definition by name.
func (b *InMemoryBackend) DescribeModelExplainabilityJobDefinition(
	ctx context.Context,
	name string,
) (*JobDefinition, error) {
	return b.describeJobDefinition(ctx, b.modelExplainJobDefsStore, name, ErrModelExplainJobDefNotFound)
}

// DeleteModelExplainabilityJobDefinition removes a model explainability job definition by name.
func (b *InMemoryBackend) DeleteModelExplainabilityJobDefinition(ctx context.Context, name string) error {
	return b.deleteJobDefinition(ctx, b.modelExplainJobDefsStore, name, ErrModelExplainJobDefNotFound)
}

// ListModelExplainabilityJobDefinitions returns model explainability job definitions matching f.
func (b *InMemoryBackend) ListModelExplainabilityJobDefinitions(
	ctx context.Context, nextToken string, f JobDefinitionFilter,
) ([]*JobDefinition, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListModelExplainabilityJobDefinitions")
	defer b.mu.RUnlock()

	return b.listJobDefinitions(b.modelExplainJobDefsStore, region, nextToken, f)
}

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

// monitoringAlertsFor returns (creating if necessary) the alert map for a
// monitoring schedule. Callers must hold b.mu (any lock kind for read-only
// lookups that tolerate a nil result; b.mu.Lock for creation).
func (b *InMemoryBackend) monitoringAlertsFor(region, scheduleName string) map[string]*MonitoringAlert {
	perSchedule := b.monitoringAlerts[region]
	if perSchedule == nil {
		perSchedule = make(map[string]map[string]*MonitoringAlert)
		b.monitoringAlerts[region] = perSchedule
	}

	if perSchedule[scheduleName] == nil {
		perSchedule[scheduleName] = make(map[string]*MonitoringAlert)
	}

	return perSchedule[scheduleName]
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

	sched, ok := b.monitoringSchedulesStore(region)[scheduleName]
	if !ok {
		return nil, "", fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, scheduleName)
	}

	alerts := b.monitoringAlertsFor(region, scheduleName)
	now := time.Now()

	a, ok := alerts[alertName]
	if !ok {
		a = &MonitoringAlert{
			MonitoringScheduleName: scheduleName,
			MonitoringAlertName:    alertName,
			AlertStatus:            "OK",
			CreationTime:           now,
		}
		alerts[alertName] = a
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

	if _, ok := b.monitoringSchedulesStore(region)[scheduleName]; !ok {
		return nil, "", fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, scheduleName)
	}

	var alerts map[string]*MonitoringAlert
	if perSchedule := b.monitoringAlerts[region]; perSchedule != nil {
		alerts = perSchedule[scheduleName]
	}

	items, next := sagemakerListKeyPaged(alerts, nextToken, cloneMonitoringAlert)

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

	list := make([]*MonitoringExecution, 0, len(b.monitoringExecutions[region]))

	for _, e := range b.monitoringExecutions[region] {
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

// ---------------------------------------------------------------------------
// HumanTaskUI
// ---------------------------------------------------------------------------

// HumanTaskUI represents a SageMaker human task UI.
type HumanTaskUI struct {
	CreationTime      time.Time         `json:"CreationTime"`
	Tags              map[string]string `json:"Tags,omitempty"`
	HumanTaskUIName   string            `json:"HumanTaskUiName"`
	HumanTaskUIArn    string            `json:"HumanTaskUiArn"`
	HumanTaskUIStatus string            `json:"HumanTaskUiStatus"`
}

func cloneHumanTaskUI(h *HumanTaskUI) *HumanTaskUI {
	cp := *h
	cp.Tags = maps.Clone(h.Tags)

	return &cp
}

// CreateHumanTaskUI creates a human task UI.
func (b *InMemoryBackend) CreateHumanTaskUI(
	ctx context.Context,
	name string,
	tags map[string]string,
) (*HumanTaskUI, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateHumanTaskUI")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: HumanTaskUiName is required", ErrValidation)
	}

	store := b.humanTaskUisStore(region)

	if _, ok := store[name]; ok {
		return nil, fmt.Errorf("%w: human task UI %q already exists", ErrValidation, name)
	}

	uiARN := arn.Build("sagemaker", region, b.accountID, "human-task-ui/"+name)

	ui := &HumanTaskUI{
		HumanTaskUIName:   name,
		HumanTaskUIArn:    uiARN,
		HumanTaskUIStatus: statusActive,
		Tags:              mergeTags(nil, tags),
		CreationTime:      time.Now(),
	}
	store[name] = ui

	return cloneHumanTaskUI(ui), nil
}

// DescribeHumanTaskUI returns a human task UI by name.
func (b *InMemoryBackend) DescribeHumanTaskUI(ctx context.Context, name string) (*HumanTaskUI, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeHumanTaskUI")
	defer b.mu.RUnlock()

	ui, ok := b.humanTaskUisStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: human task UI %q not found", ErrHumanTaskUINotFound, name)
	}

	return cloneHumanTaskUI(ui), nil
}

// DeleteHumanTaskUI removes a human task UI by name.
func (b *InMemoryBackend) DeleteHumanTaskUI(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteHumanTaskUI")
	defer b.mu.Unlock()

	store := b.humanTaskUisStore(region)

	if _, ok := store[name]; !ok {
		return fmt.Errorf("%w: human task UI %q not found", ErrHumanTaskUINotFound, name)
	}

	delete(store, name)

	return nil
}

// ---------------------------------------------------------------------------
// Workforce
// ---------------------------------------------------------------------------

// CognitoConfig configures an Amazon Cognito private workforce.
type CognitoConfig struct {
	UserPool string `json:"UserPool"`
	ClientID string `json:"ClientId"`
}

// OidcConfig configures a private workforce using a customer-owned OIDC IdP.
// ClientSecret is stored but never echoed back in responses, matching the
// real OidcConfigForResponse shape.
type OidcConfig struct {
	AuthenticationRequestExtraParams map[string]string `json:"AuthenticationRequestExtraParams,omitempty"`
	ClientID                         string            `json:"ClientId"`
	ClientSecret                     string            `json:"-"`
	Issuer                           string            `json:"Issuer"`
	AuthorizationEndpoint            string            `json:"AuthorizationEndpoint"`
	TokenEndpoint                    string            `json:"TokenEndpoint"`
	UserInfoEndpoint                 string            `json:"UserInfoEndpoint"`
	LogoutEndpoint                   string            `json:"LogoutEndpoint"`
	JwksURI                          string            `json:"JwksUri"`
	Scope                            string            `json:"Scope,omitempty"`
}

// SourceIPConfig is a CIDR allow list restricting worker access to a workforce.
type SourceIPConfig struct {
	Cidrs []string `json:"Cidrs"`
}

// WorkforceVpcConfig describes the VPC a workforce connects through.
type WorkforceVpcConfig struct {
	VpcID            string   `json:"VpcId"`
	VpcEndpointID    string   `json:"VpcEndpointId,omitempty"`
	SecurityGroupIDs []string `json:"SecurityGroupIds,omitempty"`
	Subnets          []string `json:"Subnets,omitempty"`
}

// Workforce represents a SageMaker workforce.
type Workforce struct {
	CreateDate         time.Time           `json:"CreateDate"`
	LastUpdatedDate    time.Time           `json:"LastUpdatedDate"`
	Tags               map[string]string   `json:"-"`
	CognitoConfig      *CognitoConfig      `json:"CognitoConfig,omitempty"`
	OidcConfig         *OidcConfig         `json:"OidcConfig,omitempty"`
	SourceIPConfig     *SourceIPConfig     `json:"SourceIpConfig,omitempty"`
	WorkforceVpcConfig *WorkforceVpcConfig `json:"WorkforceVpcConfig,omitempty"`
	WorkforceName      string              `json:"WorkforceName"`
	WorkforceArn       string              `json:"WorkforceArn"`
	Status             string              `json:"Status"`
	SubDomain          string              `json:"SubDomain,omitempty"`
}

func cloneWorkforce(w *Workforce) *Workforce {
	cp := *w
	cp.Tags = maps.Clone(w.Tags)

	if w.CognitoConfig != nil {
		c := *w.CognitoConfig
		cp.CognitoConfig = &c
	}

	if w.OidcConfig != nil {
		o := *w.OidcConfig
		o.AuthenticationRequestExtraParams = maps.Clone(w.OidcConfig.AuthenticationRequestExtraParams)
		cp.OidcConfig = &o
	}

	if w.SourceIPConfig != nil {
		s := *w.SourceIPConfig
		s.Cidrs = append([]string(nil), w.SourceIPConfig.Cidrs...)
		cp.SourceIPConfig = &s
	}

	if w.WorkforceVpcConfig != nil {
		v := *w.WorkforceVpcConfig
		v.SecurityGroupIDs = append([]string(nil), w.WorkforceVpcConfig.SecurityGroupIDs...)
		v.Subnets = append([]string(nil), w.WorkforceVpcConfig.Subnets...)
		cp.WorkforceVpcConfig = &v
	}

	return &cp
}

// CreateWorkforceOptions holds the parameters for creating a workforce.
type CreateWorkforceOptions struct {
	CognitoConfig      *CognitoConfig
	OidcConfig         *OidcConfig
	SourceIPConfig     *SourceIPConfig
	WorkforceVpcConfig *WorkforceVpcConfig
	Tags               map[string]string
	Name               string
}

// CreateWorkforce creates a workforce. AWS allows at most one workforce per
// account per region.
func (b *InMemoryBackend) CreateWorkforce(ctx context.Context, opts CreateWorkforceOptions) (*Workforce, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateWorkforce")
	defer b.mu.Unlock()

	if opts.Name == "" {
		return nil, fmt.Errorf("%w: WorkforceName is required", ErrValidation)
	}

	store := b.workforcesStore(region)

	if _, ok := store[opts.Name]; ok {
		return nil, fmt.Errorf("%w: workforce %q already exists", ErrValidation, opts.Name)
	}

	if len(store) > 0 {
		return nil, fmt.Errorf(
			"%w: only one workforce is allowed per Amazon Web Services account per Amazon Web Services Region",
			ErrValidation,
		)
	}

	workforceARN := arn.Build("sagemaker", region, b.accountID, "workforce/"+opts.Name)
	now := time.Now()

	w := &Workforce{
		WorkforceName:      opts.Name,
		WorkforceArn:       workforceARN,
		Status:             statusActive,
		CognitoConfig:      opts.CognitoConfig,
		OidcConfig:         opts.OidcConfig,
		SourceIPConfig:     opts.SourceIPConfig,
		WorkforceVpcConfig: opts.WorkforceVpcConfig,
		Tags:               mergeTags(nil, opts.Tags),
		CreateDate:         now,
		LastUpdatedDate:    now,
	}
	if w.OidcConfig != nil {
		w.SubDomain = "https://" + generateID() + ".labeling.sagemaker.aws"
	}

	if w.WorkforceVpcConfig != nil {
		w.WorkforceVpcConfig.VpcEndpointID = "vpce-" + generateID()[:17]
	}

	store[opts.Name] = w

	return cloneWorkforce(w), nil
}

// DescribeWorkforce returns a workforce by name.
func (b *InMemoryBackend) DescribeWorkforce(ctx context.Context, name string) (*Workforce, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeWorkforce")
	defer b.mu.RUnlock()

	w, ok := b.workforcesStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: workforce %q not found", ErrWorkforceNotFound, name)
	}

	return cloneWorkforce(w), nil
}

// UpdateWorkforceOptions holds the parameters for updating a workforce.
type UpdateWorkforceOptions struct {
	OidcConfig         *OidcConfig
	SourceIPConfig     *SourceIPConfig
	WorkforceVpcConfig *WorkforceVpcConfig
	Name               string
}

// UpdateWorkforce updates a workforce's IdP, IP allow-list, or VPC configuration.
func (b *InMemoryBackend) UpdateWorkforce(ctx context.Context, opts UpdateWorkforceOptions) (*Workforce, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateWorkforce")
	defer b.mu.Unlock()

	w, ok := b.workforcesStore(region)[opts.Name]
	if !ok {
		return nil, fmt.Errorf("%w: workforce %q not found", ErrWorkforceNotFound, opts.Name)
	}

	if opts.OidcConfig != nil {
		w.OidcConfig = opts.OidcConfig
	}

	if opts.SourceIPConfig != nil {
		w.SourceIPConfig = opts.SourceIPConfig
	}

	if opts.WorkforceVpcConfig != nil {
		opts.WorkforceVpcConfig.VpcEndpointID = "vpce-" + generateID()[:17]
		w.WorkforceVpcConfig = opts.WorkforceVpcConfig
	}

	w.LastUpdatedDate = time.Now()

	return cloneWorkforce(w), nil
}

// DeleteWorkforce removes a workforce. Fails with ResourceInUse if the
// workforce still has one or more work teams (AWS requires DeleteWorkteam
// first, per the DeleteWorkforce API contract).
func (b *InMemoryBackend) DeleteWorkforce(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteWorkforce")
	defer b.mu.Unlock()

	w, ok := b.workforcesStore(region)[name]
	if !ok {
		return fmt.Errorf("%w: workforce %q not found", ErrWorkforceNotFound, name)
	}

	for _, wt := range b.workteamsStore(region) {
		if wt.WorkforceArn == w.WorkforceArn {
			return fmt.Errorf(
				"%w: workforce %q still has associated work teams", ErrWorkteamInUse, name,
			)
		}
	}

	delete(b.workforcesStore(region), name)

	return nil
}

// ListWorkforces returns all workforces sorted by name. AWS supports at most
// one workforce per account per region, so this list contains at most one item.
func (b *InMemoryBackend) ListWorkforces(ctx context.Context, nextToken string) ([]*Workforce, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListWorkforces")
	defer b.mu.RUnlock()

	return sagemakerListKeyPaged(b.workforcesStore(region), nextToken, cloneWorkforce)
}

// ---------------------------------------------------------------------------
// FlowDefinition
// ---------------------------------------------------------------------------

// FlowDefinition represents a SageMaker Augmented AI flow definition.
type FlowDefinition struct {
	CreationTime         time.Time         `json:"CreationTime"`
	Tags                 map[string]string `json:"Tags,omitempty"`
	FlowDefinitionName   string            `json:"FlowDefinitionName"`
	FlowDefinitionArn    string            `json:"FlowDefinitionArn"`
	FlowDefinitionStatus string            `json:"FlowDefinitionStatus"`
	RoleArn              string            `json:"RoleArn,omitempty"`
}

func cloneFlowDefinition(f *FlowDefinition) *FlowDefinition {
	cp := *f
	cp.Tags = maps.Clone(f.Tags)

	return &cp
}

// CreateFlowDefinition creates a flow definition.
func (b *InMemoryBackend) CreateFlowDefinition(
	ctx context.Context,
	name, roleArn string,
	tags map[string]string,
) (*FlowDefinition, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateFlowDefinition")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: FlowDefinitionName is required", ErrValidation)
	}

	store := b.flowDefinitionsStore(region)

	if _, ok := store[name]; ok {
		return nil, fmt.Errorf("%w: flow definition %q already exists", ErrValidation, name)
	}

	flowARN := arn.Build("sagemaker", region, b.accountID, "flow-definition/"+name)

	f := &FlowDefinition{
		FlowDefinitionName:   name,
		FlowDefinitionArn:    flowARN,
		FlowDefinitionStatus: statusActive,
		RoleArn:              roleArn,
		Tags:                 mergeTags(nil, tags),
		CreationTime:         time.Now(),
	}
	store[name] = f

	return cloneFlowDefinition(f), nil
}

// DescribeFlowDefinition returns a flow definition by name.
func (b *InMemoryBackend) DescribeFlowDefinition(ctx context.Context, name string) (*FlowDefinition, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeFlowDefinition")
	defer b.mu.RUnlock()

	f, ok := b.flowDefinitionsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: flow definition %q not found", ErrFlowDefinitionNotFound, name)
	}

	return cloneFlowDefinition(f), nil
}

// DeleteFlowDefinition removes a flow definition by name.
func (b *InMemoryBackend) DeleteFlowDefinition(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteFlowDefinition")
	defer b.mu.Unlock()

	store := b.flowDefinitionsStore(region)

	if _, ok := store[name]; !ok {
		return fmt.Errorf("%w: flow definition %q not found", ErrFlowDefinitionNotFound, name)
	}

	delete(store, name)

	return nil
}

// ---------------------------------------------------------------------------
// AppImageConfig
// ---------------------------------------------------------------------------

// AppImageConfig represents a SageMaker app image configuration.
type AppImageConfig struct {
	CreationTime       time.Time         `json:"CreationTime"`
	LastModifiedTime   time.Time         `json:"LastModifiedTime"`
	Tags               map[string]string `json:"Tags,omitempty"`
	AppImageConfigName string            `json:"AppImageConfigName"`
	AppImageConfigArn  string            `json:"AppImageConfigArn"`
}

func cloneAppImageConfig(a *AppImageConfig) *AppImageConfig {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)

	return &cp
}

// CreateAppImageConfig creates an app image config.
func (b *InMemoryBackend) CreateAppImageConfig(
	ctx context.Context,
	name string,
	tags map[string]string,
) (*AppImageConfig, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateAppImageConfig")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", ErrValidation)
	}

	store := b.appImageConfigsStore(region)

	if _, ok := store[name]; ok {
		return nil, fmt.Errorf("%w: app image config %q already exists", ErrValidation, name)
	}

	configARN := arn.Build("sagemaker", region, b.accountID, "app-image-config/"+name)
	now := time.Now()

	a := &AppImageConfig{
		AppImageConfigName: name,
		AppImageConfigArn:  configARN,
		Tags:               mergeTags(nil, tags),
		CreationTime:       now,
		LastModifiedTime:   now,
	}
	store[name] = a

	return cloneAppImageConfig(a), nil
}

// DescribeAppImageConfig returns an app image config by name.
func (b *InMemoryBackend) DescribeAppImageConfig(ctx context.Context, name string) (*AppImageConfig, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeAppImageConfig")
	defer b.mu.RUnlock()

	a, ok := b.appImageConfigsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: app image config %q not found", ErrAppImageConfigNotFound, name)
	}

	return cloneAppImageConfig(a), nil
}

// UpdateAppImageConfig updates an app image config (marks it modified).
func (b *InMemoryBackend) UpdateAppImageConfig(ctx context.Context, name string) (*AppImageConfig, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateAppImageConfig")
	defer b.mu.Unlock()

	a, ok := b.appImageConfigsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: app image config %q not found", ErrAppImageConfigNotFound, name)
	}

	a.LastModifiedTime = time.Now()

	return cloneAppImageConfig(a), nil
}

// DeleteAppImageConfig removes an app image config by name.
func (b *InMemoryBackend) DeleteAppImageConfig(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteAppImageConfig")
	defer b.mu.Unlock()

	store := b.appImageConfigsStore(region)

	if _, ok := store[name]; !ok {
		return fmt.Errorf("%w: app image config %q not found", ErrAppImageConfigNotFound, name)
	}

	delete(store, name)

	return nil
}

// ---------------------------------------------------------------------------
// InferenceExperiment
// ---------------------------------------------------------------------------

// InferenceExperiment represents a SageMaker inference experiment.
type InferenceExperiment struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	Name             string            `json:"Name"`
	Arn              string            `json:"Arn"`
	Status           string            `json:"Status"`
	Type             string            `json:"Type,omitempty"`
	RoleArn          string            `json:"RoleArn,omitempty"`
}

func cloneInferenceExperiment(e *InferenceExperiment) *InferenceExperiment {
	cp := *e
	cp.Tags = maps.Clone(e.Tags)

	return &cp
}

// CreateInferenceExperiment creates an inference experiment.
func (b *InMemoryBackend) CreateInferenceExperiment(
	ctx context.Context,
	name, expType, roleArn string,
	tags map[string]string,
) (*InferenceExperiment, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	return sagemakerCreate(ctx, b,
		"CreateInferenceExperiment", name, "inference-experiment",
		b.inferenceExperimentsStore,
		func(n string) error { return sagemakerDupErr("inference experiment", n) },
		func(arnStr string, now time.Time) *InferenceExperiment {
			return &InferenceExperiment{
				Name:             name,
				Arn:              arnStr,
				Status:           "Running",
				Type:             expType,
				RoleArn:          roleArn,
				Tags:             mergeTags(nil, tags),
				CreationTime:     now,
				LastModifiedTime: now,
			}
		},
		cloneInferenceExperiment,
	)
}

// DescribeInferenceExperiment returns an inference experiment by name.
func (b *InMemoryBackend) DescribeInferenceExperiment(ctx context.Context, name string) (*InferenceExperiment, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeInferenceExperiment")
	defer b.mu.RUnlock()

	e, ok := b.inferenceExperimentsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: inference experiment %q not found", ErrInferenceExperimentNotFound, name)
	}

	return cloneInferenceExperiment(e), nil
}

// StopInferenceExperiment sets an inference experiment status to "Cancelled".
func (b *InMemoryBackend) StopInferenceExperiment(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StopInferenceExperiment")
	defer b.mu.Unlock()

	e, ok := b.inferenceExperimentsStore(region)[name]
	if !ok {
		return fmt.Errorf("%w: inference experiment %q not found", ErrInferenceExperimentNotFound, name)
	}

	e.Status = "Cancelled"
	e.LastModifiedTime = time.Now()

	return nil
}

// DeleteInferenceExperiment removes an inference experiment by name.
func (b *InMemoryBackend) DeleteInferenceExperiment(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteInferenceExperiment")
	defer b.mu.Unlock()

	store := b.inferenceExperimentsStore(region)

	if _, ok := store[name]; !ok {
		return fmt.Errorf("%w: inference experiment %q not found", ErrInferenceExperimentNotFound, name)
	}

	delete(store, name)

	return nil
}

// ---------------------------------------------------------------------------
// MlflowTrackingServer
// ---------------------------------------------------------------------------

// MlflowTrackingServer represents a SageMaker MLflow tracking server.
type MlflowTrackingServer struct {
	CreationTime         time.Time         `json:"CreationTime"`
	LastModifiedTime     time.Time         `json:"LastModifiedTime"`
	Tags                 map[string]string `json:"Tags,omitempty"`
	TrackingServerName   string            `json:"TrackingServerName"`
	TrackingServerArn    string            `json:"TrackingServerArn"`
	TrackingServerStatus string            `json:"TrackingServerStatus"`
	RoleArn              string            `json:"RoleArn,omitempty"`
	MlflowVersion        string            `json:"MlflowVersion,omitempty"`
}

func cloneMlflowTrackingServer(s *MlflowTrackingServer) *MlflowTrackingServer {
	cp := *s
	cp.Tags = maps.Clone(s.Tags)

	return &cp
}

// CreateMlflowTrackingServer creates an MLflow tracking server.
func (b *InMemoryBackend) CreateMlflowTrackingServer(
	ctx context.Context,
	name, roleArn, mlflowVersion string,
	tags map[string]string,
) (*MlflowTrackingServer, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", ErrValidation)
	}

	return sagemakerCreate(ctx, b,
		"CreateMlflowTrackingServer", name, "mlflow-tracking-server",
		b.mlflowTrackingServersStore,
		func(n string) error { return sagemakerDupErr("MLflow tracking server", n) },
		func(arnStr string, now time.Time) *MlflowTrackingServer {
			return &MlflowTrackingServer{
				TrackingServerName:   name,
				TrackingServerArn:    arnStr,
				TrackingServerStatus: "Created",
				RoleArn:              roleArn,
				MlflowVersion:        mlflowVersion,
				Tags:                 mergeTags(nil, tags),
				CreationTime:         now,
				LastModifiedTime:     now,
			}
		},
		cloneMlflowTrackingServer,
	)
}

// DescribeMlflowTrackingServer returns an MLflow tracking server by name.
func (b *InMemoryBackend) DescribeMlflowTrackingServer(
	ctx context.Context,
	name string,
) (*MlflowTrackingServer, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeMlflowTrackingServer")
	defer b.mu.RUnlock()

	s, ok := b.mlflowTrackingServersStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	return cloneMlflowTrackingServer(s), nil
}

// DeleteMlflowTrackingServer removes an MLflow tracking server by name.
func (b *InMemoryBackend) DeleteMlflowTrackingServer(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteMlflowTrackingServer")
	defer b.mu.Unlock()

	store := b.mlflowTrackingServersStore(region)

	if _, ok := store[name]; !ok {
		return fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	delete(store, name)

	return nil
}

// StartMlflowTrackingServer sets an MLflow tracking server status to "Running".
func (b *InMemoryBackend) StartMlflowTrackingServer(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StartMlflowTrackingServer")
	defer b.mu.Unlock()

	s, ok := b.mlflowTrackingServersStore(region)[name]
	if !ok {
		return fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	s.TrackingServerStatus = "Running"
	s.LastModifiedTime = time.Now()

	return nil
}

// StopMlflowTrackingServer sets an MLflow tracking server status to "Stopped".
func (b *InMemoryBackend) StopMlflowTrackingServer(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StopMlflowTrackingServer")
	defer b.mu.Unlock()

	s, ok := b.mlflowTrackingServersStore(region)[name]
	if !ok {
		return fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	s.TrackingServerStatus = pipelineStatusStopped
	s.LastModifiedTime = time.Now()

	return nil
}

// ---------------------------------------------------------------------------
// ModelCard
// ---------------------------------------------------------------------------

// ModelCard represents a SageMaker model card.
type ModelCard struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	ModelCardName    string            `json:"ModelCardName"`
	ModelCardArn     string            `json:"ModelCardArn"`
	ModelCardStatus  string            `json:"ModelCardStatus"`
	Content          string            `json:"Content,omitempty"`
	ModelCardVersion int               `json:"ModelCardVersion"`
}

func cloneModelCard(c *ModelCard) *ModelCard {
	cp := *c
	cp.Tags = maps.Clone(c.Tags)

	return &cp
}

// CreateModelCard creates a model card.
func (b *InMemoryBackend) CreateModelCard(
	ctx context.Context,
	name, content string,
	tags map[string]string,
) (*ModelCard, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateModelCard")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", ErrValidation)
	}

	store := b.modelCardsStore(region)

	if _, ok := store[name]; ok {
		return nil, fmt.Errorf("%w: model card %q already exists", ErrValidation, name)
	}

	cardARN := arn.Build("sagemaker", region, b.accountID, "model-card/"+name)
	now := time.Now()

	c := &ModelCard{
		ModelCardName:    name,
		ModelCardArn:     cardARN,
		ModelCardStatus:  "Draft",
		ModelCardVersion: 1,
		Content:          content,
		Tags:             mergeTags(nil, tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	store[name] = c

	return cloneModelCard(c), nil
}

// DescribeModelCard returns a model card by name.
func (b *InMemoryBackend) DescribeModelCard(ctx context.Context, name string) (*ModelCard, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeModelCard")
	defer b.mu.RUnlock()

	c, ok := b.modelCardsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: model card %q not found", ErrModelCardNotFound, name)
	}

	return cloneModelCard(c), nil
}

// UpdateModelCard updates a model card content and increments its version.
func (b *InMemoryBackend) UpdateModelCard(ctx context.Context, name, content string) (*ModelCard, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateModelCard")
	defer b.mu.Unlock()

	c, ok := b.modelCardsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: model card %q not found", ErrModelCardNotFound, name)
	}

	c.Content = content
	c.ModelCardVersion++
	c.LastModifiedTime = time.Now()

	return cloneModelCard(c), nil
}

// DeleteModelCard removes a model card by name.
func (b *InMemoryBackend) DeleteModelCard(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteModelCard")
	defer b.mu.Unlock()

	store := b.modelCardsStore(region)

	if _, ok := store[name]; !ok {
		return fmt.Errorf("%w: model card %q not found", ErrModelCardNotFound, name)
	}

	delete(store, name)

	return nil
}

// ---------------------------------------------------------------------------
// OptimizationJob
// ---------------------------------------------------------------------------

// OptimizationJob represents a SageMaker optimization job.
type OptimizationJob struct {
	CreationTime          time.Time         `json:"CreationTime"`
	LastModifiedTime      time.Time         `json:"LastModifiedTime"`
	Tags                  map[string]string `json:"Tags,omitempty"`
	OptimizationJobName   string            `json:"OptimizationJobName"`
	OptimizationJobArn    string            `json:"OptimizationJobArn"`
	OptimizationJobStatus string            `json:"OptimizationJobStatus"`
	RoleArn               string            `json:"RoleArn,omitempty"`
}

func cloneOptimizationJob(j *OptimizationJob) *OptimizationJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	return &cp
}

// CreateOptimizationJob creates an optimization job.
func (b *InMemoryBackend) CreateOptimizationJob(
	ctx context.Context,
	name, roleArn string,
	tags map[string]string,
) (*OptimizationJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateOptimizationJob")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: OptimizationJobName is required", ErrValidation)
	}

	store := b.optimizationJobsStore(region)

	if _, ok := store[name]; ok {
		return nil, fmt.Errorf("%w: optimization job %q already exists", ErrValidation, name)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "optimization-job/"+name)
	now := time.Now()

	j := &OptimizationJob{
		OptimizationJobName:   name,
		OptimizationJobArn:    jobARN,
		OptimizationJobStatus: "COMPLETED",
		RoleArn:               roleArn,
		Tags:                  mergeTags(nil, tags),
		CreationTime:          now,
		LastModifiedTime:      now,
	}
	store[name] = j

	return cloneOptimizationJob(j), nil
}

// DescribeOptimizationJob returns an optimization job by name.
func (b *InMemoryBackend) DescribeOptimizationJob(ctx context.Context, name string) (*OptimizationJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeOptimizationJob")
	defer b.mu.RUnlock()

	j, ok := b.optimizationJobsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: optimization job %q not found", ErrOptimizationJobNotFound, name)
	}

	return cloneOptimizationJob(j), nil
}

// DeleteOptimizationJob removes an optimization job by name.
func (b *InMemoryBackend) DeleteOptimizationJob(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteOptimizationJob")
	defer b.mu.Unlock()

	store := b.optimizationJobsStore(region)

	if _, ok := store[name]; !ok {
		return fmt.Errorf("%w: optimization job %q not found", ErrOptimizationJobNotFound, name)
	}

	delete(store, name)

	return nil
}

// StopOptimizationJob sets an optimization job status to "STOPPED".
func (b *InMemoryBackend) StopOptimizationJob(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StopOptimizationJob")
	defer b.mu.Unlock()

	j, ok := b.optimizationJobsStore(region)[name]
	if !ok {
		return fmt.Errorf("%w: optimization job %q not found", ErrOptimizationJobNotFound, name)
	}

	j.OptimizationJobStatus = jobStatusStopped
	j.LastModifiedTime = time.Now()

	return nil
}

// ---------------------------------------------------------------------------
// StudioLifecycleConfig
// ---------------------------------------------------------------------------

// StudioLifecycleConfig represents a SageMaker Studio lifecycle configuration.
type StudioLifecycleConfig struct {
	CreationTime                 time.Time         `json:"CreationTime"`
	LastModifiedTime             time.Time         `json:"LastModifiedTime"`
	Tags                         map[string]string `json:"Tags,omitempty"`
	StudioLifecycleConfigName    string            `json:"StudioLifecycleConfigName"`
	StudioLifecycleConfigArn     string            `json:"StudioLifecycleConfigArn"`
	StudioLifecycleConfigAppType string            `json:"StudioLifecycleConfigAppType,omitempty"`
}

func cloneStudioLifecycleConfig(s *StudioLifecycleConfig) *StudioLifecycleConfig {
	cp := *s
	cp.Tags = maps.Clone(s.Tags)

	return &cp
}

// CreateStudioLifecycleConfig creates a Studio lifecycle configuration.
func (b *InMemoryBackend) CreateStudioLifecycleConfig(
	ctx context.Context,
	name, appType string,
	tags map[string]string,
) (*StudioLifecycleConfig, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateStudioLifecycleConfig")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: StudioLifecycleConfigName is required", ErrValidation)
	}

	store := b.studioLifecycleConfigsStore(region)

	if _, ok := store[name]; ok {
		return nil, fmt.Errorf("%w: Studio lifecycle config %q already exists", ErrValidation, name)
	}

	configARN := arn.Build("sagemaker", region, b.accountID, "studio-lifecycle-config/"+name)
	now := time.Now()

	s := &StudioLifecycleConfig{
		StudioLifecycleConfigName:    name,
		StudioLifecycleConfigArn:     configARN,
		StudioLifecycleConfigAppType: appType,
		Tags:                         mergeTags(nil, tags),
		CreationTime:                 now,
		LastModifiedTime:             now,
	}
	store[name] = s

	return cloneStudioLifecycleConfig(s), nil
}

// DescribeStudioLifecycleConfig returns a Studio lifecycle configuration by name.
func (b *InMemoryBackend) DescribeStudioLifecycleConfig(
	ctx context.Context,
	name string,
) (*StudioLifecycleConfig, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeStudioLifecycleConfig")
	defer b.mu.RUnlock()

	s, ok := b.studioLifecycleConfigsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: Studio lifecycle config %q not found", ErrStudioLifecycleConfigNotFound, name)
	}

	return cloneStudioLifecycleConfig(s), nil
}

// DeleteStudioLifecycleConfig removes a Studio lifecycle configuration by name.
func (b *InMemoryBackend) DeleteStudioLifecycleConfig(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteStudioLifecycleConfig")
	defer b.mu.Unlock()

	store := b.studioLifecycleConfigsStore(region)

	if _, ok := store[name]; !ok {
		return fmt.Errorf("%w: Studio lifecycle config %q not found", ErrStudioLifecycleConfigNotFound, name)
	}

	delete(store, name)

	return nil
}

// ---------------------------------------------------------------------------
// PartnerApp
// ---------------------------------------------------------------------------

// PartnerApp represents a SageMaker partner app.
type PartnerApp struct {
	CreationTime time.Time         `json:"CreationTime"`
	Tags         map[string]string `json:"Tags,omitempty"`
	Name         string            `json:"Name"`
	Arn          string            `json:"Arn"`
	Status       string            `json:"Status"`
	Type         string            `json:"Type,omitempty"`
}

func clonePartnerApp(p *PartnerApp) *PartnerApp {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	return &cp
}

// CreatePartnerApp creates a partner app. Stores by ARN; returns both name and ARN.
func (b *InMemoryBackend) CreatePartnerApp(
	ctx context.Context,
	name, appType string,
	tags map[string]string,
) (*PartnerApp, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreatePartnerApp")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	appARN := arn.Build("sagemaker", region, b.accountID, "partner-app/"+name)

	store := b.partnerAppsStore(region)

	if _, ok := store[appARN]; ok {
		return nil, fmt.Errorf("%w: partner app %q already exists", ErrValidation, name)
	}

	p := &PartnerApp{
		Name:         name,
		Arn:          appARN,
		Status:       "Available",
		Type:         appType,
		Tags:         mergeTags(nil, tags),
		CreationTime: time.Now(),
	}
	store[appARN] = p

	return clonePartnerApp(p), nil
}

// DescribePartnerApp returns a partner app by ARN.
func (b *InMemoryBackend) DescribePartnerApp(ctx context.Context, arnStr string) (*PartnerApp, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribePartnerApp")
	defer b.mu.RUnlock()

	p, ok := b.partnerAppsStore(region)[arnStr]
	if !ok {
		return nil, fmt.Errorf("%w: partner app %q not found", ErrPartnerAppNotFound, arnStr)
	}

	return clonePartnerApp(p), nil
}

// DeletePartnerApp removes a partner app by ARN.
func (b *InMemoryBackend) DeletePartnerApp(ctx context.Context, arnStr string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeletePartnerApp")
	defer b.mu.Unlock()

	store := b.partnerAppsStore(region)

	if _, ok := store[arnStr]; !ok {
		return fmt.Errorf("%w: partner app %q not found", ErrPartnerAppNotFound, arnStr)
	}

	delete(store, arnStr)

	return nil
}

// ---------------------------------------------------------------------------
// TrainingPlan
// ---------------------------------------------------------------------------

// TrainingPlan represents a SageMaker training plan.
type TrainingPlan struct {
	CreationTime     time.Time         `json:"CreationTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	TrainingPlanName string            `json:"TrainingPlanName"`
	TrainingPlanArn  string            `json:"TrainingPlanArn"`
	Status           string            `json:"Status"`
}

func cloneTrainingPlan(t *TrainingPlan) *TrainingPlan {
	cp := *t
	cp.Tags = maps.Clone(t.Tags)

	return &cp
}

// CreateTrainingPlan creates a training plan.
func (b *InMemoryBackend) CreateTrainingPlan(
	ctx context.Context,
	name string,
	tags map[string]string,
) (*TrainingPlan, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateTrainingPlan")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: TrainingPlanName is required", ErrValidation)
	}

	store := b.trainingPlansStore(region)

	if _, ok := store[name]; ok {
		return nil, fmt.Errorf("%w: training plan %q already exists", ErrValidation, name)
	}

	planARN := arn.Build("sagemaker", region, b.accountID, "training-plan/"+name)

	t := &TrainingPlan{
		TrainingPlanName: name,
		TrainingPlanArn:  planARN,
		Status:           statusActive,
		Tags:             mergeTags(nil, tags),
		CreationTime:     time.Now(),
	}
	store[name] = t

	return cloneTrainingPlan(t), nil
}

// DescribeTrainingPlan returns a training plan by name.
func (b *InMemoryBackend) DescribeTrainingPlan(ctx context.Context, name string) (*TrainingPlan, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeTrainingPlan")
	defer b.mu.RUnlock()

	t, ok := b.trainingPlansStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: training plan %q not found", ErrTrainingPlanNotFound, name)
	}

	return cloneTrainingPlan(t), nil
}
