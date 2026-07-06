package glue

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// --- New model types ---

// UsageProfile represents a Glue usage profile.
type UsageProfile struct {
	CreatedOn      time.Time         `json:"CreatedOn"`
	LastModifiedOn time.Time         `json:"LastModifiedOn"`
	Tags           map[string]string `json:"Tags,omitempty"`
	Name           string            `json:"Name"`
	Description    string            `json:"Description,omitempty"`
}

// BlueprintRun represents a single execution of a Glue blueprint.
type BlueprintRun struct {
	StartedOn     time.Time `json:"StartedOn"`
	BlueprintName string    `json:"BlueprintName"`
	RunID         string    `json:"RunId"`
	WorkflowName  string    `json:"WorkflowName"`
	State         string    `json:"State"`
}

// DQRuleRecommendationRun represents a data quality rule recommendation run.
type DQRuleRecommendationRun struct {
	StartedOn           time.Time `json:"StartedOn"`
	RecommendationRunID string    `json:"RecommendationRunId"`
	DataSourceS3Path    string    `json:"DataSourceS3Path,omitempty"`
	Status              string    `json:"Status"`
}

// ColumnStatisticsTaskSettings represents column statistics task settings.
type ColumnStatisticsTaskSettings struct {
	Schedule       CrawlerSchedule `json:"Schedule,omitzero"`
	DatabaseName   string          `json:"DatabaseName"`
	TableName      string          `json:"TableName"`
	RoleArn        string          `json:"RoleArn,omitempty"`
	ColumnNameList []string        `json:"ColumnNameList,omitempty"`
}

// ColumnStatisticsTaskRun represents a column statistics task run.
type ColumnStatisticsTaskRun struct {
	StartedOn                 time.Time `json:"StartedOn"`
	DatabaseName              string    `json:"DatabaseName"`
	TableName                 string    `json:"TableName"`
	ColumnStatisticsTaskRunID string    `json:"ColumnStatisticsTaskRunId"`
	Status                    string    `json:"Status"`
}

// MaterializedViewRefreshRun represents a materialized view refresh task run.
type MaterializedViewRefreshRun struct {
	StartedOn    time.Time `json:"StartedOn"`
	DatabaseName string    `json:"DatabaseName"`
	TableName    string    `json:"TableName"`
	TaskRunID    string    `json:"TaskRunId"`
	Status       string    `json:"Status"`
}

// Integration represents a Glue integration.
type Integration struct {
	CreatedAt       time.Time         `json:"CreateTime"`
	Tags            map[string]string `json:"Tags,omitempty"`
	IntegrationName string            `json:"IntegrationName"`
	Status          string            `json:"Status"`
}

// IdentityCenterConfig represents the Glue Identity Center configuration.
type IdentityCenterConfig struct {
	InstanceARN string `json:"InstanceArn,omitempty"`
	Status      string `json:"Status"`
}

// --- Error variables ---

var (
	ErrUsageProfileNotFound        = fmt.Errorf("usage profile not found: %w", ErrNotFound)
	ErrBlueprintRunNotFound        = fmt.Errorf("blueprint run not found: %w", ErrNotFound)
	ErrDQRecommendationRunNotFound = fmt.Errorf("data quality recommendation run not found: %w", ErrNotFound)
	ErrColumnStatTaskRunNotFound   = fmt.Errorf("column statistics task run not found: %w", ErrNotFound)
	ErrMaterializedViewRunNotFound = fmt.Errorf("materialized view refresh run not found: %w", ErrNotFound)
	ErrIntegrationNotFound         = fmt.Errorf("integration not found: %w", ErrNotFound)
)

// --- Blueprint CRUD ---

// CreateBlueprint stores a new blueprint.
func (b *InMemoryBackend) CreateBlueprint(name string) error {
	if name == "" {
		return fmt.Errorf("%w: blueprint Name is required", ErrValidation)
	}

	b.mu.Lock("CreateBlueprint")
	defer b.mu.Unlock()

	if b.blueprints.Has(name) {
		return fmt.Errorf("blueprint %q already exists: %w", name, ErrAlreadyExists)
	}

	b.blueprints.Put(&Blueprint{Name: name, Status: "ACTIVE"})

	return nil
}

// DeleteBlueprint removes a blueprint.
func (b *InMemoryBackend) DeleteBlueprint(name string) error {
	b.mu.Lock("DeleteBlueprint")
	defer b.mu.Unlock()

	if !b.blueprints.Has(name) {
		return fmt.Errorf("blueprint %q not found: %w", name, ErrNotFound)
	}

	b.blueprints.Delete(name)

	return nil
}

// UpdateBlueprint updates an existing blueprint.
func (b *InMemoryBackend) UpdateBlueprint(name string) (*Blueprint, error) {
	b.mu.Lock("UpdateBlueprint")
	defer b.mu.Unlock()

	bp, ok := b.blueprints.Get(name)
	if !ok {
		return nil, fmt.Errorf("blueprint %q not found: %w", name, ErrNotFound)
	}

	cp := *bp

	return &cp, nil
}

// ListBlueprints returns all blueprint names.
func (b *InMemoryBackend) ListBlueprints() []string {
	b.mu.RLock("ListBlueprints")
	defer b.mu.RUnlock()

	src := b.blueprints.Snapshot()
	names := make([]string, len(src))
	for i, bp := range src {
		names[i] = bp.Name
	}

	return names
}

// --- Blueprint Runs ---

// StartBlueprintRun creates a new blueprint run record.
func (b *InMemoryBackend) StartBlueprintRun(blueprintName string) (*BlueprintRun, error) {
	b.mu.Lock("StartBlueprintRun")
	defer b.mu.Unlock()

	if !b.blueprints.Has(blueprintName) {
		return nil, fmt.Errorf("blueprint %q not found: %w", blueprintName, ErrNotFound)
	}

	runID := "bp-run-" + uuid.NewString()[:8]
	run := &BlueprintRun{
		BlueprintName: blueprintName,
		RunID:         runID,
		WorkflowName:  "workflow-" + runID,
		State:         stateRunning,
		StartedOn:     time.Now().UTC(),
	}
	b.blueprintRuns.Put(run)

	cp := *run

	return &cp, nil
}

// GetBlueprintRun returns a blueprint run by ID.
func (b *InMemoryBackend) GetBlueprintRun(blueprintName, runID string) (*BlueprintRun, error) {
	b.mu.RLock("GetBlueprintRun")
	defer b.mu.RUnlock()

	run, ok := b.blueprintRuns.Get(runID)
	if !ok || (blueprintName != "" && run.BlueprintName != blueprintName) {
		return nil, ErrBlueprintRunNotFound
	}

	cp := *run

	return &cp, nil
}

// GetBlueprintRuns returns all runs for a blueprint.
func (b *InMemoryBackend) GetBlueprintRuns(blueprintName string) []*BlueprintRun {
	b.mu.RLock("GetBlueprintRuns")
	defer b.mu.RUnlock()

	var runs []*BlueprintRun
	for _, r := range b.blueprintRuns.All() {
		if blueprintName == "" || r.BlueprintName == blueprintName {
			cp := *r
			runs = append(runs, &cp)
		}
	}

	sort.Slice(runs, func(i, k int) bool {
		return runs[i].StartedOn.Before(runs[k].StartedOn)
	})

	return runs
}

// --- UsageProfile CRUD ---

// CreateUsageProfile creates a new usage profile.
func (b *InMemoryBackend) CreateUsageProfile(name, description string, tags map[string]string) (*UsageProfile, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: UsageProfile Name is required", ErrValidation)
	}

	b.mu.Lock("CreateUsageProfile")
	defer b.mu.Unlock()

	if b.usageProfiles.Has(name) {
		return nil, fmt.Errorf("usage profile %q already exists: %w", name, ErrAlreadyExists)
	}

	now := time.Now().UTC()
	p := &UsageProfile{
		Name:           name,
		Description:    description,
		Tags:           tags,
		CreatedOn:      now,
		LastModifiedOn: now,
	}
	b.usageProfiles.Put(p)
	cp := *p

	return &cp, nil
}

// GetUsageProfile returns a usage profile by name.
func (b *InMemoryBackend) GetUsageProfile(name string) (*UsageProfile, error) {
	b.mu.RLock("GetUsageProfile")
	defer b.mu.RUnlock()

	p, ok := b.usageProfiles.Get(name)
	if !ok {
		return nil, ErrUsageProfileNotFound
	}

	cp := *p

	return &cp, nil
}

// DeleteUsageProfile removes a usage profile.
func (b *InMemoryBackend) DeleteUsageProfile(name string) error {
	b.mu.Lock("DeleteUsageProfile")
	defer b.mu.Unlock()

	if !b.usageProfiles.Has(name) {
		return ErrUsageProfileNotFound
	}

	b.usageProfiles.Delete(name)

	return nil
}

// ListUsageProfiles returns all usage profiles.
func (b *InMemoryBackend) ListUsageProfiles() []*UsageProfile {
	b.mu.RLock("ListUsageProfiles")
	defer b.mu.RUnlock()

	src := b.usageProfiles.All()
	profiles := make([]*UsageProfile, 0, len(src))
	for _, p := range src {
		cp := *p
		profiles = append(profiles, &cp)
	}

	sort.Slice(profiles, func(i, k int) bool {
		return profiles[i].Name < profiles[k].Name
	})

	return profiles
}

// UpdateUsageProfile updates a usage profile.
func (b *InMemoryBackend) UpdateUsageProfile(name, description string) (*UsageProfile, error) {
	b.mu.Lock("UpdateUsageProfile")
	defer b.mu.Unlock()

	p, ok := b.usageProfiles.Get(name)
	if !ok {
		return nil, ErrUsageProfileNotFound
	}

	p.Description = description
	p.LastModifiedOn = time.Now().UTC()

	cp := *p

	return &cp, nil
}

// --- CustomEntityType individual CRUD ---

// CreateCustomEntityType stores a new custom entity type.
func (b *InMemoryBackend) CreateCustomEntityType(
	name, regexString string,
	contextWords []string,
) (*CustomEntityType, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: CustomEntityType Name is required", ErrValidation)
	}
	if regexString == "" {
		return nil, fmt.Errorf("%w: CustomEntityType RegexString is required", ErrValidation)
	}

	b.mu.Lock("CreateCustomEntityType")
	defer b.mu.Unlock()

	cet := &CustomEntityType{
		Name:         name,
		RegexString:  regexString,
		ContextWords: contextWords,
	}
	b.customEntityTypes.Put(cet)
	cp := *cet

	return &cp, nil
}

// GetCustomEntityType returns a custom entity type by name.
func (b *InMemoryBackend) GetCustomEntityType(name string) (*CustomEntityType, error) {
	b.mu.RLock("GetCustomEntityType")
	defer b.mu.RUnlock()

	cet, ok := b.customEntityTypes.Get(name)
	if !ok {
		return nil, fmt.Errorf("custom entity type %q not found: %w", name, ErrNotFound)
	}

	cp := *cet

	return &cp, nil
}

// DeleteCustomEntityType removes a custom entity type.
func (b *InMemoryBackend) DeleteCustomEntityType(name string) error {
	b.mu.Lock("DeleteCustomEntityType")
	defer b.mu.Unlock()

	if !b.customEntityTypes.Has(name) {
		return fmt.Errorf("custom entity type %q not found: %w", name, ErrNotFound)
	}

	b.customEntityTypes.Delete(name)

	return nil
}

// ListCustomEntityTypes returns all custom entity type names.
func (b *InMemoryBackend) ListCustomEntityTypes() []*CustomEntityType {
	b.mu.RLock("ListCustomEntityTypes")
	defer b.mu.RUnlock()

	src := b.customEntityTypes.All()
	list := make([]*CustomEntityType, 0, len(src))
	for _, cet := range src {
		cp := *cet
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, k int) bool {
		return list[i].Name < list[k].Name
	})

	return list
}

// --- DataQuality Recommendation Runs ---

// StartDataQualityRuleRecommendationRun creates a recommendation run.
func (b *InMemoryBackend) StartDataQualityRuleRecommendationRun(s3Path string) (*DQRuleRecommendationRun, error) {
	b.mu.Lock("StartDataQualityRuleRecommendationRun")
	defer b.mu.Unlock()

	runID := "dqrec-" + uuid.NewString()[:8]
	run := &DQRuleRecommendationRun{
		RecommendationRunID: runID,
		DataSourceS3Path:    s3Path,
		Status:              stateRunning,
		StartedOn:           time.Now().UTC(),
	}
	b.dqRecommendationRuns.Put(run)
	cp := *run

	return &cp, nil
}

// GetDataQualityRuleRecommendationRun returns a recommendation run.
func (b *InMemoryBackend) GetDataQualityRuleRecommendationRun(runID string) (*DQRuleRecommendationRun, error) {
	b.mu.RLock("GetDataQualityRuleRecommendationRun")
	defer b.mu.RUnlock()

	run, ok := b.dqRecommendationRuns.Get(runID)
	if !ok {
		return nil, ErrDQRecommendationRunNotFound
	}

	cp := *run

	return &cp, nil
}

// CancelDataQualityRuleRecommendationRun marks a recommendation run as cancelled.
func (b *InMemoryBackend) CancelDataQualityRuleRecommendationRun(runID string) error {
	b.mu.Lock("CancelDataQualityRuleRecommendationRun")
	defer b.mu.Unlock()

	run, ok := b.dqRecommendationRuns.Get(runID)
	if !ok {
		return ErrDQRecommendationRunNotFound
	}

	run.Status = "CANCELLED"

	return nil
}

// ListDataQualityRuleRecommendationRuns returns all recommendation runs.
func (b *InMemoryBackend) ListDataQualityRuleRecommendationRuns() []*DQRuleRecommendationRun {
	b.mu.RLock("ListDataQualityRuleRecommendationRuns")
	defer b.mu.RUnlock()

	src := b.dqRecommendationRuns.All()
	runs := make([]*DQRuleRecommendationRun, 0, len(src))
	for _, r := range src {
		cp := *r
		runs = append(runs, &cp)
	}

	sort.Slice(runs, func(i, k int) bool {
		return runs[i].StartedOn.Before(runs[k].StartedOn)
	})

	return runs
}

// --- ColumnStatisticsTask CRUD ---

// columnStatTaskKey builds the key for task settings.
func columnStatTaskKey(dbName, tableName string) string {
	return dbName + "|" + tableName
}

// CreateColumnStatisticsTaskSettings stores task settings.
func (b *InMemoryBackend) CreateColumnStatisticsTaskSettings(
	dbName, tableName, roleArn string,
	columns []string,
) (*ColumnStatisticsTaskSettings, error) {
	b.mu.Lock("CreateColumnStatisticsTaskSettings")
	defer b.mu.Unlock()

	settings := &ColumnStatisticsTaskSettings{
		DatabaseName:   dbName,
		TableName:      tableName,
		ColumnNameList: columns,
		RoleArn:        roleArn,
	}
	b.columnStatTaskSettings.Put(settings)
	cp := *settings

	return &cp, nil
}

// GetColumnStatisticsTaskSettings returns task settings.
func (b *InMemoryBackend) GetColumnStatisticsTaskSettings(
	dbName, tableName string,
) (*ColumnStatisticsTaskSettings, error) {
	b.mu.RLock("GetColumnStatisticsTaskSettings")
	defer b.mu.RUnlock()

	key := columnStatTaskKey(dbName, tableName)
	s, ok := b.columnStatTaskSettings.Get(key)

	if !ok {
		return &ColumnStatisticsTaskSettings{DatabaseName: dbName, TableName: tableName}, nil
	}

	cp := *s

	return &cp, nil
}

// UpdateColumnStatisticsTaskSettings updates task settings.
func (b *InMemoryBackend) UpdateColumnStatisticsTaskSettings(
	dbName, tableName, roleArn string,
) error {
	b.mu.Lock("UpdateColumnStatisticsTaskSettings")
	defer b.mu.Unlock()

	key := columnStatTaskKey(dbName, tableName)
	if s, ok := b.columnStatTaskSettings.Get(key); ok {
		s.RoleArn = roleArn
	}

	return nil
}

// DeleteColumnStatisticsTaskSettings removes task settings.
func (b *InMemoryBackend) DeleteColumnStatisticsTaskSettings(dbName, tableName string) error {
	b.mu.Lock("DeleteColumnStatisticsTaskSettings")
	defer b.mu.Unlock()

	b.columnStatTaskSettings.Delete(columnStatTaskKey(dbName, tableName))

	return nil
}

// StartColumnStatisticsTaskRunSchedule enables the run schedule for a table's
// column statistics task settings. The settings must already exist (created via
// CreateColumnStatisticsTaskSettings), matching AWS's requirement that a
// schedule can only be attached to existing task settings.
func (b *InMemoryBackend) StartColumnStatisticsTaskRunSchedule(dbName, tableName string) error {
	if dbName == "" || tableName == "" {
		return fmt.Errorf("%w: DatabaseName and TableName are required", ErrValidation)
	}

	b.mu.Lock("StartColumnStatisticsTaskRunSchedule")
	defer b.mu.Unlock()

	s, ok := b.columnStatTaskSettings.Get(columnStatTaskKey(dbName, tableName))
	if !ok {
		return fmt.Errorf(
			"column statistics task settings not found for %s.%s: %w", dbName, tableName, ErrNotFound,
		)
	}

	s.Schedule.State = stateScheduled

	return nil
}

// StopColumnStatisticsTaskRunSchedule disables the run schedule for a table's
// column statistics task settings.
func (b *InMemoryBackend) StopColumnStatisticsTaskRunSchedule(dbName, tableName string) error {
	if dbName == "" || tableName == "" {
		return fmt.Errorf("%w: DatabaseName and TableName are required", ErrValidation)
	}

	b.mu.Lock("StopColumnStatisticsTaskRunSchedule")
	defer b.mu.Unlock()

	s, ok := b.columnStatTaskSettings.Get(columnStatTaskKey(dbName, tableName))
	if !ok {
		return fmt.Errorf(
			"column statistics task settings not found for %s.%s: %w", dbName, tableName, ErrNotFound,
		)
	}

	s.Schedule.State = stateNotScheduled

	return nil
}

// StartColumnStatisticsTaskRun starts a column statistics task run.
func (b *InMemoryBackend) StartColumnStatisticsTaskRun(dbName, tableName string) (*ColumnStatisticsTaskRun, error) {
	b.mu.Lock("StartColumnStatisticsTaskRun")
	defer b.mu.Unlock()

	runID := "cstr-" + uuid.NewString()[:8]
	run := &ColumnStatisticsTaskRun{
		DatabaseName:              dbName,
		TableName:                 tableName,
		ColumnStatisticsTaskRunID: runID,
		Status:                    "STARTED",
		StartedOn:                 time.Now().UTC(),
	}
	b.columnStatTaskRuns.Put(run)
	cp := *run

	return &cp, nil
}

// StopColumnStatisticsTaskRun stops a task run.
func (b *InMemoryBackend) StopColumnStatisticsTaskRun(runID string) error {
	b.mu.Lock("StopColumnStatisticsTaskRun")
	defer b.mu.Unlock()

	r, ok := b.columnStatTaskRuns.Get(runID)
	if !ok {
		return ErrColumnStatTaskRunNotFound
	}

	r.Status = stateStopped

	return nil
}

// GetColumnStatisticsTaskRun returns a task run.
func (b *InMemoryBackend) GetColumnStatisticsTaskRun(runID string) (*ColumnStatisticsTaskRun, error) {
	b.mu.RLock("GetColumnStatisticsTaskRun")
	defer b.mu.RUnlock()

	r, ok := b.columnStatTaskRuns.Get(runID)
	if !ok {
		return nil, ErrColumnStatTaskRunNotFound
	}

	cp := *r

	return &cp, nil
}

// ListColumnStatisticsTaskRuns returns all task runs.
func (b *InMemoryBackend) ListColumnStatisticsTaskRuns() []*ColumnStatisticsTaskRun {
	b.mu.RLock("ListColumnStatisticsTaskRuns")
	defer b.mu.RUnlock()

	src := b.columnStatTaskRuns.All()
	runs := make([]*ColumnStatisticsTaskRun, 0, len(src))
	for _, r := range src {
		cp := *r
		runs = append(runs, &cp)
	}

	sort.Slice(runs, func(i, k int) bool {
		return runs[i].StartedOn.Before(runs[k].StartedOn)
	})

	return runs
}

// GetColumnStatisticsTaskRuns returns task runs as a slice (alias for ListColumnStatisticsTaskRuns).
func (b *InMemoryBackend) GetColumnStatisticsTaskRuns() []*ColumnStatisticsTaskRun {
	return b.ListColumnStatisticsTaskRuns()
}

// --- MaterializedViewRefreshRun ---

// StartMaterializedViewRefreshTaskRun starts a refresh run.
func (b *InMemoryBackend) StartMaterializedViewRefreshTaskRun(
	dbName, tableName string,
) (*MaterializedViewRefreshRun, error) {
	b.mu.Lock("StartMaterializedViewRefreshTaskRun")
	defer b.mu.Unlock()

	taskID := "mvr-" + uuid.NewString()[:8]
	run := &MaterializedViewRefreshRun{
		DatabaseName: dbName,
		TableName:    tableName,
		TaskRunID:    taskID,
		Status:       stateRunning,
		StartedOn:    time.Now().UTC(),
	}
	b.materializedViewRuns.Put(run)
	cp := *run

	return &cp, nil
}

// StopMaterializedViewRefreshTaskRun stops a refresh run.
func (b *InMemoryBackend) StopMaterializedViewRefreshTaskRun(taskRunID string) error {
	b.mu.Lock("StopMaterializedViewRefreshTaskRun")
	defer b.mu.Unlock()

	r, ok := b.materializedViewRuns.Get(taskRunID)
	if !ok {
		return ErrMaterializedViewRunNotFound
	}

	r.Status = stateStopped

	return nil
}

// GetMaterializedViewRefreshTaskRun returns a refresh run.
func (b *InMemoryBackend) GetMaterializedViewRefreshTaskRun(taskRunID string) (*MaterializedViewRefreshRun, error) {
	b.mu.RLock("GetMaterializedViewRefreshTaskRun")
	defer b.mu.RUnlock()

	r, ok := b.materializedViewRuns.Get(taskRunID)
	if !ok {
		return nil, ErrMaterializedViewRunNotFound
	}

	cp := *r

	return &cp, nil
}

// ListMaterializedViewRefreshTaskRuns returns all refresh runs.
func (b *InMemoryBackend) ListMaterializedViewRefreshTaskRuns() []*MaterializedViewRefreshRun {
	b.mu.RLock("ListMaterializedViewRefreshTaskRuns")
	defer b.mu.RUnlock()

	src := b.materializedViewRuns.All()
	runs := make([]*MaterializedViewRefreshRun, 0, len(src))
	for _, r := range src {
		cp := *r
		runs = append(runs, &cp)
	}

	sort.Slice(runs, func(i, k int) bool {
		return runs[i].StartedOn.Before(runs[k].StartedOn)
	})

	return runs
}

// --- Integration CRUD ---

// CreateIntegration stores a new integration.
func (b *InMemoryBackend) CreateIntegration(name string, tags map[string]string) (*Integration, error) {
	b.mu.Lock("CreateIntegration")
	defer b.mu.Unlock()

	ig := &Integration{
		IntegrationName: name,
		Status:          "CREATING",
		Tags:            tags,
		CreatedAt:       time.Now().UTC(),
	}
	b.integrations.Put(ig)
	cp := *ig

	return &cp, nil
}

// DeleteIntegration removes an integration.
func (b *InMemoryBackend) DeleteIntegration(name string) error {
	b.mu.Lock("DeleteIntegration")
	defer b.mu.Unlock()

	if !b.integrations.Has(name) {
		return fmt.Errorf("integration %q not found: %w", name, ErrNotFound)
	}

	b.integrations.Delete(name)

	return nil
}

// ListIntegrations returns all integrations.
func (b *InMemoryBackend) ListIntegrations() []*Integration {
	b.mu.RLock("ListIntegrations")
	defer b.mu.RUnlock()

	src := b.integrations.All()
	list := make([]*Integration, 0, len(src))
	for _, ig := range src {
		cp := *ig
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, k int) bool {
		return list[i].IntegrationName < list[k].IntegrationName
	})

	return list
}

// ModifyIntegration updates an integration.
func (b *InMemoryBackend) ModifyIntegration(name string) error {
	b.mu.Lock("ModifyIntegration")
	defer b.mu.Unlock()

	if !b.integrations.Has(name) {
		return ErrIntegrationNotFound
	}

	return nil
}

// --- GlueIdentityCenter CRUD ---

// CreateGlueIdentityCenterConfiguration creates the configuration.
func (b *InMemoryBackend) CreateGlueIdentityCenterConfiguration(instanceARN string) error {
	b.mu.Lock("CreateGlueIdentityCenterConfiguration")
	defer b.mu.Unlock()

	b.glueIdentityCenterConfig = &IdentityCenterConfig{
		InstanceARN: instanceARN,
		Status:      "ENABLED",
	}

	return nil
}

// GetGlueIdentityCenterConfiguration returns the configuration.
func (b *InMemoryBackend) GetGlueIdentityCenterConfiguration() (*IdentityCenterConfig, error) {
	b.mu.RLock("GetGlueIdentityCenterConfiguration")
	defer b.mu.RUnlock()

	if b.glueIdentityCenterConfig == nil {
		return &IdentityCenterConfig{Status: "DISABLED"}, nil
	}

	cp := *b.glueIdentityCenterConfig

	return &cp, nil
}

// UpdateGlueIdentityCenterConfiguration updates the configuration.
func (b *InMemoryBackend) UpdateGlueIdentityCenterConfiguration(instanceARN string) error {
	b.mu.Lock("UpdateGlueIdentityCenterConfiguration")
	defer b.mu.Unlock()

	if b.glueIdentityCenterConfig == nil {
		b.glueIdentityCenterConfig = &IdentityCenterConfig{}
	}

	b.glueIdentityCenterConfig.InstanceARN = instanceARN

	return nil
}

// DeleteGlueIdentityCenterConfiguration removes the configuration.
func (b *InMemoryBackend) DeleteGlueIdentityCenterConfiguration() error {
	b.mu.Lock("DeleteGlueIdentityCenterConfiguration")
	defer b.mu.Unlock()

	b.glueIdentityCenterConfig = nil

	return nil
}

// --- IntegrationResourceProperty ---

// IntegrationResourceProperty stores resource-level properties for a Zero-ETL integration.
type IntegrationResourceProperty struct {
	CreatedAt        time.Time         `json:"CreateTime"`
	SourceProperties map[string]string `json:"SourceProperties,omitempty"`
	TargetProperties map[string]string `json:"TargetProperties,omitempty"`
	ResourceArn      string            `json:"ResourceArn"`
}

// CreateIntegrationResourceProperty stores properties for an integration resource.
func (b *InMemoryBackend) CreateIntegrationResourceProperty(
	resourceArn string,
	sourceProps, targetProps map[string]string,
) (*IntegrationResourceProperty, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.Lock("CreateIntegrationResourceProperty")
	defer b.mu.Unlock()

	prop := &IntegrationResourceProperty{
		CreatedAt:        time.Now(),
		ResourceArn:      resourceArn,
		SourceProperties: sourceProps,
		TargetProperties: targetProps,
	}
	b.integrationResourceProps.Put(prop)

	return prop, nil
}

// GetIntegrationResourceProperty retrieves stored resource properties.
func (b *InMemoryBackend) GetIntegrationResourceProperty(resourceArn string) (*IntegrationResourceProperty, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.RLock("GetIntegrationResourceProperty")
	defer b.mu.RUnlock()

	prop, ok := b.integrationResourceProps.Get(resourceArn)
	if !ok {
		return nil, fmt.Errorf("resource property for %q not found: %w", resourceArn, ErrNotFound)
	}

	return prop, nil
}

// UpdateIntegrationResourceProperty updates a previously created resource property.
func (b *InMemoryBackend) UpdateIntegrationResourceProperty(
	resourceArn string,
	sourceProps, targetProps map[string]string,
) (*IntegrationResourceProperty, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.Lock("UpdateIntegrationResourceProperty")
	defer b.mu.Unlock()

	prop, ok := b.integrationResourceProps.Get(resourceArn)
	if !ok {
		return nil, fmt.Errorf("resource property for %q not found: %w", resourceArn, ErrNotFound)
	}

	if sourceProps != nil {
		prop.SourceProperties = sourceProps
	}

	if targetProps != nil {
		prop.TargetProperties = targetProps
	}

	return prop, nil
}

// ListIntegrationResourceProperties returns all stored integration resource
// properties, sorted by resource ARN for a deterministic response.
func (b *InMemoryBackend) ListIntegrationResourceProperties() []*IntegrationResourceProperty {
	b.mu.RLock("ListIntegrationResourceProperties")
	defer b.mu.RUnlock()

	return b.integrationResourceProps.Snapshot()
}

// --- IntegrationTableProperties ---

// IntegrationTableProperties stores table-level properties for a Zero-ETL integration.
type IntegrationTableProperties struct {
	SourceTableConfig map[string]any `json:"SourceTableConfig,omitempty"`
	TargetTableConfig map[string]any `json:"TargetTableConfig,omitempty"`
	ResourceArn       string         `json:"ResourceArn"`
	TableName         string         `json:"TableName"`
}

// CreateIntegrationTableProperties stores properties for an integration table.
func (b *InMemoryBackend) CreateIntegrationTableProperties(
	resourceArn, tableName string,
	sourceConfig, targetConfig map[string]any,
) error {
	if resourceArn == "" || tableName == "" {
		return fmt.Errorf("%w: ResourceArn and TableName are required", ErrValidation)
	}

	b.mu.Lock("CreateIntegrationTableProperties")
	defer b.mu.Unlock()

	b.integrationTableProps.Put(&IntegrationTableProperties{
		ResourceArn:       resourceArn,
		TableName:         tableName,
		SourceTableConfig: sourceConfig,
		TargetTableConfig: targetConfig,
	})

	return nil
}

// GetIntegrationTableProperties retrieves stored table properties.
func (b *InMemoryBackend) GetIntegrationTableProperties(
	resourceArn, tableName string,
) (*IntegrationTableProperties, error) {
	if resourceArn == "" || tableName == "" {
		return nil, fmt.Errorf("%w: ResourceArn and TableName are required", ErrValidation)
	}

	b.mu.RLock("GetIntegrationTableProperties")
	defer b.mu.RUnlock()

	key := resourceArn + "|" + tableName

	prop, ok := b.integrationTableProps.Get(key)
	if !ok {
		return nil, fmt.Errorf("table property for %q/%q not found: %w", resourceArn, tableName, ErrNotFound)
	}

	return prop, nil
}

// UpdateIntegrationTableProperties updates a previously created table property.
func (b *InMemoryBackend) UpdateIntegrationTableProperties(
	resourceArn, tableName string,
	sourceConfig, targetConfig map[string]any,
) error {
	if resourceArn == "" || tableName == "" {
		return fmt.Errorf("%w: ResourceArn and TableName are required", ErrValidation)
	}

	b.mu.Lock("UpdateIntegrationTableProperties")
	defer b.mu.Unlock()

	key := resourceArn + "|" + tableName

	prop, ok := b.integrationTableProps.Get(key)
	if !ok {
		return fmt.Errorf("table property for %q/%q not found: %w", resourceArn, tableName, ErrNotFound)
	}

	if sourceConfig != nil {
		prop.SourceTableConfig = sourceConfig
	}

	if targetConfig != nil {
		prop.TargetTableConfig = targetConfig
	}

	return nil
}

// --- Data quality statistic annotations ---

// StatisticAnnotation records an inclusion annotation applied via
// PutDataQualityProfileAnnotation (profile-wide, StatisticID == "") or
// BatchPutDataQualityStatisticAnnotation (per-statistic).
type StatisticAnnotation struct {
	ProfileID      string  `json:"ProfileId"`
	StatisticID    string  `json:"StatisticId,omitempty"`
	Inclusion      string  `json:"Inclusion,omitempty"`
	RecordedOn     float64 `json:"RecordedOn,omitempty"`
	LastModifiedOn float64 `json:"LastModifiedOn,omitempty"`
}

func dqAnnotationKey(profileID, statisticID string) string {
	return profileID + "|" + statisticID
}

// PutDataQualityStatisticAnnotation stores (or overwrites) the inclusion
// annotation for a profile/statistic pair.
func (b *InMemoryBackend) PutDataQualityStatisticAnnotation(profileID, statisticID, inclusion string) {
	b.mu.Lock("PutDataQualityStatisticAnnotation")
	defer b.mu.Unlock()

	now := float64(time.Now().Unix())
	key := dqAnnotationKey(profileID, statisticID)

	recordedOn := now
	if existing, ok := b.dqStatisticAnnotations.Get(key); ok {
		recordedOn = existing.RecordedOn
	}

	b.dqStatisticAnnotations.Put(&StatisticAnnotation{
		ProfileID:      profileID,
		StatisticID:    statisticID,
		Inclusion:      inclusion,
		RecordedOn:     recordedOn,
		LastModifiedOn: now,
	})
}

// ListDataQualityStatisticAnnotations returns stored annotations, optionally
// filtered by profile ID and/or statistic ID, sorted by key for a deterministic
// response.
func (b *InMemoryBackend) ListDataQualityStatisticAnnotations(profileID, statisticID string) []*StatisticAnnotation {
	b.mu.RLock("ListDataQualityStatisticAnnotations")
	defer b.mu.RUnlock()

	src := b.dqStatisticAnnotations.Snapshot()
	out := make([]*StatisticAnnotation, 0, len(src))

	for _, e := range src {
		if profileID != "" && e.ProfileID != profileID {
			continue
		}

		if statisticID != "" && e.StatisticID != statisticID {
			continue
		}

		cp := *e
		out = append(out, &cp)
	}

	return out
}
