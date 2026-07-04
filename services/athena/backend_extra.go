package athena

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"
)

// --- Constants for new resources ---

const (
	sessionStateCreating    = "CREATING"
	sessionStateCreated     = "CREATED"
	sessionStateIdle        = "IDLE"
	sessionStateBusy        = "BUSY"
	sessionStateTerminating = "TERMINATING"
	sessionStateTerminated  = "TERMINATED"
	sessionStateFailed      = "FAILED"

	calcStateCreating  = "CREATING"
	calcStateCreated   = "CREATED"
	calcStateQueued    = "QUEUED"
	calcStateRunning   = "RUNNING"
	calcStateCompleted = "COMPLETED"
	calcStateFailed    = "FAILED"
	calcStateCanceled  = "CANCELED"

	notebookEndpointBase = "https://athena.%s.amazonaws.com/sessions/"

	defaultDPU = 1
)

// --- Session-related types ---

// EngineConfiguration is the engine configuration for a session.
type EngineConfiguration struct {
	AdditionalConfigs      map[string]string `json:"AdditionalConfigs,omitempty"`
	SparkProperties        map[string]string `json:"SparkProperties,omitempty"`
	DefaultExecutorDpuSize int32             `json:"DefaultExecutorDpuSize,omitempty"`
	MaxConcurrentDpus      int32             `json:"MaxConcurrentDpus,omitempty"`
	CoordinatorDpuSize     int32             `json:"CoordinatorDpuSize,omitempty"`
}

// SessionConfiguration is the configuration for a session.
type SessionConfiguration struct {
	EncryptionConfiguration EncryptionConfiguration `json:"EncryptionConfiguration,omitzero"`
	WorkingDirectory        string                  `json:"WorkingDirectory,omitempty"`
	ExecutionRole           string                  `json:"ExecutionRole,omitempty"`
	IdleTimeoutSeconds      int64                   `json:"IdleTimeoutSeconds,omitempty"`
}

// SessionStatus tracks the lifecycle of a session.
type SessionStatus struct {
	StateChangeReason    string  `json:"StateChangeReason,omitempty"`
	State                string  `json:"State"`
	StartDateTime        float64 `json:"StartDateTime,omitempty"`
	LastModifiedDateTime float64 `json:"LastModifiedDateTime,omitempty"`
	EndDateTime          float64 `json:"EndDateTime,omitempty"`
	IdleSinceDateTime    float64 `json:"IdleSinceDateTime,omitempty"`
}

// SessionStatistics holds session-level statistics.
type SessionStatistics struct {
	DpuExecutionInMillis int64 `json:"DpuExecutionInMillis,omitempty"`
}

// Session represents an interactive notebook session.
type Session struct {
	EngineConfiguration  EngineConfiguration  `json:"EngineConfiguration,omitzero"`
	SessionConfiguration SessionConfiguration `json:"SessionConfiguration,omitzero"`
	SessionID            string               `json:"SessionId"`
	Description          string               `json:"Description,omitempty"`
	WorkGroup            string               `json:"WorkGroup"`
	NotebookVersion      string               `json:"NotebookVersion,omitempty"`
	NotebookID           string               `json:"NotebookId,omitempty"`
	Status               SessionStatus        `json:"Status"`
	Statistics           SessionStatistics    `json:"Statistics,omitzero"`
}

// SessionSummary is the list view of a session.
type SessionSummary struct {
	SessionID       string        `json:"SessionId"`
	Description     string        `json:"Description,omitempty"`
	NotebookVersion string        `json:"NotebookVersion,omitempty"`
	Status          SessionStatus `json:"Status,omitzero"`
}

// --- Calculation execution types ---

// CalculationStatistics holds calculation runtime stats.
type CalculationStatistics struct {
	DpuExecutionInMillis int64 `json:"DpuExecutionInMillis,omitempty"`
	Progress             int64 `json:"Progress,omitempty"`
}

// CalculationStatus holds the lifecycle of a calculation.
type CalculationStatus struct {
	StateChangeReason  string  `json:"StateChangeReason,omitempty"`
	State              string  `json:"State"`
	SubmissionDateTime float64 `json:"SubmissionDateTime,omitempty"`
	CompletionDateTime float64 `json:"CompletionDateTime,omitempty"`
}

// CalculationResult holds output references for a calculation.
type CalculationResult struct {
	StdOutS3URI   string `json:"StdOutS3Uri,omitempty"`
	StdErrorS3URI string `json:"StdErrorS3Uri,omitempty"`
	ResultS3URI   string `json:"ResultS3Uri,omitempty"`
	ResultType    string `json:"ResultType,omitempty"`
}

// CalculationExecution is a Spark calculation run within a session.
type CalculationExecution struct {
	Result        CalculationResult     `json:"Result,omitzero"`
	CalculationID string                `json:"CalculationExecutionId"`
	SessionID     string                `json:"SessionId"`
	Description   string                `json:"Description,omitempty"`
	WorkingDir    string                `json:"WorkingDirectory,omitempty"`
	CodeBlock     string                `json:"CodeBlock,omitempty"`
	Status        CalculationStatus     `json:"Status"`
	Statistics    CalculationStatistics `json:"Statistics,omitzero"`
}

// CalculationSummary is the list view of a calculation execution.
type CalculationSummary struct {
	CalculationID string            `json:"CalculationExecutionId"`
	Description   string            `json:"Description,omitempty"`
	Status        CalculationStatus `json:"Status,omitzero"`
}

// --- Database / table metadata types ---

// Column describes a single column in a table.
type Column struct {
	Name    string `json:"Name"`
	Type    string `json:"Type,omitempty"`
	Comment string `json:"Comment,omitempty"`
}

// Database describes an Athena database.
type Database struct {
	Parameters  map[string]string `json:"Parameters,omitempty"`
	Name        string            `json:"Name"`
	Description string            `json:"Description,omitempty"`
}

// TableMetadata describes a table.
type TableMetadata struct {
	Parameters     map[string]string `json:"Parameters,omitempty"`
	Name           string            `json:"Name"`
	TableType      string            `json:"TableType,omitempty"`
	Columns        []Column          `json:"Columns,omitempty"`
	PartitionKeys  []Column          `json:"PartitionKeys,omitempty"`
	CreateTime     float64           `json:"CreateTime,omitempty"`
	LastAccessTime float64           `json:"LastAccessTime,omitempty"`
}

// --- Capacity assignment types ---

// CapacityAssignment maps a list of workgroup ARNs to a reservation.
type CapacityAssignment struct {
	WorkGroupNames []string `json:"WorkGroupNames"`
}

// CapacityAssignmentConfiguration is the config attached to a capacity reservation.
type CapacityAssignmentConfiguration struct {
	CapacityReservationName string               `json:"CapacityReservationName"`
	CapacityAssignments     []CapacityAssignment `json:"CapacityAssignments,omitempty"`
}

// --- Engine / executor / runtime types ---

// Executor describes a Spark executor.
type Executor struct {
	ExecutorID          string  `json:"ExecutorId"`
	ExecutorType        string  `json:"ExecutorType"`
	ExecutorState       string  `json:"ExecutorState"`
	StartDateTime       float64 `json:"StartDateTime,omitempty"`
	TerminationDateTime float64 `json:"TerminationDateTime,omitempty"`
	ExecutorSize        int64   `json:"ExecutorSize,omitempty"`
}

// EngineVersionDescriptor describes an available engine version.
type EngineVersionDescriptor struct {
	AuthEngineVersion      string `json:"AuthEngineVersion,omitempty"`
	EffectiveEngineVersion string `json:"EffectiveEngineVersion,omitempty"`
	SelectedEngineVersion  string `json:"SelectedEngineVersion,omitempty"`
}

// ApplicationDPUSizes lists DPU sizes for a Spark application.
type ApplicationDPUSizes struct {
	ApplicationRuntimeID string  `json:"ApplicationRuntimeId"`
	SupportedDPUSizes    []int32 `json:"SupportedDPUSizes,omitempty"`
}

// QueryRuntimeStatistics aggregates runtime stats for a query execution.
type QueryRuntimeStatistics struct {
	OutputStage QueryStage                     `json:"OutputStage,omitzero"`
	Timeline    QueryRuntimeStatisticsTimeline `json:"Timeline,omitzero"`
	Rows        QueryRuntimeStatisticsRows     `json:"Rows,omitzero"`
}

// QueryRuntimeStatisticsTimeline is the timeline portion.
type QueryRuntimeStatisticsTimeline struct {
	QueryQueueTimeInMillis        int64 `json:"QueryQueueTimeInMillis,omitempty"`
	QueryPlanningTimeInMillis     int64 `json:"QueryPlanningTimeInMillis,omitempty"`
	EngineExecutionTimeInMillis   int64 `json:"EngineExecutionTimeInMillis,omitempty"`
	ServiceProcessingTimeInMillis int64 `json:"ServiceProcessingTimeInMillis,omitempty"`
	TotalExecutionTimeInMillis    int64 `json:"TotalExecutionTimeInMillis,omitempty"`
}

// QueryRuntimeStatisticsRows is the rows portion.
type QueryRuntimeStatisticsRows struct {
	InputRows   int64 `json:"InputRows,omitempty"`
	InputBytes  int64 `json:"InputBytes,omitempty"`
	OutputRows  int64 `json:"OutputRows,omitempty"`
	OutputBytes int64 `json:"OutputBytes,omitempty"`
}

// QueryStage is a single stage in the runtime statistics tree.
type QueryStage struct {
	State         string `json:"State,omitempty"`
	StageID       int64  `json:"StageId,omitempty"`
	OutputBytes   int64  `json:"OutputBytes,omitempty"`
	OutputRows    int64  `json:"OutputRows,omitempty"`
	InputBytes    int64  `json:"InputBytes,omitempty"`
	InputRows     int64  `json:"InputRows,omitempty"`
	ExecutionTime int64  `json:"ExecutionTime,omitempty"`
}

// --- Helpers ---

func nowSeconds() float64 {
	return float64(time.Now().UnixMilli()) / millisToSeconds
}

// --- Sessions ---

// StartSession creates a new session in the specified workgroup.
func (b *InMemoryBackend) StartSession(workGroup, description, notebookVersion string,
	engineCfg EngineConfiguration, sessionCfg SessionConfiguration, notebookID string,
) (string, string, error) {
	if workGroup == "" {
		return "", "", fmt.Errorf("%w: WorkGroup is required", ErrValidation)
	}

	b.mu.Lock("StartSession")
	defer b.mu.Unlock()

	if _, ok := b.workGroups[workGroup]; !ok {
		return "", "", fmt.Errorf("%w: workgroup %q not found", ErrNotFound, workGroup)
	}

	if engineCfg.CoordinatorDpuSize == 0 {
		engineCfg.CoordinatorDpuSize = defaultDPU
	}

	if engineCfg.DefaultExecutorDpuSize == 0 {
		engineCfg.DefaultExecutorDpuSize = defaultDPU
	}

	id := randomID()
	now := nowSeconds()
	b.sessions[id] = &Session{
		SessionID:            id,
		Description:          description,
		WorkGroup:            workGroup,
		NotebookVersion:      notebookVersion,
		NotebookID:           notebookID,
		EngineConfiguration:  engineCfg,
		SessionConfiguration: sessionCfg,
		Status: SessionStatus{
			State:                sessionStateIdle,
			StartDateTime:        now,
			LastModifiedDateTime: now,
			IdleSinceDateTime:    now,
		},
	}

	return id, sessionStateIdle, nil
}

// GetSession returns the session matching the given ID.
func (b *InMemoryBackend) GetSession(id string) (*Session, error) {
	b.mu.RLock("GetSession")
	defer b.mu.RUnlock()

	s, ok := b.sessions[id]
	if !ok {
		return nil, fmt.Errorf("%w: session %q not found", ErrResourceNotFound, id)
	}

	cp := *s

	return &cp, nil
}

// GetSessionStatus returns just the status for the session.
func (b *InMemoryBackend) GetSessionStatus(id string) (SessionStatus, error) {
	b.mu.RLock("GetSessionStatus")
	defer b.mu.RUnlock()

	s, ok := b.sessions[id]
	if !ok {
		return SessionStatus{}, fmt.Errorf("%w: session %q not found", ErrResourceNotFound, id)
	}

	return s.Status, nil
}

// GetSessionEndpoint returns a presigned endpoint URL for the given session.
func (b *InMemoryBackend) GetSessionEndpoint(id string) (string, error) {
	b.mu.RLock("GetSessionEndpoint")
	defer b.mu.RUnlock()

	if _, ok := b.sessions[id]; !ok {
		return "", fmt.Errorf("%w: session %q not found", ErrResourceNotFound, id)
	}

	return fmt.Sprintf(notebookEndpointBase, b.region) + id, nil
}

// TerminateSession terminates an existing session.
func (b *InMemoryBackend) TerminateSession(id string) (string, error) {
	b.mu.Lock("TerminateSession")
	defer b.mu.Unlock()

	s, ok := b.sessions[id]
	if !ok {
		return "", fmt.Errorf("%w: session %q not found", ErrResourceNotFound, id)
	}

	if s.Status.State == sessionStateTerminated {
		return s.Status.State, fmt.Errorf("%w: session %q is already terminated", ErrValidation, id)
	}

	now := nowSeconds()
	s.Status.State = sessionStateTerminated
	s.Status.EndDateTime = now
	s.Status.LastModifiedDateTime = now

	return sessionStateTerminated, nil
}

// ListSessions returns sessions for a workgroup, optionally filtered by state.
func (b *InMemoryBackend) ListSessions(workGroup, stateFilter string) ([]SessionSummary, error) {
	b.mu.RLock("ListSessions")
	defer b.mu.RUnlock()

	out := make([]SessionSummary, 0, len(b.sessions))

	for _, s := range b.sessions {
		if workGroup != "" && s.WorkGroup != workGroup {
			continue
		}

		if stateFilter != "" && s.Status.State != stateFilter {
			continue
		}

		out = append(out, SessionSummary{
			SessionID:       s.SessionID,
			Description:     s.Description,
			NotebookVersion: s.NotebookVersion,
			Status:          s.Status,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].SessionID < out[j].SessionID })

	return out, nil
}

// ListNotebookSessions returns sessions associated with a notebook.
func (b *InMemoryBackend) ListNotebookSessions(notebookID string) ([]SessionSummary, error) {
	if notebookID == "" {
		return nil, fmt.Errorf("%w: NotebookId is required", ErrValidation)
	}

	b.mu.RLock("ListNotebookSessions")
	defer b.mu.RUnlock()

	if _, ok := b.notebooks[notebookID]; !ok {
		return nil, fmt.Errorf("%w: notebook %q not found", ErrNotFound, notebookID)
	}

	out := make([]SessionSummary, 0, len(b.sessions))

	for _, s := range b.sessions {
		if s.NotebookID != notebookID {
			continue
		}

		out = append(out, SessionSummary{
			SessionID:       s.SessionID,
			Description:     s.Description,
			NotebookVersion: s.NotebookVersion,
			Status:          s.Status,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].SessionID < out[j].SessionID })

	return out, nil
}

// --- Calculations ---

// StartCalculationExecution starts a Spark calculation in the given session.
func (b *InMemoryBackend) StartCalculationExecution(
	sessionID, description, codeBlock string,
) (string, string, error) {
	if sessionID == "" {
		return "", "", fmt.Errorf("%w: SessionId is required", ErrValidation)
	}

	b.mu.Lock("StartCalculationExecution")
	defer b.mu.Unlock()

	s, ok := b.sessions[sessionID]
	if !ok {
		return "", "", fmt.Errorf("%w: session %q not found", ErrResourceNotFound, sessionID)
	}

	if s.Status.State == sessionStateTerminated {
		return "", "", fmt.Errorf("%w: session %q is terminated", ErrValidation, sessionID)
	}

	id := randomID()
	now := nowSeconds()
	b.calculations[id] = &CalculationExecution{
		CalculationID: id,
		SessionID:     sessionID,
		Description:   description,
		CodeBlock:     codeBlock,
		Status: CalculationStatus{
			State:              calcStateCompleted,
			SubmissionDateTime: now,
			CompletionDateTime: now,
		},
		Statistics: CalculationStatistics{Progress: 100}, //nolint:mnd // mock progress
		Result: CalculationResult{
			ResultType:    "JSON",
			ResultS3URI:   fmt.Sprintf("s3://athena-mock/%s/result.json", id),
			StdOutS3URI:   fmt.Sprintf("s3://athena-mock/%s/stdout.log", id),
			StdErrorS3URI: fmt.Sprintf("s3://athena-mock/%s/stderr.log", id),
		},
	}

	return id, calcStateCompleted, nil
}

// GetCalculationExecution returns a calculation execution by ID.
func (b *InMemoryBackend) GetCalculationExecution(id string) (*CalculationExecution, error) {
	b.mu.RLock("GetCalculationExecution")
	defer b.mu.RUnlock()

	c, ok := b.calculations[id]
	if !ok {
		return nil, fmt.Errorf("%w: calculation execution %q not found", ErrResourceNotFound, id)
	}

	cp := *c

	return &cp, nil
}

// GetCalculationExecutionStatus returns just the status of a calculation.
func (b *InMemoryBackend) GetCalculationExecutionStatus(id string) (CalculationStatus, CalculationStatistics, error) {
	b.mu.RLock("GetCalculationExecutionStatus")
	defer b.mu.RUnlock()

	c, ok := b.calculations[id]
	if !ok {
		return CalculationStatus{}, CalculationStatistics{},
			fmt.Errorf("%w: calculation execution %q not found", ErrResourceNotFound, id)
	}

	return c.Status, c.Statistics, nil
}

// GetCalculationExecutionCode returns just the code block of a calculation.
func (b *InMemoryBackend) GetCalculationExecutionCode(id string) (string, error) {
	b.mu.RLock("GetCalculationExecutionCode")
	defer b.mu.RUnlock()

	c, ok := b.calculations[id]
	if !ok {
		return "", fmt.Errorf("%w: calculation execution %q not found", ErrResourceNotFound, id)
	}

	return c.CodeBlock, nil
}

// StopCalculationExecution cancels a running calculation.
func (b *InMemoryBackend) StopCalculationExecution(id string) (string, error) {
	b.mu.Lock("StopCalculationExecution")
	defer b.mu.Unlock()

	c, ok := b.calculations[id]
	if !ok {
		return "", fmt.Errorf("%w: calculation execution %q not found", ErrResourceNotFound, id)
	}

	switch c.Status.State {
	case calcStateCompleted, calcStateFailed, calcStateCanceled:
		return c.Status.State, fmt.Errorf(
			"%w: calculation %q is in terminal state %q",
			ErrValidation,
			id,
			c.Status.State,
		)
	}

	c.Status.State = calcStateCanceled
	c.Status.CompletionDateTime = nowSeconds()

	return calcStateCanceled, nil
}

// ListCalculationExecutions lists calculations within a session.
func (b *InMemoryBackend) ListCalculationExecutions(sessionID, stateFilter string) ([]CalculationSummary, error) {
	if sessionID == "" {
		return nil, fmt.Errorf("%w: SessionId is required", ErrValidation)
	}

	b.mu.RLock("ListCalculationExecutions")
	defer b.mu.RUnlock()

	if _, ok := b.sessions[sessionID]; !ok {
		return nil, fmt.Errorf("%w: session %q not found", ErrResourceNotFound, sessionID)
	}

	out := make([]CalculationSummary, 0, len(b.calculations))

	for _, c := range b.calculations {
		if c.SessionID != sessionID {
			continue
		}

		if stateFilter != "" && c.Status.State != stateFilter {
			continue
		}

		out = append(out, CalculationSummary{
			CalculationID: c.CalculationID,
			Description:   c.Description,
			Status:        c.Status,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].CalculationID < out[j].CalculationID })

	return out, nil
}

// --- Capacity reservations: get/list/update ---

// GetCapacityReservation returns a capacity reservation.
func (b *InMemoryBackend) GetCapacityReservation(name string) (*CapacityReservation, error) {
	b.mu.RLock("GetCapacityReservation")
	defer b.mu.RUnlock()

	cr, ok := b.capacityReservations[name]
	if !ok {
		return nil, fmt.Errorf("%w: capacity reservation %q not found", ErrNotFound, name)
	}

	cp := *cr
	cp.Tags = maps.Clone(cr.Tags)

	return &cp, nil
}

// ListCapacityReservations returns all capacity reservations.
func (b *InMemoryBackend) ListCapacityReservations() ([]CapacityReservation, error) {
	b.mu.RLock("ListCapacityReservations")
	defer b.mu.RUnlock()

	out := make([]CapacityReservation, 0, len(b.capacityReservations))
	for _, cr := range b.capacityReservations {
		cp := *cr
		cp.Tags = maps.Clone(cr.Tags)
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

// UpdateCapacityReservation changes the target DPUs of a capacity reservation.
func (b *InMemoryBackend) UpdateCapacityReservation(name string, targetDPUs int32) error {
	const minDPUs = 24
	if targetDPUs < minDPUs {
		return fmt.Errorf("%w: TargetDpus minimum is 24", ErrValidation)
	}

	b.mu.Lock("UpdateCapacityReservation")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations[name]
	if !ok {
		return fmt.Errorf("%w: capacity reservation %q not found", ErrNotFound, name)
	}

	now := nowSeconds()
	cr.TargetDpus = targetDPUs
	cr.AllocatedDpus = targetDPUs
	cr.LastAllocation = &CapacityAllocation{
		RequestTime:           now,
		RequestCompletionTime: now,
		Status:                stateSucceeded,
	}
	cr.LastSuccessfulAllocationTime = now

	return nil
}

// PutCapacityAssignmentConfiguration sets the assignment configuration on a reservation.
func (b *InMemoryBackend) PutCapacityAssignmentConfiguration(
	name string, assignments []CapacityAssignment,
) error {
	if name == "" {
		return fmt.Errorf("%w: CapacityReservationName is required", ErrValidation)
	}

	b.mu.Lock("PutCapacityAssignmentConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.capacityReservations[name]; !ok {
		return fmt.Errorf("%w: capacity reservation %q not found", ErrNotFound, name)
	}

	cp := make([]CapacityAssignment, len(assignments))
	for i, a := range assignments {
		dst := make([]string, len(a.WorkGroupNames))
		copy(dst, a.WorkGroupNames)
		cp[i] = CapacityAssignment{WorkGroupNames: dst}
	}

	b.capacityAssignments[name] = &CapacityAssignmentConfiguration{
		CapacityReservationName: name,
		CapacityAssignments:     cp,
	}

	return nil
}

// GetCapacityAssignmentConfiguration returns the assignment configuration for a reservation.
func (b *InMemoryBackend) GetCapacityAssignmentConfiguration(name string) (*CapacityAssignmentConfiguration, error) {
	b.mu.RLock("GetCapacityAssignmentConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.capacityAssignments[name]
	if !ok {
		return nil, fmt.Errorf("%w: capacity assignment %q not found", ErrNotFound, name)
	}

	cp := *cfg
	cp.CapacityAssignments = make([]CapacityAssignment, len(cfg.CapacityAssignments))

	for i, a := range cfg.CapacityAssignments {
		dst := make([]string, len(a.WorkGroupNames))
		copy(dst, a.WorkGroupNames)
		cp.CapacityAssignments[i] = CapacityAssignment{WorkGroupNames: dst}
	}

	return &cp, nil
}

// --- Database / table metadata ---

// GetDatabase returns a database by catalog and name.
func (b *InMemoryBackend) GetDatabase(catalog, name string) (*Database, error) {
	if catalog == "" {
		return nil, fmt.Errorf("%w: CatalogName is required", ErrValidation)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", ErrValidation)
	}

	b.mu.RLock("GetDatabase")
	defer b.mu.RUnlock()

	dbs := b.databases[catalog]

	d, ok := dbs[name]
	if !ok {
		return nil, fmt.Errorf("%w: database %q not found in catalog %q", ErrMetadata, name, catalog)
	}

	cp := *d
	cp.Parameters = maps.Clone(d.Parameters)

	return &cp, nil
}

// ListDatabases returns all databases for a catalog.
func (b *InMemoryBackend) ListDatabases(catalog string) ([]Database, error) {
	if catalog == "" {
		return nil, fmt.Errorf("%w: CatalogName is required", ErrValidation)
	}

	b.mu.RLock("ListDatabases")
	defer b.mu.RUnlock()

	dbs := b.databases[catalog]
	out := make([]Database, 0, len(dbs))

	for _, d := range dbs {
		cp := *d
		cp.Parameters = maps.Clone(d.Parameters)
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

// GetTableMetadata returns the metadata for a single table.
func (b *InMemoryBackend) GetTableMetadata(catalog, database, table string) (*TableMetadata, error) {
	if catalog == "" || database == "" || table == "" {
		return nil, fmt.Errorf("%w: CatalogName, DatabaseName, TableName are required", ErrValidation)
	}

	b.mu.RLock("GetTableMetadata")
	defer b.mu.RUnlock()

	tables := b.tables[catalog+"/"+database]

	t, ok := tables[table]
	if !ok {
		return nil, fmt.Errorf("%w: table %q not found in %s/%s", ErrMetadata, table, catalog, database)
	}

	cp := *t
	cp.Parameters = maps.Clone(t.Parameters)
	cp.Columns = append([]Column(nil), t.Columns...)
	cp.PartitionKeys = append([]Column(nil), t.PartitionKeys...)

	return &cp, nil
}

// ListTableMetadata returns all tables for a database, optionally filtered by name prefix.
func (b *InMemoryBackend) ListTableMetadata(catalog, database, expr string) ([]TableMetadata, error) {
	if catalog == "" || database == "" {
		return nil, fmt.Errorf("%w: CatalogName and DatabaseName are required", ErrValidation)
	}

	b.mu.RLock("ListTableMetadata")
	defer b.mu.RUnlock()

	tables := b.tables[catalog+"/"+database]
	out := make([]TableMetadata, 0, len(tables))

	for _, t := range tables {
		if expr != "" && !strings.Contains(t.Name, expr) {
			continue
		}

		cp := *t
		cp.Parameters = maps.Clone(t.Parameters)
		cp.Columns = append([]Column(nil), t.Columns...)
		cp.PartitionKeys = append([]Column(nil), t.PartitionKeys...)
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

// --- Notebook metadata + import / update ---

// GetNotebookMetadata returns the metadata for a notebook by ID.
func (b *InMemoryBackend) GetNotebookMetadata(notebookID string) (*NotebookMetadata, error) {
	b.mu.RLock("GetNotebookMetadata")
	defer b.mu.RUnlock()

	nb, ok := b.notebooks[notebookID]
	if !ok {
		return nil, fmt.Errorf("%w: notebook %q not found", ErrNotFound, notebookID)
	}

	return &NotebookMetadata{
		NotebookID:       nb.NotebookID,
		Name:             nb.Name,
		WorkGroup:        nb.WorkGroup,
		Type:             nb.Type,
		CreationTime:     nb.CreationTime,
		LastModifiedTime: nb.LastModifiedTime,
	}, nil
}

// ListNotebookMetadata lists all notebooks (optionally filtered by workgroup and name).
func (b *InMemoryBackend) ListNotebookMetadata(workGroup, namePrefix string) ([]NotebookMetadata, error) {
	b.mu.RLock("ListNotebookMetadata")
	defer b.mu.RUnlock()

	out := make([]NotebookMetadata, 0, len(b.notebooks))

	for _, nb := range b.notebooks {
		if workGroup != "" && nb.WorkGroup != workGroup {
			continue
		}

		if namePrefix != "" && !strings.HasPrefix(nb.Name, namePrefix) {
			continue
		}

		out = append(out, NotebookMetadata{
			NotebookID:       nb.NotebookID,
			Name:             nb.Name,
			WorkGroup:        nb.WorkGroup,
			Type:             nb.Type,
			CreationTime:     nb.CreationTime,
			LastModifiedTime: nb.LastModifiedTime,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

// ImportNotebook creates a new notebook from inline payload.
func (b *InMemoryBackend) ImportNotebook(workGroup, name, payload, notebookType string) (string, error) {
	switch {
	case workGroup == "":
		return "", fmt.Errorf("%w: WorkGroup is required", ErrValidation)
	case name == "":
		return "", fmt.Errorf("%w: Name is required", ErrValidation)
	case payload == "":
		return "", fmt.Errorf("%w: Payload is required", ErrValidation)
	}

	if notebookType == "" {
		notebookType = "IPYNB"
	}

	b.mu.Lock("ImportNotebook")
	defer b.mu.Unlock()

	nameKey := notebookNameKey(workGroup, name)
	if _, exists := b.notebookNames[nameKey]; exists {
		return "", fmt.Errorf("%w: notebook %q already exists in workgroup %q", ErrAlreadyExists, name, workGroup)
	}

	id := randomID()
	now := nowSeconds()
	b.notebooks[id] = &Notebook{
		NotebookID:       id,
		Name:             name,
		WorkGroup:        workGroup,
		Type:             notebookType,
		Content:          payload,
		CreationTime:     now,
		LastModifiedTime: now,
	}
	b.notebookNames[nameKey] = struct{}{}

	return id, nil
}

// UpdateNotebook replaces the payload of an existing notebook.
func (b *InMemoryBackend) UpdateNotebook(notebookID, payload, notebookType, sessionID string) error {
	if notebookID == "" {
		return fmt.Errorf("%w: NotebookId is required", ErrValidation)
	}

	if payload == "" {
		return fmt.Errorf("%w: Payload is required", ErrValidation)
	}

	b.mu.Lock("UpdateNotebook")
	defer b.mu.Unlock()

	nb, ok := b.notebooks[notebookID]
	if !ok {
		return fmt.Errorf("%w: notebook %q not found", ErrNotFound, notebookID)
	}

	if sessionID != "" {
		if _, sok := b.sessions[sessionID]; !sok {
			return fmt.Errorf("%w: session %q not found", ErrNotFound, sessionID)
		}
	}

	nb.Content = payload

	if notebookType != "" {
		nb.Type = notebookType
	}

	nb.LastModifiedTime = nowSeconds()

	return nil
}

// UpdateNotebookMetadata renames a notebook.
func (b *InMemoryBackend) UpdateNotebookMetadata(notebookID, newName string) error {
	if notebookID == "" {
		return fmt.Errorf("%w: NotebookId is required", ErrValidation)
	}

	if newName == "" {
		return fmt.Errorf("%w: Name is required", ErrValidation)
	}

	b.mu.Lock("UpdateNotebookMetadata")
	defer b.mu.Unlock()

	nb, ok := b.notebooks[notebookID]
	if !ok {
		return fmt.Errorf("%w: notebook %q not found", ErrNotFound, notebookID)
	}

	if nb.Name == newName {
		return nil
	}

	newKey := notebookNameKey(nb.WorkGroup, newName)
	if _, exists := b.notebookNames[newKey]; exists {
		return fmt.Errorf("%w: notebook %q already exists in workgroup %q", ErrAlreadyExists, newName, nb.WorkGroup)
	}

	delete(b.notebookNames, notebookNameKey(nb.WorkGroup, nb.Name))
	b.notebookNames[newKey] = struct{}{}
	nb.Name = newName
	nb.LastModifiedTime = nowSeconds()

	return nil
}

// --- Misc updaters ---

// UpdateNamedQuery updates an existing named query's name, description, or query string.
func (b *InMemoryBackend) UpdateNamedQuery(id, name, description, queryString string) error {
	if id == "" {
		return fmt.Errorf("%w: NamedQueryId is required", ErrValidation)
	}

	b.mu.Lock("UpdateNamedQuery")
	defer b.mu.Unlock()

	q, ok := b.namedQueries[id]
	if !ok {
		return fmt.Errorf("%w: named query %q not found", ErrNotFound, id)
	}

	if name != "" {
		q.Name = name
	}

	if description != "" {
		q.Description = description
	}

	if queryString != "" {
		q.QueryString = queryString
	}

	return nil
}

// UpdatePreparedStatement updates an existing prepared statement.
func (b *InMemoryBackend) UpdatePreparedStatement(name, workGroup, queryStatement, description string) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: StatementName is required", ErrValidation)
	case workGroup == "":
		return fmt.Errorf("%w: WorkGroup is required", ErrValidation)
	case queryStatement == "":
		return fmt.Errorf("%w: QueryStatement is required", ErrValidation)
	}

	b.mu.Lock("UpdatePreparedStatement")
	defer b.mu.Unlock()

	key := preparedStatementKey(workGroup, name)

	ps, ok := b.preparedStatements[key]
	if !ok {
		return fmt.Errorf("%w: prepared statement %q not found in workgroup %q", ErrResourceNotFound, name, workGroup)
	}

	ps.QueryStatement = queryStatement

	if description != "" {
		ps.Description = description
	}

	ps.LastModifiedTime = nowSeconds()

	return nil
}

// --- Engine version / executor / dpu listings ---

const (
	athenaEngineV3  = "Athena engine version 3"
	pysparkEngineV3 = "PySpark engine version 3"
)

// ListEngineVersions returns the engines available to a workgroup.
func (b *InMemoryBackend) ListEngineVersions() []EngineVersionDescriptor {
	return []EngineVersionDescriptor{
		{AuthEngineVersion: stateAuto, EffectiveEngineVersion: athenaEngineV3, SelectedEngineVersion: stateAuto},
		{
			AuthEngineVersion:      athenaEngineV3,
			EffectiveEngineVersion: athenaEngineV3,
			SelectedEngineVersion:  athenaEngineV3,
		},
		{
			AuthEngineVersion:      pysparkEngineV3,
			EffectiveEngineVersion: pysparkEngineV3,
			SelectedEngineVersion:  pysparkEngineV3,
		},
	}
}

// ListApplicationDPUSizes returns the available DPU sizes for Spark applications.
func (b *InMemoryBackend) ListApplicationDPUSizes() []ApplicationDPUSizes {
	return []ApplicationDPUSizes{
		{ApplicationRuntimeID: "Athena notebook version 1", SupportedDPUSizes: []int32{1, 2, 4, 8, 16, 20}},
		{ApplicationRuntimeID: "PySpark notebook version 3", SupportedDPUSizes: []int32{1, 2, 4, 8, 16}},
	}
}

// ListExecutors returns executors associated with a session.
func (b *InMemoryBackend) ListExecutors(sessionID, stateFilter string) ([]Executor, error) {
	if sessionID == "" {
		return nil, fmt.Errorf("%w: SessionId is required", ErrValidation)
	}

	b.mu.RLock("ListExecutors")
	defer b.mu.RUnlock()

	s, ok := b.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("%w: session %q not found", ErrResourceNotFound, sessionID)
	}

	out := make([]Executor, 0, 1)

	if s.Status.State == sessionStateTerminated {
		return out, nil
	}

	executor := Executor{
		ExecutorID:    "executor-" + sessionID,
		ExecutorType:  "GATEWAY",
		ExecutorState: "REGISTERED",
		StartDateTime: s.Status.StartDateTime,
		ExecutorSize:  int64(s.EngineConfiguration.DefaultExecutorDpuSize),
	}

	if stateFilter != "" && executor.ExecutorState != stateFilter {
		return out, nil
	}

	out = append(out, executor)

	return out, nil
}

// GetQueryRuntimeStatistics returns runtime statistics for a query execution.
func (b *InMemoryBackend) GetQueryRuntimeStatistics(id string) (*QueryRuntimeStatistics, error) {
	b.mu.RLock("GetQueryRuntimeStatistics")
	defer b.mu.RUnlock()

	qe, ok := b.queryExecutions[id]
	if !ok {
		return nil, fmt.Errorf("%w: query execution %q not found", ErrNotFound, id)
	}

	engineMs := qe.Statistics.EngineExecutionTimeInMillis

	return &QueryRuntimeStatistics{
		Timeline: QueryRuntimeStatisticsTimeline{
			EngineExecutionTimeInMillis: engineMs,
			TotalExecutionTimeInMillis:  engineMs,
		},
		Rows: QueryRuntimeStatisticsRows{
			InputBytes: qe.Statistics.DataScannedInBytes,
		},
		OutputStage: QueryStage{
			StageID: 0,
			State:   qe.Status.State,
		},
	}, nil
}
