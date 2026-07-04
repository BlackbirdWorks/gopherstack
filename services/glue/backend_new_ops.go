package glue

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// ---------------------------------------------------------------------------
// UserDefinedFunction
// ---------------------------------------------------------------------------

// ResourceURI holds a URI for a UDF resource.
type ResourceURI struct {
	ResourceType string `json:"ResourceType,omitempty"`
	URI          string `json:"Uri,omitempty"`
}

// UserDefinedFunction represents a Glue UDF.
type UserDefinedFunction struct {
	DatabaseName string        `json:"DatabaseName"`
	FunctionName string        `json:"FunctionName"`
	ClassName    string        `json:"ClassName,omitempty"`
	OwnerName    string        `json:"OwnerName,omitempty"`
	OwnerType    string        `json:"OwnerType,omitempty"`
	FunctionARN  string        `json:"FunctionArn,omitempty"`
	ResourceURIs []ResourceURI `json:"ResourceUris,omitempty"`
	CreateTime   float64       `json:"CreateTime,omitempty"`
}

func cloneUDF(u *UserDefinedFunction) *UserDefinedFunction {
	cp := *u
	cp.ResourceURIs = make([]ResourceURI, len(u.ResourceURIs))
	copy(cp.ResourceURIs, u.ResourceURIs)

	return &cp
}

func (b *InMemoryBackend) udfKey(dbName, name string) string {
	return dbName + "|" + name
}

func (b *InMemoryBackend) udfARN(dbName, name string) string {
	return arn.Build("glue", b.region, b.accountID, "userDefinedFunction/"+dbName+"/"+name)
}

func (b *InMemoryBackend) CreateUserDefinedFunction(
	dbName string,
	input UserDefinedFunction,
	tags map[string]string,
) (*UserDefinedFunction, error) {
	b.mu.Lock("CreateUserDefinedFunction")
	defer b.mu.Unlock()

	if _, ok := b.databases[dbName]; !ok {
		return nil, fmt.Errorf("database %q not found: %w", dbName, ErrNotFound)
	}
	key := b.udfKey(dbName, input.FunctionName)
	if _, exists := b.udfs[key]; exists {
		return nil, fmt.Errorf(
			"user defined function %q already exists in database %q: %w",
			input.FunctionName,
			dbName,
			ErrAlreadyExists,
		)
	}
	udf := input
	udf.DatabaseName = dbName
	udf.FunctionARN = b.udfARN(dbName, input.FunctionName)
	udf.CreateTime = float64(time.Now().Unix())
	b.udfs[key] = &udf
	if len(tags) > 0 {
		_ = b.tagResource(udf.FunctionARN, tags)
	}

	return cloneUDF(&udf), nil
}

func (b *InMemoryBackend) GetUserDefinedFunction(
	dbName, name string,
) (*UserDefinedFunction, error) {
	b.mu.RLock("GetUserDefinedFunction")
	defer b.mu.RUnlock()

	u, ok := b.udfs[b.udfKey(dbName, name)]
	if !ok {
		return nil, fmt.Errorf(
			"user defined function %q not found in database %q: %w",
			name,
			dbName,
			ErrNotFound,
		)
	}

	return cloneUDF(u), nil
}

func (b *InMemoryBackend) GetUserDefinedFunctions(dbName string) []*UserDefinedFunction {
	b.mu.RLock("GetUserDefinedFunctions")
	defer b.mu.RUnlock()

	var out []*UserDefinedFunction
	for key, u := range b.udfs {
		if dbName == "" || u.DatabaseName == dbName {
			_ = key
			out = append(out, cloneUDF(u))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FunctionName < out[j].FunctionName })

	return out
}

func (b *InMemoryBackend) UpdateUserDefinedFunction(
	dbName, name string,
	input UserDefinedFunction,
) error {
	b.mu.Lock("UpdateUserDefinedFunction")
	defer b.mu.Unlock()

	key := b.udfKey(dbName, name)
	existing, ok := b.udfs[key]
	if !ok {
		return fmt.Errorf(
			"user defined function %q not found in database %q: %w",
			name,
			dbName,
			ErrNotFound,
		)
	}
	input.DatabaseName = dbName
	input.FunctionName = name
	input.FunctionARN = existing.FunctionARN
	input.CreateTime = existing.CreateTime
	b.udfs[key] = &input

	return nil
}

func (b *InMemoryBackend) DeleteUserDefinedFunction(dbName, name string) error {
	b.mu.Lock("DeleteUserDefinedFunction")
	defer b.mu.Unlock()

	key := b.udfKey(dbName, name)
	if _, ok := b.udfs[key]; !ok {
		return fmt.Errorf(
			"user defined function %q not found in database %q: %w",
			name,
			dbName,
			ErrNotFound,
		)
	}
	delete(b.udfs, key)

	return nil
}

// ---------------------------------------------------------------------------
// SecurityConfiguration
// ---------------------------------------------------------------------------

// EncryptionConfiguration holds encryption settings for a SecurityConfiguration.
type EncryptionConfiguration struct {
	CloudWatchEncryption   *CloudWatchEncryption   `json:"CloudWatchEncryption,omitempty"`
	JobBookmarksEncryption *JobBookmarksEncryption `json:"JobBookmarksEncryption,omitempty"`
	S3Encryption           []S3EncryptionEntry     `json:"S3Encryption,omitempty"`
}

// S3EncryptionEntry holds per-S3-bucket encryption config.
type S3EncryptionEntry struct {
	S3EncryptionMode string `json:"S3EncryptionMode,omitempty"`
	KMSKeyARN        string `json:"KmsKeyArn,omitempty"`
}

// CloudWatchEncryption holds CloudWatch encryption config.
type CloudWatchEncryption struct {
	CloudWatchEncryptionMode string `json:"CloudWatchEncryptionMode,omitempty"`
	KMSKeyARN                string `json:"KmsKeyArn,omitempty"`
}

// JobBookmarksEncryption holds job bookmarks encryption config.
type JobBookmarksEncryption struct {
	JobBookmarksEncryptionMode string `json:"JobBookmarksEncryptionMode,omitempty"`
	KMSKeyARN                  string `json:"KmsKeyArn,omitempty"`
}

// SecurityConfiguration represents a Glue security configuration.
type SecurityConfiguration struct {
	Name                    string                  `json:"Name"`
	EncryptionConfiguration EncryptionConfiguration `json:"EncryptionConfiguration"`
	CreatedTimeStamp        float64                 `json:"CreatedTimeStamp,omitempty"`
}

func cloneSecurityConfig(sc *SecurityConfiguration) *SecurityConfiguration {
	cp := *sc

	return &cp
}

func (b *InMemoryBackend) CreateSecurityConfiguration(
	name string,
	enc EncryptionConfiguration,
) (*SecurityConfiguration, error) {
	b.mu.Lock("CreateSecurityConfiguration")
	defer b.mu.Unlock()

	if _, exists := b.securityConfigs[name]; exists {
		return nil, fmt.Errorf(
			"security configuration %q already exists: %w",
			name,
			ErrAlreadyExists,
		)
	}
	sc := &SecurityConfiguration{
		Name:                    name,
		EncryptionConfiguration: enc,
		CreatedTimeStamp:        float64(time.Now().Unix()),
	}
	b.securityConfigs[name] = sc

	return cloneSecurityConfig(sc), nil
}

func (b *InMemoryBackend) GetSecurityConfiguration(name string) (*SecurityConfiguration, error) {
	b.mu.RLock("GetSecurityConfiguration")
	defer b.mu.RUnlock()

	sc, ok := b.securityConfigs[name]
	if !ok {
		return nil, fmt.Errorf("security configuration %q not found: %w", name, ErrNotFound)
	}

	return cloneSecurityConfig(sc), nil
}

func (b *InMemoryBackend) DeleteSecurityConfiguration(name string) error {
	b.mu.Lock("DeleteSecurityConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.securityConfigs[name]; !ok {
		return fmt.Errorf("security configuration %q not found: %w", name, ErrNotFound)
	}
	delete(b.securityConfigs, name)

	return nil
}

func (b *InMemoryBackend) ListSecurityConfigurations() []*SecurityConfiguration {
	b.mu.RLock("ListSecurityConfigurations")
	defer b.mu.RUnlock()

	out := make([]*SecurityConfiguration, 0, len(b.securityConfigs))
	for _, sc := range b.securityConfigs {
		out = append(out, cloneSecurityConfig(sc))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// ---------------------------------------------------------------------------
// Session + Statement
// ---------------------------------------------------------------------------

// SessionCommand holds the command info for a Glue interactive session.
type SessionCommand struct {
	Name          string `json:"Name,omitempty"`
	PythonVersion string `json:"PythonVersion,omitempty"`
}

// Session represents a Glue interactive session.
type Session struct {
	DefaultArguments map[string]string `json:"DefaultArguments,omitempty"`
	Command          SessionCommand    `json:"Command,omitzero"`
	SessionID        string            `json:"Id"`
	Role             string            `json:"Role,omitempty"`
	Status           string            `json:"Status"`
	Description      string            `json:"Description,omitempty"`
	CreatedOn        float64           `json:"CreatedOn,omitempty"`
	MaxCapacity      float64           `json:"MaxCapacity,omitempty"`
	Timeout          int32             `json:"Timeout,omitempty"`
}

// Statement represents a statement run within a Glue session.
type Statement struct {
	Output      any     `json:"Output,omitempty"`
	SessionId   string  `json:"SessionId,omitempty"` //nolint:revive,staticcheck // AWS API naming
	Code        string  `json:"Code,omitempty"`
	State       string  `json:"State"`
	Progress    float64 `json:"Progress,omitempty"`
	StartedOn   float64 `json:"StartedOn,omitempty"`
	CompletedOn float64 `json:"CompletedOn,omitempty"`
	Id          int32   `json:"Id"` //nolint:revive,staticcheck // AWS API uses Id not ID
}

func cloneSession(s *Session) *Session {
	cp := *s
	if s.DefaultArguments != nil {
		cp.DefaultArguments = make(map[string]string, len(s.DefaultArguments))
		maps.Copy(cp.DefaultArguments, s.DefaultArguments)
	}

	return &cp
}

func cloneStatement(st *Statement) *Statement {
	cp := *st

	return &cp
}

func (b *InMemoryBackend) CreateSession(
	id, role string,
	cmd SessionCommand,
	opts Session,
) (*Session, error) {
	b.mu.Lock("CreateSession")
	defer b.mu.Unlock()

	if _, exists := b.sessions[id]; exists {
		return nil, fmt.Errorf("session %q already exists: %w", id, ErrAlreadyExists)
	}
	s := &Session{
		SessionID:        id,
		Role:             role,
		Command:          cmd,
		Status:           "PROVISIONING",
		CreatedOn:        float64(time.Now().Unix()),
		Timeout:          opts.Timeout,
		MaxCapacity:      opts.MaxCapacity,
		Description:      opts.Description,
		DefaultArguments: opts.DefaultArguments,
	}
	b.sessions[id] = s
	b.sessionStatements[id] = nil

	return cloneSession(s), nil
}

func (b *InMemoryBackend) GetSession(id string) (*Session, error) {
	b.mu.RLock("GetSession")
	defer b.mu.RUnlock()

	s, ok := b.sessions[id]
	if !ok {
		return nil, fmt.Errorf("session %q not found: %w", id, ErrNotFound)
	}

	return cloneSession(s), nil
}

func (b *InMemoryBackend) ListSessions() []*Session {
	b.mu.RLock("ListSessions")
	defer b.mu.RUnlock()

	out := make([]*Session, 0, len(b.sessions))
	for _, s := range b.sessions {
		out = append(out, cloneSession(s))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SessionID < out[j].SessionID })

	return out
}

func (b *InMemoryBackend) DeleteSession(id string) error {
	b.mu.Lock("DeleteSession")
	defer b.mu.Unlock()

	if _, ok := b.sessions[id]; !ok {
		return fmt.Errorf("session %q not found: %w", id, ErrNotFound)
	}
	delete(b.sessions, id)
	delete(b.sessionStatements, id)

	return nil
}

func (b *InMemoryBackend) StopSession(id string) error {
	b.mu.Lock("StopSession")
	defer b.mu.Unlock()

	s, ok := b.sessions[id]
	if !ok {
		return fmt.Errorf("session %q not found: %w", id, ErrNotFound)
	}
	s.Status = "STOPPING"

	return nil
}

func (b *InMemoryBackend) RunStatement(sessionID, code string) (*Statement, error) {
	b.mu.Lock("RunStatement")
	defer b.mu.Unlock()

	if _, ok := b.sessions[sessionID]; !ok {
		return nil, fmt.Errorf("session %q not found: %w", sessionID, ErrNotFound)
	}
	stmts := b.sessionStatements[sessionID]
	st := &Statement{
		Id:        int32(len(stmts) + 1), //nolint:gosec // statement count always small
		SessionId: sessionID,
		Code:      code,
		State:     "WAITING",
		StartedOn: float64(time.Now().Unix()),
	}
	b.sessionStatements[sessionID] = append(stmts, st)

	return cloneStatement(st), nil
}

func (b *InMemoryBackend) GetStatement(sessionID string, statementID int32) (*Statement, error) {
	b.mu.RLock("GetStatement")
	defer b.mu.RUnlock()

	stmts, ok := b.sessionStatements[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found: %w", sessionID, ErrNotFound)
	}
	for _, st := range stmts {
		if st.Id == statementID {
			return cloneStatement(st), nil
		}
	}

	return nil, fmt.Errorf(
		"statement %d not found in session %q: %w",
		statementID,
		sessionID,
		ErrNotFound,
	)
}

func (b *InMemoryBackend) GetStatements(sessionID string) ([]*Statement, error) {
	b.mu.RLock("GetStatements")
	defer b.mu.RUnlock()

	if _, ok := b.sessions[sessionID]; !ok {
		return nil, fmt.Errorf("session %q not found: %w", sessionID, ErrNotFound)
	}
	stmts := b.sessionStatements[sessionID]
	out := make([]*Statement, len(stmts))
	for i, st := range stmts {
		out[i] = cloneStatement(st)
	}

	return out, nil
}

func (b *InMemoryBackend) CancelStatement(sessionID string, statementID int32) error {
	b.mu.Lock("CancelStatement")
	defer b.mu.Unlock()

	stmts, ok := b.sessionStatements[sessionID]
	if !ok {
		return fmt.Errorf("session %q not found: %w", sessionID, ErrNotFound)
	}
	for _, st := range stmts {
		if st.Id == statementID {
			st.State = "CANCELLING"

			return nil
		}
	}

	return fmt.Errorf(
		"statement %d not found in session %q: %w",
		statementID,
		sessionID,
		ErrNotFound,
	)
}

// ---------------------------------------------------------------------------
// TableOptimizer
// ---------------------------------------------------------------------------

// TableOptimizerConfiguration holds the config for a table optimizer.
type TableOptimizerConfiguration struct {
	RoleARN string `json:"RoleArn,omitempty"`
	Enabled bool   `json:"Enabled"`
}

// TableOptimizerRun holds a single run record for a table optimizer. The Glue
// Iceberg table-optimizer sub-API serializes this nested document with
// lowerCamelCase keys, unlike the rest of the Glue JSON protocol.
type TableOptimizerRun struct {
	Metrics   any     `json:"metrics,omitempty"`
	EventType string  `json:"eventType,omitempty"`
	Error     string  `json:"error,omitempty"`
	StartedAt float64 `json:"startTimestamp,omitempty"`
	EndedAt   float64 `json:"endTimestamp,omitempty"`
}

// TableOptimizer represents a single table optimizer resource.
type TableOptimizer struct {
	LastRun       *TableOptimizerRun          `json:"LastRun,omitempty"`
	CatalogID     string                      `json:"CatalogId,omitempty"`
	DatabaseName  string                      `json:"DatabaseName"`
	TableName     string                      `json:"TableName"`
	Type          string                      `json:"Type"`
	Configuration TableOptimizerConfiguration `json:"Configuration,omitzero"`
}

func (b *InMemoryBackend) tableOptimizerKey(dbName, tableName, optimizerType string) string {
	return dbName + "|" + tableName + "|" + optimizerType
}

func cloneTableOptimizer(to *TableOptimizer) *TableOptimizer {
	cp := *to
	if to.LastRun != nil {
		lastRun := *to.LastRun
		cp.LastRun = &lastRun
	}

	return &cp
}

// CreateTableOptimizer registers a table optimizer and, mirroring the automatic
// compaction run AWS kicks off shortly after an optimizer is enabled, seeds an
// initial completed run so ListTableOptimizerRuns has real history to return.
func (b *InMemoryBackend) CreateTableOptimizer(
	catalogID, dbName, tableName, optimizerType string,
	config TableOptimizerConfiguration,
) error {
	b.mu.Lock("CreateTableOptimizer")
	defer b.mu.Unlock()

	key := b.tableOptimizerKey(dbName, tableName, optimizerType)
	if _, exists := b.tableOptimizers[key]; exists {
		return fmt.Errorf(
			"table optimizer type %q already exists for %s.%s: %w",
			optimizerType,
			dbName,
			tableName,
			ErrAlreadyExists,
		)
	}

	now := float64(time.Now().Unix())
	b.tableOptimizers[key] = &TableOptimizer{
		CatalogID:     catalogID,
		DatabaseName:  dbName,
		TableName:     tableName,
		Type:          optimizerType,
		Configuration: config,
		LastRun: &TableOptimizerRun{
			EventType: "completed",
			StartedAt: now,
			EndedAt:   now,
		},
	}

	return nil
}

func (b *InMemoryBackend) GetTableOptimizer(
	dbName, tableName, optimizerType string,
) (*TableOptimizer, error) {
	b.mu.RLock("GetTableOptimizer")
	defer b.mu.RUnlock()

	to, ok := b.tableOptimizers[b.tableOptimizerKey(dbName, tableName, optimizerType)]
	if !ok {
		return nil, fmt.Errorf(
			"table optimizer %q not found for %s.%s: %w",
			optimizerType,
			dbName,
			tableName,
			ErrNotFound,
		)
	}

	return cloneTableOptimizer(to), nil
}

func (b *InMemoryBackend) UpdateTableOptimizer(
	dbName, tableName, optimizerType string,
	config TableOptimizerConfiguration,
) error {
	b.mu.Lock("UpdateTableOptimizer")
	defer b.mu.Unlock()

	key := b.tableOptimizerKey(dbName, tableName, optimizerType)
	to, ok := b.tableOptimizers[key]
	if !ok {
		return fmt.Errorf(
			"table optimizer %q not found for %s.%s: %w",
			optimizerType,
			dbName,
			tableName,
			ErrNotFound,
		)
	}
	to.Configuration = config

	return nil
}

func (b *InMemoryBackend) DeleteTableOptimizer(dbName, tableName, optimizerType string) error {
	b.mu.Lock("DeleteTableOptimizer")
	defer b.mu.Unlock()

	key := b.tableOptimizerKey(dbName, tableName, optimizerType)
	if _, ok := b.tableOptimizers[key]; !ok {
		return fmt.Errorf(
			"table optimizer %q not found for %s.%s: %w",
			optimizerType,
			dbName,
			tableName,
			ErrNotFound,
		)
	}
	delete(b.tableOptimizers, key)

	return nil
}

// BatchGetTableOptimizerEntry is one request entry for BatchGetTableOptimizer.
type BatchGetTableOptimizerEntry struct {
	CatalogID    string `json:"CatalogId,omitempty"`
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	Type         string `json:"Type"`
}

// BatchGetTableOptimizerError is one error entry from BatchGetTableOptimizer.
type BatchGetTableOptimizerError struct {
	CatalogID    string      `json:"CatalogId,omitempty"`
	DatabaseName string      `json:"DatabaseName,omitempty"`
	TableName    string      `json:"TableName,omitempty"`
	Type         string      `json:"Type,omitempty"`
	Error        ErrorDetail `json:"Error"`
}

func (b *InMemoryBackend) BatchGetTableOptimizer(
	entries []BatchGetTableOptimizerEntry,
) ([]*TableOptimizer, []BatchGetTableOptimizerError) {
	b.mu.RLock("BatchGetTableOptimizer")
	defer b.mu.RUnlock()

	var found []*TableOptimizer
	var errs []BatchGetTableOptimizerError
	for _, e := range entries {
		key := b.tableOptimizerKey(e.DatabaseName, e.TableName, e.Type)
		if to, ok := b.tableOptimizers[key]; ok {
			found = append(found, cloneTableOptimizer(to))
		} else {
			errs = append(errs, BatchGetTableOptimizerError{
				CatalogID:    e.CatalogID,
				DatabaseName: e.DatabaseName,
				TableName:    e.TableName,
				Type:         e.Type,
				Error:        ErrorDetail{ErrorCode: errEntityNotFoundCode, ErrorMessage: "table optimizer not found"},
			})
		}
	}

	return found, errs
}

// ---------------------------------------------------------------------------
// Column Statistics
// ---------------------------------------------------------------------------

// ColumnStatisticsData holds statistics for a column.
type ColumnStatisticsData struct {
	BooleanColumnStatisticsData any    `json:"BooleanColumnStatisticsData,omitempty"`
	DateColumnStatisticsData    any    `json:"DateColumnStatisticsData,omitempty"`
	DecimalColumnStatisticsData any    `json:"DecimalColumnStatisticsData,omitempty"`
	DoubleColumnStatisticsData  any    `json:"DoubleColumnStatisticsData,omitempty"`
	LongColumnStatisticsData    any    `json:"LongColumnStatisticsData,omitempty"`
	StringColumnStatisticsData  any    `json:"StringColumnStatisticsData,omitempty"`
	BinaryColumnStatisticsData  any    `json:"BinaryColumnStatisticsData,omitempty"`
	Type                        string `json:"Type"`
}

// ColumnStatistics represents statistics for a single column.
type ColumnStatistics struct {
	StatisticsData ColumnStatisticsData `json:"StatisticsData"`
	ColumnName     string               `json:"ColumnName"`
	ColumnType     string               `json:"ColumnType,omitempty"`
	AnalyzedTime   float64              `json:"AnalyzedTime,omitempty"`
}

func cloneColumnStats(cs *ColumnStatistics) *ColumnStatistics {
	cp := *cs

	return &cp
}

func (b *InMemoryBackend) columnStatsKey(dbName, tableName, colName string) string {
	return dbName + "|" + tableName + "|" + colName
}

func (b *InMemoryBackend) UpdateColumnStatisticsForTable(
	dbName, tableName string,
	stats []*ColumnStatistics,
) error {
	b.mu.Lock("UpdateColumnStatisticsForTable")
	defer b.mu.Unlock()

	if _, ok := b.databases[dbName]; !ok {
		return fmt.Errorf("database %q not found: %w", dbName, ErrNotFound)
	}
	for _, cs := range stats {
		key := b.columnStatsKey(dbName, tableName, cs.ColumnName)
		cp := cloneColumnStats(cs)
		cp.AnalyzedTime = float64(time.Now().Unix())
		b.tableColumnStats[key] = cp
	}

	return nil
}

func (b *InMemoryBackend) GetColumnStatisticsForTable(
	dbName, tableName string,
	columnNames []string,
) ([]*ColumnStatistics, error) {
	b.mu.RLock("GetColumnStatisticsForTable")
	defer b.mu.RUnlock()

	var out []*ColumnStatistics
	if len(columnNames) == 0 {
		prefix := dbName + "|" + tableName + "|"
		for key, cs := range b.tableColumnStats {
			if len(key) > len(prefix) && key[:len(prefix)] == prefix {
				out = append(out, cloneColumnStats(cs))
			}
		}
	} else {
		for _, col := range columnNames {
			key := b.columnStatsKey(dbName, tableName, col)
			if cs, ok := b.tableColumnStats[key]; ok {
				out = append(out, cloneColumnStats(cs))
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ColumnName < out[j].ColumnName })

	return out, nil
}

func (b *InMemoryBackend) DeleteColumnStatisticsForTable(
	dbName, tableName, columnName string,
) error {
	b.mu.Lock("DeleteColumnStatisticsForTable")
	defer b.mu.Unlock()

	key := b.columnStatsKey(dbName, tableName, columnName)
	delete(b.tableColumnStats, key)

	return nil
}

func (b *InMemoryBackend) UpdateColumnStatisticsForPartition(
	dbName, tableName string,
	partitionValues []string,
	stats []*ColumnStatistics,
) error {
	b.mu.Lock("UpdateColumnStatisticsForPartition")
	defer b.mu.Unlock()

	pk := partitionKey(dbName, tableName, partitionValues)
	for _, cs := range stats {
		key := pk + "|" + cs.ColumnName
		cp := cloneColumnStats(cs)
		cp.AnalyzedTime = float64(time.Now().Unix())
		b.partitionColumnStats[key] = cp
	}

	return nil
}

func (b *InMemoryBackend) GetColumnStatisticsForPartition(
	dbName, tableName string,
	partitionValues []string,
	columnNames []string,
) ([]*ColumnStatistics, error) {
	b.mu.RLock("GetColumnStatisticsForPartition")
	defer b.mu.RUnlock()

	pk := partitionKey(dbName, tableName, partitionValues)
	var out []*ColumnStatistics
	if len(columnNames) == 0 {
		prefix := pk + "|"
		for key, cs := range b.partitionColumnStats {
			if len(key) > len(prefix) && key[:len(prefix)] == prefix {
				out = append(out, cloneColumnStats(cs))
			}
		}
	} else {
		for _, col := range columnNames {
			key := pk + "|" + col
			if cs, ok := b.partitionColumnStats[key]; ok {
				out = append(out, cloneColumnStats(cs))
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ColumnName < out[j].ColumnName })

	return out, nil
}

func (b *InMemoryBackend) DeleteColumnStatisticsForPartition(
	dbName, tableName string,
	partitionValues []string,
	columnName string,
) error {
	b.mu.Lock("DeleteColumnStatisticsForPartition")
	defer b.mu.Unlock()

	pk := partitionKey(dbName, tableName, partitionValues)
	key := pk + "|" + columnName
	delete(b.partitionColumnStats, key)

	return nil
}

// ---------------------------------------------------------------------------
// Resource Policy
// ---------------------------------------------------------------------------

const globalPolicyKey = "__global__"

func (b *InMemoryBackend) PutResourcePolicy(policy, resourceARN string) (string, error) {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	key := resourceARN
	if key == "" {
		key = globalPolicyKey
	}
	hash := uuid.NewString()[:8]
	now := float64(time.Now().Unix())

	createTime := now
	if existing, ok := b.resourcePolicies[key]; ok {
		createTime = existing.CreateTime
	}

	b.resourcePolicies[key] = &resourcePolicyEntry{
		Policy:     policy,
		Hash:       hash,
		CreateTime: createTime,
		UpdateTime: now,
	}

	return hash, nil
}

type resourcePolicyEntry struct {
	Policy     string  `json:"Policy"`
	Hash       string  `json:"Hash"`
	CreateTime float64 `json:"CreateTime,omitempty"`
	UpdateTime float64 `json:"UpdateTime,omitempty"`
}

// ListResourcePolicies returns every stored resource policy (per-resource ARN
// policies plus the account-level policy, if set), sorted by key for a
// deterministic response.
func (b *InMemoryBackend) ListResourcePolicies() []*resourcePolicyEntry {
	b.mu.RLock("ListResourcePolicies")
	defer b.mu.RUnlock()

	keys := collections.SortedKeys(b.resourcePolicies)
	out := make([]*resourcePolicyEntry, 0, len(keys))

	for _, k := range keys {
		cp := *b.resourcePolicies[k]
		out = append(out, &cp)
	}

	return out
}

func (b *InMemoryBackend) GetResourcePolicy(resourceARN string) (string, string, error) {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	key := resourceARN
	if key == "" {
		key = globalPolicyKey
	}
	e, ok := b.resourcePolicies[key]
	if !ok {
		return "", "", fmt.Errorf("resource policy not found: %w", ErrNotFound)
	}

	return e.Policy, e.Hash, nil
}

func (b *InMemoryBackend) DeleteResourcePolicy(resourceARN, _ string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	key := resourceARN
	if key == "" {
		key = globalPolicyKey
	}
	if _, ok := b.resourcePolicies[key]; !ok {
		return fmt.Errorf("resource policy not found: %w", ErrNotFound)
	}
	delete(b.resourcePolicies, key)

	return nil
}

// ---------------------------------------------------------------------------
// MLTransform
// ---------------------------------------------------------------------------

// MLTransformParameter holds transform hyperparameters.
type MLTransformParameter struct {
	FindMatchesParameters any    `json:"FindMatchesParameters,omitempty"`
	TransformType         string `json:"TransformType,omitempty"`
}

// GlueTable holds a reference to a Glue catalog table used by an ML transform.
type GlueTable struct { //nolint:revive // GlueTable is distinct from Table type; renaming would cause conflicts
	CatalogID      string `json:"CatalogId,omitempty"`
	DatabaseName   string `json:"DatabaseName"`
	TableName      string `json:"TableName"`
	ConnectionName string `json:"ConnectionName,omitempty"`
}

// MLTransform represents an AWS Glue ML transform.
type MLTransform struct {
	Parameters        MLTransformParameter `json:"Parameters,omitzero"`
	TransformID       string               `json:"TransformId"`
	Name              string               `json:"Name"`
	Description       string               `json:"Description,omitempty"`
	Role              string               `json:"Role,omitempty"`
	GlueVersion       string               `json:"GlueVersion,omitempty"`
	WorkerType        string               `json:"WorkerType,omitempty"`
	Status            string               `json:"Status"`
	InputRecordTables []GlueTable          `json:"InputRecordTables,omitempty"`
	MaxCapacity       float64              `json:"MaxCapacity,omitempty"`
	CreatedOn         float64              `json:"CreatedOn,omitempty"`
	LastModifiedOn    float64              `json:"LastModifiedOn,omitempty"`
	NumberOfWorkers   int32                `json:"NumberOfWorkers,omitempty"`
}

func cloneMLTransform(m *MLTransform) *MLTransform {
	cp := *m
	cp.InputRecordTables = make([]GlueTable, len(m.InputRecordTables))
	copy(cp.InputRecordTables, m.InputRecordTables)

	return &cp
}

func (b *InMemoryBackend) mlTransformARN(id string) string {
	return arn.Build("glue", b.region, b.accountID, "mlTransform/"+id)
}

func (b *InMemoryBackend) CreateMLTransform(
	name, description, role string,
	tables []GlueTable,
	params MLTransformParameter,
	tags map[string]string,
) (*MLTransform, error) {
	b.mu.Lock("CreateMLTransform")
	defer b.mu.Unlock()

	id := "transform-" + uuid.NewString()[:8]
	m := &MLTransform{
		TransformID:       id,
		Name:              name,
		Description:       description,
		Role:              role,
		InputRecordTables: tables,
		Parameters:        params,
		Status:            "NOT_READY",
		CreatedOn:         float64(time.Now().Unix()),
		LastModifiedOn:    float64(time.Now().Unix()),
	}
	b.mlTransforms[id] = m
	if len(tags) > 0 {
		_ = b.tagResource(b.mlTransformARN(id), tags)
	}

	return cloneMLTransform(m), nil
}

func (b *InMemoryBackend) GetMLTransform(id string) (*MLTransform, error) {
	b.mu.RLock("GetMLTransform")
	defer b.mu.RUnlock()

	m, ok := b.mlTransforms[id]
	if !ok {
		return nil, fmt.Errorf("ML transform %q not found: %w", id, ErrNotFound)
	}

	return cloneMLTransform(m), nil
}

func (b *InMemoryBackend) GetMLTransforms() []*MLTransform {
	b.mu.RLock("GetMLTransforms")
	defer b.mu.RUnlock()

	out := make([]*MLTransform, 0, len(b.mlTransforms))
	for _, m := range b.mlTransforms {
		out = append(out, cloneMLTransform(m))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

func (b *InMemoryBackend) UpdateMLTransform(id string, update MLTransform) error {
	b.mu.Lock("UpdateMLTransform")
	defer b.mu.Unlock()

	existing, ok := b.mlTransforms[id]
	if !ok {
		return fmt.Errorf("ML transform %q not found: %w", id, ErrNotFound)
	}
	update.TransformID = id
	update.CreatedOn = existing.CreatedOn
	update.LastModifiedOn = float64(time.Now().Unix())
	b.mlTransforms[id] = &update

	return nil
}

func (b *InMemoryBackend) DeleteMLTransform(id string) error {
	b.mu.Lock("DeleteMLTransform")
	defer b.mu.Unlock()

	if _, ok := b.mlTransforms[id]; !ok {
		return fmt.Errorf("ML transform %q not found: %w", id, ErrNotFound)
	}
	delete(b.mlTransforms, id)

	return nil
}

// ---------------------------------------------------------------------------
// Catalog (AWS Glue Data Catalog — federated catalog entries)
// ---------------------------------------------------------------------------

// CatalogEntry represents a named AWS Glue catalog.
type CatalogEntry struct {
	Parameters  map[string]string `json:"Parameters,omitzero"`
	CatalogID   string            `json:"CatalogId"`
	Name        string            `json:"Name,omitempty"`
	Description string            `json:"Description,omitempty"`
	CreateTime  float64           `json:"CreateTime,omitempty"`
}

func cloneCatalogEntry(c *CatalogEntry) *CatalogEntry {
	cp := *c
	if c.Parameters != nil {
		cp.Parameters = make(map[string]string, len(c.Parameters))
		maps.Copy(cp.Parameters, c.Parameters)
	}

	return &cp
}

func (b *InMemoryBackend) CreateCatalog(
	catalogID, name, description string,
	params map[string]string,
) error {
	b.mu.Lock("CreateCatalog")
	defer b.mu.Unlock()

	if _, exists := b.catalogs[catalogID]; exists {
		return fmt.Errorf("catalog %q already exists: %w", catalogID, ErrAlreadyExists)
	}
	b.catalogs[catalogID] = &CatalogEntry{
		CatalogID:   catalogID,
		Name:        name,
		Description: description,
		Parameters:  params,
		CreateTime:  float64(time.Now().Unix()),
	}

	return nil
}

func (b *InMemoryBackend) GetCatalog(catalogID string) (*CatalogEntry, error) {
	b.mu.RLock("GetCatalog")
	defer b.mu.RUnlock()

	c, ok := b.catalogs[catalogID]
	if !ok {
		return nil, fmt.Errorf("catalog %q not found: %w", catalogID, ErrNotFound)
	}

	return cloneCatalogEntry(c), nil
}

func (b *InMemoryBackend) GetCatalogs() []*CatalogEntry {
	b.mu.RLock("GetCatalogs")
	defer b.mu.RUnlock()

	out := make([]*CatalogEntry, 0, len(b.catalogs))
	for _, c := range b.catalogs {
		out = append(out, cloneCatalogEntry(c))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CatalogID < out[j].CatalogID })

	return out
}

func (b *InMemoryBackend) UpdateCatalog(
	catalogID, description string,
	params map[string]string,
) error {
	b.mu.Lock("UpdateCatalog")
	defer b.mu.Unlock()

	c, ok := b.catalogs[catalogID]
	if !ok {
		return fmt.Errorf("catalog %q not found: %w", catalogID, ErrNotFound)
	}
	c.Description = description
	if params != nil {
		c.Parameters = params
	}

	return nil
}

func (b *InMemoryBackend) DeleteCatalog(catalogID string) error {
	b.mu.Lock("DeleteCatalog")
	defer b.mu.Unlock()

	if _, ok := b.catalogs[catalogID]; !ok {
		return fmt.Errorf("catalog %q not found: %w", catalogID, ErrNotFound)
	}
	delete(b.catalogs, catalogID)

	return nil
}

// ---------------------------------------------------------------------------
// StopWorkflowRun / PutWorkflowRunProperties / DeleteTableVersion / UpdateDevEndpoint
// ---------------------------------------------------------------------------

func (b *InMemoryBackend) StopWorkflowRun(workflowName, runID string) error {
	b.mu.Lock("StopWorkflowRun")
	defer b.mu.Unlock()

	runs, ok := b.workflowRuns[workflowName]
	if !ok {
		return fmt.Errorf("workflow %q not found: %w", workflowName, ErrNotFound)
	}
	for _, r := range runs {
		if r.RunID == runID {
			r.Status = "STOPPING"

			return nil
		}
	}

	return fmt.Errorf(
		"workflow run %q not found for workflow %q: %w",
		runID,
		workflowName,
		ErrNotFound,
	)
}

func (b *InMemoryBackend) PutWorkflowRunProperties(
	workflowName, runID string,
	props map[string]string,
) error {
	b.mu.Lock("PutWorkflowRunProperties")
	defer b.mu.Unlock()

	runs, ok := b.workflowRuns[workflowName]
	if !ok {
		return fmt.Errorf("workflow %q not found: %w", workflowName, ErrNotFound)
	}
	for _, r := range runs {
		if r.RunID == runID {
			if r.Properties == nil {
				r.Properties = make(map[string]string)
			}
			maps.Copy(r.Properties, props)

			return nil
		}
	}

	return fmt.Errorf(
		"workflow run %q not found for workflow %q: %w",
		runID,
		workflowName,
		ErrNotFound,
	)
}

func (b *InMemoryBackend) DeleteTableVersion(dbName, tableName, versionID string) error {
	b.mu.Lock("DeleteTableVersion")
	defer b.mu.Unlock()

	key := tableVersionKey(dbName, tableName, versionID)
	if _, ok := b.tableVersions[key]; !ok {
		return fmt.Errorf(
			"table version %q not found for %s.%s: %w",
			versionID,
			dbName,
			tableName,
			ErrNotFound,
		)
	}
	delete(b.tableVersions, key)

	return nil
}

func (b *InMemoryBackend) UpdateDevEndpoint(name string, args map[string]string) error {
	b.mu.Lock("UpdateDevEndpoint")
	defer b.mu.Unlock()

	dep, ok := b.devEndpoints[name]
	if !ok {
		return fmt.Errorf("dev endpoint %q not found: %w", name, ErrNotFound)
	}
	if dep.Arguments == nil {
		dep.Arguments = make(map[string]string)
	}
	maps.Copy(dep.Arguments, args)

	return nil
}

// ---------------------------------------------------------------------------
// DataCatalogEncryptionSettings
// ---------------------------------------------------------------------------

// DataCatalogEncryptionSettings holds catalog encryption settings.
type DataCatalogEncryptionSettings struct {
	EncryptionAtRest             *EncryptionAtRest             `json:"EncryptionAtRest,omitempty"`
	ConnectionPasswordEncryption *ConnectionPasswordEncryption `json:"ConnectionPasswordEncryption,omitempty"`
}

// EncryptionAtRest holds at-rest encryption config.
type EncryptionAtRest struct {
	CatalogEncryptionMode        string `json:"CatalogEncryptionMode,omitempty"`
	SseAwsKmsKeyID               string `json:"SseAwsKmsKeyId,omitempty"`
	CatalogEncryptionServiceRole string `json:"CatalogEncryptionServiceRole,omitempty"`
}

// ConnectionPasswordEncryption holds connection password encryption config.
type ConnectionPasswordEncryption struct {
	AwsKmsKeyID                       string `json:"AwsKmsKeyId,omitempty"`
	ReturnConnectionPasswordEncrypted bool   `json:"ReturnConnectionPasswordEncrypted"`
}

func (b *InMemoryBackend) PutDataCatalogEncryptionSettings(
	catalogID string,
	settings DataCatalogEncryptionSettings,
) error {
	b.mu.Lock("PutDataCatalogEncryptionSettings")
	defer b.mu.Unlock()

	key := catalogID
	if key == "" {
		key = b.accountID
	}
	b.catalogEncryptionSettings[key] = &settings

	return nil
}

func (b *InMemoryBackend) GetDataCatalogEncryptionSettings(
	catalogID string,
) (*DataCatalogEncryptionSettings, error) {
	b.mu.RLock("GetDataCatalogEncryptionSettings")
	defer b.mu.RUnlock()

	key := catalogID
	if key == "" {
		key = b.accountID
	}
	if s, ok := b.catalogEncryptionSettings[key]; ok {
		cp := *s

		return &cp, nil
	}
	// Return empty default settings (AWS returns defaults even if never set)
	return &DataCatalogEncryptionSettings{}, nil
}
