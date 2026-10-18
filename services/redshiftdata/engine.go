package redshiftdata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// EngineLogin identifies the master login of a cluster's live SQL engine.
type EngineLogin struct {
	Addr     string
	User     string
	Password string
	DBName   string
}

// ClusterEngines resolves the live SQL engine behind a provisioned cluster, if any.
type ClusterEngines interface {
	EngineLogin(region, clusterID string) (EngineLogin, bool)
}

// Field is a Redshift Data API record value (types.Field): exactly one member is set.
type Field struct {
	BooleanValue *bool    `json:"booleanValue,omitempty"`
	DoubleValue  *float64 `json:"doubleValue,omitempty"`
	LongValue    *int64   `json:"longValue,omitempty"`
	StringValue  *string  `json:"stringValue,omitempty"`
	BlobValue    *string  `json:"blobValue,omitempty"`
	IsNull       bool     `json:"isNull,omitempty"`
}

// ColumnMetadata describes one result column (types.ColumnMetadata).
type ColumnMetadata struct {
	Name            string `json:"name"`
	Label           string `json:"label"`
	TypeName        string `json:"typeName"`
	SchemaName      string `json:"schemaName"`
	TableName       string `json:"tableName"`
	Length          int32  `json:"length"`
	Nullable        int32  `json:"nullable"`
	Precision       int32  `json:"precision"`
	Scale           int32  `json:"scale"`
	IsCaseSensitive bool   `json:"isCaseSensitive"`
	IsCurrency      bool   `json:"isCurrency"`
	IsSigned        bool   `json:"isSigned"`
}

// ResultSet is the rows and columns one statement returned.
type ResultSet struct {
	Columns []ColumnMetadata
	Rows    [][]Field
}

// SQLOutcome is the result of one SQL statement; Result is nil for statements without a result set.
type SQLOutcome struct {
	Err    error
	Result *ResultSet
}

// SQLExecutor runs SQL against a live engine. It returns one outcome per statement attempted and stops
// after the first failure; atomic runs the statements in a single transaction.
type SQLExecutor interface {
	ExecuteSQL(ctx context.Context, login EngineLogin, sqls []string, params []SQLParameter, atomic bool) []SQLOutcome
}

type engineState struct {
	engines ClusterEngines
	exec    SQLExecutor
}

// SetClusterEngines wires the lookup of live cluster engines; statements against such clusters run real SQL.
func (b *InMemoryBackend) SetClusterEngines(e ClusterEngines) {
	b.mu.Lock("SetClusterEngines")
	defer b.mu.Unlock()

	if b.engineState == nil {
		b.engineState = &engineState{exec: NewPGXExecutor()}
	}

	b.engineState.engines = e
}

// SetSQLExecutor replaces the executor that runs SQL against live engines (default: Postgres wire).
func (b *InMemoryBackend) SetSQLExecutor(x SQLExecutor) {
	b.mu.Lock("SetSQLExecutor")
	defer b.mu.Unlock()

	if b.engineState == nil {
		b.engineState = &engineState{}
	}

	b.engineState.exec = x
}

// SetClusterEngines wires the live cluster engine lookup into the Data API backend.
func (h *Handler) SetClusterEngines(e ClusterEngines) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.SetClusterEngines(e)
	}
}

type engineRun struct {
	started  time.Time
	outcomes []SQLOutcome
	elapsed  time.Duration
}

// runEngine executes sqls on the live engine behind clusterID; live is false when no engine backs it.
func (b *InMemoryBackend) runEngine(
	ctx context.Context, region, clusterID, database string, sqls []string, params []SQLParameter, atomic bool,
) (engineRun, bool) {
	if clusterID == "" {
		return engineRun{}, false
	}

	b.mu.RLock("runEngine")
	st := b.engineState
	b.mu.RUnlock()

	if st == nil || st.engines == nil || st.exec == nil {
		return engineRun{}, false
	}

	login, ok := st.engines.EngineLogin(region, clusterID)
	if !ok {
		return engineRun{}, false
	}

	if database != "" {
		login.DBName = database
	}

	started := time.Now()
	outcomes := st.exec.ExecuteSQL(ctx, login, sqls, params, atomic)

	return engineRun{outcomes: outcomes, started: started, elapsed: time.Since(started)}, true
}

var errEngineNoOutcome = errors.New("the engine returned no outcome for the statement")

func resultRows(rs *ResultSet) int64 {
	if rs == nil {
		return -1
	}

	return int64(len(rs.Rows))
}

func resultSize(rs *ResultSet) int64 {
	if rs == nil {
		return 0
	}

	raw, err := json.Marshal(rs.Rows)
	if err != nil {
		return 0
	}

	return int64(len(raw))
}

func hasResultSet(rs *ResultSet) bool { return rs != nil && len(rs.Columns) > 0 }

func outcomeStatus(o SQLOutcome) string {
	if o.Err != nil {
		return statusFailed
	}

	return statusFinished
}

// applyEngineStatement records a single engine outcome on stmt.
func applyEngineStatement(stmt *Statement, run engineRun, now time.Time) {
	stmt.EngineBacked = true
	stmt.UpdatedAt = now
	stmt.DurationMs = run.elapsed.Milliseconds()

	o := SQLOutcome{Err: errEngineNoOutcome}
	if len(run.outcomes) > 0 {
		o = run.outcomes[0]
	}

	stmt.Status = outcomeStatus(o)

	if o.Err != nil {
		stmt.Error = o.Err.Error()
		stmt.ResultRows = -1
		stmt.HasResultSet = false

		return
	}

	stmt.HasResultSet = hasResultSet(o.Result)
	stmt.ResultRows = resultRows(o.Result)
	stmt.ResultSize = resultSize(o.Result)

	if stmt.HasResultSet {
		stmt.results = map[string]*ResultSet{stmt.ID: o.Result}
	}
}

// applyEngineBatch records the per-statement outcomes of a batch on stmt and its sub-statements.
func applyEngineBatch(stmt *Statement, run engineRun, now time.Time) {
	stmt.EngineBacked = true
	stmt.UpdatedAt = now
	stmt.DurationMs = run.elapsed.Milliseconds()
	stmt.Status = statusFinished
	stmt.results = make(map[string]*ResultSet)

	var total int64

	for i := range stmt.SubStatements {
		sub := &stmt.SubStatements[i]
		sub.ID = stmt.ID + ":" + strconv.Itoa(i+1)
		sub.UpdatedAt = now

		if i >= len(run.outcomes) {
			sub.Status = statusAborted
			sub.ResultRows = -1

			continue
		}

		o := run.outcomes[i]
		sub.Status = outcomeStatus(o)
		sub.DurationMs = run.elapsed.Milliseconds() / int64(len(stmt.SubStatements))

		if o.Err != nil {
			sub.Error = o.Err.Error()
			sub.ResultRows = -1
			stmt.Status = statusFailed
			stmt.Error = o.Err.Error()

			continue
		}

		sub.HasResultSet = hasResultSet(o.Result)
		sub.ResultRows = resultRows(o.Result)
		sub.ResultSize = resultSize(o.Result)
		total += sub.ResultSize

		if sub.HasResultSet {
			stmt.results[sub.ID] = o.Result
		}
	}

	stmt.ResultSize = total
	stmt.ResultRows = -1
}

// EngineResult returns the stored result set of an engine-run statement or batch sub-statement.
// found is false when id is not an engine-run statement, so callers fall back to the default result.
func (b *InMemoryBackend) EngineResult(ctx context.Context, id string) (*ResultSet, bool, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("EngineResult")
	defer b.mu.RUnlock()

	store := b.storeForRead(region)
	if store == nil {
		return nil, false, nil
	}

	stmt, ok := store.statements[id]
	if !ok {
		stmt, ok = store.statements[parentStatementID(id)]
	}

	if !ok || !stmt.EngineBacked {
		return nil, false, nil
	}

	if id == stmt.ID && stmt.Status != statusFinished {
		return nil, true, fmt.Errorf("%w: statement %s is %s; results are available once it is FINISHED",
			ErrValidation, id, stmt.Status)
	}

	rs, ok := stmt.results[id]
	if !ok {
		return nil, true, fmt.Errorf("%w: statement %s does not have a result set", ErrNoResultSet, id)
	}

	return rs, true, nil
}

func parentStatementID(id string) string {
	for i := len(id) - 1; i >= 0; i-- {
		if id[i] == ':' {
			return id[:i]
		}
	}

	return id
}
