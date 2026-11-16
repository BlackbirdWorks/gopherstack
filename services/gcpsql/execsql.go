package gcpsql

import (
	"context"
	"net/http"
	"strconv"
	"time"
)

const (
	execTimeout     = 30 * time.Second
	maxExecRequest  = 512 << 10
	maxExecBytes    = 10 << 20
	maxExecMessages = 10
	maxExecParallel = 10
	dataAPIAllow    = "ALLOW_DATA_API"
	partialAllow    = "ALLOW_PARTIAL_RESULT"
	statusInvalid   = 3
)

// ExecuteSQLInput is the request body of instances.executeSql.
type ExecuteSQLInput struct {
	RowLimit              any    `json:"rowLimit"`
	Application           string `json:"application"`
	Database              string `json:"database"`
	PartialResultMode     string `json:"partialResultMode"`
	PasswordSecretVersion string `json:"passwordSecretVersion"`
	SQLStatement          string `json:"sqlStatement"`
	User                  string `json:"user"`
	AutoIamAuthn          bool   `json:"autoIamAuthn"`
}

// SecretReader resolves a Secret Manager version name to its payload.
type SecretReader interface {
	AccessSecret(versionName string) ([]byte, error)
}

// ExecRequest is one batch of statements an engine runs as a single database user.
type ExecRequest struct {
	Database string
	User     string
	Password string
	SQL      string
	RowLimit int64
	MaxBytes int
	Partial  bool
}

// ExecColumn names a result column and its database type.
type ExecColumn struct {
	Name string `json:"name"`
	Type string `json:"type,omitempty"`
}

// ExecValue is one cell; Null marks SQL NULL.
type ExecValue struct {
	Value     string `json:"value,omitempty"`
	NullValue bool   `json:"nullValue,omitempty"`
}

// ExecRow is one result row.
type ExecRow struct {
	Values []ExecValue `json:"values"`
}

// ExecStatus is the google.rpc.Status of a failed execution.
type ExecStatus struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}

// ExecResult is the outcome of one statement.
type ExecResult struct {
	Status        *ExecStatus  `json:"status,omitempty"`
	Message       string       `json:"message,omitempty"`
	Columns       []ExecColumn `json:"columns,omitempty"`
	Rows          []ExecRow    `json:"rows,omitempty"`
	PartialResult bool         `json:"partialResult,omitempty"`
}

// ExecMessage is a database notice or warning.
type ExecMessage struct {
	Message  string `json:"message"`
	Severity string `json:"severity,omitempty"`
}

// ExecOutcome is what an engine reports for a batch; Failure is the database's own error text, in which case no
// result is returned.
type ExecOutcome struct {
	Failure  string
	Results  []ExecResult
	Messages []ExecMessage
}

// SQLExecutor is implemented by engines that can run client SQL for instances.executeSql.
type SQLExecutor interface {
	ExecuteSQL(ctx context.Context, req ExecRequest) (ExecOutcome, error)
}

type execMetadata struct {
	SQLStatementExecutionTime string `json:"sqlStatementExecutionTime"`
}

// ExecuteSQLResponse is the response of instances.executeSql.
type ExecuteSQLResponse struct {
	Metadata *execMetadata `json:"metadata,omitempty"`
	Status   *ExecStatus   `json:"status,omitempty"`
	Results  []ExecResult  `json:"results,omitempty"`
	Messages []ExecMessage `json:"messages,omitempty"`
}

// SetSecretReader wires the Secret Manager emulator that resolves passwordSecretVersion.
func (b *InMemoryBackend) SetSecretReader(r SecretReader) {
	b.mu.Lock("SetSecretReader")
	defer b.mu.Unlock()

	b.secrets = r
}

func rowLimitOf(v any) (int64, error) {
	switch t := v.(type) {
	case nil:
		return 0, nil
	case string:
		n, err := strconv.ParseInt(t, 10, 64)
		if err != nil || n < 0 {
			return 0, errInvalid("rowLimit %q is not a non-negative integer.", t)
		}

		return n, nil
	case float64:
		if t < 0 || t != float64(int64(t)) {
			return 0, errInvalid("rowLimit must be a non-negative integer.")
		}

		return int64(t), nil
	}

	return 0, errInvalid("rowLimit must be a non-negative integer.")
}

func (b *InMemoryBackend) execTarget(project, name string, in *ExecuteSQLInput) (*instRec, SQLExecutor, error) {
	b.mu.RLock("ExecuteSQL")
	defer b.mu.RUnlock()

	rec, err := b.lookupLocked(project, name)
	if err != nil {
		return nil, nil, err
	}

	if settingString(rec.Inst.Settings, "dataApiAccess") != dataAPIAllow {
		return nil, nil, newErr(http.StatusForbidden, "forbidden", "Data API access not allowed on the instance.")
	}

	if rec.Inst.State != stateRunnable {
		return nil, nil, newErr(http.StatusConflict, "operationInProgress", "The database is currently unavailable.")
	}

	if isPG(rec.Inst.DatabaseVersion) && in.Database == "" {
		return nil, nil, errInvalid("database is required for PostgreSQL instances.")
	}

	ex, ok := rec.eng.(SQLExecutor)
	if !ok {
		return nil, nil, errInvalid("The instance has no running database engine to execute SQL on.")
	}

	return rec, ex, nil
}

// execPassword picks the password the statements authenticate with.
func (b *InMemoryBackend) execPassword(rec *instRec, in *ExecuteSQLInput) (string, error) {
	b.mu.RLock("execPassword")
	defer b.mu.RUnlock()

	if in.AutoIamAuthn {
		if in.PasswordSecretVersion != "" {
			return "", errInvalid("A secret can't be provided when autoIamAuthn is true.")
		}

		return "", errInvalid("IAM authentication isn't enabled: the emulator has no IAM database identities.")
	}

	if in.User == "" || isBuiltinAdmin(rec.Inst.DatabaseVersion, in.User) {
		return rec.RootPassword, nil
	}

	if in.PasswordSecretVersion != "" && b.secrets != nil {
		data, err := b.secrets.AccessSecret(in.PasswordSecretVersion)
		if err != nil {
			return "", errInvalid("Cannot read passwordSecretVersion: %v", err)
		}

		return string(data), nil
	}

	for _, u := range rec.Users {
		if u.User.Name == in.User {
			return u.Password, nil
		}
	}

	return "", nil
}

// ExecuteSQL runs the statements of in on the instance engine (instances.executeSql).
func (b *InMemoryBackend) ExecuteSQL(
	ctx context.Context, project, name string, in ExecuteSQLInput,
) (*ExecuteSQLResponse, error) {
	if in.SQLStatement == "" {
		return nil, errInvalid("Missing parameter: sqlStatement.")
	}

	if len(in.SQLStatement) > maxExecRequest {
		return nil, newErr(http.StatusRequestEntityTooLarge, "invalid", "The request is larger than 0.5 MB.")
	}

	limit, err := rowLimitOf(in.RowLimit)
	if err != nil {
		return nil, err
	}

	rec, ex, err := b.execTarget(project, name, &in)
	if err != nil {
		return nil, err
	}

	pass, err := b.execPassword(rec, &in)
	if err != nil {
		return nil, err
	}

	if rec.execs.Add(1) > maxExecParallel {
		rec.execs.Add(-1)

		return nil, newErr(http.StatusTooManyRequests, "rateLimitExceeded",
			"Too many concurrent executeSql requests; try again later.")
	}
	defer rec.execs.Add(-1)

	ctx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()

	start := b.now()
	out, err := ex.ExecuteSQL(ctx, ExecRequest{
		Database: in.Database, User: in.User, Password: pass, SQL: in.SQLStatement, RowLimit: limit,
		MaxBytes: maxExecBytes, Partial: in.PartialResultMode == partialAllow,
	})

	if err != nil {
		return nil, err
	}

	took := strconv.FormatFloat(b.now().Sub(start).Seconds(), 'f', 3, 64) + "s"
	resp := &ExecuteSQLResponse{Metadata: &execMetadata{SQLStatementExecutionTime: took}}

	if out.Failure != "" {
		resp.Status = &ExecStatus{Code: statusInvalid, Message: out.Failure}

		return resp, nil
	}

	resp.Results = out.Results
	resp.Messages = out.Messages[:min(len(out.Messages), maxExecMessages)]

	return resp, nil
}
