package rdsdata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	realMaxTransactions  = 100
	realStatementTimeout = 45 * time.Second
	realDialTimeout      = 5 * time.Second
)

// realLogin identifies a database login on a real engine; Password is never logged.
type realLogin struct {
	Kind     string
	Addr     string
	User     string
	Password string
	DBName   string
}

// realResult is the Data API shape of one statement's outcome.
type realResult struct {
	Records   [][]Field
	Columns   []ColumnMetadata
	Generated []Field
	Updated   int64
}

type realTx struct {
	tx     *sql.Tx
	db     *sql.DB
	cancel context.CancelFunc
	kind   string
}

// realEngine runs Data API statements against the real databases of docker-backed clusters.
type realEngine struct {
	resolver   ClusterResolver
	secrets    SecretReader
	open       func(realLogin) (*sql.DB, error)
	baseCtx    context.Context
	baseCancel context.CancelFunc
	mu         *lockmetrics.RWMutex
	txs        map[string]*realTx
	maxTx      int
	closed     bool
}

func newRealEngine(r ClusterResolver, s SecretReader) *realEngine {
	ctx, cancel := context.WithCancel(context.Background())

	return &realEngine{
		resolver: r, secrets: s, open: openRealDB,
		baseCtx: ctx, baseCancel: cancel,
		mu:  lockmetrics.New("rdsdata-real"),
		txs: make(map[string]*realTx), maxTx: realMaxTransactions,
	}
}

func realTxKey(region, id string) string { return region + "/" + id }

func openRealDB(l realLogin) (*sql.DB, error) {
	if l.Kind == kindPostgres {
		name := l.DBName
		if name == "" {
			name = l.User
		}

		u := url.URL{
			Scheme: "postgres", User: url.UserPassword(l.User, l.Password), Host: l.Addr, Path: "/" + name,
			RawQuery: "sslmode=disable&connect_timeout=5&default_query_exec_mode=describe_exec",
		}

		return sql.Open("pgx", u.String())
	}

	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName = l.User, l.Password, "tcp", l.Addr, l.DBName
	cfg.ClientFoundRows, cfg.Timeout = true, realDialTimeout

	conn, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("mysql connector: %w", err)
	}

	return sql.OpenDB(conn), nil
}

// loginFor resolves the cluster and its secret; ok is false for clusters without a real engine.
func (e *realEngine) loginFor(ctx context.Context, resourceARN string) (realLogin, bool, error) {
	target, err := e.resolver.DataAPITarget(resourceARN)
	if errors.Is(err, ErrNotRealCluster) {
		return realLogin{}, false, nil
	}

	if err != nil {
		return realLogin{}, false, err
	}

	rt := getRequestTarget(ctx)

	raw, err := e.secrets.SecretString(ctx, rt.SecretARN)
	if err != nil {
		return realLogin{}, false, errSecretsError("the secret provided was not found or could not be read")
	}

	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if json.Unmarshal([]byte(raw), &creds) != nil || creds.Username == "" || creds.Password == "" {
		return realLogin{}, false, errInvalidSecret("the secret must be a JSON object with username and password")
	}

	db := target.DBName
	if rt.Database != "" {
		db = rt.Database
	}

	return realLogin{
		Kind: target.Kind, Addr: target.Addr, User: creds.Username, Password: creds.Password, DBName: db,
	}, true, nil
}

func (e *realEngine) hasTx(key string) bool {
	e.mu.RLock("hasTx")
	defer e.mu.RUnlock()

	_, ok := e.txs[key]

	return ok
}

func (e *realEngine) lookupTx(key string) (*realTx, bool) {
	e.mu.RLock("lookupTx")
	defer e.mu.RUnlock()

	t, ok := e.txs[key]

	return t, ok
}

// begin opens a held transaction on the engine's own lifetime, not the request's, so it outlives the call.
func (e *realEngine) begin(lg realLogin, key string) error {
	e.mu.RLock("beginCheck")
	base, full, closed := e.baseCtx, len(e.txs) >= e.maxTx, e.closed
	e.mu.RUnlock()

	if closed {
		return errDatabaseUnavailable
	}

	if full {
		return errTooManyTransactions
	}

	db, err := e.open(lg)
	if err != nil {
		return mapDriverError(err)
	}

	db.SetMaxOpenConns(1)

	txCtx, cancel := context.WithCancel(base)

	tx, err := db.BeginTx(txCtx, nil)
	if err != nil {
		cancel()
		_ = db.Close()

		return mapDriverError(err)
	}

	e.mu.Lock("begin")
	defer e.mu.Unlock()

	if e.closed || len(e.txs) >= e.maxTx {
		_ = tx.Rollback()
		cancel()
		_ = db.Close()

		return errTooManyTransactions
	}

	e.txs[key] = &realTx{tx: tx, db: db, cancel: cancel, kind: lg.Kind}

	return nil
}

// finalize commits or rolls back a held transaction; false means it was not held.
func (e *realEngine) finalize(key string, commit bool) (bool, error) {
	e.mu.Lock("finalize")
	t, ok := e.txs[key]
	delete(e.txs, key)
	e.mu.Unlock()

	if !ok {
		return false, nil
	}

	var err error
	if commit {
		err = t.tx.Commit()
	} else {
		err = t.tx.Rollback()
	}

	t.cancel()
	_ = t.db.Close()

	return true, mapDriverError(err)
}

func (e *realEngine) rollbackAll() {
	e.mu.Lock("rollbackAll")
	txs := e.txs
	e.txs = make(map[string]*realTx)
	e.mu.Unlock()

	for _, t := range txs {
		_ = t.tx.Rollback()
		t.cancel()
		_ = t.db.Close()
	}
}

func (e *realEngine) reset() {
	e.rollbackAll()

	e.mu.Lock("reset")
	defer e.mu.Unlock()

	e.baseCancel()
	e.baseCtx, e.baseCancel = context.WithCancel(context.Background())
}

func (e *realEngine) close() {
	e.mu.Lock("close")
	e.closed = true
	e.mu.Unlock()

	e.rollbackAll()
	e.baseCancel()
}

// withRunner runs fn against the held transaction for txKey, or an autocommit connection when txKey is empty.
func (e *realEngine) withRunner(
	ctx context.Context, lg realLogin, txKey string, fn func(context.Context, string, querier) error,
) error {
	ctx, cancel := context.WithTimeout(ctx, realStatementTimeout)
	defer cancel()

	if txKey != "" {
		t, ok := e.lookupTx(txKey)
		if !ok {
			return errNoEngineTx
		}

		return fn(ctx, t.kind, t.tx)
	}

	db, err := e.open(lg)
	if err != nil {
		return mapDriverError(err)
	}

	defer func() { _ = db.Close() }()

	return fn(ctx, lg.Kind, db)
}

func (e *realEngine) execute(
	ctx context.Context, lg realLogin, txKey, stmt string, params []SQLParameter,
) (realResult, error) {
	var res realResult

	err := e.withRunner(ctx, lg, txKey, func(ctx context.Context, kind string, run querier) error {
		var runErr error

		res, runErr = runRealStatement(ctx, kind, run, stmt, params, getResultSetOptions(ctx))

		return runErr
	})

	return res, err
}

// executeBatch runs one statement per parameter set; outside a transaction the batch is atomic.
func (e *realEngine) executeBatch(
	ctx context.Context, lg realLogin, txKey, stmt string, sets [][]SQLParameter,
) ([]UpdateResult, error) {
	if txKey != "" {
		return e.batchOn(ctx, lg, txKey, stmt, sets)
	}

	key := "batch/" + uuid.NewString()
	if err := e.begin(lg, key); err != nil {
		return nil, err
	}

	results, err := e.batchOn(ctx, lg, key, stmt, sets)
	_, finErr := e.finalize(key, err == nil)

	if err != nil {
		return nil, err
	}

	return results, finErr
}

func (e *realEngine) batchOn(
	ctx context.Context, lg realLogin, txKey, stmt string, sets [][]SQLParameter,
) ([]UpdateResult, error) {
	results := make([]UpdateResult, 0, len(sets))

	err := e.withRunner(ctx, lg, txKey, func(ctx context.Context, kind string, run querier) error {
		for _, params := range sets {
			r, err := runRealStatement(ctx, kind, run, stmt, params, resultSetOptions{})
			if err != nil {
				return err
			}

			results = append(results, UpdateResult{GeneratedFields: r.Generated})
		}

		return nil
	})

	return results, err
}

func realIsQuery(kind, stmt string, returning bool) bool {
	if returning || isQuery(stmt) {
		return true
	}

	fields := strings.Fields(strings.TrimLeft(stmt, " \t\r\n("))
	if len(fields) == 0 {
		return false
	}

	switch strings.ToUpper(fields[0]) {
	case "SHOW", "DESCRIBE", "DESC", "CALL":
		return kind == kindMySQL
	case "TABLE":
		return kind == kindPostgres
	}

	return false
}

func runRealStatement(
	ctx context.Context, kind string, run querier, stmt string, params []SQLParameter, opts resultSetOptions,
) (realResult, error) {
	tr, err := translatePlaceholders(kind, stmt, params)
	if err != nil {
		return realResult{}, err
	}

	if realIsQuery(kind, stmt, tr.Returning) {
		rows, qerr := run.QueryContext(ctx, tr.SQL, tr.Args...)
		if qerr != nil {
			return realResult{}, mapDriverError(qerr)
		}

		defer func() { _ = rows.Close() }()

		return scanRealRows(kind, rows, opts)
	}

	out, xerr := run.ExecContext(ctx, tr.SQL, tr.Args...)
	if xerr != nil {
		return realResult{}, mapDriverError(xerr)
	}

	updated, _ := out.RowsAffected()
	res := realResult{Records: [][]Field{}, Columns: []ColumnMetadata{}, Generated: []Field{}, Updated: updated}

	if id, idErr := out.LastInsertId(); kind == kindMySQL && idErr == nil && id > 0 {
		res.Generated = []Field{{LongValue: &id}}
	}

	return res, nil
}

func scanRealRows(kind string, rows *sql.Rows, opts resultSetOptions) (realResult, error) {
	cts, err := rows.ColumnTypes()
	if err != nil {
		return realResult{}, mapDriverError(err)
	}

	cols := make([]ColumnMetadata, len(cts))
	for i, ct := range cts {
		cols[i] = realColumnMetadata(kind, ct)
	}

	res := realResult{Records: [][]Field{}, Columns: cols, Generated: []Field{}}

	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))

		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err = rows.Scan(ptrs...); err != nil {
			return realResult{}, mapDriverError(err)
		}

		rec := make([]Field, len(cols))

		for i, v := range vals {
			f, ferr := realField(kind, cols[i], v)
			if ferr != nil {
				return realResult{}, ferr
			}

			rec[i] = shapeRealField(f, cols[i], opts)
		}

		res.Records = append(res.Records, rec)
	}

	if err = rows.Err(); err != nil {
		return realResult{}, mapDriverError(err)
	}

	return res, nil
}
