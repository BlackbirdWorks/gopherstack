package rds

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

var errReplicaNotStreaming = errors.New("replication is not running")

// ReplicaStatus is a read replica's replication health as reported by its engine.
type ReplicaStatus struct {
	Message     string
	LagSeconds  float64
	Replicating bool
}

func (e *dbEngine) mysqlExec(ctx context.Context, in unitInfo, stmt string) error {
	x, ok := e.execer()
	if !ok {
		return container.ErrExecUnsupported
	}

	client, _ := mysqlClients(in.login.Kind)
	_, err := e.run(ctx, x, in.containerID, []string{
		cmdEnv, "MYSQL_PWD=" + in.login.Password, client, "-u" + mysqlRootUser, "-e", stmt,
	})

	return err
}

func mysqlResetStmt(lb *liveDB) string {
	if lb.kind == engineMariaDB || !strings.HasSuffix(engineImage(lb.kind, lb.version), ":8.4") {
		return "RESET MASTER"
	}

	return "RESET BINARY LOGS AND GTIDS"
}

func mysqlChangeSourceStmt(kind string, plan replPlan) string {
	if kind == engineMariaDB {
		return fmt.Sprintf("CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='%s', "+
			"MASTER_PASSWORD='%s', MASTER_USE_GTID=slave_pos; START REPLICA",
			plan.srcHost, mysqlContainerPort, replUser, plan.srcPass)
	}

	return fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=%d, SOURCE_USER='%s', "+
		"SOURCE_PASSWORD='%s', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1; START REPLICA",
		plan.srcHost, mysqlContainerPort, replUser, plan.srcPass)
}

// seedReplica finishes a MySQL/MariaDB replica: it loads a consistent dump of the source and starts replication
// from the dump's position. Postgres replicas arrive complete from pg_basebackup.
func (b *InMemoryBackend) seedReplica(ctx context.Context, lb *liveDB, seed engineSeed, in unitInfo) error {
	if lb.kind == enginePostgres {
		return nil
	}

	b.mu.RLock("rdsReplicaSeed")
	plan := lb.repl.plan
	b.mu.RUnlock()

	_, srcIn, err := b.awaitLiveSource(ctx, seed.fromKey)
	if err != nil {
		return err
	}

	dump, err := b.engine.dumpUnitFor(ctx, srcIn, true)
	if err != nil {
		return err
	}

	e := b.engine
	if err = e.mysqlExec(ctx, in, mysqlResetStmt(lb)); err != nil {
		return err
	}

	if err = e.loadDump(ctx, in, dump); err != nil {
		return err
	}

	return e.mysqlExec(ctx, in, mysqlChangeSourceStmt(lb.kind, plan))
}

// guardReplica keeps a MySQL/MariaDB replica read-only; it runs on every start because the setting is not persisted.
func (b *InMemoryBackend) guardReplica(ctx context.Context, lb *liveDB) error {
	b.mu.RLock("rdsReplicaGuard")
	isReplica := lb.repl != nil && !lb.repl.promoting && lb.kind != enginePostgres
	in := b.unitInfoLocked(lb)
	b.mu.RUnlock()

	if !isReplica {
		return nil
	}

	err := b.engine.mysqlExec(ctx, in, replicaReadOnlyStmt(lb, in.login.User))
	if errors.Is(err, container.ErrExecUnsupported) {
		return nil
	}

	return err
}

// replicaReadOnlyStmt makes the replica reject writes. MariaDB has no super_read_only, so the master user also
// loses the privilege that bypasses read_only (SUPER before 10.11, READ ONLY ADMIN since).
func replicaReadOnlyStmt(lb *liveDB, user string) string {
	if lb.kind != engineMariaDB {
		return "SET GLOBAL super_read_only=ON"
	}

	stmt := "SET GLOBAL read_only=ON"
	if user == mysqlRootUser {
		return stmt
	}

	priv := "READ ONLY ADMIN"
	if strings.HasSuffix(engineImage(lb.kind, lb.version), ":10.6") {
		priv = "SUPER"
	}

	return stmt + "; REVOKE " + priv + " ON *.* FROM '" + user + "'@'%'"
}

// promoteEngine detaches a replica from its source and makes it writable.
func (e *dbEngine) promoteEngine(ctx context.Context, in unitInfo) error {
	x, ok := e.execer()
	if !ok {
		return container.ErrExecUnsupported
	}

	l := in.login
	if l.Kind == enginePostgres {
		db, err := pgDatabase(l)
		if err != nil {
			return err
		}

		out, err := e.run(ctx, x, in.containerID, []string{
			cmdEnv, "PGPASSWORD=" + l.Password, "psql", "-tA", "-U", l.User, "-d", db,
			"-c", "SELECT pg_promote(true, 60)",
		})
		if err != nil {
			return err
		}

		if strings.TrimSpace(out) != "t" {
			return fmt.Errorf("%w: pg_promote did not complete", errEngineExec)
		}

		return nil
	}

	stmts := "STOP REPLICA; RESET REPLICA ALL; SET GLOBAL read_only=OFF"
	if l.Kind != engineMariaDB {
		stmts += "; SET GLOBAL super_read_only=OFF"
	} else if l.User != mysqlRootUser {
		stmts += "; GRANT ALL PRIVILEGES ON *.* TO '" + l.User + "'@'%' WITH GRANT OPTION"
	}

	return e.mysqlExec(ctx, in, stmts)
}

func queryReplicaStatus(ctx context.Context, l EngineLogin) (ReplicaStatus, error) {
	db, err := openLogin(l)
	if err != nil {
		return ReplicaStatus{}, err
	}

	defer func() { _ = db.Close() }()

	if l.Kind == enginePostgres {
		return pgReplicaStatus(ctx, db)
	}

	return mysqlReplicaStatus(ctx, db)
}

func pgReplicaStatus(ctx context.Context, db *sql.DB) (ReplicaStatus, error) {
	var (
		status string
		lag    float64
	)

	err := db.QueryRowContext(ctx, `SELECT COALESCE((SELECT status FROM pg_stat_wal_receiver LIMIT 1), ''), `+
		`CASE WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0 `+
		`ELSE COALESCE(EXTRACT(EPOCH FROM now() - pg_last_xact_replay_timestamp()), 0) END::float8`).Scan(&status, &lag)
	if err != nil {
		return ReplicaStatus{}, fmt.Errorf("replica status: %w", err)
	}

	if status != "streaming" {
		return ReplicaStatus{Message: "WAL receiver status: " + cmp.Or(status, "not running")}, nil
	}

	return ReplicaStatus{Replicating: true, LagSeconds: lag}, nil
}

func mysqlReplicaStatus(ctx context.Context, db *sql.DB) (ReplicaStatus, error) {
	rows, err := db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		return ReplicaStatus{}, fmt.Errorf("replica status: %w", err)
	}

	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return ReplicaStatus{}, fmt.Errorf("replica status columns: %w", err)
	}

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return ReplicaStatus{}, fmt.Errorf("replica status rows: %w", err)
		}

		return ReplicaStatus{}, errReplicaNotStreaming
	}

	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))

	for i := range vals {
		ptrs[i] = &vals[i]
	}

	if err = rows.Scan(ptrs...); err != nil {
		return ReplicaStatus{}, fmt.Errorf("replica status scan: %w", err)
	}

	row := make(map[string]string, len(cols))
	for i, c := range cols {
		row[strings.ToLower(c)] = vals[i].String
	}

	return interpretMySQLReplica(row), nil
}

func firstOf(row map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := row[k]; v != "" {
			return v
		}
	}

	return ""
}

func interpretMySQLReplica(row map[string]string) ReplicaStatus {
	ioOn := firstOf(row, "replica_io_running", "slave_io_running") == "Yes"
	sqlOn := firstOf(row, "replica_sql_running", "slave_sql_running") == "Yes"

	if !ioOn || !sqlOn {
		msg := firstOf(row, "last_io_error", "last_sql_error")

		return ReplicaStatus{Message: cmp.Or(msg, "replication threads are not running")}
	}

	lag, _ := strconv.ParseFloat(firstOf(row, "seconds_behind_source", "seconds_behind_master"), 64)

	return ReplicaStatus{Replicating: true, LagSeconds: lag}
}
