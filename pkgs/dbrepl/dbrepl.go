// Package dbrepl holds the engine-level pieces of Postgres streaming and MySQL binlog replication that the
// container-backed database emulators share.
package dbrepl

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// PasswordEnv is the container environment variable carrying the replication password into PGReplicaScript.
const PasswordEnv = "REPL_KEY"

// ErrNotStreaming is returned when a MySQL replica has no replication channel configured.
var ErrNotStreaming = errors.New("replication is not running")

// Status is a replica's replication health as reported by its engine.
type Status struct {
	Message     string
	LagSeconds  float64
	Replicating bool
}

// PGReplicaScript is a container entrypoint that seeds an empty PGDATA with pg_basebackup from the source
// ($1 host, $2 port) before handing over to the image entrypoint.
func PGReplicaScript(user string) string {
	return `set -e
host="$1"; port="$2"; shift 2
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  gosu postgres pg_basebackup -d "host=$host port=$port user=` + user + ` password=$` + PasswordEnv + `" ` +
		`-D "$PGDATA" -R -X stream -c fast
fi
exec docker-entrypoint.sh "$@"`
}

// PGHBAScript appends a replication rule for user to the source's pg_hba.conf, once.
func PGHBAScript(user string) string {
	return `f="$PGDATA/pg_hba.conf"; grep -q "^host replication ` + user + ` " "$f" || ` +
		`echo "host replication ` + user + ` samenet scram-sha-256" >> "$f"`
}

// PGReplicaRoleSQL creates or resets the replication login on a Postgres source.
func PGReplicaRoleSQL(user, pass string) string {
	return fmt.Sprintf("SET password_encryption='scram-sha-256'; DO $$ BEGIN "+
		"CREATE ROLE %[1]s WITH LOGIN REPLICATION PASSWORD '%[2]s'; "+
		"EXCEPTION WHEN duplicate_object THEN ALTER ROLE %[1]s WITH LOGIN REPLICATION PASSWORD '%[2]s'; END $$",
		user, pass)
}

// MySQLReplicaUserSQL creates or resets the replication login on a MySQL source.
func MySQLReplicaUserSQL(user, pass string) string {
	return fmt.Sprintf("CREATE USER IF NOT EXISTS '%[1]s'@'%%' IDENTIFIED BY '%[2]s'; "+
		"ALTER USER '%[1]s'@'%%' IDENTIFIED BY '%[2]s'; GRANT REPLICATION SLAVE ON *.* TO '%[1]s'@'%%'",
		user, pass)
}

// MySQLChangeSourceSQL points a MySQL 8 replica at host:port with GTID auto-positioning and starts it.
func MySQLChangeSourceSQL(host string, port int, user, pass string) string {
	return fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=%d, SOURCE_USER='%s', "+
		"SOURCE_PASSWORD='%s', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1; START REPLICA",
		host, port, user, pass)
}

// QueryStatus reads the replication health of a Postgres replica (pg) or a MySQL replica.
func QueryStatus(ctx context.Context, db *sql.DB, postgres bool) (Status, error) {
	if postgres {
		return pgStatus(ctx, db)
	}

	return mysqlStatus(ctx, db)
}

func pgStatus(ctx context.Context, db *sql.DB) (Status, error) {
	var (
		status string
		lag    float64
	)

	err := db.QueryRowContext(ctx, `SELECT COALESCE((SELECT status FROM pg_stat_wal_receiver LIMIT 1), ''), `+
		`CASE WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0 `+
		`ELSE COALESCE(EXTRACT(EPOCH FROM now() - pg_last_xact_replay_timestamp()), 0) END::float8`).Scan(&status, &lag)
	if err != nil {
		return Status{}, fmt.Errorf("replica status: %w", err)
	}

	if status != "streaming" {
		return Status{Message: "WAL receiver status: " + cmp.Or(status, "not running")}, nil
	}

	return Status{Replicating: true, LagSeconds: lag}, nil
}

func mysqlStatus(ctx context.Context, db *sql.DB) (Status, error) {
	rows, err := db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		return Status{}, fmt.Errorf("replica status: %w", err)
	}

	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return Status{}, fmt.Errorf("replica status columns: %w", err)
	}

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return Status{}, fmt.Errorf("replica status rows: %w", err)
		}

		return Status{}, ErrNotStreaming
	}

	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))

	for i := range vals {
		ptrs[i] = &vals[i]
	}

	if err = rows.Scan(ptrs...); err != nil {
		return Status{}, fmt.Errorf("replica status scan: %w", err)
	}

	row := make(map[string]string, len(cols))
	for i, c := range cols {
		row[strings.ToLower(c)] = vals[i].String
	}

	return InterpretMySQL(row), nil
}

func firstOf(row map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := row[k]; v != "" {
			return v
		}
	}

	return ""
}

// InterpretMySQL turns a lower-cased SHOW REPLICA STATUS row into a Status.
func InterpretMySQL(row map[string]string) Status {
	ioOn := firstOf(row, "replica_io_running", "slave_io_running") == "Yes"
	sqlOn := firstOf(row, "replica_sql_running", "slave_sql_running") == "Yes"

	if !ioOn || !sqlOn {
		msg := firstOf(row, "last_io_error", "last_sql_error")

		return Status{Message: cmp.Or(msg, "replication threads are not running")}
	}

	lag, _ := strconv.ParseFloat(firstOf(row, "seconds_behind_source", "seconds_behind_master"), 64)

	return Status{Replicating: true, LagSeconds: lag}
}
