package gcpsql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/dbrepl"
	"github.com/blackbirdworks/gopherstack/pkgs/mysqlengine"
)

// mysqlEngine adapts a pkgs/mysqlengine server to Engine, Replicator, Recoverer, Purger and VolumeRemover.
type mysqlEngine struct {
	srv *mysqlengine.Server
}

func mapMySQLErr(err error) error {
	switch {
	case errors.Is(err, mysqlengine.ErrNoReplication):
		return errNoReplication
	case errors.Is(err, mysqlengine.ErrNoPITR):
		return errNoPITR
	case errors.Is(err, mysqlengine.ErrPITRNotOn):
		return errPITRNotOn
	case errors.Is(err, mysqlengine.ErrTooLarge):
		return errTooLarge
	}

	return err
}

func mysqlSpec(spec LaunchSpec, image string) mysqlengine.Spec {
	ms := mysqlengine.Spec{
		ID:           spec.ID,
		Image:        image,
		RootPassword: spec.Password,
		Persist:      spec.Persist,
		Fresh:        spec.Fresh,
		Volume:       volumeName("data", spec.Project, spec.ID),
		VolumeLabels: map[string]string{volumeLabel: "true", volumeLabel + ".instance": spec.Project + "/" + spec.ID},
		Resource:     spec.Project + "/" + spec.ID,
		Adopt:        spec.Replica == nil && spec.Recovery == nil && !spec.Fresh,
		ServerID:     max(spec.ServerID, 1),
		MemoryBytes:  spec.MemoryBytes,
		NanoCPUs:     spec.NanoCPUs,
	}

	switch {
	case spec.Replica != nil:
		ms.Network, ms.NetworkID = spec.Replica.Network, spec.Replica.NetworkID
	case spec.Recovery != nil && spec.Recovery.Source != nil:
		ms.Network, ms.NetworkID = spec.Recovery.Source.Network, spec.Recovery.Source.NetworkID
	}

	return ms
}

func (e *mysqlEngine) Endpoint() (string, int)         { return e.srv.Endpoint() }
func (e *mysqlEngine) Stop(ctx context.Context) error  { return e.srv.Stop(ctx) }
func (e *mysqlEngine) Start(ctx context.Context) error { return e.srv.Start(ctx) }
func (e *mysqlEngine) Close(ctx context.Context)       { e.srv.Close(ctx, false) }
func (e *mysqlEngine) Persistent() bool                { return e.srv.Persistent() }
func (e *mysqlEngine) Purge(ctx context.Context)       { e.srv.Close(ctx, true) }
func (e *mysqlEngine) exec(ctx context.Context, stmt string, args ...any) error {
	conn, err := e.srv.DB("")
	if err != nil {
		return err //nolint:wrapcheck // already wrapped by the engine
	}
	defer conn.Close()

	if _, err = conn.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("sql: %w", err)
	}

	return nil
}

// RemoveVolumes implements VolumeRemover; the engine must be closed first.
func (e *mysqlEngine) RemoveVolumes(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), teardownLimit)
	defer cancel()

	_ = e.srv.RemoveVolume(ctx)
}

// CreateDatabase implements Engine.
func (e *mysqlEngine) CreateDatabase(ctx context.Context, name, charset, collation string) error {
	conn, err := e.srv.DB("")
	if err != nil {
		return err //nolint:wrapcheck // already wrapped by the engine
	}
	defer conn.Close()

	ident, err := mysqlQuoteIdent(ctx, conn, name)
	if err != nil {
		return err
	}

	stmt, args := "CREATE DATABASE "+ident, []any{}

	if charset != "" && charsetRe.MatchString(charset) {
		stmt += " CHARACTER SET ?"

		args = append(args, charset)
	}

	if collation != "" && charsetRe.MatchString(collation) {
		stmt += " COLLATE ?"

		args = append(args, collation)
	}

	return e.exec(ctx, stmt, args...)
}

// DropDatabase implements Engine.
func (e *mysqlEngine) DropDatabase(ctx context.Context, name string) error {
	conn, err := e.srv.DB("")
	if err != nil {
		return err //nolint:wrapcheck // already wrapped by the engine
	}
	defer conn.Close()

	ident, err := mysqlQuoteIdent(ctx, conn, name)
	if err != nil {
		return err
	}

	return e.exec(ctx, "DROP DATABASE "+ident)
}

// ListDatabases implements Engine.
func (e *mysqlEngine) ListDatabases(ctx context.Context) ([]string, error) {
	rows, err := e.Query(ctx, "", "SHOW DATABASES")
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(rows))

	for _, r := range rows[1:] {
		out = append(out, r[0])
	}

	return out, nil
}

// UpsertUser implements Engine.
func (e *mysqlEngine) UpsertUser(ctx context.Context, u UserSpec) error {
	conn, err := e.srv.DB("")
	if err != nil {
		return err //nolint:wrapcheck // already wrapped by the engine
	}
	defer conn.Close()

	if _, err = conn.ExecContext(ctx, "CREATE USER IF NOT EXISTS ?@?", u.Name, u.Host); err != nil {
		return fmt.Errorf("sql: %w", err)
	}

	if u.Password != "" {
		if _, err = conn.ExecContext(ctx, "ALTER USER ?@? IDENTIFIED BY ?", u.Name, u.Host, u.Password); err != nil {
			return fmt.Errorf("sql: %w", err)
		}
	}

	if _, err = conn.ExecContext(ctx, "GRANT ALL PRIVILEGES ON *.* TO ?@? WITH GRANT OPTION",
		u.Name, u.Host); err != nil {
		return fmt.Errorf("sql: %w", err)
	}

	if u.Password != "" && u.Name == mysqlAdminUser {
		e.srv.SetRootPassword(u.Password)
	}

	return nil
}

// DropUser implements Engine.
func (e *mysqlEngine) DropUser(ctx context.Context, name, host string) error {
	return e.exec(ctx, "DROP USER ?@?", name, host)
}

// Restart implements Engine.
func (e *mysqlEngine) Restart(ctx context.Context, flags []Flag) error {
	_ = e.exec(ctx, "RESET PERSIST")

	for _, f := range flags {
		if !flagNameRe.MatchString(f.Name) {
			return errInvalid("flag name %q is invalid", f.Name)
		}

		if err := e.setFlag(ctx, f); err != nil {
			return err
		}
	}

	if err := e.Stop(ctx); err != nil {
		return err
	}

	return e.Start(ctx)
}

// setFlag persists a server variable; numbers and booleans must not be quoted.
func (e *mysqlEngine) setFlag(ctx context.Context, f Flag) error {
	conn, err := e.srv.DB("")
	if err != nil {
		return err //nolint:wrapcheck // already wrapped by the engine
	}
	defer conn.Close()

	name, err := mysqlQuoteDotted(ctx, conn, f.Name)
	if err != nil {
		return err
	}

	return e.exec(ctx, "SET PERSIST_ONLY "+name+" = ?", mysqlFlagArg(f.Value))
}

// Dump implements Engine.
func (e *mysqlEngine) Dump(ctx context.Context, dbs []string, _ DumpOptions) ([]byte, error) {
	b, err := e.srv.DumpDatabases(ctx, dbs)

	return b, mapMySQLErr(err)
}

// Restore implements Engine.
func (e *mysqlEngine) Restore(ctx context.Context, db string, data []byte) error {
	return e.srv.RestoreInto(ctx, db, data) //nolint:wrapcheck // engine errors carry the command
}

// Query implements Engine.
func (e *mysqlEngine) Query(ctx context.Context, db, query string) ([][]string, error) {
	rows, err := e.srv.QueryMax(ctx, db, query, maxQueryCells)

	return rows, mapMySQLErr(err)
}

// InsertRows implements Engine.
func (e *mysqlEngine) InsertRows(ctx context.Context, db, table string, columns []string, rows [][]string) error {
	conn, err := e.srv.DB(db)
	if err != nil {
		return err //nolint:wrapcheck // already wrapped by the engine
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sql: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := mysqlInsertStmt(ctx, conn, table, columns, len(rows[0]))
	if err != nil {
		return err
	}

	for _, r := range rows {
		args := make([]any, len(r))
		for i, v := range r {
			args[i] = v
		}

		if _, err = tx.ExecContext(ctx, stmt, args...); err != nil {
			return fmt.Errorf("sql: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("sql: %w", err)
	}

	return nil
}

func mysqlInsertStmt(ctx context.Context, q rowQuerier, table string, columns []string, width int) (string, error) {
	var cols, marks []string

	for i := range width {
		if i < len(columns) {
			col, err := mysqlQuoteIdent(ctx, q, columns[i])
			if err != nil {
				return "", err
			}

			cols = append(cols, col)
		}

		marks = append(marks, "?")
	}

	colList := ""
	if len(cols) > 0 {
		colList = " (" + strings.Join(cols, ", ") + ")"
	}

	tbl, err := mysqlQuoteDotted(ctx, q, table)
	if err != nil {
		return "", err
	}

	return "INSERT INTO " + tbl + colList + " VALUES (" + strings.Join(marks, ", ") + ")", nil
}

// ReplicationSource implements Replicator.
func (e *mysqlEngine) ReplicationSource(ctx context.Context) (ReplicaSource, error) {
	src, err := e.srv.ReplicationSource(ctx)

	return ReplicaSource(src), mapMySQLErr(err)
}

// AttachReplica implements Replicator.
func (e *mysqlEngine) AttachReplica(ctx context.Context, src ReplicaSource, seed []byte) error {
	return e.srv.AttachReplica(ctx, mysqlengine.ReplSource(src), seed) //nolint:wrapcheck // carries the command
}

// ReplicaSeed implements Replicator.
func (e *mysqlEngine) ReplicaSeed(ctx context.Context, dbs []string) ([]byte, error) {
	return e.srv.ReplicaSeed(ctx, dbs) //nolint:wrapcheck // carries the command
}

// Promote implements Replicator.
func (e *mysqlEngine) Promote(ctx context.Context) error {
	return e.srv.Promote(ctx) //nolint:wrapcheck // carries the command
}

// SetReplication implements Replicator.
func (e *mysqlEngine) SetReplication(ctx context.Context, on bool) error {
	return e.srv.SetReplication(ctx, on) //nolint:wrapcheck // carries the command
}

// ReplicationStatus implements Replicator.
func (e *mysqlEngine) ReplicationStatus(ctx context.Context) (dbrepl.Status, error) {
	return e.srv.ReplicationStatus(ctx) //nolint:wrapcheck // carries the command
}

// EnablePITR implements Recoverer.
func (e *mysqlEngine) EnablePITR(ctx context.Context, dbs []string) (time.Time, error) {
	t, err := e.srv.EnablePITR(ctx, dbs)

	return t, mapMySQLErr(err)
}

// DisablePITR implements Recoverer.
func (e *mysqlEngine) DisablePITR(ctx context.Context) error {
	return e.srv.DisablePITR(ctx) //nolint:wrapcheck // carries the command
}

// PrepareRecovery implements Recoverer.
func (e *mysqlEngine) PrepareRecovery(ctx context.Context, at time.Time) (Recovery, error) {
	r, err := e.srv.PrepareRecovery(ctx, at)
	if err != nil {
		return Recovery{}, mapMySQLErr(err)
	}

	src := ReplicaSource(*r.Source)

	return Recovery{
		At: r.At, Source: &src, FromFile: r.FromFile, UntilFile: r.UntilFile, Script: r.Script,
		FromPos: r.FromPos, UntilPos: r.UntilPos,
	}, nil
}

// ApplyRecovery implements Recoverer.
func (e *mysqlEngine) ApplyRecovery(ctx context.Context, rec Recovery) error {
	if rec.Source == nil {
		return nil
	}

	src := mysqlengine.ReplSource(*rec.Source)

	return e.srv.ApplyRecovery(ctx, mysqlengine.Recovery{ //nolint:wrapcheck // carries the command
		At: rec.At, Source: &src, FromFile: rec.FromFile, UntilFile: rec.UntilFile, Script: rec.Script,
		FromPos: rec.FromPos, UntilPos: rec.UntilPos,
	})
}

// mysqlQuoteIdent has the MySQL server quote s, so no client-built string reaches the DDL statement.
func mysqlQuoteIdent(ctx context.Context, q rowQuerier, s string) (string, error) {
	if !identRe.MatchString(s) {
		return "", errInvalid("identifier %q is invalid", s)
	}

	var out string
	if err := q.QueryRowContext(ctx, "SELECT CONCAT('`', REPLACE(?, '`', '``'), '`')", s).Scan(&out); err != nil {
		return "", fmt.Errorf("sql: %w", err)
	}

	return out, nil
}

// mysqlQuoteDotted quotes each dot-separated part of s and rejoins them.
func mysqlQuoteDotted(ctx context.Context, q rowQuerier, s string) (string, error) {
	parts := strings.Split(s, ".")

	for i, p := range parts {
		qp, err := mysqlQuoteIdent(ctx, q, p)
		if err != nil {
			return "", err
		}

		parts[i] = qp
	}

	return strings.Join(parts, "."), nil
}

// mysqlFlagArg types numeric and boolean values so the driver interpolates them unquoted.
func mysqlFlagArg(v string) any {
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n
	}

	if f, err := strconv.ParseFloat(v, 64); err == nil && !math.IsInf(f, 0) && !math.IsNaN(f) &&
		!strings.ContainsAny(v, "eExXpP_") {
		return f
	}

	switch strings.ToUpper(v) {
	case "TRUE":
		return true
	case "FALSE":
		return false
	}

	return v
}
