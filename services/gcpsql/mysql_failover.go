package gcpsql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Quiesce implements Failoverable.
func (e *mysqlEngine) Quiesce(ctx context.Context) (string, error) {
	db, err := e.srv.DB("")
	if err != nil {
		return "", err //nolint:wrapcheck // already wrapped by the engine
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		return "", fmt.Errorf("sql: %w", err)
	}
	defer conn.Close()

	if _, err = conn.ExecContext(ctx, "SET GLOBAL super_read_only = ON"); err != nil {
		return "", fmt.Errorf("sql: %w", err)
	}

	if err = killClients(ctx, conn); err != nil {
		return "", err
	}

	var gtid string
	if err = conn.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&gtid); err != nil {
		return "", fmt.Errorf("sql: %w", err)
	}

	return strings.Join(strings.Fields(gtid), ""), nil
}

func clientSessions(ctx context.Context, conn *sql.Conn) ([]int64, error) {
	rows, err := conn.QueryContext(ctx, "SELECT ID FROM information_schema.processlist "+
		"WHERE ID <> CONNECTION_ID() AND USER NOT IN ('system user', 'event_scheduler') "+
		"AND COMMAND NOT LIKE 'Binlog Dump%'")
	if err != nil {
		return nil, fmt.Errorf("sql: %w", err)
	}
	defer rows.Close()

	var ids []int64

	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("sql: %w", err)
		}

		ids = append(ids, id)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("sql: %w", err)
	}

	return ids, nil
}

func killClients(ctx context.Context, conn *sql.Conn) error {
	ids, err := clientSessions(ctx, conn)
	if err != nil {
		return err
	}

	for _, id := range ids {
		_, _ = conn.ExecContext(ctx, fmt.Sprintf("KILL %d", id))
	}

	return nil
}

// Seal implements Failoverable; GTID auto-positioning lets every replica resume from the promoted server.
func (e *mysqlEngine) Seal(context.Context) error { return nil }

// Settle implements Failoverable.
func (e *mysqlEngine) Settle(context.Context) error { return nil }

// Unquiesce implements Failoverable.
func (e *mysqlEngine) Unquiesce(ctx context.Context) error {
	if err := e.exec(ctx, "SET GLOBAL super_read_only = OFF"); err != nil {
		return err
	}

	return e.exec(ctx, "SET GLOBAL read_only = OFF")
}

// WaitCaughtUp implements Failoverable.
func (e *mysqlEngine) WaitCaughtUp(ctx context.Context, pos string) error {
	if pos == "" {
		return nil
	}

	db, err := e.srv.DB("")
	if err != nil {
		return err //nolint:wrapcheck // already wrapped by the engine
	}
	defer db.Close()

	var rc int
	if err = db.QueryRowContext(ctx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 120)", pos).Scan(&rc); err != nil {
		return fmt.Errorf("sql: %w", err)
	}

	if rc != 0 {
		return fmt.Errorf("%w: replica did not apply %s", errExec, pos)
	}

	return nil
}
