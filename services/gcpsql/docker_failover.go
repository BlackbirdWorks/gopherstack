package gcpsql

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const catchUpPoll = 200 * time.Millisecond

// Quiesce implements Failoverable.
func (e *dockerEngine) Quiesce(ctx context.Context) (string, error) {
	for _, stmt := range []string{
		"ALTER SYSTEM SET default_transaction_read_only = on",
		stmtReloadConf,
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity " +
			"WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()",
	} {
		if _, err := e.psql(ctx, stmt); err != nil {
			return "", err
		}
	}

	pos, err := e.psql(ctx, "SELECT pg_current_wal_lsn()")

	return strings.TrimSpace(pos), err
}

// Seal implements Failoverable: a clean shutdown streams all remaining WAL, including the shutdown checkpoint, to
// every standby, so none can end up ahead of the one that is promoted.
func (e *dockerEngine) Seal(ctx context.Context) error { return e.Stop(ctx) }

// Settle implements Failoverable.
func (e *dockerEngine) Settle(ctx context.Context) error {
	tick := time.NewTicker(catchUpPoll)
	defer tick.Stop()

	for {
		out, err := e.psql(ctx, "SELECT NOT EXISTS (SELECT 1 FROM pg_stat_wal_receiver WHERE status = 'streaming') "+
			"AND pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn()")
		if err == nil && strings.TrimSpace(out) == "t" {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("replica did not settle: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

// Unquiesce implements Failoverable.
func (e *dockerEngine) Unquiesce(ctx context.Context) error {
	if err := e.Start(ctx); err != nil {
		return err
	}

	for _, stmt := range []string{"ALTER SYSTEM RESET default_transaction_read_only", stmtReloadConf} {
		if _, err := e.psql(ctx, stmt); err != nil {
			return err
		}
	}

	return nil
}

// WaitCaughtUp implements Failoverable.
func (e *dockerEngine) WaitCaughtUp(ctx context.Context, pos string) error {
	lit, err := e.quoteLiteralSimple(pos)
	if err != nil {
		return err
	}

	tick := time.NewTicker(catchUpPoll)
	defer tick.Stop()

	for {
		out, qerr := e.psql(ctx, "SELECT pg_last_wal_replay_lsn() >= "+lit+"::pg_lsn")
		if qerr == nil && strings.TrimSpace(out) == "t" {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("replica did not reach %s: %w", pos, ctx.Err())
		case <-tick.C:
		}
	}
}

// quoteLiteralSimple quotes an LSN, which is hex digits and one slash.
func (e *dockerEngine) quoteLiteralSimple(lsn string) (string, error) {
	for _, r := range lsn {
		if (r < '0' || r > '9') && (r < 'A' || r > 'F') && (r < 'a' || r > 'f') && r != '/' {
			return "", errInvalid("position %q is not a WAL location", lsn)
		}
	}

	return "'" + lsn + "'", nil
}
