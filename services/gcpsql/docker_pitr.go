package gcpsql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	pitrWait       = 90 * time.Second
	pitrReplayWait = 6 * time.Minute
	pitrPoll       = 250 * time.Millisecond
	pgArchiveCmd   = "test ! -f " + pitrMount + "/wal/%f && cp %p " + pitrMount + "/wal/%f"
	pitrBaseTime   = "base-time"
)

var (
	errNoPITR    = errors.New("gcpsql: point-in-time recovery needs persistent volumes from the container runtime")
	errPITRNotOn = errors.New("gcpsql: point-in-time recovery is not enabled on the source engine")
)

func (e *dockerEngine) readFile(ctx context.Context, path string) (string, error) {
	return e.runOut(ctx, []string{"cat", path})
}

func (e *dockerEngine) writeFile(ctx context.Context, path, content string) error {
	return e.run(ctx, []string{"sh", "-c", `printf %s "$1" > "$0"`, path, content})
}

func (e *dockerEngine) baseTimeFile() string { return pitrMount + "/" + pitrBaseTime }

// storedBase returns the time of the base backup a previous run left in the volume.
func (e *dockerEngine) storedBase(ctx context.Context) (time.Time, bool) {
	out, err := e.readFile(ctx, e.baseTimeFile())
	if err != nil {
		return time.Time{}, false
	}

	t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(out))

	return t, err == nil
}

// EnablePITR implements Recoverer.
func (e *dockerEngine) EnablePITR(ctx context.Context, _ []string) (time.Time, error) {
	if e.volData == "" {
		return time.Time{}, errNoPITR
	}

	if t, ok := e.storedBase(ctx); ok {
		if err := e.setArchiving(ctx, true); err != nil {
			return time.Time{}, err
		}

		return t, nil
	}

	if err := e.pgBase(ctx); err != nil {
		return time.Time{}, err
	}

	now := time.Now().UTC()

	return now, e.writeFile(ctx, e.baseTimeFile(), now.Format(time.RFC3339Nano))
}

func (e *dockerEngine) setArchiving(ctx context.Context, on bool) error {
	cur, err := e.psql(ctx, "SHOW archive_mode")
	if err != nil {
		return err
	}

	want := "off"
	if on {
		want = "on"
	}

	if strings.TrimSpace(cur) == want {
		return nil
	}

	for _, stmt := range []string{
		"ALTER SYSTEM SET archive_mode = '" + want + "'",
		"ALTER SYSTEM SET archive_command = '" + pgArchiveCmd + "'",
	} {
		if _, err = e.psql(ctx, stmt); err != nil {
			return err
		}
	}

	if err = e.Stop(ctx); err != nil {
		return err
	}

	return e.Start(ctx)
}

func (e *dockerEngine) pgBase(ctx context.Context) error {
	if err := e.setArchiving(ctx, true); err != nil {
		return err
	}

	return e.run(ctx, []string{
		"sh", "-c", "rm -rf " + pitrMount + "/base && pg_basebackup -U " + pgAdminUser + " -D " + pitrMount +
			"/base -X fetch -c fast",
	})
}

// DisablePITR implements Recoverer.
func (e *dockerEngine) DisablePITR(ctx context.Context) error {
	if e.volData == "" {
		return nil
	}

	if err := e.setArchiving(ctx, false); err != nil {
		return err
	}

	return e.run(
		ctx,
		[]string{"sh", "-c", "rm -rf " + pitrMount + "/base " + pitrMount + "/wal/* " + e.baseTimeFile()},
	)
}

// PrepareRecovery implements Recoverer.
func (e *dockerEngine) PrepareRecovery(ctx context.Context, at time.Time) (Recovery, error) {
	if e.volData == "" {
		return Recovery{}, errNoPITR
	}

	if _, ok := e.storedBase(ctx); !ok {
		return Recovery{}, errPITRNotOn
	}

	return Recovery{At: at, Volume: e.volPITR}, e.archiveNow(ctx)
}

// archiveNow forces a commit record and a WAL switch, then waits until the segment is in the archive.
func (e *dockerEngine) archiveNow(ctx context.Context) error {
	if _, err := e.psql(ctx, "SELECT pg_current_xact_id()"); err != nil {
		return err
	}

	seg, err := e.psql(ctx, "SELECT pg_walfile_name(pg_current_wal_lsn())")
	if err != nil {
		return err
	}

	seg = strings.TrimSpace(seg)

	if _, err = e.psql(ctx, "SELECT pg_switch_wal()"); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, pitrWait)
	defer cancel()

	tick := time.NewTicker(pitrPoll)
	defer tick.Stop()

	for {
		last, qerr := e.psql(ctx, "SELECT COALESCE(last_archived_wal, '') FROM pg_stat_archiver")
		if qerr == nil && strings.TrimSpace(last) >= seg && !strings.HasSuffix(strings.TrimSpace(last), ".history") {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: WAL segment %s was not archived", errExec, seg)
		case <-tick.C:
		}
	}
}

// waitPromoted blocks until a recovery has reached its target and the server accepts writes, then drops the
// recovery settings.
func (e *dockerEngine) waitPromoted(ctx context.Context) error {
	tick := time.NewTicker(pitrPoll)
	defer tick.Stop()

	for {
		out, err := e.psql(ctx, "SELECT pg_is_in_recovery()")
		if err == nil && strings.TrimSpace(out) == "f" {
			break
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("recovery did not finish: %w", ctx.Err())
		case <-tick.C:
		}
	}

	for _, s := range []string{"restore_command", "recovery_target_time", "recovery_target_action"} {
		if _, err := e.psql(ctx, "ALTER SYSTEM RESET "+s); err != nil {
			return err
		}
	}

	_, err := e.psql(ctx, "SELECT pg_reload_conf()")

	return err
}

// ApplyRecovery implements Recoverer; PostgreSQL recovers during startup.
func (*dockerEngine) ApplyRecovery(context.Context, Recovery) error { return nil }
