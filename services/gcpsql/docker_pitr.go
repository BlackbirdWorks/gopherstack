package gcpsql

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	pitrWait       = 90 * time.Second
	pitrReplayWait = 6 * time.Minute
	pitrPoll       = 250 * time.Millisecond
	pgArchiveCmd   = "test ! -f " + pitrMount + "/wal/%f && cp %p " + pitrMount + "/wal/%f"
	pitrBaseTime   = "base-time"
	mysqlPITRDir   = mysqlDataDir + "/gopherstack-pitr"
	mysqlBaseFile  = mysqlPITRDir + "/base.sql"
	mysqlBaseTime  = mysqlPITRDir + "/" + pitrBaseTime
)

var (
	errNoPITR      = errors.New("gcpsql: point-in-time recovery needs persistent volumes from the container runtime")
	errPITRNotOn   = errors.New("gcpsql: point-in-time recovery is not enabled on the source engine")
	binlogCoordsRe = regexp.MustCompile(`SOURCE_LOG_FILE='([^']+)', SOURCE_LOG_POS=(\d+)`)
)

func (e *dockerEngine) readFile(ctx context.Context, path string) (string, error) {
	return e.runOut(ctx, []string{"cat", path})
}

func (e *dockerEngine) writeFile(ctx context.Context, path, content string) error {
	return e.run(ctx, []string{"sh", "-c", `printf %s "$1" > "$0"`, path, content})
}

func (e *dockerEngine) baseTimeFile() string {
	if e.kind == kindPostgres {
		return pitrMount + "/" + pitrBaseTime
	}

	return mysqlBaseTime
}

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
func (e *dockerEngine) EnablePITR(ctx context.Context, dbs []string) (time.Time, error) {
	if e.volData == "" {
		return time.Time{}, errNoPITR
	}

	if t, ok := e.storedBase(ctx); ok {
		if e.kind == kindPostgres {
			if err := e.setArchiving(ctx, true); err != nil {
				return time.Time{}, err
			}
		}

		return t, nil
	}

	var err error

	if e.kind == kindPostgres {
		err = e.pgBase(ctx)
	} else {
		err = e.mysqlBase(ctx, dbs)
	}

	if err != nil {
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

func (e *dockerEngine) mysqlBase(ctx context.Context, dbs []string) error {
	if err := e.run(ctx, []string{"mkdir", "-p", mysqlPITRDir}); err != nil {
		return err
	}

	if len(dbs) == 0 {
		out, err := e.mysqlCmd(ctx, "FLUSH BINARY LOGS; SHOW BINARY LOGS")
		if err != nil {
			return err
		}

		lines := strings.Split(strings.TrimSpace(out), "\n")
		file := strings.Fields(lines[len(lines)-1])[0]

		return e.writeFile(ctx, mysqlBaseFile, "-- CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='"+file+
			"', SOURCE_LOG_POS=4;\n")
	}

	cmd := append([]string{
		cmdEnv, rootUserKey + e.pass(), mysqldumpBin, "-u" + mysqlAdminUser, argSingleTx, argRoutines,
		"--events", "--triggers", "--skip-dump-date", argAddDrop, "--source-data=2", "--flush-logs",
		"--set-gtid-purged=OFF", "-r", mysqlBaseFile, argDatabases,
	}, dbs...)

	return e.run(ctx, cmd)
}

// DisablePITR implements Recoverer.
func (e *dockerEngine) DisablePITR(ctx context.Context) error {
	if e.volData == "" {
		return nil
	}

	if e.kind == kindPostgres {
		if err := e.setArchiving(ctx, false); err != nil {
			return err
		}

		return e.run(
			ctx,
			[]string{"sh", "-c", "rm -rf " + pitrMount + "/base " + pitrMount + "/wal/* " + e.baseTimeFile()},
		)
	}

	return e.run(ctx, []string{"rm", "-rf", mysqlPITRDir})
}

// PrepareRecovery implements Recoverer.
func (e *dockerEngine) PrepareRecovery(ctx context.Context, at time.Time) (Recovery, error) {
	if e.volData == "" {
		return Recovery{}, errNoPITR
	}

	if _, ok := e.storedBase(ctx); !ok {
		return Recovery{}, errPITRNotOn
	}

	if e.kind == kindPostgres {
		return Recovery{At: at, Volume: e.volPITR}, e.archiveNow(ctx)
	}

	return e.mysqlRecovery(ctx, at)
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

// binlogPlan locates the binary log range a clone replays: from the base dump's coordinates up to the first
// transaction at or after at (second resolution), as source file and position pairs.
func (e *dockerEngine) binlogPlan(ctx context.Context, at time.Time) (Recovery, error) {
	base, err := e.readFile(ctx, mysqlBaseFile)
	if err != nil {
		return Recovery{}, err
	}

	m := binlogCoordsRe.FindStringSubmatch(base)
	if m == nil {
		return Recovery{}, fmt.Errorf("%w: base dump has no binary log coordinates", errExec)
	}

	from, _ := strconv.ParseUint(m[2], 10, 64)

	if _, err = e.mysqlCmd(ctx, "FLUSH BINARY LOGS"); err != nil {
		return Recovery{}, err
	}

	out, err := e.mysqlCmd(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return Recovery{}, err
	}

	rec := Recovery{At: at, Script: []byte(base), FromFile: m[1], FromPos: from}
	stop := uint32(at.UTC().Truncate(time.Second).Add(time.Second).Unix()) //nolint:gosec // bounded unix seconds

	for ln := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		f := strings.Fields(ln)
		if len(f) == 0 || f[0] < m[1] {
			continue
		}

		data, rerr := e.copyOut(ctx, mysqlDataDir+"/"+f[0])
		if rerr != nil {
			return Recovery{}, rerr
		}

		lo := uint64(binlogHeaderLen)
		if f[0] == m[1] {
			lo = from
		}

		pos, found := binlogCutoff(data, lo, stop)
		rec.UntilFile, rec.UntilPos = f[0], pos

		if found {
			break
		}
	}

	return rec, nil
}

func (e *dockerEngine) copyOut(ctx context.Context, path string) ([]byte, error) {
	if err := e.run(ctx, []string{
		"sh", "-c", `cp "$0" "$1" && chmod 644 "$1"`, path, dumpMount + "/binlog.copy",
	}); err != nil {
		return nil, err
	}

	return e.readDump("binlog.copy")
}

const (
	binlogHeaderLen = 4
	eventHeaderLen  = 19
	eventGTID       = 33
	eventAnonGTID   = 34
	eventSizeOffset = 9
	eventTypeOffset = 4
)

// binlogCutoff walks a binary log from lo and returns the start of the first transaction stamped at or after stop;
// without one it returns the end of the data.
func binlogCutoff(data []byte, lo uint64, stop uint32) (uint64, bool) {
	pos := lo

	for pos+eventHeaderLen <= uint64(len(data)) {
		hdr := data[pos : pos+eventHeaderLen]
		ts := binary.LittleEndian.Uint32(hdr[0:4])
		size := uint64(binary.LittleEndian.Uint32(hdr[eventSizeOffset : eventSizeOffset+4]))

		if t := hdr[eventTypeOffset]; (t == eventGTID || t == eventAnonGTID) && ts >= stop {
			return pos, true
		}

		if size < eventHeaderLen {
			break
		}

		pos += size
	}

	return uint64(len(data)), false
}

// PrepareRecovery MySQL half: the clone follows the source's binary log up to the cutoff.
func (e *dockerEngine) mysqlRecovery(ctx context.Context, at time.Time) (Recovery, error) {
	rec, err := e.binlogPlan(ctx, at)
	if err != nil {
		return Recovery{}, err
	}

	src, err := e.ReplicationSource(ctx)
	if err != nil {
		return Recovery{}, err
	}

	rec.Source = &src

	return rec, nil
}

// ApplyRecovery implements Recoverer.
func (e *dockerEngine) ApplyRecovery(ctx context.Context, rec Recovery) error {
	if e.kind == kindPostgres || rec.Source == nil {
		return nil
	}

	if len(rec.Script) > 0 {
		if err := e.restoreFile(ctx, "", rec.Script); err != nil {
			return err
		}
	}

	stmt := fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=%d, SOURCE_USER='%s', "+
		"SOURCE_PASSWORD='%s', SOURCE_AUTO_POSITION=0, SOURCE_LOG_FILE='%s', SOURCE_LOG_POS=%d, "+
		"GET_SOURCE_PUBLIC_KEY=1; START REPLICA UNTIL SOURCE_LOG_FILE='%s', SOURCE_LOG_POS=%d",
		rec.Source.Host, rec.Source.Port, replUser, rec.Source.Password, rec.FromFile, rec.FromPos,
		rec.UntilFile, rec.UntilPos)

	if _, err := e.mysqlCmd(ctx, stmt); err != nil {
		return err
	}

	if err := e.waitReplayed(ctx); err != nil {
		return err
	}

	_, err := e.mysqlCmd(ctx, "STOP REPLICA; RESET REPLICA ALL")

	return err
}

// waitReplayed blocks until the SQL thread has reached the UNTIL position and stopped.
func (e *dockerEngine) waitReplayed(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, pitrReplayWait)
	defer cancel()

	tick := time.NewTicker(pitrPoll)
	defer tick.Stop()

	for {
		rows, err := e.Query(ctx, "", "SHOW REPLICA STATUS")
		if err == nil && len(rows) == 2 {
			row := map[string]string{}
			for i, c := range rows[0] {
				row[strings.ToLower(c)] = rows[1][i]
			}

			if msg := row["last_sql_error"]; msg != "" {
				return fmt.Errorf("%w: binary log replay failed: %s", errExec, msg)
			}

			if row["replica_sql_running"] == "No" {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: binary log replay did not finish", errExec)
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
