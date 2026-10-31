package mysqlengine

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
	pitrReplayWait = 6 * time.Minute
	pitrPoll       = 250 * time.Millisecond
	pitrDir        = dataDir + "/gopherstack-pitr"
	baseFile       = pitrDir + "/base.sql"
	baseTimeFile   = pitrDir + "/base-time"
)

var (
	// ErrNoPITR is returned when point-in-time recovery has no persistent volume to keep its base in.
	ErrNoPITR = errors.New("mysqlengine: point-in-time recovery needs a persistent volume")
	// ErrPITRNotOn is returned when recovery is prepared on a server that never enabled it.
	ErrPITRNotOn = errors.New("mysqlengine: point-in-time recovery is not enabled on the source engine")

	binlogCoordsRe = regexp.MustCompile(`SOURCE_LOG_FILE='([^']+)', SOURCE_LOG_POS=(\d+)`)
)

// Recovery describes a binary log replay: a clone loads Script, then follows Source from FromFile/FromPos up to
// UntilFile/UntilPos.
type Recovery struct {
	At        time.Time
	Source    *ReplSource
	FromFile  string
	UntilFile string
	Script    []byte
	FromPos   uint64
	UntilPos  uint64
}

func (s *Server) storedBase(ctx context.Context) (time.Time, bool) {
	out, err := s.ReadFile(ctx, baseTimeFile)
	if err != nil {
		return time.Time{}, false
	}

	t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(out))

	return t, err == nil
}

// EnablePITR takes a base dump of dbs (or just the log coordinates) and returns the earliest recoverable time.
func (s *Server) EnablePITR(ctx context.Context, dbs []string) (time.Time, error) {
	if s.volume == "" {
		return time.Time{}, ErrNoPITR
	}

	if t, ok := s.storedBase(ctx); ok {
		return t, nil
	}

	if err := s.writeBase(ctx, dbs); err != nil {
		return time.Time{}, err
	}

	now := time.Now().UTC()

	return now, s.WriteFile(ctx, baseTimeFile, now.Format(time.RFC3339Nano))
}

func (s *Server) writeBase(ctx context.Context, dbs []string) error {
	if _, err := s.Run(ctx, []string{"mkdir", "-p", pitrDir}); err != nil {
		return err
	}

	if len(dbs) == 0 {
		out, err := s.Statement(ctx, "FLUSH BINARY LOGS; SHOW BINARY LOGS")
		if err != nil {
			return err
		}

		lines := strings.Split(strings.TrimSpace(out), "\n")
		file := strings.Fields(lines[len(lines)-1])[0]

		return s.WriteFile(ctx, baseFile, "-- CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='"+file+
			"', SOURCE_LOG_POS=4;\n")
	}

	cmd := append([]string{
		mysqldumpBin, "-u" + RootUser, argSingleTx, argRoutines,
		"--events", "--triggers", "--skip-dump-date", argAddDrop, "--source-data=2", "--flush-logs",
		"--set-gtid-purged=OFF", "-r", baseFile, argDatabases,
	}, dbs...)

	_, err := s.RunAuth(ctx, cmd)

	return err
}

// DisablePITR discards the base so no recovery can target this server.
func (s *Server) DisablePITR(ctx context.Context) error {
	if s.volume == "" {
		return nil
	}

	_, err := s.Run(ctx, []string{"rm", "-rf", pitrDir})

	return err
}

// PrepareRecovery locates the binary log range that reaches at, and readies the server as a replication source.
func (s *Server) PrepareRecovery(ctx context.Context, at time.Time) (Recovery, error) {
	if s.volume == "" {
		return Recovery{}, ErrNoPITR
	}

	if _, ok := s.storedBase(ctx); !ok {
		return Recovery{}, ErrPITRNotOn
	}

	rec, err := s.binlogPlan(ctx, at)
	if err != nil {
		return Recovery{}, err
	}

	src, err := s.ReplicationSource(ctx)
	if err != nil {
		return Recovery{}, err
	}

	rec.Source = &src

	return rec, nil
}

// binlogPlan locates the binary log range a clone replays: from the base dump's coordinates up to the first
// transaction at or after at (second resolution).
func (s *Server) binlogPlan(ctx context.Context, at time.Time) (Recovery, error) {
	base, err := s.ReadFile(ctx, baseFile)
	if err != nil {
		return Recovery{}, err
	}

	m := binlogCoordsRe.FindStringSubmatch(base)
	if m == nil {
		return Recovery{}, fmt.Errorf("%w: base dump has no binary log coordinates", errExec)
	}

	from, _ := strconv.ParseUint(m[2], 10, 64)

	if _, err = s.Statement(ctx, "FLUSH BINARY LOGS"); err != nil {
		return Recovery{}, err
	}

	out, err := s.Statement(ctx, "SHOW BINARY LOGS")
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

		data, rerr := s.CopyOut(ctx, dataDir+"/"+f[0])
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

// ApplyRecovery replays rec on a freshly launched server and detaches it from the source.
func (s *Server) ApplyRecovery(ctx context.Context, rec Recovery) error {
	if rec.Source == nil {
		return nil
	}

	if len(rec.Script) > 0 {
		if err := s.RestoreInto(ctx, "", rec.Script); err != nil {
			return err
		}
	}

	stmt := fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=%d, SOURCE_USER='%s', "+
		"SOURCE_PASSWORD='%s', SOURCE_AUTO_POSITION=0, SOURCE_LOG_FILE='%s', SOURCE_LOG_POS=%d, "+
		"GET_SOURCE_PUBLIC_KEY=1; START REPLICA UNTIL SOURCE_LOG_FILE='%s', SOURCE_LOG_POS=%d",
		rec.Source.Host, rec.Source.Port, s.e.cfg.ReplUser, rec.Source.Password, rec.FromFile, rec.FromPos,
		rec.UntilFile, rec.UntilPos)

	if _, err := s.Statement(ctx, stmt); err != nil {
		return err
	}

	if err := s.waitReplayed(ctx); err != nil {
		return err
	}

	_, err := s.Statement(ctx, "STOP REPLICA; RESET REPLICA ALL")

	return err
}

// waitReplayed blocks until the SQL thread has reached the UNTIL position and stopped.
func (s *Server) waitReplayed(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, pitrReplayWait)
	defer cancel()

	tick := time.NewTicker(pitrPoll)
	defer tick.Stop()

	for {
		rows, err := s.Query(ctx, "", "SHOW REPLICA STATUS")
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
