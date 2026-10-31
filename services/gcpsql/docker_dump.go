package gcpsql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	stderrLimit  = 512
	connectLine  = `\connect `
	dumpFileMode = 0o600
	psqlBin      = "psql"
	pgDumpBin    = "pg_dump"
)

var errExec = errors.New("command failed")

func (e *dockerEngine) run(ctx context.Context, cmd []string) error {
	_, err := e.runOut(ctx, cmd)

	return err
}

func (e *dockerEngine) runOut(ctx context.Context, cmd []string) (string, error) {
	return e.runOutEnv(ctx, cmd, nil)
}

func (e *dockerEngine) runOutEnv(ctx context.Context, cmd, env []string) (string, error) {
	name := cmdLabel(cmd)

	res, err := container.ExecWithEnv(ctx, e.rt, e.container(), cmd, env)
	if err != nil {
		return "", fmt.Errorf("%w: exec %s: %s", errExec, name, e.scrub(err.Error()))
	}

	if res.ExitCode != 0 {
		msg := e.scrub(strings.TrimSpace(res.Stderr + res.Stdout))
		if len(msg) > stderrLimit {
			msg = msg[:stderrLimit]
		}

		return "", fmt.Errorf("%w: %s exited %d: %s", errExec, name, res.ExitCode, msg)
	}

	return res.Stdout, nil
}

// cmdLabel maps argv to a fixed label so credentials in argv never reach errors or logs.
func cmdLabel(cmd []string) string {
	for _, a := range cmd {
		switch a {
		case psqlBin:
			return psqlBin
		case pgDumpBin:
			return pgDumpBin
		}
	}

	return "command"
}

// scrub redacts the engine's credentials from command output.
func (e *dockerEngine) scrub(msg string) string {
	e.mu.RLock("scrub")
	secrets := []string{e.password, e.replPass}
	e.mu.RUnlock()

	for _, sec := range secrets {
		if sec != "" {
			msg = strings.ReplaceAll(msg, sec, "***")
		}
	}

	return msg
}

func (e *dockerEngine) readDump(name string) ([]byte, error) {
	path := filepath.Join(e.dir, name)
	defer os.Remove(path)

	st, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("dump file: %w", err)
	}

	if st.Size() > maxDumpBytes {
		return nil, fmt.Errorf("%w: dump exceeds %d bytes", errExec, maxDumpBytes)
	}

	return os.ReadFile(path)
}

// Dump implements Engine.
func (e *dockerEngine) Dump(ctx context.Context, dbs []string, opts DumpOptions) ([]byte, error) {
	var out bytes.Buffer

	for i, db := range dbs {
		name := "dump" + strconv.Itoa(i) + ".sql"

		cmd := []string{pgDumpBin, "-U", pgAdminUser, "-d", db, "--clean", "--if-exists", "-f", dumpMount + "/" + name}
		if opts.Portable {
			cmd = append(cmd, "--no-owner", "--no-privileges")
		}

		err := e.run(ctx, cmd)
		if err != nil {
			return nil, err
		}

		data, err := e.readDump(name)
		if err != nil {
			return nil, err
		}

		if opts.Markers {
			out.WriteString(connectLine + `"` + db + "\"\n")
		}

		out.Write(data)

		if out.Len() > maxDumpBytes {
			return nil, fmt.Errorf("%w: dump exceeds %d bytes", errExec, maxDumpBytes)
		}
	}

	return out.Bytes(), nil
}

// Restore implements Engine.
func (e *dockerEngine) Restore(ctx context.Context, db string, data []byte) error {
	for _, seg := range splitDump(db, data) {
		if seg.db != pgAdminUser {
			if err := e.CreateDatabase(ctx, seg.db, "", ""); err != nil &&
				!strings.Contains(err.Error(), "already exists") {
				return err
			}
		}

		if err := e.restoreFile(ctx, seg.db, seg.data); err != nil {
			return err
		}
	}

	return nil
}

func (e *dockerEngine) restoreFile(ctx context.Context, db string, data []byte) error {
	path := filepath.Join(e.dir, "in.sql")
	defer os.Remove(path)

	if err := os.WriteFile(path, data, dumpFileMode); err != nil {
		return fmt.Errorf("write dump: %w", err)
	}

	file := dumpMount + "/in.sql"

	return e.run(ctx, []string{psqlBin, "-U", pgAdminUser, "-d", db, "-v", "ON_ERROR_STOP=1", "-q", "-f", file})
}

type dumpSegment struct {
	db   string
	data []byte
}

// splitDump splits a pg dump on `\connect "db"` lines; text before the first line targets def.
func splitDump(def string, data []byte) []dumpSegment {
	if def == "" {
		def = pgAdminUser
	}

	var (
		segs []dumpSegment
		cur  = dumpSegment{db: def}
		buf  bytes.Buffer
	)

	flush := func() {
		if buf.Len() > 0 {
			cur.data = append([]byte(nil), buf.Bytes()...)
			segs = append(segs, cur)
		}

		buf.Reset()
	}

	for _, line := range bytes.SplitAfter(data, []byte("\n")) {
		if rest, ok := bytes.CutPrefix(line, []byte(connectLine)); ok {
			flush()

			cur = dumpSegment{db: strings.Trim(strings.TrimSpace(string(rest)), `"`)}

			continue
		}

		buf.Write(line)
	}

	flush()

	return segs
}
