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
)

const (
	stderrLimit  = 512
	connectLine  = `\connect `
	dumpFileMode = 0o600
	mysqlClient  = "mysql"
)

var errExec = errors.New("command failed")

func (e *dockerEngine) run(ctx context.Context, cmd []string) error {
	res, err := e.rt.Exec(ctx, e.container(), cmd)
	if err != nil {
		return fmt.Errorf("exec %s: %w", cmd[0], err)
	}

	if res.ExitCode != 0 {
		msg := strings.TrimSpace(res.Stderr + res.Stdout)
		if len(msg) > stderrLimit {
			msg = msg[:stderrLimit]
		}

		return fmt.Errorf("%w: %s exited %d: %s", errExec, cmd[0], res.ExitCode, msg)
	}

	return nil
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
	if e.kind == kindMySQL {
		return e.dumpMySQL(ctx, dbs)
	}

	var out bytes.Buffer

	for i, db := range dbs {
		name := "dump" + strconv.Itoa(i) + ".sql"

		cmd := []string{"pg_dump", "-U", pgAdminUser, "-d", db, "--clean", "--if-exists", "-f", dumpMount + "/" + name}
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

func (e *dockerEngine) dumpMySQL(ctx context.Context, dbs []string) ([]byte, error) {
	if len(dbs) == 0 {
		return nil, nil
	}

	cmd := append([]string{
		"env", "MYSQL_PWD=" + e.pass(), "mysqldump", "-u" + mysqlAdminUser, "--single-transaction", "--routines",
		"--add-drop-database", "-r", dumpMount + "/dump.sql", "--databases",
	}, dbs...)

	if err := e.run(ctx, cmd); err != nil {
		return nil, err
	}

	return e.readDump("dump.sql")
}

// Restore implements Engine.
func (e *dockerEngine) Restore(ctx context.Context, db string, data []byte) error {
	if e.kind == kindMySQL {
		return e.restoreFile(ctx, db, data)
	}

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

	if e.kind == kindPostgres {
		return e.run(ctx, []string{"psql", "-U", pgAdminUser, "-d", db, "-v", "ON_ERROR_STOP=1", "-q", "-f", file})
	}

	cmd := []string{"env", "MYSQL_PWD=" + e.pass(), mysqlClient, "-u" + mysqlAdminUser}
	if db != "" {
		cmd = append(cmd, "--database="+db)
	}

	return e.run(ctx, append(cmd, "-e", "source "+file))
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
