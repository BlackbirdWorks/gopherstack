package ocidatabase

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

var _ Archiver = (*dockerInstance)(nil)

const (
	pdbName       = "FREEPDB1"
	dumpDirectory = "DATA_PUMP_DIR"
	pumpTail      = 200
	outputCap     = 64 << 10
	parTagBytes   = 8
	rmArgs        = 2
	dumpExport    = "gs_export.dmp"
	dumpSafety    = "gs_safety.dmp"
	dumpImport    = "gs_import.dmp"
)

var (
	errNoExec    = errors.New("the container runtime cannot run commands inside containers")
	errExitCode  = errors.New("the command exited with an error")
	errBadDir    = errors.New("the dump directory reported by the database is not a plain path")
	errNoDumpDir = errors.New("the database reported no dump directory")
	dirPattern   = regexp.MustCompile(`^/[A-Za-z0-9_./-]+$`)
)

type execAPI interface {
	ExecStream(ctx context.Context, containerID string, cmd []string, out io.Writer) (int, error)
	ExecStdin(
		ctx context.Context, containerID string, cmd []string, stdin io.Reader, out io.Writer, rows, cols uint,
	) (int, error)
}

// capped keeps the first outputCap bytes written to it.
type capped struct{ buf bytes.Buffer }

func (c *capped) Write(p []byte) (int, error) {
	if room := outputCap - c.buf.Len(); room > 0 {
		c.buf.Write(p[:min(room, len(p))])
	}

	return len(p), nil
}

func (d *dockerInstance) run(ctx context.Context, stdin io.Reader, cmd []string, out io.Writer) error {
	if d.exec == nil {
		return errNoExec
	}

	code, err := d.exec.ExecStdin(ctx, d.id, cmd, stdin, out, 0, 0)
	if err != nil {
		return fmt.Errorf("run %s in the database container: %w", cmd[0], err)
	}

	if code != 0 {
		return fmt.Errorf("%w: %s status %d", errExitCode, cmd[0], code)
	}

	return nil
}

// sysdba runs a SQL*Plus script as SYSDBA inside the container; the script travels on stdin.
func (d *dockerInstance) sysdba(ctx context.Context, script string) (string, error) {
	var out capped

	err := d.run(ctx, strings.NewReader(script), []string{"sqlplus", "-S", "/", "as", "sysdba"}, &out)

	return out.buf.String(), err
}

// pumpDir lets ADMIN use the Data Pump directory and returns its path with the current SCN.
func (d *dockerInstance) pumpDir(ctx context.Context) (string, int64, error) {
	out, err := d.sysdba(ctx, "set pagesize 0 feedback off heading off verify off\n"+
		"alter session set container="+pdbName+";\n"+
		"grant read, write on directory "+dumpDirectory+" to "+adminUser+";\n"+
		"select 'DIR=' || directory_path from dba_directories where directory_name = '"+dumpDirectory+"';\n"+
		"select 'SCN=' || current_scn from v$database;\nexit\n")
	if err != nil {
		return "", 0, err
	}

	var (
		dir string
		scn int64
	)

	for line := range strings.SplitSeq(out, "\n") {
		line = strings.TrimSpace(line)

		switch {
		case strings.HasPrefix(line, "DIR="):
			dir = strings.TrimPrefix(line, "DIR=")
		case strings.HasPrefix(line, "SCN="):
			scn, _ = strconv.ParseInt(strings.TrimPrefix(line, "SCN="), 10, 64)
		}
	}

	if dir == "" {
		return "", 0, errNoDumpDir
	}

	if !dirPattern.MatchString(dir) || strings.Contains(dir, "..") {
		return "", 0, errBadDir
	}

	return dir, scn, nil
}

// pump runs expdp or impdp with the connect string in a parameter file written through stdin, so the password
// never appears in a command line; the file is removed afterwards.
func (d *dockerInstance) pump(ctx context.Context, tool string, params ...string) error {
	tag := make([]byte, parTagBytes)
	_, _ = rand.Read(tag)
	par := "/tmp/gs-" + hex.EncodeToString(tag) + ".par"

	content := `userid=` + adminUser + `/"` + d.password + `"@localhost/` + pdbName + "\n" +
		strings.Join(params, "\n") + "\n"

	write := []string{"sh", "-c", "umask 077; cat > " + par}
	if err := d.run(ctx, strings.NewReader(content), write, io.Discard); err != nil {
		return err
	}

	defer func() {
		_ = d.run(context.WithoutCancel(ctx), nil, []string{"rm", "-f", par}, io.Discard)
	}()

	var out capped

	if err := d.run(ctx, nil, []string{tool, "parfile=" + par}, &out); err != nil {
		return fmt.Errorf("%w%s", err, firstOracleError(out.buf.String()))
	}

	return nil
}

func firstOracleError(out string) string {
	for line := range strings.SplitSeq(out, "\n") {
		line = strings.TrimSpace(line)

		for _, p := range []string{"ORA-", "UDE-", "UDI-"} {
			if strings.HasPrefix(line, p) {
				return ": " + line[:min(len(line), pumpTail)]
			}
		}
	}

	return ""
}

func (d *dockerInstance) dropDumps(ctx context.Context, dir string, names ...string) {
	args := append(make([]string, 0, len(names)+rmArgs), "rm", "-f")

	for _, n := range names {
		args = append(args, dir+"/"+n)
	}

	_ = d.run(context.WithoutCancel(ctx), nil, args, io.Discard)
}

// Export dumps the ADMIN schema with Data Pump and streams the dump file to w.
func (d *dockerInstance) Export(ctx context.Context, w io.Writer) (int64, error) {
	dir, scn, err := d.pumpDir(ctx)
	if err != nil {
		return 0, err
	}
	defer d.dropDumps(ctx, dir, dumpExport)

	if err = d.pump(ctx, "expdp", "schemas="+adminUser, "directory="+dumpDirectory, "dumpfile="+dumpExport,
		"nologfile=yes", "reuse_dumpfiles=yes"); err != nil {
		return 0, err
	}

	code, err := d.exec.ExecStream(ctx, d.id, []string{"cat", dir + "/" + dumpExport}, w)
	if err != nil {
		return 0, fmt.Errorf("read the dump file: %w", err)
	}

	if code != 0 {
		return 0, fmt.Errorf("%w: cat status %d", errExitCode, code)
	}

	return scn, nil
}

// Import replaces the ADMIN schema with the dump in r. The current schema is exported first and loaded back
// when the import fails.
func (d *dockerInstance) Import(ctx context.Context, r io.Reader) error {
	dir, _, err := d.pumpDir(ctx)
	if err != nil {
		return err
	}
	defer d.dropDumps(ctx, dir, dumpSafety, dumpImport)

	if err = d.pump(ctx, "expdp", "schemas="+adminUser, "directory="+dumpDirectory, "dumpfile="+dumpSafety,
		"nologfile=yes", "reuse_dumpfiles=yes"); err != nil {
		return fmt.Errorf("save the current schema: %w", err)
	}

	if err = d.run(ctx, r, []string{"sh", "-c", "umask 077; cat > " + dir + "/" + dumpImport}, io.Discard); err != nil {
		return fmt.Errorf("upload the backup: %w", err)
	}

	if err = d.wipeSchema(ctx); err != nil {
		return err
	}

	load := func(file string) error {
		return d.pump(ctx, "impdp", "schemas="+adminUser, "directory="+dumpDirectory, "dumpfile="+file,
			"nologfile=yes", "table_exists_action=replace")
	}

	if err = load(dumpImport); err != nil {
		if d.wipeSchema(ctx) == nil && load(dumpSafety) == nil {
			return fmt.Errorf("load the backup: %w: %w", ErrRolledBack, err)
		}

		return fmt.Errorf("load the backup: %w", err)
	}

	return nil
}

const wipeBlock = `BEGIN
  FOR o IN (SELECT object_name, object_type FROM user_objects
            WHERE object_type IN ('MATERIALIZED VIEW','VIEW','SYNONYM','PROCEDURE','FUNCTION','PACKAGE','TYPE',
                                  'SEQUENCE','TABLE')
            ORDER BY CASE object_type WHEN 'MATERIALIZED VIEW' THEN 1 WHEN 'VIEW' THEN 2 WHEN 'TABLE' THEN 9
                                      ELSE 5 END) LOOP
    BEGIN
      IF o.object_type = 'TABLE' THEN
        EXECUTE IMMEDIATE 'DROP TABLE "' || o.object_name || '" CASCADE CONSTRAINTS PURGE';
      ELSIF o.object_type = 'TYPE' THEN
        EXECUTE IMMEDIATE 'DROP TYPE "' || o.object_name || '" FORCE';
      ELSE
        EXECUTE IMMEDIATE 'DROP ' || o.object_type || ' "' || o.object_name || '"';
      END IF;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
  END LOOP;
END;`

// wipeSchema drops every object ADMIN owns. Names come from the data dictionary and are quoted.
func (d *dockerInstance) wipeSchema(ctx context.Context) error {
	db, err := d.open(adminUser, d.password)
	if err != nil {
		return err
	}
	defer db.Close()

	return execBlock(ctx, db, wipeBlock)
}

func execBlock(ctx context.Context, db *sql.DB, block string) error {
	if _, err := db.ExecContext(ctx, block); err != nil {
		return fmt.Errorf("clear the ADMIN schema: %w", err)
	}

	return nil
}
