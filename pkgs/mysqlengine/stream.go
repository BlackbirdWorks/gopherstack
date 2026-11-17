package mysqlengine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const headLimit = 512

var (
	// ErrInvalidVariable reports a server variable name or value the engine refuses to put into SQL.
	ErrInvalidVariable = errors.New("mysqlengine: invalid server variable")
	// ErrUnsupported reports an operation the server's version cannot perform.
	ErrUnsupported = errors.New("mysqlengine: unsupported by this server version")
	// ErrExecUnsupported is returned when the container runtime cannot relay standard input to a command.
	ErrExecUnsupported = errors.New("mysqlengine: the container runtime cannot run commands with standard input")
	// ErrInvalidPassword is returned when the root password cannot be handed to a client tool on standard input.
	ErrInvalidPassword = errors.New("mysqlengine: the root password cannot be passed to a client tool")

	varNameRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

// SetVariable persists a global system variable (SET PERSIST). static variables use SET PERSIST_ONLY and take
// effect after the next restart; reset restores the default. name is matched against an identifier pattern and
// the value is bound as a parameter. MySQL 5.7 has no SET PERSIST, so it only supports dynamic variables, set for
// the life of the server.
func (s *Server) SetVariable(ctx context.Context, name, value string, reset, static bool) error {
	if !varNameRE.MatchString(name) {
		return fmt.Errorf("%w: name", ErrInvalidVariable)
	}

	verb := "SET PERSIST "
	if static {
		verb = "SET PERSIST_ONLY "
	}

	if strings.HasSuffix(s.spec.Image, ":5.7") {
		if static {
			return ErrUnsupported
		}

		verb = "SET GLOBAL "
	}

	if reset {
		return s.exec(ctx, verb+name+" = DEFAULT")
	}

	return s.exec(ctx, verb+name+" = ?", variableArg(value))
}

// variableArg binds numbers as numbers: SET rejects a quoted string for a numeric variable.
func variableArg(v string) any {
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n
	}

	if f, err := strconv.ParseFloat(v, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
		return f
	}

	return v
}

// SetReadOnly turns super_read_only (and read_only) on or off, persisted across restarts.
func (s *Server) SetReadOnly(ctx context.Context, on bool) error {
	v := "OFF"
	if on {
		v = "ON"
	}

	for _, name := range []string{"read_only", "super_read_only"} {
		if err := s.exec(ctx, "SET PERSIST "+name+" = "+v); err != nil {
			return err
		}
	}

	return nil
}

type headBuf struct{ buf []byte }

func (h *headBuf) Write(p []byte) (int, error) {
	if room := headLimit - len(h.buf); room > 0 {
		h.buf = append(h.buf, p[:min(room, len(p))]...)
	}

	return len(p), nil
}

// clientScript runs a mysql client tool with the root password read from the first line of standard input, so it
// is never in argv or the exec environment record.
const clientScript = `IFS= read -r pw; MYSQL_PWD="$pw" exec "$@"`

func (s *Server) stdinRun(ctx context.Context, cmd []string, in io.Reader, out io.Writer) (int, error) {
	x, ok := s.e.cfg.Runtime.(container.StdinExecer)
	if !ok {
		return 0, ErrExecUnsupported
	}

	pw := s.pass()
	if strings.ContainsAny(pw, "\r\n") {
		return 0, ErrInvalidPassword
	}

	stdin := io.Reader(strings.NewReader(pw + "\n"))
	if in != nil {
		stdin = io.MultiReader(stdin, in)
	}

	code, err := x.ExecStdin(
		ctx,
		s.container(),
		append([]string{"sh", "-c", clientScript, "sh"}, cmd...),
		stdin,
		out,
		0,
		0,
	)
	if err != nil {
		return 0, fmt.Errorf("%w: exec: %s", errExec, s.scrub(err.Error()))
	}

	return code, nil
}

// DumpTo streams a logical dump of dbs to w; accounts are not included. Standard error is discarded.
func (s *Server) DumpTo(ctx context.Context, dbs []string, w io.Writer) error {
	if len(dbs) == 0 {
		return nil
	}

	for _, d := range dbs {
		if !dbNameRE.MatchString(d) {
			return ErrInvalidIdentifier
		}
	}

	cmd := append([]string{
		mysqldumpBin, "-u" + RootUser, argSingleTx, argRoutines, argEvents, argTriggers,
		argAddDrop, argGtidOff, argDatabases,
	}, dbs...)

	cmd = append([]string{"sh", "-c", `exec "$@" 2>/dev/null`, "sh"}, cmd...)

	code, err := s.stdinRun(ctx, cmd, nil, w)
	if err != nil {
		return err
	}

	if code != 0 {
		return fmt.Errorf("%w: mysqldump exited %d", errExec, code)
	}

	return nil
}

// RestoreFrom runs a SQL stream such as DumpTo's output.
func (s *Server) RestoreFrom(ctx context.Context, r io.Reader) error {
	var head headBuf

	code, err := s.stdinRun(ctx, []string{mysqlBin, "-u" + RootUser}, r, &head)
	if err != nil {
		return err
	}

	if code != 0 {
		return fmt.Errorf("%w: mysql exited %d: %s", errExec, code, s.scrub(strings.TrimSpace(string(head.buf))))
	}

	return nil
}

var dbNameRE = regexp.MustCompile(`^[A-Za-z0-9_$][A-Za-z0-9_$-]{0,63}$`)
