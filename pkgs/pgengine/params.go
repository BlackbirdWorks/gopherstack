package pgengine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"unicode"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	stdinReadBuf = 512
	psqlBin      = "psql"
	pgDumpBin    = "pg_dump"
)

var (
	// ErrInvalidParameter reports a parameter name or value the engine refuses to put into SQL.
	ErrInvalidParameter = errors.New("pgengine: invalid parameter")
	// ErrInvalidDatabase reports a database name that cannot be passed to pg_dump safely.
	ErrInvalidDatabase = errors.New("pgengine: invalid database name")
	// ErrExecUnsupported is returned when the container runtime cannot relay standard input to a command.
	ErrExecUnsupported = errors.New("pgengine: the container runtime cannot run commands with standard input")

	paramNameRE = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)?$`)
	dumpDBRE    = regexp.MustCompile(`^[A-Za-z0-9_$.][A-Za-z0-9_$.-]{0,62}$`)
)

func validParamValue(v string) bool {
	for _, r := range v {
		if !unicode.IsPrint(r) || r == '\\' {
			return false
		}
	}

	return true
}

// SetParameter persists a server parameter with ALTER SYSTEM; reset removes it. Dynamic parameters take effect
// after ReloadConf, static ones after the next restart. name must be a lower-case parameter name and value
// printable text without backslashes.
func (s *Server) SetParameter(ctx context.Context, name, value string, reset bool) error {
	stmt, err := parameterStmt(name, value, reset)
	if err != nil {
		return err
	}

	return s.Exec(ctx, "", stmt)
}

func parameterStmt(name, value string, reset bool) (string, error) {
	if !paramNameRE.MatchString(name) {
		return "", fmt.Errorf("%w: name", ErrInvalidParameter)
	}

	if reset {
		return "ALTER SYSTEM RESET " + Ident(name), nil
	}

	if !validParamValue(value) {
		return "", fmt.Errorf("%w: value", ErrInvalidParameter)
	}

	return "ALTER SYSTEM SET " + Ident(name) + " = " + Literal(value), nil
}

// ReloadConf makes the server re-read its configuration files.
func (s *Server) ReloadConf(ctx context.Context) error {
	return s.Exec(ctx, "", "SELECT pg_reload_conf()")
}

func (s *Server) stdinExec() (container.StdinExecer, error) {
	x, ok := s.e.cfg.Runtime.(container.StdinExecer)
	if !ok {
		return nil, ErrExecUnsupported
	}

	return x, nil
}

type headWriter struct {
	buf []byte
}

func (h *headWriter) Write(p []byte) (int, error) {
	if room := stdinReadBuf - len(h.buf); room > 0 {
		h.buf = append(h.buf, p[:min(room, len(p))]...)
	}

	return len(p), nil
}

// Dump streams a logical dump of dbs to w: for each database a \connect line, SET ROLE role when role is not
// empty, and the pg_dump output without ownership or privileges. Names are checked before they reach a command.
func (s *Server) Dump(ctx context.Context, w io.Writer, dbs []string, role string) error {
	for _, db := range dbs {
		if !dumpDBRE.MatchString(db) {
			return ErrInvalidDatabase
		}
	}

	x, err := s.stdinExec()
	if err != nil {
		return err
	}

	for _, db := range dbs {
		head := "\\connect " + Ident(db) + "\n"
		if role != "" {
			head += "SET ROLE " + Ident(role) + ";\n"
		}

		if _, err = io.WriteString(w, head); err != nil {
			return fmt.Errorf("pgengine: dump: %w", err)
		}

		cmd := []string{
			"sh",
			"-c",
			"exec " + pgDumpBin + " -U " + Superuser + " --no-owner --no-acl --dbname \"$0\" 2>/dev/null",
			db,
		}

		code, rerr := x.ExecStdin(ctx, s.container(), cmd, nil, w, 0, 0)
		if rerr != nil {
			return fmt.Errorf("pgengine: dump: %w", rerr)
		}

		if code != 0 {
			return opErr("pg_dump failed")
		}
	}

	return nil
}

// Restore runs a Dump stream in the server as the superuser and stops at the first error.
func (s *Server) Restore(ctx context.Context, r io.Reader) error {
	x, err := s.stdinExec()
	if err != nil {
		return err
	}

	cmd := []string{psqlBin, "-X", "-q", "-U", Superuser, "-d", "postgres", "-v", "ON_ERROR_STOP=1"}

	var head headWriter

	code, err := x.ExecStdin(ctx, s.container(), cmd, r, &head, 0, 0)
	if err != nil {
		return fmt.Errorf("pgengine: restore: %w", err)
	}

	if code != 0 {
		return opErr("psql restore failed")
	}

	return nil
}

// ReadOnly sets or clears default_transaction_read_only for the whole server.
func (s *Server) ReadOnly(ctx context.Context, on bool) error {
	v := "off"
	if on {
		v = "on"
	}

	if err := s.SetParameter(ctx, "default_transaction_read_only", v, false); err != nil {
		return err
	}

	return s.ReloadConf(ctx)
}
