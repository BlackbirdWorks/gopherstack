package mysqlengine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
)

const (
	mysqldumpBin = "mysqldump"
	catBin       = "cat"
)

// DB opens a root connection to db ("" for none); the caller closes it.
func (s *Server) DB(db string) (*sql.DB, error) { return s.open(db) }

// Persistent reports whether the data lives in a volume that survives the container.
func (s *Server) Persistent() bool { return s.volume != "" }

// ContainerName is the name replicas use to reach this server on a shared network.
func (s *Server) ContainerName() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ctrNm
}

// Run executes cmd in the container and returns its stdout.
func (s *Server) Run(ctx context.Context, cmd []string) (string, error) {
	return s.runOut(ctx, cmdLabel(cmd), cmd, nil)
}

// RunAuth is Run with the root password in the process environment, for the mysql client tools.
func (s *Server) RunAuth(ctx context.Context, cmd []string) (string, error) {
	return s.runOut(ctx, cmdLabel(cmd), cmd, s.pwdEnv())
}

// Statement runs SQL through the mysql client and returns its tab-separated output.
func (s *Server) Statement(ctx context.Context, stmt string) (string, error) {
	return s.RunAuth(ctx, []string{mysqlBin, "-u" + RootUser, "-N", "-B", "-e", stmt})
}

// cmdLabel maps argv to a fixed label so credentials in argv never reach errors or logs.
func cmdLabel(cmd []string) string {
	for _, a := range cmd {
		if a == mysqldumpBin || a == mysqlBin {
			return a
		}
	}

	return "command"
}

// ReadFile returns a file inside the container.
func (s *Server) ReadFile(ctx context.Context, path string) (string, error) {
	return s.Run(ctx, []string{catBin, path})
}

// WriteFile replaces a file inside the container.
func (s *Server) WriteFile(ctx context.Context, path, content string) error {
	_, err := s.Run(ctx, []string{"sh", "-c", `printf %s "$1" > "$0"`, path, content})

	return err
}

// CopyOut returns a file inside the container through the dump mount.
func (s *Server) CopyOut(ctx context.Context, path string) ([]byte, error) {
	if _, err := s.Run(ctx, []string{
		"sh", "-c", `cp "$0" "$1" && chmod 644 "$1"`, path, dumpMount + "/copy.out",
	}); err != nil {
		return nil, err
	}

	return s.readDump("copy.out")
}

func (s *Server) readDump(name string) ([]byte, error) {
	path := filepath.Join(s.dir, name)
	defer os.Remove(path)

	st, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("dump file: %w", err)
	}

	if st.Size() > maxDumpBytes {
		return nil, ErrTooLarge
	}

	return os.ReadFile(path)
}

// RemoveVolume deletes this server's data volume; the server must be closed first.
func (s *Server) RemoveVolume(ctx context.Context) error {
	if s.volume == "" {
		return nil
	}

	return s.e.cfg.Volumes.RemoveVolume(ctx, s.volume) //nolint:wrapcheck // runtime errors carry the volume
}

// Volume is the data volume name, empty without persistence.
func (s *Server) Volume() string { return s.volume }
