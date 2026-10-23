package gcpsql

import (
	"context"
	"errors"
	"regexp"
	"strings"
)

// ErrRuntimeUnavailable is returned by a Launcher when no container runtime is reachable; the instance
// then runs state-only.
var ErrRuntimeUnavailable = errors.New("gcpsql: container runtime unavailable")

type dbKind int

const (
	kindPostgres dbKind = iota
	kindMySQL
)

const (
	pgAdminUser    = "postgres"
	mysqlAdminUser = "root"
	pgSuperRole    = "cloudsqlsuperuser"
)

// Flag is one database flag.
type Flag struct {
	Name  string
	Value string
}

// LaunchSpec describes the engine of one instance.
type LaunchSpec struct {
	ID       string
	Version  string
	Password string
	Kind     dbKind
}

// DumpOptions tunes a dump. Markers prefixes each database with a switch line; Portable drops ownership and
// privileges so the file loads on an instance that lacks the original roles.
type DumpOptions struct {
	Markers  bool
	Portable bool
}

// UserSpec is a database account to create or update.
type UserSpec struct {
	Name     string
	Host     string
	Password string
}

// Engine is one running database. Methods are safe for sequential use per instance.
type Engine interface {
	// Endpoint is the host and published port clients connect to.
	Endpoint() (string, int)
	CreateDatabase(ctx context.Context, name, charset, collation string) error
	DropDatabase(ctx context.Context, name string) error
	ListDatabases(ctx context.Context) ([]string, error)
	UpsertUser(ctx context.Context, u UserSpec) error
	DropUser(ctx context.Context, name, host string) error
	// Restart applies flags (persisted in the data directory) and restarts the database.
	Restart(ctx context.Context, flags []Flag) error
	Stop(ctx context.Context) error
	Start(ctx context.Context) error
	// Dump returns a logical SQL dump of dbs.
	Dump(ctx context.Context, dbs []string, opts DumpOptions) ([]byte, error)
	// Restore runs a SQL file; db is the default database when the file carries no switch lines.
	Restore(ctx context.Context, db string, data []byte) error
	Query(ctx context.Context, db, query string) ([][]string, error)
	InsertRows(ctx context.Context, db, table string, columns []string, rows [][]string) error
	Close(ctx context.Context)
}

// Launcher starts engines.
type Launcher interface {
	Launch(ctx context.Context, spec LaunchSpec) (Engine, error)
}

var (
	errVersion = errors.New("unsupported database version")
	versionRe  = regexp.MustCompile(`^(POSTGRES_(\d+)|MYSQL_(\d+)_(\d+)(?:_\d+)?)$`)
)

var supportedVersions = []string{ //nolint:gochecknoglobals // immutable
	"POSTGRES_14", "POSTGRES_15", "POSTGRES_16", "POSTGRES_17", "MYSQL_8_0", "MYSQL_8_4",
}

// parseVersion maps a Cloud SQL databaseVersion to the engine kind and docker image tag.
func parseVersion(v string) (dbKind, string, error) {
	m := versionRe.FindStringSubmatch(v)
	if m == nil {
		return 0, "", errVersion
	}

	if m[2] != "" {
		return pgImage(m[2])
	}

	tag := m[3] + "." + m[4]
	if tag != "8.0" && tag != "8.4" {
		return 0, "", errVersion
	}

	return kindMySQL, "mysql:" + tag, nil
}

func pgImage(major string) (dbKind, string, error) {
	switch major {
	case "14", "15", "16", "17":
		return kindPostgres, "postgres:" + major, nil
	}

	return 0, "", errVersion
}

func versionError(v string) *APIError {
	return errInvalid("Database version %q is not supported by the emulator; supported: %s.",
		v, strings.Join(supportedVersions, ", "))
}

var nameRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_$.-]{0,62}$`)
var flagNameRe = regexp.MustCompile(`^[a-z_][a-z0-9_.]{0,62}$`)
var charsetRe = regexp.MustCompile(`^[a-z0-9_]{1,40}$`)
