package gcpsql

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/dbrepl"
)

var errNoReplication = errors.New("gcpsql: the container runtime cannot run replication")

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
	Replica     *ReplicaSource
	Recovery    *Recovery
	ID          string
	Project     string
	Version     string
	Password    string
	MemoryBytes int64
	NanoCPUs    int64
	Kind        dbKind
	ServerID    uint32
	Persist     bool
	Fresh       bool
}

// ReplicaSource tells a replica's engine how to reach its primary.
type ReplicaSource struct {
	Network   string
	NetworkID string
	Host      string
	Password  string
	Port      int
}

// Recovery seeds an engine from another instance's point-in-time recovery state. Volume names the source's
// PostgreSQL archive; for MySQL, Script is the base dump and Source the engine whose binary log is replayed.
type Recovery struct {
	At     time.Time
	Source *ReplicaSource
	Volume string
	// FromFile and FromPos, UntilFile and UntilPos bound the MySQL binary log range a clone replays from Source.
	FromFile  string
	UntilFile string
	Script    []byte
	FromPos   uint64
	UntilPos  uint64
}

// Replicator is implemented by engines that can feed or follow a replica.
type Replicator interface {
	// ReplicationSource readies the engine as a replication source and says how a replica reaches it.
	ReplicationSource(ctx context.Context) (ReplicaSource, error)
	// AttachReplica starts replication from src; seed is a source dump for engines that cannot copy the data
	// themselves.
	AttachReplica(ctx context.Context, src ReplicaSource, seed []byte) error
	// ReplicaSeed returns the dump a MySQL replica starts from; PostgreSQL replicas copy the data themselves.
	ReplicaSeed(ctx context.Context, dbs []string) ([]byte, error)
	// Promote detaches a replica from its source and makes it writable.
	Promote(ctx context.Context) error
	// SetReplication pauses (false) or resumes (true) replication on a replica.
	SetReplication(ctx context.Context, on bool) error
	ReplicationStatus(ctx context.Context) (dbrepl.Status, error)
}

// Recoverer is implemented by engines that keep the logs point-in-time recovery needs.
type Recoverer interface {
	// EnablePITR starts archiving and returns the earliest time a recovery can target.
	EnablePITR(ctx context.Context, dbs []string) (time.Time, error)
	// DisablePITR stops archiving.
	DisablePITR(ctx context.Context) error
	// PrepareRecovery makes everything up to at recoverable and describes how a new engine starts from it.
	PrepareRecovery(ctx context.Context, at time.Time) (Recovery, error)
	// ApplyRecovery finishes a recovery on a freshly launched engine.
	ApplyRecovery(ctx context.Context, rec Recovery) error
}

// Purger is implemented by engines that own persistent storage; Purge also deletes it.
type Purger interface {
	Purge(ctx context.Context)
}

// VolumeRemover is implemented by engines whose persistent volumes can be deleted after Close; Reset closes every
// engine before removing any volume so a clone's mount of its source's archive cannot block the removal.
type VolumeRemover interface {
	RemoveVolumes(ctx context.Context)
}

// VolumeSweeper is implemented by launchers that can delete persistent volumes no instance owns.
type VolumeSweeper interface {
	SweepVolumes(ctx context.Context, keep map[string]bool)
}

// DumpOptions tunes a dump. Markers prefixes each database with a switch line; Portable drops ownership and
// privileges so the file loads on an instance that lacks the original roles.
type DumpOptions struct {
	Markers  bool
	Portable bool
}

const volumePrefix = "gopherstack-gcpsql-"

// volumeName is the data volume of an instance; kind is "data" or "pitr".
func volumeName(kind, project, name string) string {
	return volumePrefix + kind + "-" + sanitizeName(project) + "-" + sanitizeName(name)
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
