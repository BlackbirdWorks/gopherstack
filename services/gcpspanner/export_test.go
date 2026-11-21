package gcpspanner

import (
	"log/slog"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"

	"context"
	"time"

	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

type fixedLauncher struct {
	stop func()
	addr string
}

func (f fixedLauncher) launch(context.Context) (*running, error) {
	return &running{addr: f.addr, stop: func(context.Context) {
		if f.stop != nil {
			f.stop()
		}
	}}, nil
}

// UseEmulatorAt makes the data plane connect to an already running emulator at addr.
func (b *InMemoryBackend) UseEmulatorAt(addr string, stop func()) {
	b.SetLauncher(fixedLauncher{addr: addr, stop: stop})
}

type failingLauncher struct{ err error }

func (f failingLauncher) launch(context.Context) (*running, error) { return nil, f.err }

// UseNoRuntime makes launching fail as if no container runtime existed.
func (b *InMemoryBackend) UseNoRuntime() {
	b.SetLauncher(failingLauncher{err: ErrRuntimeUnavailable})
}

// SetClock replaces the backend clock.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// SetDumpTimeout replaces the wall-clock budget of one table-data dump.
func (b *InMemoryBackend) SetDumpTimeout(d time.Duration) {
	b.mu.Lock("SetDumpTimeout")
	defer b.mu.Unlock()

	b.dumpTimeout = d
}

// EmulatorRunning reports whether a data plane connection is open.
func (b *InMemoryBackend) EmulatorRunning() bool { return b.emu.current() != nil }

// NewDockerLauncher exposes the Docker launcher to tests.
func NewDockerLauncher(log *slog.Logger, pool *portalloc.Allocator, image string, startup time.Duration) Launcher {
	return newDockerLauncher(log, pool, image, startup)
}

// Launcher is the data-plane launcher accepted by SetLauncher.
type Launcher = launcher

// SetInstanceType overrides the billing type of an instance.
func (b *InMemoryBackend) SetInstanceType(name string, t instancepb.Instance_InstanceType) {
	b.mu.Lock("SetInstanceType")
	defer b.mu.Unlock()

	if r, ok := b.instances[name]; ok {
		r.pb.InstanceType = t
	}
}

// ExplainUnimplemented exposes explainUnimplemented to tests.
func ExplainUnimplemented(method string, err error) error { return explainUnimplemented(method, err) }

// FoldDDL exposes foldDDL to tests.
func FoldDDL(current, stmts []string, postgres bool) ([]string, error) {
	out, _, err := foldDDL(current, stmts, postgres)

	return out, err
}

// FoldDDLApplied is FoldDDL with the number of statements applied.
func FoldDDLApplied(current, stmts []string, postgres bool) ([]string, int, error) {
	return foldDDL(current, stmts, postgres)
}

// InsertInstanceDirect stores an instance as CreateInstance does after validation.
func (b *InMemoryBackend) InsertInstanceDirect(pb *instancepb.Instance) error {
	return b.insertInstance(newInstRec(pb))
}

// InsertPartitionDirect stores a partition as CreateInstancePartition does after validation.
func (b *InMemoryBackend) InsertPartitionDirect(instance, id string, pb *instancepb.InstancePartition) error {
	b.mu.Lock("InsertPartitionDirect")
	defer b.mu.Unlock()

	rec, err := b.instanceLocked(instance)
	if err != nil {
		return err
	}

	return b.insertPartitionLocked(rec, id, pb)
}

// MoveDirect moves an instance to target as MoveInstance does after validation.
func (b *InMemoryBackend) MoveDirect(name string, target *instancepb.InstanceConfig) error {
	b.mu.Lock("MoveDirect")
	defer b.mu.Unlock()

	return b.moveLocked(name, target)
}

// InsertDatabaseDirect stores a database as CreateDatabase does once its keys were checked against checkedConfig.
func (b *InMemoryBackend) InsertDatabaseDirect(
	instance, id string,
	pb *databasepb.Database,
	checkedConfig string,
) error {
	return b.insertDatabase(instance, id, &dbRec{pb: pb}, checkedConfig)
}

// ParseOpFilter exposes the operation filter parser to tests.
func ParseOpFilter(s string) error {
	_, err := parseOpFilter(s)

	return err
}
