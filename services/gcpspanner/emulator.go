package gcpspanner

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	opPollInterval = 20 * time.Millisecond
	maxMessageSize = 64 << 20
	teardownLimit  = 30 * time.Second
)

var (
	errNoDataPlane = errors.New("cloud spanner data plane unavailable")
	// ErrRuntimeUnavailable is returned by a launcher when no container runtime can be reached.
	ErrRuntimeUnavailable = errors.New("container runtime unavailable")
)

// running is a started emulator: the gRPC address it serves and how to stop it.
type running struct {
	stop func(ctx context.Context)
	addr string
}

type launcher interface {
	launch(ctx context.Context) (*running, error)
}

// emulator owns the lazily started Cloud Spanner emulator that serves the data plane and executes DDL.
type emulator struct {
	b        *InMemoryBackend
	launchMu *lockmetrics.RWMutex
	mu       *lockmetrics.RWMutex
	l        launcher
	cc       *grpc.ClientConn
	run      *running
	noRT     string
}

func newEmulator(b *InMemoryBackend, l launcher) *emulator {
	return &emulator{
		b:        b,
		l:        l,
		launchMu: lockmetrics.New("gcpspanner.launch"),
		mu:       lockmetrics.New("gcpspanner.emulator"),
	}
}

func (e *emulator) setLauncher(l launcher) {
	e.mu.Lock("setLauncher")
	e.l = l
	e.mu.Unlock()
}

// current returns the connection to a running emulator, or nil.
func (e *emulator) current() *grpc.ClientConn {
	e.mu.RLock("current")
	defer e.mu.RUnlock()

	return e.cc
}

func (e *emulator) state() (launcher, string) {
	e.mu.RLock("state")
	defer e.mu.RUnlock()

	return e.l, e.noRT
}

// conn returns a connection to the emulator, starting and reconciling it on first use. errNoDataPlane means
// the backend runs metadata only.
func (e *emulator) conn(ctx context.Context) (*grpc.ClientConn, error) {
	if c := e.current(); c != nil {
		return c, nil
	}

	e.launchMu.Lock("conn")
	defer e.launchMu.Unlock()

	if c := e.current(); c != nil {
		return c, nil
	}

	l, noRT := e.state()
	if l == nil {
		return nil, fmt.Errorf("%w: the emulator engine is disabled", errNoDataPlane)
	}

	if noRT != "" {
		return nil, fmt.Errorf("%w: %s", errNoDataPlane, noRT)
	}

	return e.start(ctx, l)
}

func (e *emulator) start(ctx context.Context, l launcher) (*grpc.ClientConn, error) {
	run, err := l.launch(context.WithoutCancel(ctx))
	if err != nil {
		if errors.Is(err, ErrRuntimeUnavailable) {
			e.mu.Lock("start")
			e.noRT = err.Error()
			e.mu.Unlock()

			return nil, fmt.Errorf("%w: %w", errNoDataPlane, err)
		}

		return nil, err
	}

	cc, err := grpc.NewClient(
		"passthrough:///"+run.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)),
	)
	if err != nil {
		run.stop(ctx)

		return nil, errUnavailable("Cannot reach the Cloud Spanner emulator: %v", err)
	}

	if err = e.b.reconcile(context.WithoutCancel(ctx), cc); err != nil {
		_ = cc.Close()
		run.stop(ctx)

		return nil, err
	}

	e.mu.Lock("start")
	e.cc, e.run = cc, run
	e.mu.Unlock()

	return cc, nil
}

// dataConn is conn for requests that need the emulator itself: failure is UNAVAILABLE.
func (e *emulator) dataConn(ctx context.Context) (*grpc.ClientConn, error) {
	cc, err := e.conn(ctx)
	if err == nil {
		return cc, nil
	}

	if errors.Is(err, errNoDataPlane) {
		return nil, errUnavailable("The Cloud Spanner data plane is unavailable: %v. Instances and database "+
			"metadata work without it; sessions and queries need the emulator container.", err)
	}

	return nil, err
}

func (e *emulator) mirrorInstance(ctx context.Context, project string, pb *instancepb.Instance) error {
	cc, err := e.conn(ctx)
	if err != nil {
		return err
	}

	return createOnEmulator(ctx, cc, project, pb)
}

func (e *emulator) dropInstance(ctx context.Context, name string) {
	cc := e.current()
	if cc == nil {
		return
	}

	_, err := instancepb.NewInstanceAdminClient(cc).DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{Name: name})
	if err != nil && status.Code(err) != codes.NotFound {
		logger.Load(ctx).WarnContext(ctx, "gcpspanner: emulator instance delete failed", "instance", name, "error", err)
	}
}

// shutdown is close plus releasing the launcher's runtime client.
func (e *emulator) shutdown(ctx context.Context) {
	e.close(ctx)

	l, _ := e.state()
	if c, ok := l.(interface{ Close() }); ok {
		c.Close()
	}
}

func (e *emulator) close(ctx context.Context) {
	e.launchMu.Lock("close")
	defer e.launchMu.Unlock()

	e.mu.Lock("close")
	cc, run := e.cc, e.run
	e.cc, e.run, e.noRT = nil, nil, ""
	e.mu.Unlock()

	if cc != nil {
		_ = cc.Close()
	}

	if run != nil {
		tctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), teardownLimit)
		defer cancel()

		run.stop(tctx)
	}
}

func createOnEmulator(ctx context.Context, cc *grpc.ClientConn, project string, pb *instancepb.Instance) error {
	_, id, err := parseInstance(pb.GetName())
	if err != nil {
		return err
	}

	op, err := instancepb.NewInstanceAdminClient(cc).CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent: "projects/" + project, InstanceId: id,
		Instance: &instancepb.Instance{
			Config: configName(project, emulatorConfig), DisplayName: pb.GetDisplayName(), NodeCount: 1,
		},
	})
	if status.Code(err) == codes.AlreadyExists {
		return nil
	}

	if err != nil {
		return fmt.Errorf("emulator create instance: %w", err)
	}

	return waitOp(ctx, cc, op)
}

// waitOp polls an emulator operation to completion and returns its error.
func waitOp(ctx context.Context, cc *grpc.ClientConn, op *longrunningpb.Operation) error {
	ops := longrunningpb.NewOperationsClient(cc)
	tick := time.NewTicker(opPollInterval)

	defer tick.Stop()

	for !op.GetDone() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for emulator operation: %w", ctx.Err())
		case <-tick.C:
		}

		var err error
		if op, err = ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: op.GetName()}); err != nil {
			return fmt.Errorf("poll emulator operation: %w", err)
		}
	}

	if e := op.GetError(); e != nil {
		return status.FromProto(e).Err()
	}

	return nil
}

type replayDB struct {
	id      string
	ddl     []string
	dialect databasepb.DatabaseDialect
}

type replayInstance struct {
	pb  *instancepb.Instance
	dbs []replayDB
}

func (b *InMemoryBackend) replayPlan() []replayInstance {
	b.mu.RLock("replayPlan")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.instances))
	for n := range b.instances {
		names = append(names, n)
	}

	slices.Sort(names)

	out := make([]replayInstance, 0, len(names))

	for _, n := range names {
		rec := b.instances[n]
		ri := replayInstance{pb: cloneInstance(rec.pb)}

		for id, d := range rec.dbs {
			ri.dbs = append(ri.dbs, replayDB{id: id, ddl: slices.Clone(d.ddl), dialect: d.pb.GetDatabaseDialect()})
		}

		slices.SortFunc(ri.dbs, func(a, c replayDB) int { return cmp.Compare(a.id, c.id) })
		out = append(out, ri)
	}

	return out
}

// reconcile recreates every known instance and replays every database's DDL on a fresh emulator.
func (b *InMemoryBackend) reconcile(ctx context.Context, cc *grpc.ClientConn) error {
	for _, ri := range b.replayPlan() {
		project, _, err := parseInstance(ri.pb.GetName())
		if err != nil {
			return err
		}

		if err = createOnEmulator(ctx, cc, project, ri.pb); err != nil {
			return err
		}

		for _, d := range ri.dbs {
			if err = createDBOnEmulator(ctx, cc, ri.pb.GetName(), d); err != nil {
				return err
			}
		}
	}

	return nil
}

func createStatement(id string, dialect databasepb.DatabaseDialect) string {
	if dialect == databasepb.DatabaseDialect_POSTGRESQL {
		return `CREATE DATABASE "` + id + `"`
	}

	return "CREATE DATABASE `" + id + "`"
}

func createDBOnEmulator(ctx context.Context, cc *grpc.ClientConn, instance string, d replayDB) error {
	op, err := databasepb.NewDatabaseAdminClient(cc).CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent: instance, CreateStatement: createStatement(d.id, d.dialect), ExtraStatements: d.ddl,
		DatabaseDialect: d.dialect,
	})
	if err != nil {
		return fmt.Errorf("emulator create database %s: %w", d.id, err)
	}

	return waitOp(ctx, cc, op)
}
