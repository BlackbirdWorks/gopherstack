package rds

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/dbrepl"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	replUser     = "rdsrepl"
	serverIDMin  = 2
	serverIDSpan = 1<<31 - serverIDMin
)

var errNoNetworkRuntime = errors.New("container runtime cannot create networks")

// replNet is the docker network a source shares with its replicas; it is removed once nothing uses it.
type replNet struct {
	id        atomic.Pointer[string]
	gate      chan struct{}
	name      string
	refs      atomic.Int32
	ready     atomic.Bool
	connected atomic.Bool
}

// replPlan tells a replica's container how to reach its source.
type replPlan struct {
	network string
	srcHost string
	srcPass string
}

// replicaState is a replica unit's replication bookkeeping, guarded by the backend lock.
type replicaState struct {
	plan      replPlan
	srcKey    string
	status    string
	message   string
	lag       float64
	promoting bool
}

func randomServerID() uint32 {
	var buf [4]byte

	_, _ = rand.Read(buf[:])

	return binary.BigEndian.Uint32(buf[:])%serverIDSpan + serverIDMin
}

// canStream reports whether the runtime can run streaming replication (networks plus exec).
func (e *dbEngine) canStream() bool {
	_, hasNet := e.cfg.Runtime.(container.NetworkRuntime)
	_, hasExec := e.execer()

	return hasNet && hasExec
}

// acquireNet returns the source's replica network, allocating the bookkeeping on first use. Caller holds b.mu.
func (lb *liveDB) acquireNet() *replNet {
	n := lb.net.Load()
	if n == nil {
		n = &replNet{name: "gopherstack-rds-net-" + randomPassword(), gate: make(chan struct{}, 1)}
		n.refs.Store(1)
		lb.net.Store(n)
	}

	n.refs.Add(1)

	return n
}

// seedReplicaLocked gives inst its own engine as a replica of src: streaming when the runtime allows, else a copy.
func (b *InMemoryBackend) seedReplicaLocked(inst *DBInstance, src *liveDB) {
	if src == nil {
		return
	}

	seed := liveSeedLocked(src)
	if b.engine.canStream() && src.kind != engineSQLServer {
		seed.replicate, seed.net = true, src.acquireNet()
	}

	b.seedRestoredInstanceLocked(inst, src, seed)
}

// readyNet creates the network and attaches the source once; a failed attempt is retried by the next caller.
func (e *dbEngine) readyNet(ctx context.Context, n *replNet, sourceContainer string) error {
	if n.ready.Load() {
		return nil
	}

	select {
	case n.gate <- struct{}{}:
		defer func() { <-n.gate }()
	case <-ctx.Done():
		return fmt.Errorf("replication network: %w", ctx.Err())
	}

	if n.ready.Load() {
		return nil
	}

	if err := e.createNet(ctx, n, sourceContainer); err != nil {
		return err
	}

	n.ready.Store(true)

	return nil
}

func (e *dbEngine) createNet(ctx context.Context, n *replNet, sourceContainer string) error {
	rt, ok := e.cfg.Runtime.(container.NetworkRuntime)
	if !ok {
		return errNoNetworkRuntime
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), engineOpTimeout)
	defer cancel()

	idp := n.id.Load()
	if idp == nil {
		id, err := rt.CreateNetwork(ctx, n.name)
		if err != nil {
			return fmt.Errorf("create replication network: %w", err)
		}

		idp = &id
		n.id.Store(idp)
	}

	if n.connected.Load() {
		return nil
	}

	if err := rt.ConnectNetwork(ctx, *idp, sourceContainer); err != nil {
		return fmt.Errorf("connect source to replication network: %w", err)
	}

	n.connected.Store(true)

	return nil
}

// releaseNet drops one holder of the network and removes it when the last one is gone.
func (e *dbEngine) releaseNet(n *replNet) {
	if n == nil || n.refs.Add(-1) > 0 {
		return
	}

	id := n.id.Load()

	rt, ok := e.cfg.Runtime.(container.NetworkRuntime)
	if id == nil || !ok {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), engineStopTimeout)
	defer cancel()

	if err := rt.RemoveNetwork(ctx, *id); err != nil {
		logger.Load(ctx).WarnContext(ctx, "rds: replication network removal failed", "network", *id, "error", err)
	}
}

// replPassword returns the source's replication password, creating it on first use.
func (b *InMemoryBackend) replPassword(src *liveDB) string {
	b.mu.Lock("rdsReplPassword")
	defer b.mu.Unlock()

	if src.replPass == "" {
		src.replPass = randomPassword()
	}

	return src.replPass
}

// replicaPlan readies the source and the shared network for a streaming replica; the zero plan means none is needed.
func (b *InMemoryBackend) replicaPlan(ctx context.Context, lb *liveDB) (replPlan, error) {
	seed := lb.seed
	if !seed.replicate {
		return replPlan{}, nil
	}

	src, in, err := b.awaitLiveSource(ctx, seed.fromKey)
	if err != nil {
		if b.standaloneAfterSourceLoss(ctx, lb) {
			return replPlan{}, nil
		}

		return replPlan{}, err
	}

	pass := b.replPassword(src)
	if err = b.engine.prepareSource(ctx, in, pass); err != nil {
		return replPlan{}, err
	}

	n := lb.net.Load()
	if err = b.engine.readyNet(ctx, n, in.containerID); err != nil {
		return replPlan{}, err
	}

	plan := replPlan{network: n.name, srcHost: src.ctrName, srcPass: pass}

	b.mu.Lock("rdsReplicaPlan")
	lb.repl.plan = plan
	b.mu.Unlock()

	return plan, nil
}

func pgServerFlags() []string {
	return []string{"-c", "wal_level=replica", "-c", "max_wal_senders=10", "-c", "max_replication_slots=10"}
}

func mysqlServerFlags(lb *liveDB) []string {
	id := "--server-id=" + strconv.FormatUint(uint64(lb.serverID), 10)
	if lb.kind == engineMariaDB {
		return []string{id, "--log-bin=mysql-bin"}
	}

	return []string{id, "--log-bin=mysql-bin", "--gtid-mode=ON", "--enforce-gtid-consistency=ON"}
}

// applyReplicationFlags makes the unit usable as a replication source, or as a streaming replica when plan is set.
func applyReplicationFlags(s *container.Spec, lb *liveDB, plan replPlan) {
	if lb.kind != enginePostgres {
		s.Cmd = mysqlServerFlags(lb)

		return
	}

	args := append([]string{enginePostgres}, pgServerFlags()...)
	if plan.network == "" {
		s.Cmd = args

		return
	}

	s.Entrypoint = append([]string{
		"sh", "-c", dbrepl.PGReplicaScript(replUser), "pg-replica", plan.srcHost, strconv.Itoa(pgContainerPort),
	}, args...)
	s.Env = append(s.Env, dbrepl.PasswordEnv+"="+plan.srcPass)
}

// prepareSource creates the replication login on the source and lets it connect; safe to repeat.
func (e *dbEngine) prepareSource(ctx context.Context, in unitInfo, pass string) error {
	x, ok := e.execer()
	if !ok {
		return container.ErrExecUnsupported
	}

	l := in.login
	if l.Kind != enginePostgres {
		client, _ := mysqlClients(l.Kind)
		stmt := dbrepl.MySQLReplicaUserSQL(replUser, pass)

		_, err := e.runEnv(ctx, x, in.containerID, []string{
			client, "-u" + mysqlRootUser, "-e", stmt,
		}, mysqlPwdEnv(l.Password))

		return err
	}

	db, err := pgDatabase(l)
	if err != nil {
		return err
	}

	psql := func(sqlText string) []string {
		return []string{"psql", "-q", "-v", "ON_ERROR_STOP=1", "-U", l.User, "-d", db, "-c", sqlText}
	}

	role := dbrepl.PGReplicaRoleSQL(replUser, pass)

	for _, cmd := range [][]string{
		psql(role), {"sh", "-c", dbrepl.PGHBAScript(replUser), "pg-hba"}, psql("SELECT pg_reload_conf()"),
	} {
		if _, err = e.runEnv(ctx, x, in.containerID, cmd, pgPwdEnv(l.Password)); err != nil {
			return err
		}
	}

	return nil
}

// standaloneAfterSourceLoss turns a replica whose source was deleted mid-creation into a plain primary, which is
// what its promotion amounts to.
func (b *InMemoryBackend) standaloneAfterSourceLoss(ctx context.Context, lb *liveDB) bool {
	if ctx.Err() != nil {
		return false
	}

	b.mu.Lock("rdsReplicaSourceLost")
	defer b.mu.Unlock()

	if lb.repl == nil || !lb.repl.promoting {
		return false
	}

	lb.seed, lb.repl = engineSeed{}, nil

	return true
}
