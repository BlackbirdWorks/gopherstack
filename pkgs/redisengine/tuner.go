package redisengine

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
)

const sweepInterval = 100 * time.Millisecond

type dbKey struct {
	key string
	db  int
}

type peerState struct {
	lastActive time.Time
	queue      []queuedCmd
	db         int
	multi      bool
}

type queuedCmd struct {
	cmd  string
	args []string
}

// tuner wraps command execution on one server: memory limit and eviction, keyspace notifications, SLOWLOG,
// INFO sections, SELECT range and idle-client timeout.
type tuner struct {
	stop     chan struct{}
	done     chan struct{}
	cfg      atomic.Pointer[Settings]
	cmds     map[string]cmdInfo
	peers    map[*server.Peer]*peerState
	inflight map[*server.Peer]struct{}
	srv      *server.Server
	clock    func() time.Time
	mr       *miniredis.Miniredis
	deadline map[dbKey]time.Time
	auth     string
	mem      memAccount
	slow     slowLog
	evicted  int64
	expired  int64
	mu       sync.Mutex
}

func newTuner(mr *miniredis.Miniredis, auth string) (*tuner, error) {
	srv := mr.Server()
	if srv == nil {
		return nil, errNoServer
	}

	t := &tuner{
		mr: mr, srv: srv, auth: auth, clock: time.Now,
		cmds:     commandTable(),
		peers:    map[*server.Peer]*peerState{},
		inflight: map[*server.Peer]struct{}{},
		mem:      newMemAccount(),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		deadline: map[dbKey]time.Time{},
	}

	s := DefaultSettings()
	t.cfg.Store(&s)
	srv.SetPreHook(t.hook)

	go t.sweepLoop()

	return t, nil
}

func (t *tuner) apply(s Settings) {
	t.cfg.Store(&s)
	t.slow.resize(s.SlowlogMaxLen)
	t.mu.Lock()
	t.mem.dirty = true
	t.mu.Unlock()
}

func (t *tuner) close() {
	if t == nil {
		return
	}

	t.srv.SetPreHook(nil)
	close(t.stop)
	<-t.done
}

// invalidate makes the next memory check re-measure every key; direct writes to the server bypass the hook.
func (t *tuner) invalidate() {
	if t == nil {
		return
	}

	t.mu.Lock()
	t.mem.dirty = true
	t.mu.Unlock()
}

func (t *tuner) state(p *server.Peer) (*peerState, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if ps, ok := t.peers[p]; ok {
		return ps, false
	}

	ps := &peerState{lastActive: t.clock()}
	if p.Ctx != nil {
		return ps, true
	}

	t.peers[p] = ps
	p.OnDisconnect(func() {
		t.mu.Lock()
		delete(t.peers, p)
		t.mu.Unlock()
	})

	return ps, false
}

func (t *tuner) enter(p *server.Peer) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.inflight[p]; ok {
		return false
	}

	t.inflight[p] = struct{}{}

	return true
}

func (t *tuner) leave(p *server.Peer) {
	t.mu.Lock()
	delete(t.inflight, p)
	t.mu.Unlock()
}

// hook is the server's pre-command hook; it returns true once it has run the command itself.
func (t *tuner) hook(p *server.Peer, cmd string, args ...string) bool {
	if p.ClientName == internalPeerName || !t.owns(cmd) || !t.enter(p) {
		return false
	}

	defer t.leave(p)

	ps, nested := t.state(p)
	if nested {
		return false
	}

	cfg := t.cfg.Load()

	if t.idleExpired(ps, cfg) {
		p.Close()

		return true
	}

	ps.lastActive = t.clock()

	start := time.Now()

	if t.intercept(p, ps, cmd, args, cfg) {
		t.slow.record(cfg, queuedCmd{cmd: cmd, args: args}, time.Since(start), false)

		return true
	}

	t.execute(p, ps, cmd, args, cfg)

	return true
}

func (t *tuner) idleExpired(ps *peerState, cfg *Settings) bool {
	return cfg.Timeout > 0 && t.clock().Sub(ps.lastActive) > cfg.Timeout
}

func (t *tuner) dispatch(p *server.Peer, cmd string, args []string) {
	t.srv.Dispatch(p, append([]string{cmd}, args...))
}

func (t *tuner) execute(p *server.Peer, ps *peerState, cmd string, args []string, cfg *Settings) {
	switch cmd {
	case "MULTI":
		ps.multi, ps.queue = true, nil
		t.dispatch(p, cmd, args)

		return
	case "DISCARD":
		ps.multi, ps.queue = false, nil
		t.dispatch(p, cmd, args)

		return
	case "EXEC":
		t.execTx(p, ps, cfg)

		return
	}

	if ps.multi {
		ps.queue = append(ps.queue, queuedCmd{cmd: cmd, args: args})
		t.dispatch(p, cmd, args)

		return
	}

	t.runOne(p, ps, queuedCmd{cmd: cmd, args: args}, cfg)
}

func (t *tuner) runOne(p *server.Peer, ps *peerState, c queuedCmd, cfg *Settings) {
	info := t.cmds[strings.ToLower(c.cmd)]

	if info.denyOOM && !t.admit(cfg) {
		p.WriteError(oomMessage)

		return
	}

	obs := t.observe(ps.db, []queuedCmd{c}, cfg)
	start := time.Now()

	t.dispatch(p, c.cmd, c.args)

	t.after(ps.db, obs, cfg)
	t.slow.record(cfg, c, time.Since(start), isBlocking(c))
}

func (t *tuner) execTx(p *server.Peer, ps *peerState, cfg *Settings) {
	queue := ps.queue
	ps.multi, ps.queue = false, nil

	if t.queueNeedsMemory(queue) && !t.admit(cfg) {
		t.dispatch(p, "DISCARD", nil)
		p.WriteError("EXECABORT Transaction discarded because of previous errors.")

		return
	}

	obs := t.observe(ps.db, queue, cfg)
	start := time.Now()

	t.dispatch(p, "EXEC", nil)

	t.after(ps.db, obs, cfg)
	t.slow.record(cfg, queuedCmd{cmd: "EXEC"}, time.Since(start), false)
}

func (t *tuner) queueNeedsMemory(queue []queuedCmd) bool {
	for _, q := range queue {
		if t.cmds[strings.ToLower(q.cmd)].denyOOM {
			return true
		}
	}

	return false
}

func isBlocking(c queuedCmd) bool {
	switch c.cmd {
	case "BLPOP", "BRPOP", "BRPOPLPUSH", "BLMOVE", "BZPOPMIN", "BZPOPMAX", "WAIT", "SUBSCRIBE", "PSUBSCRIBE":
		return true
	case cmdXRead, cmdXReadGroup:
		for _, a := range c.args {
			if strings.EqualFold(a, "BLOCK") {
				return true
			}
		}
	}

	return false
}

func (t *tuner) owns(cmd string) bool { return cmd == "SLOWLOG" || t.srv.IsRegisteredCommand(cmd) }
