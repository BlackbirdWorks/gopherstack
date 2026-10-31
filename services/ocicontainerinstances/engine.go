package ocicontainerinstances

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/registryauth"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	teardownTimeout = 20 * time.Second
	backoffBase     = 10 * time.Second
	backoffMax      = 5 * time.Minute
	startTimeout    = 10 * time.Minute
	nanoPerCPU      = 1e9
	bytesPerGB      = 1 << 30
	nonceLen        = 6
	killedExitCode  = 137
	maxLogBytes     = 20 << 20
	httpOKMin       = 200
	httpOKMax       = 399
)

var (
	errEngineDisabled = errors.New("the container engine is disabled")
	errNoPreviousRun  = errors.New("the container has not been restarted, so there is no previous run")
	errStartTimeout   = errors.New("containers did not start in time")
)

var errNoLogsYet = errors.New("the container has no logs: it never started, or its instance was deleted")

// Sink is the backend side the engine reports container state to.
type Sink interface {
	Mutate(instID string, fn func(*Instance))
	Now() time.Time
}

// Timer returns a channel that fires after d and a function that releases it.
type Timer func(d time.Duration) (<-chan time.Time, func())

// Prober checks one health check against the container's loopback-published port.
type Prober func(ctx context.Context, hc HealthCheck, hostPort int) error

func realTimer(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTimer(d)

	return t.C, func() { t.Stop() }
}

type run struct {
	ctx       context.Context
	rt        Runtime
	cancel    context.CancelFunc
	scratch   *scratch
	hostPorts map[string]int
	docker    map[string]string
	prev      map[string]string
	killed    map[string]bool
	inst      *Instance
	creds     []Credential
	wg        sync.WaitGroup
}

// Engine runs container instances in the container runtime.
type Engine struct {
	sink    Sink
	ports   gcpserve.PortPool
	timer   Timer
	backoff func(restarts int) time.Duration
	probe   Prober
	runtime func() (Runtime, error)
	ctx     context.Context //nolint:containedctx // engine lifetime
	cancel  context.CancelFunc
	mu      *lockmetrics.RWMutex
	runs    map[string]*run
	last    map[string]string
	onClose func()
	nonce   string
	startup time.Duration
	wg      sync.WaitGroup
	seq     atomic.Uint64
}

// NewEngine creates an engine; runtime is dialled on first use.
func NewEngine(runtime func() (Runtime, error)) *Engine {
	ctx, cancel := context.WithCancel(context.Background())

	return &Engine{
		ctx: ctx, cancel: cancel, runtime: runtime, timer: realTimer, probe: probeHealth, backoff: defaultBackoff,
		mu: lockmetrics.New("ocicontainerinstances.engine"), runs: map[string]*run{}, last: map[string]string{},
		nonce: awsid.NewN("", nonceLen), startup: startTimeout,
	}
}

func defaultBackoff(restarts int) time.Duration {
	d := backoffBase

	for i := 1; i < restarts && d < backoffMax; i++ {
		d *= 2
	}

	return min(d, backoffMax)
}

// SetPortPool wires the host port source for health check ports.
func (e *Engine) SetPortPool(p gcpserve.PortPool) {
	e.mu.Lock("SetPortPool")
	e.ports = p
	e.mu.Unlock()
}

// SetTiming overrides the timer, restart backoff and prober; for tests.
func (e *Engine) SetTiming(t Timer, backoff func(int) time.Duration, p Prober) {
	e.mu.Lock("SetTiming")
	defer e.mu.Unlock()

	if t != nil {
		e.timer = t
	}

	if backoff != nil {
		e.backoff = backoff
	}

	if p != nil {
		e.probe = p
	}
}

// Launch (re)creates the instance's containers; done gets nil once every container started, or the failure.
func (e *Engine) Launch(
	inst *Instance, creds func([]PullSecret) ([]Credential, error), done func(error),
) {
	e.wg.Go(func() {
		e.halt(inst.ID)
		done(e.start(inst, creds))
	})
}

// Relaunch stops the instance's containers and starts them again.
func (e *Engine) Relaunch(inst *Instance, creds func([]PullSecret) ([]Credential, error), done func(error)) {
	e.Launch(inst, creds, done)
}

func (e *Engine) start(inst *Instance, credsFn func([]PullSecret) ([]Credential, error)) error {
	creds, err := credsFn(inst.PullSecrets)
	if err != nil {
		return fmt.Errorf("image pull secrets: %w", err)
	}

	e.mu.RLock("start")
	source := e.runtime
	e.mu.RUnlock()

	if source == nil {
		return errEngineDisabled
	}

	rt, err := source()
	if err != nil {
		return fmt.Errorf("the container runtime is unavailable: %w", err)
	}

	r := &run{
		rt: rt, inst: inst, creds: creds, hostPorts: map[string]int{}, docker: map[string]string{},
		prev: map[string]string{}, killed: map[string]bool{},
	}
	r.ctx, r.cancel = context.WithCancel(e.ctx)

	e.mu.Lock("start")
	e.runs[inst.ID] = r
	for _, c := range inst.Containers {
		delete(e.last, c.ID)
	}
	e.mu.Unlock()

	if err = e.prepare(r); err != nil {
		e.halt(inst.ID)

		return err
	}

	results := make(chan error, len(inst.Containers))

	for i := range inst.Containers {
		r.wg.Go(func() { e.supervise(r, i, results) })
	}

	timeout, stop := e.timer(e.startup)
	defer stop()

	for range inst.Containers {
		select {
		case err = <-results:
		case <-timeout:
			err = fmt.Errorf("%w (%s)", errStartTimeout, e.startup)
		case <-r.ctx.Done():
			return r.ctx.Err()
		}

		if err != nil {
			e.halt(inst.ID)

			return err
		}
	}

	return nil
}

func (e *Engine) prepare(r *run) error {
	sc, err := newScratch()
	if err != nil {
		return err
	}

	r.scratch = sc
	if err = sc.fill(r.inst.Volumes, r.inst.DNS); err != nil {
		return err
	}

	e.mu.RLock("prepare")
	pool := e.ports
	e.mu.RUnlock()

	for _, c := range r.inst.Containers {
		for _, h := range c.Health {
			key := c.ID + ":" + strconv.Itoa(h.Port)
			if _, ok := r.hostPorts[key]; ok {
				continue
			}

			hp, perr := gcpserve.AcquirePort(r.ctx, pool, "ocicontainerinstances")
			if perr != nil {
				return fmt.Errorf("allocate host port for health check port %d: %w", h.Port, perr)
			}

			r.hostPorts[key] = hp
		}
	}

	return nil
}

func (e *Engine) spec(r *run, c *Container) container.Spec {
	sp := container.Spec{
		Image: c.ImageURL, WorkingDir: c.WorkingDir, Cmd: c.Args, Mounts: r.scratch.binds(c.Mounts),
		User: securityUser(c.Security),
		Name: "gopherstack-oci-ci-" + e.nonce + "-" + strconv.FormatUint(e.seq.Add(1), 10),
	}

	if sp.User == "" {
		sp.User = securityUser(r.inst.Security)
	}

	if len(c.Command) > 0 {
		sp.Entrypoint = c.Command
	}

	for k, v := range c.Env {
		sp.Env = append(sp.Env, k+"="+v)
	}

	if rc := c.Resource; rc != nil {
		if rc.VcpusLimit != nil {
			sp.NanoCPUs = int64(float64(*rc.VcpusLimit) * nanoPerCPU)
		}

		if rc.MemoryLimitInGBs != nil {
			sp.MemoryBytes = int64(float64(*rc.MemoryLimitInGBs) * bytesPerGB)
		}
	}

	seen := map[int]bool{}

	for _, h := range c.Health {
		if seen[h.Port] {
			continue
		}

		seen[h.Port] = true
		hp := r.hostPorts[c.ID+":"+strconv.Itoa(h.Port)]
		sp.Ports = append(sp.Ports, container.PortSpec(container.LoopbackHost, strconv.Itoa(hp), strconv.Itoa(h.Port)))
	}

	return sp
}

func credentialFor(ref string, creds []Credential) *Credential {
	host := strs.Fold(registryauth.HostOf(ref))

	for _, c := range creds {
		if strs.Fold(strings.TrimSuffix(c.Endpoint, "/")) == host {
			return &c
		}
	}

	return nil
}

func (e *Engine) launchOne(r *run, c *Container) (string, error) {
	if err := r.rt.Pull(r.ctx, c.ImageURL, credentialFor(c.ImageURL, r.creds)); err != nil {
		return "", fmt.Errorf("pull image %q: %w", c.ImageURL, err)
	}

	e.mu.Lock("launchOne")
	spec := e.spec(r, c)
	limit := e.startup
	e.mu.Unlock()

	// not r.ctx: cancelling mid-create can orphan a container the daemon still creates
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.ctx), limit)
	defer cancel()

	id, err := r.rt.Start(ctx, spec)
	if err != nil {
		return "", fmt.Errorf("start container: %w", err)
	}

	return id, nil
}

func restartWanted(policy string, exit int) bool {
	switch policy {
	case restartAlways:
		return true
	case restartOnFailure:
		return exit != 0
	}

	return false
}

// supervise runs one container and applies the restart policy until it ends or the instance is halted.
func (e *Engine) supervise(r *run, idx int, results chan<- error) {
	c := r.inst.Containers[idx]
	first, restarts := true, 0

	for {
		id, err := e.launchOne(r, c)
		if first {
			results <- err

			first = false
		}

		if err != nil {
			e.markFailed(r, c, err)

			return
		}

		e.mu.Lock("supervise")
		r.docker[c.ID] = id
		e.mu.Unlock()

		e.markRunning(r, c, restarts)

		stopHealth := e.watchHealth(r, c)
		code, werr := r.rt.Wait(r.ctx, id)

		stopHealth()

		if r.ctx.Err() != nil {
			return
		}

		e.mu.RLock("supervise")
		killed := r.killed[c.ID]
		e.mu.RUnlock()

		if werr != nil && !killed {
			e.markExited(r, c, 0, werr)

			return
		}

		if killed {
			code = killedExitCode
		}

		restart := restartWanted(r.inst.Restart, code)
		e.markExited(r, c, code, nil)

		if !restart {
			e.markInstanceIfDone(r)

			return
		}

		restarts++

		e.removeContainer(r, c, id)

		if !e.sleep(r, e.backoff(restarts)) {
			return
		}
	}
}

func (e *Engine) sleep(r *run, d time.Duration) bool {
	if d <= 0 {
		return r.ctx.Err() == nil
	}

	ch, stop := e.timer(d)
	defer stop()

	select {
	case <-ch:
		return true
	case <-r.ctx.Done():
		return false
	}
}

func (e *Engine) markRunning(r *run, c *Container, restarts int) {
	now := e.sink.Now()

	e.sink.Mutate(r.inst.ID, func(in *Instance) {
		if cc := findCtr(in, c.ID); cc != nil {
			cc.State, cc.Details, cc.ExitCode, cc.Terminated, cc.Updated = stateActive, "", nil, nil, &now
			cc.RestartCount = restarts

			for _, h := range cc.Health {
				h.Status, h.StatusDetails, h.LastChecked = healthUnknown, "", nil
			}
		}
	})
}

func (e *Engine) markFailed(r *run, c *Container, err error) {
	now := e.sink.Now()

	e.sink.Mutate(r.inst.ID, func(in *Instance) {
		if cc := findCtr(in, c.ID); cc != nil {
			cc.State, cc.Details, cc.Updated = stateFailed, err.Error(), &now
		}
	})
}

func (e *Engine) markExited(r *run, c *Container, code int, werr error) {
	now := e.sink.Now()

	e.sink.Mutate(r.inst.ID, func(in *Instance) {
		cc := findCtr(in, c.ID)
		if cc == nil {
			return
		}

		cc.State, cc.Updated, cc.Terminated = stateInactive, &now, &now

		if werr != nil {
			cc.Details = werr.Error()

			return
		}

		cc.ExitCode = &code
		cc.Details = "Exited with code " + strconv.Itoa(code)
	})
}

// markInstanceIfDone moves the instance to INACTIVE once every container has terminated for good.
func (e *Engine) markInstanceIfDone(r *run) {
	e.sink.Mutate(r.inst.ID, func(in *Instance) {
		if in.State != stateActive {
			return
		}

		for _, c := range in.Containers {
			if c.State != stateInactive {
				return
			}
		}

		in.State, in.Details, in.Etag = stateInactive, "All containers have terminated", newEtag()
	})
}

func findCtr(in *Instance, id string) *Container {
	for _, c := range in.Containers {
		if c.ID == id {
			return c
		}
	}

	return nil
}

func (e *Engine) removeContainer(r *run, c *Container, id string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.ctx), teardownTimeout)
	defer cancel()

	logs, _ := r.rt.Logs(ctx, id)

	e.mu.Lock("removeContainer")
	if logs != "" {
		r.prev[c.ID] = tailBytes(logs)
	}

	delete(r.docker, c.ID)
	e.mu.Unlock()

	if err := r.rt.Remove(ctx, id); err != nil {
		logger.Load(ctx).
			WarnContext(ctx, "ocicontainerinstances: container removal failed", "container", id, "error", err)
	}
}

func tailBytes(s string) string {
	if len(s) > maxLogBytes {
		return s[len(s)-maxLogBytes:]
	}

	return s
}

// halt stops the instance's containers and frees its volumes and ports; it keeps their last logs.
func (e *Engine) halt(instID string) {
	e.mu.Lock("halt")
	r := e.runs[instID]
	delete(e.runs, instID)
	e.mu.Unlock()

	if r == nil {
		return
	}

	r.cancel()
	r.wg.Wait()

	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.ctx), teardownTimeout)
	defer cancel()

	e.mu.Lock("halt")
	ids := make(map[string]string, len(r.docker))
	maps.Copy(ids, r.docker)
	e.mu.Unlock()

	for ctr, id := range ids {
		logs, _ := r.rt.Logs(ctx, id)

		e.mu.Lock("halt")
		e.last[ctr] = tailBytes(logs)
		e.mu.Unlock()

		if err := r.rt.Remove(ctx, id); err != nil {
			logger.Load(ctx).WarnContext(ctx, "ocicontainerinstances: container removal failed",
				"container", id, "error", err)
		}
	}

	e.mu.RLock("halt")
	pool := e.ports
	e.mu.RUnlock()

	for _, hp := range r.hostPorts {
		gcpserve.ReleasePort(pool, hp)
	}

	r.scratch.removeAll()
}

// HaltAsync stops the instance's containers in the background and then calls done.
func (e *Engine) HaltAsync(instID string, done func()) {
	e.wg.Go(func() {
		e.halt(instID)
		done()
	})
}

// HaltAll stops every instance.
func (e *Engine) HaltAll() {
	e.mu.RLock("HaltAll")
	ids := make([]string, 0, len(e.runs))
	for id := range e.runs {
		ids = append(ids, id)
	}
	e.mu.RUnlock()

	for _, id := range ids {
		e.halt(id)
	}
}

// Close stops every instance and the engine.
func (e *Engine) Close() {
	e.cancel()
	e.wg.Wait()
	e.HaltAll()

	if e.onClose != nil {
		e.onClose()
	}
}

// Logs returns the container's stdout and stderr, from the previous run when previous is set.
func (e *Engine) Logs(instID, ctrID string, previous bool) (string, error) {
	e.mu.RLock("Logs")
	r := e.runs[instID]

	var (
		id   string
		prev string
		rt   Runtime
		ok   bool
	)

	if r != nil {
		id, rt = r.docker[ctrID], r.rt
		prev, ok = r.prev[ctrID]
	}

	last, haveLast := e.last[ctrID]
	e.mu.RUnlock()

	switch {
	case previous && ok:
		return prev, nil
	case previous:
		return "", errNoPreviousRun
	case id != "" && rt != nil:
		ctx, cancel := context.WithTimeout(e.ctx, teardownTimeout)
		defer cancel()

		out, err := rt.Logs(ctx, id)

		return tailBytes(out), err
	case haveLast:
		return last, nil
	}

	return "", errNoLogsYet
}
