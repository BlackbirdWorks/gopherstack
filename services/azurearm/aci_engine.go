package azurearm

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	aciStateWaiting    = "Waiting"
	aciStateRunning    = "Running"
	aciStateTerminated = "Terminated"
	aciStateSucceeded  = "Succeeded"
	aciStateFailed     = "Failed"
	aciStateStopped    = "Stopped"
	aciStatePending    = "Pending"
	aciEventNormal     = "Normal"
	aciEventWarning    = "Warning"
	aciTeardownTimeout = 20 * time.Second
	aciBackoffBase     = 10 * time.Second
	aciBackoffMax      = 5 * time.Minute
	aciDefaultStartup  = 10 * time.Minute
	aciNanoPerCPU      = 1e9
	aciBytesPerGB      = 1 << 30
	aciNonceLen        = 6
)

// Sentinel errors returned by the engine.
var (
	ErrACIGroupNotFound     = errors.New("azurearm: container group not found")
	ErrACIContainerNotFound = errors.New("azurearm: container not found in the group")
	ErrACINotRunning        = errors.New("azurearm: container has no logs: never started, or the group is stopped")
)

// ACIEvent is one entry of an instanceView events list.
type ACIEvent struct {
	First   time.Time
	Last    time.Time
	Name    string
	Type    string
	Message string
	Count   int
}

// ACIState is a container's currentState or previousState.
type ACIState struct {
	StartTime  *time.Time
	FinishTime *time.Time
	ExitCode   *int
	State      string
	Detail     string
}

// ACIContainerView is a container's instanceView.
type ACIContainerView struct {
	Prev     *ACIState
	Cur      ACIState
	Name     string
	Events   []ACIEvent
	Restarts int
}

// ACIView is the live state of one container group.
type ACIView struct {
	HostPorts  map[int]int
	State      string
	Failure    string
	Events     []ACIEvent
	Containers []ACIContainerView
}

type aciConfig struct {
	Provider *ContainerInstanceProvider
	Key      string
	Name     string
	Props    aciPropsWire
}

type aciCont struct {
	startErr error
	ready    chan struct{}
	prev     *ACIState
	cur      ACIState
	name     string
	id       string
	events   []ACIEvent
	restarts int
}

type shareMount struct {
	dir      string
	account  string
	share    string
	readOnly bool
}

type aciRun struct {
	ctx       context.Context //nolint:containedctx // lifetime of the group's containers
	cancel    context.CancelFunc
	rt        ACIRuntime
	hostPorts map[int]int
	scratch   map[string]string
	failure   string
	conts     []*aciCont
	events    []ACIEvent
	shares    []shareMount
	allDirs   []string
	wg        sync.WaitGroup
	stopped   bool
}

// ACIEngine runs container groups in the container runtime.
type ACIEngine struct {
	ports    ACIPortPool
	ctx      context.Context
	shares   ACIFileShares
	backoff  func(restarts int) time.Duration
	runtime  func() (ACIRuntime, error)
	now      func() time.Time
	cancel   context.CancelFunc
	mu       *lockmetrics.RWMutex
	runs     map[string]*aciRun
	onClose  func()
	disabled string
	nonce    string
	startup  time.Duration
	seq      atomic.Uint64
}

// NewACIEngine creates an engine; runtime nil makes every group fail with disabledReason (or a default reason).
func NewACIEngine(runtime func() (ACIRuntime, error), disabledReason string) *ACIEngine {
	ctx, cancel := context.WithCancel(context.Background())

	return &ACIEngine{
		ctx: ctx, cancel: cancel, runtime: runtime, disabled: disabledReason, now: time.Now,
		mu: lockmetrics.New("azurearm.aci"), runs: map[string]*aciRun{}, startup: aciDefaultStartup,
		backoff: defaultACIBackoff, nonce: awsid.NewN("", aciNonceLen) + "-",
	}
}

func defaultACIBackoff(restarts int) time.Duration {
	d := aciBackoffBase

	for i := 1; i < restarts && d < aciBackoffMax; i++ {
		d *= 2
	}

	return min(d, aciBackoffMax)
}

// SetOnClose registers a hook run by Close after every group is stopped.
func (e *ACIEngine) SetOnClose(fn func()) {
	e.mu.Lock("SetOnClose")
	defer e.mu.Unlock()

	e.onClose = fn
}

// SetPortPool wires the host port source for published ports.
func (e *ACIEngine) SetPortPool(p ACIPortPool) {
	e.mu.Lock("SetPortPool")
	defer e.mu.Unlock()

	e.ports = p
}

// SetFileShares wires the Azure Files seam used by azureFile volumes.
func (e *ACIEngine) SetFileShares(s ACIFileShares) {
	e.mu.Lock("SetFileShares")
	defer e.mu.Unlock()

	e.shares = s
}

// SetTiming overrides the restart backoff and the startup bound; for tests and tuning.
func (e *ACIEngine) SetTiming(backoff func(int) time.Duration, startup time.Duration) {
	e.mu.Lock("SetTiming")
	defer e.mu.Unlock()

	if backoff != nil {
		e.backoff = backoff
	}

	if startup > 0 {
		e.startup = startup
	}
}

func (e *ACIEngine) record(events *[]ACIEvent, typ, name, msg string) {
	now := e.now().UTC()

	if n := len(*events); n > 0 && (*events)[n-1].Name == name && (*events)[n-1].Message == msg {
		(*events)[n-1].Count++
		(*events)[n-1].Last = now

		return
	}

	*events = append(*events, ACIEvent{Name: name, Type: typ, Message: msg, Count: 1, First: now, Last: now})
}

// SetRuntime replaces the container runtime source; nil disables the engine.
func (e *ACIEngine) SetRuntime(fn func() (ACIRuntime, error)) {
	e.mu.Lock("SetRuntime")
	defer e.mu.Unlock()

	e.runtime = fn
}

func (e *ACIEngine) resolveRuntime() (ACIRuntime, string) {
	e.mu.RLock("resolveRuntime")
	source, disabled := e.runtime, e.disabled
	e.mu.RUnlock()

	if source == nil {
		if disabled != "" {
			return nil, disabled
		}

		return nil, "the container engine is disabled; container groups cannot run"
	}

	rt, err := source()
	if err != nil {
		return nil, fmt.Sprintf("the container runtime is unavailable: %v", err)
	}

	return rt, ""
}

// Deploy (re)creates the group's containers and returns once each has started or failed.
// Failures are recorded on the group's view, not returned.
func (e *ACIEngine) Deploy(cfg aciConfig) {
	e.Remove(cfg.Key)

	run := &aciRun{hostPorts: map[int]int{}, scratch: map[string]string{}}
	run.ctx, run.cancel = context.WithCancel(e.ctx)

	for _, c := range cfg.Props.Containers {
		run.conts = append(run.conts, &aciCont{
			name: c.Name, ready: make(chan struct{}), cur: ACIState{State: aciStateWaiting},
		})
	}

	e.mu.Lock("Deploy")
	e.runs[cfg.Key] = run
	e.mu.Unlock()

	rt, reason := e.resolveRuntime()
	if reason == "" {
		run.rt = rt
		reason = e.prepare(run, cfg)
	}

	if reason != "" {
		e.fail(run, reason)

		return
	}

	for i, c := range run.conts {
		run.wg.Go(func() { e.supervise(run, cfg, i, c) })
	}

	e.awaitStart(run)
}

func (e *ACIEngine) fail(run *aciRun, reason string) {
	e.mu.Lock("fail")
	if run.failure == "" {
		run.failure = reason
		e.record(&run.events, aciEventWarning, aciStateFailed, reason)
	}
	e.mu.Unlock()

	e.teardown(run)
}

func (e *ACIEngine) awaitStart(run *aciRun) {
	e.mu.RLock("awaitStart")
	limit := e.startup
	e.mu.RUnlock()

	timer := time.NewTimer(limit)
	defer timer.Stop()

	for _, c := range run.conts {
		select {
		case <-c.ready:
		case <-timer.C:
			e.fail(run, fmt.Sprintf("containers did not start within %s", limit))

			return
		case <-run.ctx.Done():
			return
		}

		e.mu.RLock("awaitStart")
		err := c.startErr
		e.mu.RUnlock()

		if err != nil {
			e.fail(run, fmt.Sprintf("container %s: %v", c.name, err))

			return
		}
	}
}

func (e *ACIEngine) containerSpec(run *aciRun, cfg aciConfig, c aciContainerWire, n uint64) container.Spec {
	p := c.Properties
	spec := container.Spec{
		Image: p.Image,
		Name: "gopherstack-aci-" + strs.Fold(
			cfg.Name,
		) + "-" + strs.Fold(
			c.Name,
		) + "-" + e.nonce + strconv.FormatUint(
			n,
			10,
		),
	}

	if len(p.Command) > 0 {
		spec.Entrypoint = p.Command
	}

	spec.Cmd = p.Args

	for _, ev := range p.EnvironmentVariables {
		v := ""
		if ev.Value != nil {
			v = *ev.Value
		} else if ev.SecureValue != nil {
			v = *ev.SecureValue
		}

		spec.Env = append(spec.Env, ev.Name+"="+v)
	}

	if l := limitsOf(p.Resources); l != nil {
		spec.NanoCPUs = int64(l.CPU * aciNanoPerCPU)
		spec.MemoryBytes = int64(l.MemoryInGB * aciBytesPerGB)
	}

	for _, m := range p.VolumeMounts {
		bind := run.scratch[strings.ToLower(m.Name)] + ":" + m.MountPath
		if m.ReadOnly || run.readOnlyVolume(cfg, m.Name) {
			bind += ":ro"
		}

		spec.Mounts = append(spec.Mounts, bind)
	}

	for _, pt := range p.Ports {
		if hp, ok := run.hostPorts[pt.Port]; ok {
			spec.Ports = append(
				spec.Ports,
				container.PortSpec(container.LoopbackHost, strconv.Itoa(hp), strconv.Itoa(pt.Port)),
			)
		}
	}

	return spec
}

func limitsOf(r *aciResourcesWire) *aciResourceWire {
	if r == nil || r.Limits == nil || (r.Limits.CPU <= 0 && r.Limits.MemoryInGB <= 0) {
		return nil
	}

	return r.Limits
}

func (r *aciRun) readOnlyVolume(cfg aciConfig, name string) bool {
	for _, v := range cfg.Props.Volumes {
		if strings.EqualFold(v.Name, name) {
			return v.Secret != nil || (v.AzureFile != nil && v.AzureFile.ReadOnly)
		}
	}

	return false
}

// launch pulls the image if needed and starts one container; it returns the container ID.
func (e *ACIEngine) launch(run *aciRun, cfg aciConfig, idx int, c *aciCont) (string, error) {
	wire := cfg.Props.Containers[idx]
	image := wire.Properties.Image

	e.mu.Lock("launch")
	spec := e.containerSpec(run, cfg, wire, e.seq.Add(1))
	e.mu.Unlock()

	e.mu.Lock("launch")
	e.record(&c.events, aciEventNormal, "Pulling", fmt.Sprintf("pulling image %q", image))
	e.mu.Unlock()

	pulled, err := run.rt.Pull(run.ctx, image, credentialFor(image, cfg.Props.ImageRegistryCredentials))
	if err != nil {
		return "", fmt.Errorf("pull image %q: %w", image, err)
	}

	e.mu.Lock("launch")
	if pulled {
		e.record(&c.events, aciEventNormal, "Pulled", fmt.Sprintf("successfully pulled image %q", image))
	} else {
		e.record(&c.events, aciEventNormal, "Pulled", fmt.Sprintf("image %q is present", image))
	}
	e.mu.Unlock()

	e.mu.RLock("launch")
	limit := e.startup
	e.mu.RUnlock()

	// not run.ctx: cancelling mid-create can orphan a container the daemon still creates
	startCtx, cancel := context.WithTimeout(context.WithoutCancel(run.ctx), limit)
	defer cancel()

	id, err := run.rt.Start(startCtx, spec)
	if err != nil {
		return "", fmt.Errorf("start container: %w", err)
	}

	e.mu.Lock("launch")
	e.record(&c.events, aciEventNormal, "Created", "created container "+c.name)
	e.record(&c.events, aciEventNormal, "Started", "started container "+c.name)
	e.mu.Unlock()

	return id, nil
}

func restartWanted(policy string, exit int) bool {
	switch policy {
	case aciRestartAlways:
		return true
	case aciRestartOnFailure:
		return exit != 0
	}

	return false
}

// supervise runs one container and applies the group's restart policy until it ends or the group is torn down.
func (e *ACIEngine) supervise(run *aciRun, cfg aciConfig, idx int, c *aciCont) {
	first := true

	for {
		id, err := e.launch(run, cfg, idx, c)

		e.mu.Lock("supervise")
		if err == nil {
			c.id = id
			now := e.now().UTC()
			c.cur = ACIState{State: aciStateRunning, StartTime: &now}
		} else {
			c.cur = ACIState{State: aciStateWaiting, Detail: err.Error()}
			e.record(&c.events, aciEventWarning, aciStateFailed, err.Error())
		}

		if first {
			c.startErr = err
			close(c.ready)

			first = false
		}
		e.mu.Unlock()

		if err != nil || run.ctx.Err() != nil {
			return
		}

		code, werr := run.rt.Wait(run.ctx, id)
		if werr != nil {
			e.noteWaitFailure(run, c, werr)

			return
		}

		if !e.afterExit(run, cfg, c, code) {
			return
		}

		e.removeContainer(run, c, id)

		if !e.sleep(run.ctx, e.backoffFor(c)) {
			return
		}
	}
}

func (e *ACIEngine) noteWaitFailure(run *aciRun, c *aciCont, err error) {
	if run.ctx.Err() != nil {
		return
	}

	e.mu.Lock("noteWaitFailure")
	defer e.mu.Unlock()

	now := e.now().UTC()
	c.cur = ACIState{State: aciStateTerminated, FinishTime: &now, Detail: err.Error()}
	e.record(&c.events, aciEventWarning, aciStateFailed, err.Error())
}

// afterExit records the exit and reports whether the container should be restarted.
func (e *ACIEngine) afterExit(run *aciRun, cfg aciConfig, c *aciCont, code int) bool {
	now := e.now().UTC()

	e.mu.Lock("afterExit")
	prev := c.cur
	prev.State, prev.FinishTime, prev.ExitCode = aciStateTerminated, &now, &code
	prev.Detail = "Completed"

	if code != 0 {
		prev.Detail = "Error"
	}

	c.cur = prev
	restart := run.ctx.Err() == nil && restartWanted(cfg.Props.RestartPolicy, code)

	if restart {
		c.restarts++
		p := prev
		c.prev = &p
		c.cur = ACIState{State: aciStateWaiting, Detail: "CrashLoopBackOff: Back-off restarting failed container"}
		e.record(&c.events, aciEventWarning, "BackOff", "back-off restarting container "+c.name)
	}
	e.mu.Unlock()

	e.syncShares(run)

	return restart
}

func (e *ACIEngine) backoffFor(c *aciCont) time.Duration {
	e.mu.RLock("backoffFor")
	defer e.mu.RUnlock()

	return e.backoff(c.restarts)
}

func (e *ACIEngine) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (e *ACIEngine) removeContainer(run *aciRun, c *aciCont, id string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(run.ctx), aciTeardownTimeout)
	defer cancel()

	if err := run.rt.Remove(ctx, id); err != nil {
		logger.Load(ctx).WarnContext(ctx, "azurearm: container removal failed", "container", id, "error", err)
	}

	e.mu.Lock("removeContainer")
	if c.id == id {
		c.id = ""
	}
	e.mu.Unlock()
}

// teardown stops the group's containers and frees its volumes and ports; the run record stays.
func (e *ACIEngine) teardown(run *aciRun) {
	run.cancel()
	run.wg.Wait()

	e.mu.Lock("teardown")
	ids := make([]*aciCont, 0, len(run.conts))
	ids = append(ids, run.conts...)
	e.mu.Unlock()

	for _, c := range ids {
		e.mu.RLock("teardown")
		id := c.id
		e.mu.RUnlock()

		if id != "" && run.rt != nil {
			e.removeContainer(run, c, id)
		}
	}

	e.syncShares(run)
	e.releaseResources(run)

	e.mu.Lock("teardown")
	now := e.now().UTC()

	for _, c := range run.conts {
		if c.cur.State != aciStateTerminated {
			c.cur = ACIState{State: aciStateTerminated, FinishTime: &now, Detail: "Stopped"}
		}
	}

	run.stopped = true
	e.mu.Unlock()
}

// Stop tears the group's containers down but keeps its record; later reads report Stopped.
func (e *ACIEngine) Stop(key string) {
	e.mu.RLock("Stop")
	run := e.runs[key]
	e.mu.RUnlock()

	if run != nil {
		e.teardown(run)
	}
}

// Remove stops the group and forgets it.
func (e *ACIEngine) Remove(key string) {
	e.mu.Lock("Remove")
	run := e.runs[key]
	delete(e.runs, key)
	e.mu.Unlock()

	if run != nil {
		e.teardown(run)
	}
}

// Close stops every group and the engine.
func (e *ACIEngine) Close() {
	e.cancel()
	e.RemoveAll()

	e.mu.RLock("Close")
	fn := e.onClose
	e.mu.RUnlock()

	if fn != nil {
		fn()
	}
}

// RemoveAll stops and forgets every group.
func (e *ACIEngine) RemoveAll() {
	e.mu.Lock("RemoveAll")
	runs := e.runs
	e.runs = map[string]*aciRun{}
	e.mu.Unlock()

	for _, r := range runs {
		e.teardown(r)
	}
}

// RemovePrefixed stops and forgets every group whose key starts with prefix.
func (e *ACIEngine) RemovePrefixed(prefix string) {
	e.removeWhere(func(k string) bool { return strings.HasPrefix(k, prefix) })
}

// RemoveUnprefixed stops and forgets every group whose key does not start with prefix.
func (e *ACIEngine) RemoveUnprefixed(prefix string) {
	e.removeWhere(func(k string) bool { return !strings.HasPrefix(k, prefix) })
}

func (e *ACIEngine) removeWhere(match func(key string) bool) {
	e.mu.Lock("removeWhere")

	var doomed []*aciRun

	for k, r := range e.runs {
		if match(k) {
			doomed = append(doomed, r)

			delete(e.runs, k)
		}
	}
	e.mu.Unlock()

	for _, r := range doomed {
		e.teardown(r)
	}
}

// Active reports whether the group has a live (not stopped, not failed) run.
func (e *ACIEngine) Active(key string) bool {
	e.mu.RLock("Active")
	defer e.mu.RUnlock()

	r := e.runs[key]

	return r != nil && !r.stopped && r.failure == ""
}

// Logs returns the container's logs, limited to the last tail lines when tail is positive.
func (e *ACIEngine) Logs(ctx context.Context, key, name string, tail int) (string, error) {
	e.mu.RLock("Logs")
	run := e.runs[key]

	var (
		id string
		rt ACIRuntime
		ok bool
	)

	if run != nil {
		rt = run.rt

		for _, c := range run.conts {
			if strings.EqualFold(c.name, name) {
				id, ok = c.id, true
			}
		}
	}
	e.mu.RUnlock()

	switch {
	case run == nil:
		return "", ErrACIGroupNotFound
	case !ok:
		return "", ErrACIContainerNotFound
	case id == "" || rt == nil:
		return "", ErrACINotRunning
	}

	spec := ""
	if tail > 0 {
		spec = strconv.Itoa(tail)
	}

	return rt.Logs(ctx, id, spec)
}

// View returns the live state of the group.
func (e *ACIEngine) View(key string) (ACIView, bool) {
	e.mu.RLock("View")
	defer e.mu.RUnlock()

	run := e.runs[key]
	if run == nil {
		return ACIView{}, false
	}

	v := ACIView{
		Failure: run.failure, HostPorts: map[int]int{}, Events: slices.Clone(run.events),
		State: groupState(run),
	}

	maps.Copy(v.HostPorts, run.hostPorts)

	for _, c := range run.conts {
		cv := ACIContainerView{Name: c.name, Restarts: c.restarts, Cur: c.cur, Events: slices.Clone(c.events)}
		if c.prev != nil {
			p := *c.prev
			cv.Prev = &p
		}

		v.Containers = append(v.Containers, cv)
	}

	return v, true
}

func groupState(run *aciRun) string {
	switch {
	case run.failure != "":
		return aciStateFailed
	case run.stopped:
		return aciStateStopped
	}

	waiting, failed := false, false

	for _, c := range run.conts {
		switch c.cur.State {
		case aciStateRunning:
			return aciStateRunning
		case aciStateWaiting:
			waiting = true
		case aciStateTerminated:
			failed = failed || (c.cur.ExitCode != nil && *c.cur.ExitCode != 0) || c.cur.ExitCode == nil
		}
	}

	switch {
	case waiting:
		return aciStatePending
	case failed:
		return aciStateFailed
	}

	return aciStateSucceeded
}
