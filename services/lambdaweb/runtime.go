package lambdaweb

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	teardownLimit      = 60 * time.Second
	reportInterval     = time.Minute
	sweepInterval      = 5 * time.Second
	defaultIdleTimeout = 5 * time.Minute
	vcpusPerEnv        = 2
	instanceIDLen      = 32
	streamSuffixLen    = 32
)

// Environment is one running execution environment: an HTTP app serving a revision.
type Environment interface {
	// Addr is the host and port the app serves plain HTTP on.
	Addr() (string, int)
	// Close stops the environment and releases its resources.
	Close(ctx context.Context)
}

// LaunchSpec identifies the revision an environment is started for.
type LaunchSpec struct {
	Revision *Revision
	// Output receives each line the app writes to stdout or stderr; nil drops the output.
	Output func(stderr bool, line string)
	// LogStream is the CloudWatch Logs stream the environment's events go to.
	LogStream string
}

// Launcher starts execution environments and returns once the app accepts connections.
type Launcher interface {
	Launch(ctx context.Context, spec LaunchSpec) (Environment, error)
}

type window struct {
	second int64
	count  int32
}

// instance is one execution environment of a revision.
type instance struct {
	started    time.Time
	lastUsed   time.Time
	lastReport time.Time
	ready      chan struct{}
	drained    chan struct{}
	env        Environment
	err        error
	ship       *shipper
	live       map[string]string
	rev        *Revision
	metrics    cwmetric.Emitter
	id         string
	stream     string
	key        string
	inflight   int32
	peak       int32
	windowReqs int64
	totalReqs  int64
	winBytes   int64
	draining   bool
}

// runtimeState owns the execution environments, the per-endpoint rate windows and the routing randomness.
type runtimeState struct {
	launcher  Launcher
	stopped   chan struct{}
	insts     map[string][]*instance
	windows   map[string]*window
	intn      func(int) int
	mu        *lockmetrics.RWMutex
	client    *http.Client
	clock     atomic.Pointer[func() time.Time]
	tick      func()
	wg        sync.WaitGroup
	lastSweep atomic.Int64
	idle      time.Duration
	closed    bool
	janitor   bool
}

func newRuntimeState() *runtimeState {
	return &runtimeState{
		insts: map[string][]*instance{}, windows: map[string]*window{}, stopped: make(chan struct{}),
		intn: rand.IntN, mu: lockmetrics.New("lambdaweb.runtime"), idle: defaultIdleTimeout,
	}
}

func (r *runtimeState) now() time.Time {
	if f := r.clock.Load(); f != nil {
		return (*f)()
	}

	return time.Now()
}

func envKey(region, fn, rev string) string { return region + "/" + fn + "/" + rev }

// SetLauncher enables the data plane: requests to Active endpoints run in launcher-built environments.
func (b *InMemoryBackend) SetLauncher(l Launcher) {
	b.run.mu.Lock("SetLauncher")
	defer b.run.mu.Unlock()

	b.run.launcher = l
}

// SetIdleTimeout sets how long an environment with no in-flight request lives before it is scaled down.
func (b *InMemoryBackend) SetIdleTimeout(d time.Duration) {
	b.run.mu.Lock("SetIdleTimeout")
	defer b.run.mu.Unlock()

	if d <= 0 {
		d = defaultIdleTimeout
	}

	b.run.idle = d
}

// SetRand replaces the revision-weight randomness: intn(n) returns a value in [0, n).
func (b *InMemoryBackend) SetRand(intn func(int) int) {
	b.run.mu.Lock("SetRand")
	defer b.run.mu.Unlock()

	if intn == nil {
		intn = rand.IntN
	}

	b.run.intn = intn
}

// pick chooses a revision by weight.
func (r *runtimeState) pick(weights []RevisionWeight) (string, bool) {
	if len(weights) == 0 {
		return "", false
	}

	r.mu.RLock("pick")
	intn := r.intn
	r.mu.RUnlock()

	roll := intn(percentTotal)
	acc := 0

	for _, w := range weights {
		acc += int(w.Weight)
		if roll < acc {
			return w.RevisionID, true
		}
	}

	return weights[len(weights)-1].RevisionID, true
}

// admitRate applies the endpoint's rate limit (requests per second); it returns the rejecting error type or "".
func (r *runtimeState) admitRate(key string, rateLimit *int32, now time.Time) string {
	if rateLimit == nil {
		return ""
	}

	if *rateLimit == 0 {
		return errEndpointDisabled
	}

	r.mu.Lock("admitRate")
	defer r.mu.Unlock()

	w := r.windows[key]
	if w == nil || w.second != now.Unix() {
		w = &window{second: now.Unix()}
		r.windows[key] = w
	}

	if w.count >= *rateLimit {
		return errEndpointRateThrottle
	}

	w.count++

	return ""
}

// capacity bounds how far one request may scale a revision's fleet out.
type capacity struct {
	logs      LogSink
	metrics   cwmetric.Emitter
	preflight func(*Revision) error
	maxEnv    *int32
	reqID     string
	endpoint  string
	scope     []string
	regionMax int
	perEnv    int32
}

// lease is one request's hold on an environment slot.
type lease struct {
	inst    *instance
	r       *runtimeState
	reqID   string
	release sync.Once
}

func (l *lease) done(respBytes int64) {
	l.release.Do(func() { l.r.release(l.inst, l.reqID, respBytes) })
}

func (r *runtimeState) release(in *instance, reqID string, respBytes int64) {
	now := r.now()

	r.mu.Lock("release")
	in.inflight--
	in.windowReqs++
	in.totalReqs++
	in.winBytes += respBytes
	in.lastUsed = now
	delete(in.live, reqID)

	if in.draining && in.inflight == 0 {
		close(in.drained)
	}
	r.mu.Unlock()
}

// throttle names the limit that rejected a request to scale out.
type throttle int

const (
	throttleNone throttle = iota
	throttleMaxEnv
	throttleVCPU
)

func (t throttle) errType() string {
	if t == throttleVCPU {
		return errAccountVCPUThrottle
	}

	return errEndpointMaxEnvThrottle
}

func (t throttle) metric() string {
	if t == throttleVCPU {
		return "AccountActiveVCPUArm64Throttles"
	}

	return "EndpointActiveEnvironmentThrottles"
}

// acquire reserves a slot on a ready environment of rev, scaling out when every environment is full.
func (r *runtimeState) acquire(ctx context.Context, rev *Revision, c capacity) (*lease, throttle, error) {
	key := envKey(rev.Region, rev.FunctionName, rev.ID)

	r.mu.Lock("acquire")

	if r.closed || r.launcher == nil {
		r.mu.Unlock()

		return nil, throttleNone, errEngineUnavailable
	}

	in := r.freeLocked(key, c.perEnv)
	if in == nil {
		var th throttle

		if in, th = r.scaleOutLocked(rev, key, c); in == nil {
			r.mu.Unlock()

			return nil, th, nil
		}
	}

	in.inflight++
	in.peak = max(in.peak, in.inflight)
	in.live[c.reqID] = c.endpoint
	r.mu.Unlock()

	l := &lease{inst: in, r: r, reqID: c.reqID}

	select {
	case <-in.ready:
	case <-ctx.Done():
		l.done(0)

		return nil, throttleNone, fmt.Errorf("waiting for execution environment: %w", ctx.Err())
	}

	if in.err != nil {
		l.done(0)

		return nil, throttleNone, in.err
	}

	return l, throttleNone, nil
}

func (r *runtimeState) freeLocked(key string, perEnv int32) *instance {
	for _, in := range r.insts[key] {
		if !in.draining && in.inflight < perEnv {
			return in
		}
	}

	return nil
}

func (r *runtimeState) scaleOutLocked(rev *Revision, key string, c capacity) (*instance, throttle) {
	if c.maxEnv != nil && r.activeLocked(c.scope) >= int(*c.maxEnv) {
		return nil, throttleMaxEnv
	}

	if r.regionActiveLocked(rev.Region+"/") >= c.regionMax {
		return nil, throttleVCPU
	}

	now := r.now()
	in := &instance{
		ready: make(chan struct{}), drained: make(chan struct{}), live: map[string]string{}, rev: rev,
		id: awsid.NewN("", instanceIDLen), key: key, started: now, lastUsed: now, lastReport: now,
		stream:  now.UTC().Format("2006/01/02") + "/[" + rev.ID + "]" + awsid.NewN("", streamSuffixLen),
		metrics: c.metrics,
	}

	in.ship = newShipper(c.logs, r.now, rev, in.stream)
	in.ship.live = func() (string, string) { return r.attribution(in) }
	r.insts[key] = append(r.insts[key], in)

	launcher := r.launcher
	r.startJanitorLocked()
	r.wg.Go(func() { r.launch(launcher, in, c.preflight) })

	return in, throttleNone
}

// attribution reports the request a log line belongs to when exactly one is in flight.
func (r *runtimeState) attribution(in *instance) (string, string) {
	r.mu.RLock("attribution")
	defer r.mu.RUnlock()

	if len(in.live) != 1 {
		return "", ""
	}

	for id, ep := range in.live {
		return id, ep
	}

	return "", ""
}

func (r *runtimeState) activeLocked(scope []string) int {
	n := 0

	for _, key := range scope {
		for _, in := range r.insts[key] {
			if !in.draining {
				n++
			}
		}
	}

	return n
}

func (r *runtimeState) regionActiveLocked(prefix string) int {
	n := 0

	for key, list := range r.insts {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		for _, in := range list {
			if !in.draining {
				n++
			}
		}
	}

	return n
}

// activeEnvironments counts the non-draining environments of the revisions in scope.
func (r *runtimeState) activeEnvironments(scope []string) int {
	r.mu.RLock("activeEnvironments")
	defer r.mu.RUnlock()

	return r.activeLocked(scope)
}

func (r *runtimeState) containsLocked(in *instance) bool {
	return slices.Contains(r.insts[in.key], in)
}

func (r *runtimeState) removeLocked(in *instance) bool {
	list := r.insts[in.key]

	for i, x := range list {
		if x != in {
			continue
		}

		list = append(list[:i:i], list[i+1:]...)
		if len(list) == 0 {
			delete(r.insts, in.key)
		} else {
			r.insts[in.key] = list
		}

		return true
	}

	return false
}

func (r *runtimeState) launch(l Launcher, in *instance, preflight func(*Revision) error) {
	defer close(in.ready)

	start := r.now()

	in.ship.system("platform.initStart", levelInfo, map[string]any{
		keyFunctionName: in.rev.FunctionName, keyRevisionID: in.rev.ID, keyInstanceID: in.id,
		"initializationType": "native-http", "runtimeVersion": in.rev.Runtime,
	})

	env, err := r.start(l, in, preflight)

	report := map[string]any{
		keyStatus: statusSuccess, "initializationType": "native-http",
		keyMetrics: map[string]any{keyTotalMs: msSince(r.now(), start)},
	}
	level := levelInfo

	if err != nil {
		report[keyStatus], report["errorType"], level = "failure", launchErrorType(err), levelWarn
	}

	in.ship.system("platform.initReport", level, report)

	r.mu.Lock("launched")
	current := r.containsLocked(in)

	switch {
	case err != nil:
		in.err = err

		if current {
			r.removeLocked(in)
		}
	case current:
		in.env = env
		in.lastUsed, in.lastReport = r.now(), r.now()
	default:
		in.err = errEngineUnavailable
	}
	r.mu.Unlock()

	switch {
	case err != nil:
	case current:
		in.ship.system("platform.invokeStart", levelInfo, map[string]any{
			keyFunctionName: in.rev.FunctionName, keyRevisionID: in.rev.ID, keyInstanceID: in.id,
		})
	default:
		teardown(env)
	}
}

func (r *runtimeState) start(l Launcher, in *instance, preflight func(*Revision) error) (Environment, error) {
	if preflight != nil {
		if err := preflight(in.rev); err != nil {
			return nil, err
		}
	}

	return l.Launch(
		context.Background(),
		LaunchSpec{Revision: in.rev, LogStream: in.stream, Output: in.ship.application},
	)
}

func msSince(now, start time.Time) float64 { return float64(now.Sub(start).Microseconds()) / 1000 } //nolint:mnd // ms

func launchErrorType(err error) string {
	switch {
	case errors.Is(err, errCannotAssumeRole):
		return errCannotAssumeRoleType
	case errors.Is(err, errKMSDecryption):
		return errKMSDecryptionType
	case errors.Is(err, ErrCodeUnavailable):
		return "Runtime.ImportCodeError"
	default:
		return errTimeoutListen
	}
}

func teardown(env Environment) {
	ctx, cancel := context.WithTimeout(context.Background(), teardownLimit)
	defer cancel()

	env.Close(ctx)
}

var errEngineUnavailable = errors.New("web function execution is disabled (engine is stub)")

// retire stops a removed environment and reports its serving period.
func (r *runtimeState) retire(in *instance) {
	if in.env == nil {
		return
	}

	now := r.now()
	in.emitEnvironmentMetrics(r.takeWindow(in, now), now)

	in.ship.system("platform.invokeReport", levelInfo, map[string]any{
		keyStatus: statusSuccess, keyInstanceID: in.id,
		keyMetrics: map[string]any{
			keyTotalMs: msSince(now, in.started), "totalRequests": in.totalReqs, "concurrentRequests": in.peak,
		},
	})
	in.ship.system("platform.spinDownStart", levelInfo, map[string]any{
		keyFunctionName: in.rev.FunctionName, keyRevisionID: in.rev.ID, keyInstanceID: in.id,
	})

	teardown(in.env)

	in.ship.system("platform.spinDownReport", levelInfo, map[string]any{
		keyStatus: statusSuccess, keyMetrics: map[string]any{keyTotalMs: msSince(r.now(), now)},
	})
}

// drain stops routing to in, lets its in-flight requests finish, then retires it.
func (r *runtimeState) drain(in *instance, limit time.Duration) {
	r.wg.Go(func() {
		timer := time.NewTimer(limit)
		defer timer.Stop()

		select {
		case <-in.drained:
		case <-timer.C:
		case <-r.stopped:
		}

		r.retire(in)
	})
}

func (r *runtimeState) stopMatching(match func(key string) bool) {
	if r == nil {
		return
	}

	r.mu.Lock("stop")

	var stop []*instance

	for key, list := range r.insts {
		if !match(key) {
			continue
		}

		delete(r.insts, key)

		stop = append(stop, list...)
	}

	r.mu.Unlock()

	for _, in := range stop {
		r.retire(in)
	}
}

func (r *runtimeState) stopRevision(region, fn, rev string) {
	want := envKey(region, fn, rev)
	r.stopMatching(func(key string) bool { return key == want })
}

func (r *runtimeState) stopFunction(region, fn string) {
	prefix := region + "/" + fn + "/"
	r.stopMatching(func(key string) bool { return strings.HasPrefix(key, prefix) })
}

func (r *runtimeState) stopAll() {
	if r == nil {
		return
	}

	r.stopMatching(func(string) bool { return true })

	r.mu.Lock("stopAll")
	r.windows = map[string]*window{}
	r.mu.Unlock()
}

// Close stops every execution environment and refuses new launches.
func (b *InMemoryBackend) Close() {
	b.run.mu.Lock("Close")
	if !b.run.closed {
		b.run.closed = true
		close(b.run.stopped)
	}
	b.run.mu.Unlock()

	b.run.stopAll()
	b.run.wg.Wait()
}
