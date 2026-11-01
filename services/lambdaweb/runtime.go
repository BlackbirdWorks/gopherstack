package lambdaweb

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const teardownLimit = 60 * time.Second

// Environment is one running execution environment: an HTTP app serving a revision.
type Environment interface {
	// Addr is the host and port the app serves plain HTTP on.
	Addr() (string, int)
	// LogStream names the CloudWatch Logs stream carrying the environment's output.
	LogStream() string
	// Close stops the environment and releases its resources.
	Close(ctx context.Context)
}

// LaunchSpec identifies the revision an environment is started for.
type LaunchSpec struct {
	Revision *Revision
}

// Launcher starts execution environments and returns once the app accepts connections.
type Launcher interface {
	Launch(ctx context.Context, spec LaunchSpec) (Environment, error)
}

type slot struct {
	ready chan struct{}
	env   Environment
	err   error
}

type window struct {
	second int64
	count  int32
}

// runtimeState owns the execution environments, the per-endpoint rate windows and the routing randomness.
type runtimeState struct {
	launcher Launcher
	client   *http.Client
	slots    map[string]*slot
	windows  map[string]*window
	inflight map[string]int32
	intn     func(int) int
	mu       *lockmetrics.RWMutex
	wg       sync.WaitGroup
	closed   bool
}

func newRuntimeState() *runtimeState {
	return &runtimeState{
		slots: map[string]*slot{}, windows: map[string]*window{}, inflight: map[string]int32{},
		intn: rand.IntN, mu: lockmetrics.New("lambdaweb.runtime"),
	}
}

func envKey(region, fn, rev string) string { return region + "/" + fn + "/" + rev }

// SetLauncher enables the data plane: requests to Active endpoints run in launcher-built environments.
func (b *InMemoryBackend) SetLauncher(l Launcher) {
	b.run.mu.Lock("SetLauncher")
	defer b.run.mu.Unlock()

	b.run.launcher = l
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

// admit applies the endpoint's rate limit (requests per second) and environment capacity.
// It returns the platform error type that rejects the request, or "" with a release func.
func (r *runtimeState) admit(key string, rateLimit, capacity *int32, now time.Time) (string, func()) {
	r.mu.Lock("admit")
	defer r.mu.Unlock()

	if rateLimit != nil {
		if *rateLimit == 0 {
			return errEndpointDisabled, nil
		}

		w := r.windows[key]
		if w == nil || w.second != now.Unix() {
			w = &window{second: now.Unix()}
			r.windows[key] = w
		}

		if w.count >= *rateLimit {
			return errEndpointRateThrottle, nil
		}

		w.count++
	}

	if capacity != nil && r.inflight[key] >= *capacity {
		return errEndpointMaxEnvThrottle, nil
	}

	r.inflight[key]++

	return "", func() {
		r.mu.Lock("release")
		defer r.mu.Unlock()

		r.inflight[key]--
	}
}

// environment returns the revision's running environment, launching it on first use.
func (r *runtimeState) environment(ctx context.Context, rev *Revision) (Environment, error) {
	key := envKey(rev.Region, rev.FunctionName, rev.ID)

	r.mu.Lock("environment")

	if r.closed || r.launcher == nil {
		r.mu.Unlock()

		return nil, errEngineUnavailable
	}

	s, ok := r.slots[key]
	if !ok {
		s = &slot{ready: make(chan struct{})}
		r.slots[key] = s
		launcher := r.launcher

		r.wg.Go(func() { r.launch(launcher, key, s, rev) })
	}

	r.mu.Unlock()

	select {
	case <-s.ready:
	case <-ctx.Done():
		return nil, fmt.Errorf("waiting for execution environment: %w", ctx.Err())
	}

	return s.env, s.err
}

func (r *runtimeState) launch(l Launcher, key string, s *slot, rev *Revision) {
	defer close(s.ready)

	env, err := l.Launch(context.Background(), LaunchSpec{Revision: rev})

	r.mu.Lock("launched")
	current := r.slots[key] == s

	switch {
	case err != nil:
		s.err = err
		if current {
			delete(r.slots, key)
		}
	case current:
		s.env = env
	default:
		s.err = errEngineUnavailable
	}
	r.mu.Unlock()

	if err == nil && !current {
		teardown(env)
	}
}

func teardown(env Environment) {
	ctx, cancel := context.WithTimeout(context.Background(), teardownLimit)
	defer cancel()

	env.Close(ctx)
}

var errEngineUnavailable = errors.New("web function execution is disabled (engine is stub)")

func (r *runtimeState) stopMatching(match func(key string) bool) {
	if r == nil {
		return
	}

	r.mu.Lock("stop")

	var stop []Environment

	for key, s := range r.slots {
		if !match(key) {
			continue
		}

		delete(r.slots, key)

		if s.env != nil {
			stop = append(stop, s.env)
		}
	}

	r.mu.Unlock()

	for _, env := range stop {
		teardown(env)
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
	r.inflight = map[string]int32{}
	r.mu.Unlock()
}

// Close stops every execution environment and refuses new launches.
func (b *InMemoryBackend) Close() {
	b.run.mu.Lock("Close")
	b.run.closed = true
	b.run.mu.Unlock()

	b.run.stopAll()
	b.run.wg.Wait()
}
