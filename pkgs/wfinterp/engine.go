package wfinterp

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"time"
)

// Clock supplies time to a running workflow; tests inject a fake.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) error
}

// RealClock is the wall clock.
type RealClock struct{}

// Now returns the current time.
func (RealClock) Now() time.Time { return time.Now() }

// Sleep waits for d or until ctx is done.
func (RealClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// LogEntry is one sys.log call.
type LogEntry struct {
	JSON     map[string]any
	Severity string
	Text     string
}

// Host connects a running workflow to its surroundings. Every field is optional.
type Host struct {
	Clock Clock
	HTTP  *http.Client
	// Env resolves sys.get_env names.
	Env func(name string) (string, bool)
	// Log receives sys.log output.
	Log func(LogEntry)
	// Token returns a bearer token for http auth (kind is "OIDC" or "OAuth2").
	Token func(kind, audience, scope, target string) string
	// Connector runs a googleapis.* call; handled is false for an unknown connector.
	Connector func(ctx context.Context, name string, args map[string]any) (result any, handled bool, err error)
	// CreateCallback returns the URL callers POST to in order to resume events.await_callback.
	CreateCallback func(ctx context.Context, method string) (url string, err error)
	// AwaitCallback blocks until the callback at url is called or ctx ends.
	AwaitCallback func(ctx context.Context, url string) (map[string]any, error)
	// Progress is told which steps are executing each time they change.
	Progress func([]Frame)
}

func (h *Host) clock() Clock {
	if h != nil && h.Clock != nil {
		return h.Clock
	}

	return RealClock{}
}

// Engine holds the function library; it is safe for concurrent use.
type Engine struct {
	funcs map[string]*fnSpec
	// KnownConnector reports whether a googleapis.* name can be called; nil accepts every name.
	KnownConnector func(name string) bool
}

type fnSpec struct {
	fn       func(c *callCtx, a []any) (any, error)
	name     string
	params   []string
	min      int
	blocking bool
}

type callCtx struct {
	ctx context.Context
	x   *exec
}

func (c *callCtx) host() *Host {
	if c == nil || c.x == nil {
		return &Host{}
	}

	return c.x.host
}

// NewEngine returns an Engine with the full standard library.
func NewEngine() *Engine {
	e := &Engine{funcs: map[string]*fnSpec{}}
	e.addCore()
	e.addText()
	e.addData()
	e.addMisc()
	e.addSys()
	e.addHTTP()

	return e
}

func (e *Engine) reg(name string, params []string, minArgs int, fn func(*callCtx, []any) (any, error)) {
	e.funcs[name] = &fnSpec{name: name, params: params, min: minArgs, fn: fn}
}

func (e *Engine) regBlocking(name string, params []string, minArgs int, fn func(*callCtx, []any) (any, error)) {
	e.funcs[name] = &fnSpec{name: name, params: params, min: minArgs, fn: fn, blocking: true}
}

func (e *Engine) isConnector(name string) bool {
	if !strings.HasPrefix(name, "googleapis.") {
		return false
	}

	return e.KnownConnector == nil || e.KnownConnector(name)
}

// lookupConst resolves non-function library objects such as http.default_retry.
func (e *Engine) lookupConst(name string) (any, bool) {
	switch name {
	case "retry.default_backoff":
		return defaultBackoff(), true
	case "http.default_retry":
		return retryPolicy("http.default_retry_predicate"), true
	case "http.default_retry_non_idempotent":
		return retryPolicy("http.default_retry_predicate_non_idempotent"), true
	}

	if _, ok := e.funcs[name]; ok {
		return &FuncRef{Name: name}, true
	}

	return nil, false
}

func retryPolicy(predicate string) map[string]any {
	return map[string]any{
		"predicate":   &FuncRef{Name: predicate},
		"max_retries": int64(defaultMaxRetries),
		"backoff":     defaultBackoff(),
	}
}

const (
	defaultMaxRetries = 5
	defaultMaxDelay   = 60
	defaultMultiplier = 1.25
)

func defaultBackoff() map[string]any {
	return map[string]any{
		"initial_delay": int64(1), "max_delay": int64(defaultMaxDelay), "multiplier": defaultMultiplier,
	}
}

// bind maps named arguments to the positional parameters of a function.
func (f *fnSpec) bind(args map[string]any) ([]any, error) {
	out := make([]any, len(f.params))
	seen := 0

	for i, p := range f.params {
		if v, ok := args[p]; ok {
			out[i] = v
			seen++
		} else if i < f.min {
			return nil, typeErr("%s: missing required argument %q", f.name, p)
		}
	}

	if seen != len(args) {
		for k := range args {
			if !slices.Contains(f.params, k) {
				return nil, typeErr("%s: unknown argument %q", f.name, k)
			}
		}
	}

	return out, nil
}
