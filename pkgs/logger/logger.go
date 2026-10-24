// Package logger is the project-wide slog wrapper. All logging goes through
// context.Context — services and handlers must NOT embed a *slog.Logger
// field. Use Load(ctx) to fetch the logger and WithService(ctx, name) at the
// service boundary so every record carries the same "service" attribute.
//
// Output format is fixed: text handler to stderr with RFC3339 timestamps and
// stable attribute ordering. This guarantees a single consistent format
// across the entire process; do not construct ad-hoc slog.New(...) instances
// in service code — call NewLogger or NewTestLogger here so format and
// handler options stay aligned.
package logger

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/ctxval"
)

// Key is the typed context key under which the logger is stored. Exported so
// middleware that wants raw ctxval access can reuse it.
var Key = ctxval.NewKey[*slog.Logger]("logger") //nolint:gochecknoglobals // existing issue.

// serviceTagKey records the service name WithService attached.
var serviceTagKey = ctxval.NewKey[string]("logger.serviceTag") //nolint:gochecknoglobals // existing issue.

// HasService reports whether WithService already tagged ctx's logger with name.
func HasService(ctx context.Context, name string) bool {
	tagged, ok := serviceTagKey.Get(ctx)

	return ok && tagged == name
}

// ServiceTag returns the service name WithService attached to ctx, or "".
func ServiceTag(ctx context.Context) string {
	tagged, _ := serviceTagKey.Get(ctx)

	return tagged
}

// Save stores logger in ctx and returns the child context.
func Save(ctx context.Context, logger *slog.Logger) context.Context {
	if logger == nil {
		return ctx
	}

	if p, ok := pendingKey.Get(ctx); ok && p != nil {
		ctx = pendingKey.Set(ctx, nil)
	}

	return Key.Set(ctx, logger)
}

// Load returns the logger carried on ctx, or slog.Default() when none is set.
// Always returns non-nil so callers can chain methods without a guard.
func Load(ctx context.Context) *slog.Logger {
	root := rootLogger(ctx)

	if p := livePending(ctx, root); p != nil {
		return p.logger()
	}

	return root
}

// Enabled reports whether ctx's logger would emit at level, without materialising
// attrs added by AddAttrs (handler attrs never change a level check).
func Enabled(ctx context.Context, level slog.Level) bool {
	return rootLogger(ctx).Enabled(ctx, level)
}

func rootLogger(ctx context.Context) *slog.Logger {
	if logger, ok := Key.Get(ctx); ok && logger != nil {
		return logger
	}

	return slog.Default()
}

// pendingKey carries attrs added since the last materialised logger.
var pendingKey = ctxval.NewKey[*pending]("logger.pending") //nolint:gochecknoglobals // ctxval key, not mutable state.

// pending defers the handler WithAttrs clone until a record is actually logged, so
// stacked AddAttrs calls cost one clone. root is the Key logger it was built over;
// a different Save invalidates it.
type pending struct {
	root     *slog.Logger
	base     *slog.Logger
	resolved atomic.Pointer[slog.Logger]
	lazy     atomic.Pointer[slog.Logger]
	attrs    []slog.Attr
}

// logger returns a logger whose handler defers the WithAttrs clone until a record is
// handled, so Load plus Enabled checks never pay for it.
func (p *pending) logger() *slog.Logger {
	if l := p.resolved.Load(); l != nil {
		return l
	}

	if l := p.lazy.Load(); l != nil {
		return l
	}

	l := slog.New(lazyHandler{p: p})
	p.lazy.Store(l)

	return l
}

func (p *pending) materialize() *slog.Logger {
	if l := p.resolved.Load(); l != nil {
		return l
	}

	l := slog.New(p.base.Handler().WithAttrs(p.attrs))
	p.resolved.Store(l)

	return l
}

type lazyHandler struct{ p *pending }

func (h lazyHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.p.base.Handler().Enabled(ctx, level)
}

func (h lazyHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.p.materialize().Handler().Handle(ctx, r)
}

func (h lazyHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h.p.materialize().Handler().WithAttrs(attrs)
}

func (h lazyHandler) WithGroup(name string) slog.Handler {
	return h.p.materialize().Handler().WithGroup(name)
}

func livePending(ctx context.Context, root *slog.Logger) *pending {
	if p, ok := pendingKey.Get(ctx); ok && p != nil && p.root == root {
		return p
	}

	return nil
}

// AddAttrs returns a child context whose logger has attrs pre-attached to
// every record. slog.Logger.With does not mutate the parent so this is safe
// for concurrent use across requests. Consecutive calls fold into one handler clone;
// the caller must not modify attrs afterwards.
func AddAttrs(ctx context.Context, attrs ...slog.Attr) context.Context {
	if len(attrs) == 0 {
		return Save(ctx, Load(ctx))
	}

	root := rootLogger(ctx)
	next := &pending{root: root, base: root, attrs: attrs}

	if prev := livePending(ctx, root); prev != nil {
		if l := prev.resolved.Load(); l != nil {
			next.base = l
		} else {
			next.base = prev.base
			merged := make([]slog.Attr, 0, len(prev.attrs)+len(attrs))
			next.attrs = append(append(merged, prev.attrs...), attrs...)
		}
	}

	return pendingKey.Set(ctx, next)
}

// WithService returns a child context whose logger carries service=<name>.
// Call this once at the entry of every service handler so downstream records
// emitted via Load(ctx) are uniformly tagged. The service tag persists across
// AddAttrs calls because slog.Logger.With layers attributes.
func WithService(ctx context.Context, name string) context.Context {
	ctx = AddAttrs(ctx, slog.String("service", name))

	return serviceTagKey.Set(ctx, name)
}

// WithWorker returns a child context whose logger carries service=<service>
// and worker=<service>-<job>, so records emitted by a background routine are
// attributable to the specific job that produced them.
//
// Safety rules (the logger is a *slog.Logger pointer carried in ctx):
//   - Call this ONCE at the entry of the worker goroutine, never inside a loop.
//     Each call layers attributes onto a new logger; repeating it on a long-lived
//     context would grow the attribute chain (and memory) without bound.
//   - Derive ctx from the process/lifecycle context (e.g. the janitor context),
//     never from a per-request context, so worker logs don't inherit request_id
//     and don't pin a finished request alive.
//
// Enrichment is copy-on-write: slog.Logger.With does not mutate the parent, so
// concurrent workers and requests never corrupt each other's logger.
func WithWorker(ctx context.Context, service, job string) context.Context {
	return AddAttrs(ctx,
		slog.String("service", service),
		slog.String("worker", service+"-"+job),
	)
}

// handlerOptions returns the canonical handler options used by every logger
// constructed in this package. Keeping this in one place guarantees the same
// format everywhere: text output to stderr, fixed level, no AddSource.
func handlerOptions(level slog.Level) *slog.HandlerOptions {
	return &slog.HandlerOptions{
		Level: level,
	}
}

// NewLogger returns a logger writing text records to stderr at level.
func NewLogger(level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, handlerOptions(level)))
}

// NewTestLogger returns a logger with debug level enabled, useful in tests.
func NewTestLogger() *slog.Logger {
	return NewLogger(slog.LevelDebug)
}
