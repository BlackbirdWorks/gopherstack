package logger_test

import (
	"bytes"
	"context"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func newBufLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey {
				return slog.Attr{}
			}

			return a
		},
	}))
}

func TestAddAttrsMatchesLayeredLogger(t *testing.T) {
	t.Parallel()

	region := slog.String("region", "us-east-1")
	account := slog.String("account", "000000000000")
	reqID := slog.String("request_id", "abc")

	tests := []struct {
		layered func(l *slog.Logger) *slog.Logger
		withCtx func(ctx context.Context) context.Context
		name    string
	}{
		{
			name: "request_then_service",
			layered: func(l *slog.Logger) *slog.Logger {
				return l.With(region, account, reqID).With("service", "s3")
			},
			withCtx: func(ctx context.Context) context.Context {
				ctx = logger.AddAttrs(ctx, region, account, reqID)

				return logger.WithService(ctx, "s3")
			},
		},
		{
			name: "load_between_layers",
			layered: func(l *slog.Logger) *slog.Logger {
				return l.With(region, account, reqID).With("service", "s3")
			},
			withCtx: func(ctx context.Context) context.Context {
				ctx = logger.AddAttrs(ctx, region, account, reqID)
				_ = logger.Load(ctx)

				return logger.WithService(ctx, "s3")
			},
		},
		{
			name: "grouped_base",
			layered: func(l *slog.Logger) *slog.Logger {
				return l.WithGroup("g").With(region).With("service", "s3")
			},
			withCtx: func(ctx context.Context) context.Context {
				ctx = logger.Save(ctx, logger.Load(ctx).WithGroup("g"))
				ctx = logger.AddAttrs(ctx, region)

				return logger.WithService(ctx, "s3")
			},
		},
		{
			name: "worker",
			layered: func(l *slog.Logger) *slog.Logger {
				return l.With("service", "sqs", "worker", "sqs-janitor")
			},
			withCtx: func(ctx context.Context) context.Context {
				return logger.WithWorker(ctx, "sqs", "janitor")
			},
		},
		{
			name:    "no_attrs",
			layered: func(l *slog.Logger) *slog.Logger { return l },
			withCtx: func(ctx context.Context) context.Context { return logger.AddAttrs(ctx) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var want, got bytes.Buffer

			tt.layered(newBufLogger(&want)).Info("msg", "k", "v")

			ctx := logger.Save(context.Background(), newBufLogger(&got))
			logger.Load(tt.withCtx(ctx)).Info("msg", "k", "v")

			assert.Equal(t, want.String(), got.String())
		})
	}
}

func TestAddAttrsSaveDiscardsPendingAttrs(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	base := newBufLogger(&buf)
	ctx := logger.Save(context.Background(), base)
	ctx = logger.AddAttrs(ctx, slog.String("a", "1"))
	ctx = logger.Save(ctx, base)

	logger.Load(ctx).Info("msg")

	assert.Equal(t, "level=INFO msg=msg\n", buf.String())
}

func TestAddAttrsParentUnaffectedByChild(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	parent := logger.AddAttrs(logger.Save(context.Background(), newBufLogger(&buf)), slog.String("a", "1"))
	_ = logger.AddAttrs(parent, slog.String("b", "2"))

	logger.Load(parent).Info("msg")

	assert.Equal(t, "level=INFO msg=msg a=1\n", buf.String())
}

func TestEnabledFollowsHandlerLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		level slog.Level
		check slog.Level
		want  bool
	}{
		{"debug_on", slog.LevelDebug, slog.LevelDebug, true},
		{"debug_off", slog.LevelInfo, slog.LevelDebug, false},
		{"error_on", slog.LevelInfo, slog.LevelError, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := logger.Save(context.Background(), logger.NewLogger(tt.level))
			ctx = logger.WithService(ctx, "s3")

			assert.Equal(t, tt.want, logger.Enabled(ctx, tt.check))
		})
	}
}

type countingHandler struct {
	slog.Handler
	withAttrs *atomic.Int32
}

func (h countingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.withAttrs.Add(1)

	return countingHandler{Handler: h.Handler.WithAttrs(attrs), withAttrs: h.withAttrs}
}

func TestAddAttrsClonesHandlerOnlyWhenLogging(t *testing.T) {
	t.Parallel()

	var (
		buf   bytes.Buffer
		clone atomic.Int32
	)

	base := slog.New(countingHandler{Handler: newBufLogger(&buf).Handler(), withAttrs: &clone})
	ctx := logger.Save(context.Background(), base)
	ctx = logger.AddAttrs(ctx, slog.String("a", "1"))
	ctx = logger.WithService(ctx, "s3")

	log := logger.Load(ctx)
	assert.True(t, log.Enabled(ctx, slog.LevelInfo))
	assert.Zero(t, clone.Load(), "no record emitted yet")

	log.Info("msg")
	log.Info("again")

	assert.Equal(t, int32(1), clone.Load(), "attrs fold into one clone")
	assert.Equal(t, "level=INFO msg=msg a=1 service=s3\nlevel=INFO msg=again a=1 service=s3\n", buf.String())
}
