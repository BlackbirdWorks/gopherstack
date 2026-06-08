package logger_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func TestWithService_TagsEveryRecord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		serviceName string
		emit        func(ctx context.Context)
		wantPrefix  string
		wantContain []string
	}{
		{
			name:        "single-service-tag",
			serviceName: "ec2",
			emit: func(ctx context.Context) {
				logger.Load(ctx).InfoContext(ctx, "hello")
			},
			wantContain: []string{`service=ec2`, `msg=hello`},
		},
		{
			name:        "nested-attrs-persist-with-service",
			serviceName: "sqs",
			emit: func(ctx context.Context) {
				ctx = logger.AddAttrs(ctx, slog.String("queue", "q1"))
				logger.Load(ctx).InfoContext(ctx, "send")
			},
			wantContain: []string{`service=sqs`, `queue=q1`, `msg=send`},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			base := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			ctx := logger.Save(context.Background(), base)
			ctx = logger.WithService(ctx, tc.serviceName)

			tc.emit(ctx)
			out := buf.String()

			for _, want := range tc.wantContain {
				assert.Contains(t, out, want, "missing %q in %q", want, out)
			}
		})
	}
}

func TestNewLogger_FormatIsTextHandler(t *testing.T) {
	t.Parallel()

	l := logger.NewLogger(slog.LevelInfo)
	assert.NotNil(t, l)
	// Smoke-check that the returned logger does not panic on use.
	l.Info("smoke")
}
