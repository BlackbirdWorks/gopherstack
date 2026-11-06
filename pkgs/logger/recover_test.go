package logger_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func TestRecover(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		panic bool
	}{
		{name: "panics", panic: true},
		{name: "quiet", panic: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			ctx := logger.Save(context.Background(), slog.New(slog.NewTextHandler(&buf, nil)))

			var got any

			func() {
				defer logger.Recover(ctx, "unit", func(r any) { got = r })

				if tt.panic {
					panic("boom")
				}
			}()

			if !tt.panic {
				assert.Nil(t, got)
				assert.Empty(t, buf.String())

				return
			}

			require.Equal(t, "boom", got)
			assert.Contains(t, buf.String(), "component=unit")
			assert.Contains(t, buf.String(), "boom")
			assert.Contains(t, buf.String(), "recover_test.go")
		})
	}
}
