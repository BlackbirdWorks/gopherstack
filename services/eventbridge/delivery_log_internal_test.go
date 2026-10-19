package eventbridge

import (
	"bytes"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

var errSecretLeak = errors.New("upstream rejected bearer hunter2")

func TestLogTargetErr_DoesNotLogErrorText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err  error
		name string
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "secret", err: errSecretLeak, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			ctx := logger.Save(t.Context(), slog.New(slog.NewTextHandler(&buf, nil)))

			require.Equal(t, tt.want, logTargetErr(ctx, "Batch", tt.err))
			assert.NotContains(t, buf.String(), "hunter2")

			if tt.want {
				assert.Contains(t, buf.String(), "Batch")
			}
		})
	}
}
