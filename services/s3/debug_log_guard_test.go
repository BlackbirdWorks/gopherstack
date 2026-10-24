package s3_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func TestDebugLogGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  []string
		level slog.Level
	}{
		{
			name:  "debug_emits_request_logs",
			level: slog.LevelDebug,
			want: []string{
				"S3 request", "S3 putObject input", "S3 putObject output",
				"S3 Backend PutObject", "S3 getObject input", "S3 getObject output",
				"S3 Backend HeadObject", "S3 deleteObject input", "S3 deleteObject output",
			},
		},
		{name: "error_level_silent", level: slog.LevelError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "dbg-bucket")

			var buf bytes.Buffer
			lg := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: tt.level}))

			do := func(method, path, body string) {
				req := httptest.NewRequest(method, path, strings.NewReader(body))
				req = req.WithContext(logger.Save(req.Context(), lg))
				rec := httptest.NewRecorder()
				_ = handler.Handler()(echo.New().NewContext(req, rec))
				require.Less(t, rec.Code, http.StatusBadRequest)
			}

			do(http.MethodPut, "/dbg-bucket/k", "data")
			do(http.MethodGet, "/dbg-bucket/k", "")
			do(http.MethodHead, "/dbg-bucket/k", "")
			do(http.MethodDelete, "/dbg-bucket/k", "")

			if len(tt.want) == 0 {
				assert.Empty(t, buf.String())

				return
			}

			for _, msg := range tt.want {
				assert.Contains(t, buf.String(), msg)
			}
		})
	}
}
