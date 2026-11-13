package bedrockagentcore

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamResponseContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		upstream string
		want     string
	}{
		{"json", "application/json", "application/json"},
		{"json charset", "application/json; charset=utf-8", "application/json; charset=utf-8"},
		{"json bad charset", `application/json; charset="x y"`, "application/json"},
		{"ndjson", "application/x-ndjson", "application/x-ndjson"},
		{"sse", "text/event-stream", "text/event-stream"},
		{"plain", "text/plain", "text/plain; charset=utf-8"},
		{"html downgraded", "text/html; charset=utf-8", "application/octet-stream"},
		{"octet stream", "application/octet-stream", "application/octet-stream"},
		{"missing", "", "application/octet-stream"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			c := echo.NewContext(httptest.NewRequest(http.MethodPost, "/", nil), rec)
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {tt.upstream}},
				Body:       io.NopCloser(strings.NewReader("<b>x</b>")),
			}

			require.NoError(t, streamResponse(c, resp, "sess"))
			assert.Equal(t, tt.want, rec.Header().Get("Content-Type"))
			assert.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
		})
	}
}
