package iot_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicRuleBodyLimit(t *testing.T) {
	t.Parallel()

	const limit = 1 << 20

	prefix := `{"sql":"SELECT * FROM 't'","description":"`
	suffix := `"}`

	body := func(total int) string {
		return prefix + strings.Repeat("a", total-len(prefix)-len(suffix)) + suffix
	}

	tests := []struct {
		name         string
		method       string
		path         string
		size         int
		wantTooLarge bool
	}{
		{name: "create at limit", method: http.MethodPost, path: "/rules/r1", size: limit},
		{name: "create over limit", method: http.MethodPost, path: "/rules/r2", size: limit + 1, wantTooLarge: true},
		{name: "replace at limit", method: http.MethodPatch, path: "/rules/r3", size: limit},
		{name: "replace over limit", method: http.MethodPatch, path: "/rules/r4", size: limit + 1, wantTooLarge: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			req := httptest.NewRequest(tc.method, tc.path, bytes.NewReader([]byte(body(tc.size))))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

			if tc.wantTooLarge {
				assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
				assert.Contains(t, rec.Body.String(), "RequestEntityTooLargeException")

				return
			}

			assert.NotEqual(t, http.StatusRequestEntityTooLarge, rec.Code)
		})
	}
}
