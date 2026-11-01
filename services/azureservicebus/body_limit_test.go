package azureservicebus_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHTTPBodyLimit(t *testing.T) {
	t.Parallel()

	const limit = 1 << 20

	tests := []struct {
		name         string
		method       string
		path         string
		size         int
		wantTooLarge bool
	}{
		{name: "send at limit", method: http.MethodPost, path: "/q1/messages", size: limit},
		{name: "send over limit", method: http.MethodPost, path: "/q1/messages", size: limit + 1, wantTooLarge: true},
		{name: "create at limit", method: http.MethodPut, path: "/q2", size: limit},
		{name: "create over limit", method: http.MethodPut, path: "/q3", size: limit + 1, wantTooLarge: true},
		{
			name: "subscription over limit", method: http.MethodPut, path: "/t1/subscriptions/s1",
			size: limit + 1, wantTooLarge: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, []byte(strings.Repeat("a", tc.size)), nil)

			if tc.wantTooLarge {
				assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
				assert.Contains(t, rec.Body.String(), "MessageSizeExceeded")

				return
			}

			assert.NotEqual(t, http.StatusRequestEntityTooLarge, rec.Code)
		})
	}
}
