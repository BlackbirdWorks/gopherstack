package bedrockagentcore

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutboundAcceptsLargePayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		size int
	}{
		{"small", 1024},
		{"over shared cap", 20 << 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := strings.NewReader(strings.Repeat("a", tt.size))
			r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", body)
			rs := &runtimeSession{base: "http://127.0.0.1:1"}

			req, err := (&engine{}).outbound(t.Context(), r, rs, http.MethodPost, "/invocations")
			require.NoError(t, err)
			assert.EqualValues(t, tt.size, req.ContentLength)
		})
	}
}
