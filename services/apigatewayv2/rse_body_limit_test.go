package apigatewayv2_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

func TestEvalRouteSelectionExpression_BodyCap(t *testing.T) {
	t.Parallel()

	const limit = 10 * 1024 * 1024

	prefix := `{"action":"go","pad":"`
	suffix := `"}`

	tests := []struct {
		name string
		want string
		size int
	}{
		{name: "at cap", size: limit, want: "go"},
		{name: "over cap", size: limit + 1, want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			body := prefix + strings.Repeat("a", tc.size-len(prefix)-len(suffix)) + suffix
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))

			got := apigatewayv2.EvalRouteSelectionExpression("${request.body.action}", req)
			assert.Equal(t, tc.want, got)

			rest, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			assert.Len(t, rest, tc.size)
		})
	}
}
