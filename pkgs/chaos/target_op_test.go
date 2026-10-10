package chaos_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
)

func TestMiddleware_NamespacedTargetOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		op     string
		want   int
	}{
		{"short", "CloudTrail_20131101.DescribeTrails", "DescribeTrails", http.StatusServiceUnavailable},
		{
			"botocore",
			"com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.DescribeTrails",
			"DescribeTrails",
			http.StatusServiceUnavailable,
		},
		{
			"botocore_other_op",
			"com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.LookupEvents",
			"DescribeTrails",
			http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()
			store.SetRules([]chaos.FaultRule{{Service: "cloudtrail", Operation: tt.op}})

			wrapped := chaos.Middleware(store)(func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			})

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
			req.Header.Set("Authorization", buildSigV4Auth("cloudtrail", "us-east-1"))
			req.Header.Set("X-Amz-Target", tt.target)

			rec := httptest.NewRecorder()
			require.NoError(t, wrapped(echo.New().NewContext(req, rec)))
			assert.Equal(t, tt.want, rec.Code)
		})
	}
}
