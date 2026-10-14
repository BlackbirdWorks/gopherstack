package iot_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// GET /rules is IoT ListTopicRules and also Route 53 Recovery Readiness ListRules; the SigV4 scope decides.
func TestRouteMatcher_RulesPathHonoursSigningScope(t *testing.T) {
	t.Parallel()

	matcher := iot.NewHandler(iot.NewInMemoryBackend(), nil).RouteMatcher()

	tests := []struct {
		name  string
		scope string
		want  bool
	}{
		{"iot scope", "iot", true},
		{"unsigned", "", true},
		{"recovery readiness scope", "route53-recovery-readiness", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/rules", nil)
			if tt.scope != "" {
				req.Header.Set(
					"Authorization",
					"AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/"+tt.scope+"/aws4_request",
				)
			}

			assert.Equal(t, tt.want, matcher(echo.New().NewContext(req, httptest.NewRecorder())))
		})
	}
}
