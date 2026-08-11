package bedrockruntime_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
)

// buildSigV4Auth builds a minimal AWS SigV4 Authorization header for testing,
// mirroring pkgs/chaos's own middleware_test.go helper.
func buildSigV4Auth(svc, region string) string {
	cred := "Credential=AKIAIOSFODNN7EXAMPLE/20231225/" + region + "/" + svc + "/aws4_request"

	return "AWS4-HMAC-SHA256 " + cred + ", SignedHeaders=host;x-amz-date, Signature=sig"
}

// TestInvokeModel_ChaosFaultInjection proves that a fault rule targeting
// Handler.ChaosServiceName() actually intercepts a real InvokeModel request
// once routed through pkgs/chaos.Middleware, the same wrapper every service
// registers behind in production. Before ChaosServiceName was corrected from
// "bedrockruntime" to "bedrock" (real Bedrock Runtime's SigV4 signing name,
// see the doc comment on Handler.ChaosServiceName), a rule discovered via
// the chaos dashboard's GET /targets and targeted at "bedrockruntime" could
// never match a real client's Authorization header and so could never fire.
func TestInvokeModel_ChaosFaultInjection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		ruleService  string
		wantHandler  bool
		wantFaultHit bool
	}{
		{
			name:         "rule for the real signing name intercepts the call",
			ruleService:  "bedrock",
			wantHandler:  false,
			wantFaultHit: true,
		},
		{
			name:         "rule for the stale gopherstack-package name never matches",
			ruleService:  "bedrockruntime",
			wantHandler:  true,
			wantFaultHit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.Equal(t, "bedrock", h.ChaosServiceName())

			store := chaos.NewFaultStore()
			store.SetRules([]chaos.FaultRule{
				{
					Service: tt.ruleService,
					Error:   &chaos.FaultError{Code: "ModelErrorException", StatusCode: http.StatusInternalServerError},
				},
			})

			handlerCalled := false
			inner := func(c *echo.Context) error {
				handlerCalled = true

				return h.Handler()(c)
			}
			wrapped := chaos.Middleware(store)(inner)

			body := []byte(`{"prompt":"hi"}`)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/model/anthropic.claude-v2/invoke", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", buildSigV4Auth("bedrock", "us-east-1"))
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			require.NoError(t, wrapped(c))

			assert.Equal(t, tt.wantHandler, handlerCalled)

			if tt.wantFaultHit {
				assert.Equal(t, http.StatusInternalServerError, rec.Code)
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}
