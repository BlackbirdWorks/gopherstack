package chaos_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildSigV4Auth builds a minimal AWS SigV4 Authorization header for testing.
func buildSigV4Auth(svc, region string) string {
	cred := "Credential=AKIAIOSFODNN7EXAMPLE/20231225/" + region + "/" + svc + "/aws4_request"

	return "AWS4-HMAC-SHA256 " + cred + ", SignedHeaders=host;x-amz-date, Signature=sig"
}

func TestMiddleware_PassThrough(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		auth   string
		target string
		rules  []chaos.FaultRule
		wantOK bool
	}{
		{
			name:   "no rules: request passes through",
			rules:  []chaos.FaultRule{},
			auth:   buildSigV4Auth("s3", "us-east-1"),
			wantOK: true,
		},
		{
			name:   "rule for different service: request passes through",
			rules:  []chaos.FaultRule{{Service: "dynamodb"}},
			auth:   buildSigV4Auth("s3", "us-east-1"),
			wantOK: true,
		},
		{
			name:   "rule for different region: request passes through",
			rules:  []chaos.FaultRule{{Service: "s3", Region: "eu-west-1"}},
			auth:   buildSigV4Auth("s3", "us-east-1"),
			wantOK: true,
		},
		{
			name:  "rule for different operation: request passes through",
			rules: []chaos.FaultRule{{Service: "dynamodb", Operation: "GetItem"}},
			auth:  buildSigV4Auth("dynamodb", "us-east-1"),
			// X-Amz-Target sets operation to PutItem, rule is for GetItem
			target: "DynamoDB_20120810.PutItem",
			wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()
			store.SetRules(tt.rules)

			mw := chaos.Middleware(store)
			handlerCalled := false

			inner := func(c *echo.Context) error {
				handlerCalled = true

				return c.String(http.StatusOK, "ok")
			}

			wrapped := mw(inner)

			e := echo.New()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
			req.Header.Set("Authorization", tt.auth)

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := wrapped(c)
			require.NoError(t, err)
			assert.True(t, handlerCalled, "expected handler to be called")
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestMiddleware_FaultInjection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		auth           string
		target         string
		wantBody       string
		rules          []chaos.FaultRule
		wantStatusCode int
	}{
		{
			name:           "match by service: inject default 503",
			rules:          []chaos.FaultRule{{Service: "s3"}},
			auth:           buildSigV4Auth("s3", "us-east-1"),
			wantStatusCode: 503,
			wantBody:       "ServiceUnavailable",
		},
		{
			name: "match by service+operation: inject custom 400",
			rules: []chaos.FaultRule{
				{
					Service:   "dynamodb",
					Operation: "PutItem",
					Error:     &chaos.FaultError{StatusCode: 400, Code: "ProvisionedThroughputExceededException"},
				},
			},
			auth:           buildSigV4Auth("dynamodb", "us-east-1"),
			target:         "DynamoDB_20120810.PutItem",
			wantStatusCode: 400,
			wantBody:       "ProvisionedThroughputExceededException",
		},
		{
			name: "empty rule matches all services",
			rules: []chaos.FaultRule{
				{},
			},
			auth:           buildSigV4Auth("kinesis", "eu-west-1"),
			wantStatusCode: 503,
		},
		{
			name: "match by region: inject fault for specific region",
			rules: []chaos.FaultRule{
				{Service: "s3", Region: "us-east-1"},
			},
			auth:           buildSigV4Auth("s3", "us-east-1"),
			wantStatusCode: 503,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()
			store.SetRules(tt.rules)

			mw := chaos.Middleware(store)
			handlerCalled := false

			inner := func(c *echo.Context) error {
				handlerCalled = true

				return c.String(http.StatusOK, "ok")
			}

			wrapped := mw(inner)

			e := echo.New()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
			req.Header.Set("Authorization", tt.auth)

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := wrapped(c)
			require.NoError(t, err)
			assert.False(t, handlerCalled, "expected handler NOT to be called when fault fires")
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestMiddleware_NoAuthHeader(t *testing.T) {
	t.Parallel()

	// Without an Authorization header, service/region extraction returns "".
	// An empty rule {} matches everything, so the fault should still fire.
	store := chaos.NewFaultStore()
	store.SetRules([]chaos.FaultRule{{}})

	mw := chaos.Middleware(store)
	handlerCalled := false

	inner := func(c *echo.Context) error {
		handlerCalled = true

		return c.String(http.StatusOK, "ok")
	}

	wrapped := mw(inner)

	e := echo.New()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := wrapped(c)
	require.NoError(t, err)
	assert.False(t, handlerCalled)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestMiddleware_ServiceRuleNoAuthDoesNotMatch(t *testing.T) {
	t.Parallel()

	// A service-specific rule should NOT match when auth header is absent
	// (extracted service is ""), so the request passes through.
	store := chaos.NewFaultStore()
	store.SetRules([]chaos.FaultRule{{Service: "s3"}})

	mw := chaos.Middleware(store)
	handlerCalled := false

	inner := func(c *echo.Context) error {
		handlerCalled = true

		return c.String(http.StatusOK, "ok")
	}

	wrapped := mw(inner)

	e := echo.New()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/dashboard/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := wrapped(c)
	require.NoError(t, err)
	assert.True(t, handlerCalled)
	assert.Equal(t, http.StatusOK, rec.Code)
}
