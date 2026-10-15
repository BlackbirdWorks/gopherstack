package route53recoverycluster_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/route53recoverycluster"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	cluster "github.com/blackbirdworks/gopherstack/services/route53recoverycluster"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	sdkcheck.CheckCompleteness(
		t,
		&route53recoverycluster.Client{},
		cluster.NewHandler(nil).GetSupportedOperations(),
		[]string{},
	)
}

func TestRouting(t *testing.T) {
	t.Parallel()

	h := cluster.NewHandler(nil)

	tests := []struct {
		name, target, auth, method string
		want                       bool
	}{
		{"toggle target", "ToggleCustomerAPI.GetRoutingControlState", "", http.MethodPost, true},
		{"signing name", "", "route53-recovery-cluster", http.MethodPost, true},
		{"other target", "DynamoDB_20120810.GetItem", "dynamodb", http.MethodPost, false},
		{"signing name wrong method", "", "route53-recovery-cluster", http.MethodGet, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(
				t.Context(),
				tt.method,
				"/route53-recovery-cluster/abc/us-west-2/",
				nil,
			)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			if tt.auth != "" {
				req.Header.Set(
					"Authorization",
					"AWS4-HMAC-SHA256 Credential=test/20260101/us-west-2/"+tt.auth+"/aws4_request",
				)
			}

			c := echo.New().NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.RouteMatcher()(c))

			if tt.want && tt.target != "" {
				assert.Equal(t, "GetRoutingControlState", h.ExtractOperation(c))
			}
		})
	}
}

func TestErrorBodyShape(t *testing.T) {
	t.Parallel()

	h := cluster.NewHandler(nil)

	tests := []struct {
		name, target, body string
	}{
		{"store unavailable", "ToggleCustomerAPI.GetRoutingControlState", `{"RoutingControlArn":"x"}`},
		{"unknown operation", "ToggleCustomerAPI.Nope", `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
			assert.GreaterOrEqual(t, rec.Code, http.StatusBadRequest)

			var body map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.NotEmpty(t, body["message"])
			assert.Equal(t, body["message"], body["Message"])
			assert.False(
				t,
				strings.HasPrefix(body["message"], body["__type"]),
				"message %q repeats code",
				body["message"],
			)
		})
	}
}

func TestExtractOperationIgnoresCredentialHeaders(t *testing.T) {
	t.Parallel()

	h := cluster.NewHandler(nil)

	tests := []struct {
		name, target, want string
	}{
		{"known op", "ToggleCustomerAPI.GetRoutingControlState", "GetRoutingControlState"},
		{"unknown op", "ToggleCustomerAPI.Bogus", "Unknown"},
		{"no target", "", "Unknown"},
		{"foreign prefix", "Other.GetRoutingControlState", "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIASECRET/20260101/us-east-1/s3/aws4_request")
			req.Header.Set("X-Amz-Security-Token", "secret-token")

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			c := echo.New().NewContext(req, httptest.NewRecorder())
			got := h.ExtractOperation(c)
			assert.Equal(t, tt.want, got)
			assert.Empty(t, h.ExtractResource(c))
			assert.NotContains(t, got, "AKIASECRET")
		})
	}
}
