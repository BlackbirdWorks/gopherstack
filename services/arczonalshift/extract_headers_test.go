package arczonalshift_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	svc "github.com/blackbirdworks/gopherstack/services/arczonalshift"
)

func TestExtractIgnoresCredentialHeaders(t *testing.T) {
	t.Parallel()

	h := svc.NewHandler(svc.NewInMemoryBackend(testAccount))

	tests := []struct {
		headers map[string]string
		name    string
	}{
		{nil, "none"},
		{map[string]string{
			"Authorization": "AWS4-HMAC-SHA256 Credential=AKIASECRET/20260101/us-east-1/s3/aws4_request",
		}, "sigv4"},
		{map[string]string{"X-Amz-Security-Token": "secret-token"}, "token"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/zonalshifts/zs-1", nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			c := echo.New().NewContext(req, httptest.NewRecorder())
			assert.Equal(t, "CancelZonalShift", h.ExtractOperation(c))
			assert.Equal(t, "zs-1", h.ExtractResource(c))
		})
	}
}
