package rdsdata //nolint:testpackage // white-box tests of the unexported placeholder, mapping and engine code.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandlerMapsModeledExceptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resolverErr error
		secrets     fakeSecrets
		name        string
		wantType    string
		wantStatus  int
	}{
		{
			name: "endpoint_disabled", resolverErr: ErrHTTPEndpointNotEnabled,
			wantType: "HttpEndpointNotEnabledException", wantStatus: http.StatusBadRequest,
		},
		{
			name: "secret_missing", secrets: fakeSecrets{},
			wantType: "SecretsErrorException", wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			secrets := tt.secrets
			if secrets == nil {
				secrets = fakeSecrets{testSecretARN: testCreds}
			}

			h := newRealHarness(t, tt.resolverErr, secrets)
			handler := NewHandler(h.b)

			body, err := json.Marshal(map[string]string{
				"resourceArn": testClusterARN, "secretArn": testSecretARN, "sql": "SELECT 1",
			})
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/Execute", bytes.NewReader(body))
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=t/20230101/us-east-1/rds-data/aws4_request")

			rec := httptest.NewRecorder()
			require.NoError(t, handler.Handler()(echo.New().NewContext(req, rec)))

			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]string

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantType, out["__type"])
		})
	}
}
