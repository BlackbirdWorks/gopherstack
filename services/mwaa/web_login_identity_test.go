package mwaa_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

func TestCreateWebLoginTokenIamIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
		want string
	}{
		{name: "assumed role", arn: "arn:aws:sts::123456789012:assumed-role/Admin/me", want: "assumed-role/Admin/me"},
		{name: "user", arn: "arn:aws:iam::123456789012:user/bob", want: "user/bob"},
		{name: "unresolved", arn: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			b.AddEnvironmentInternal("wl-env").Status = "AVAILABLE"
			h := mwaa.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			req := httptest.NewRequest(http.MethodPost, "/webtoken/wl-env", bytes.NewReader(nil))
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/airflow/aws4_request")

			meta := &awsmeta.Metadata{Region: testRegion, Account: testAccountID}
			if tt.arn != "" {
				meta.Principal = &awsmeta.Principal{Arn: tt.arn}
			}

			req = req.WithContext(awsmeta.Set(req.Context(), meta))
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var out map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.want, out["IamIdentity"])
		})
	}
}
