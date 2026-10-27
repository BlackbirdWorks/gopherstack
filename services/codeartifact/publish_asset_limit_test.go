package codeartifact //nolint:testpackage // sets the unexported asset limit.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublishPackageVersion_AssetSizeLimit(t *testing.T) {
	t.Parallel()

	const limit = 64

	tests := []struct {
		name       string
		wantCode   string
		size       int
		wantStatus int
	}{
		{name: "at limit", size: limit, wantStatus: http.StatusNotFound, wantCode: "ResourceNotFoundException"},
		{
			name: "over limit", size: limit + 1,
			wantStatus: http.StatusPaymentRequired, wantCode: "ServiceQuotaExceededException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := NewHandler(NewInMemoryBackend("000000000000", "us-east-1"))
			h.maxAssetBytes = limit

			body := []byte(strings.Repeat("a", tc.size))
			sum := sha256.Sum256(body)
			req := httptest.NewRequest(
				http.MethodPost,
				"/v1/package/version/publish?domain=d&repository=r&format=generic&package=p&version=1&asset=a",
				bytes.NewReader(body),
			)
			req.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum[:]))
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

			assert.Equal(t, tc.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tc.wantCode)
		})
	}
}
