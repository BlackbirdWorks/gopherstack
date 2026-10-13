package s3control_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	s3control "github.com/blackbirdworks/gopherstack/services/s3control"
)

func TestCreateBucket_OutpostARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		outpostID string
		account   string
		wantInARN string
	}{
		{
			name:      "outpost header",
			outpostID: "op-0123456789abcdef0",
			account:   "111122223333",
			wantInARN: "111122223333:outpost/op-0123456789abcdef0/bucket/b1",
		},
		{
			name:      "no outpost header",
			outpostID: "",
			account:   "111122223333",
			wantInARN: "111122223333:outpost/op-00000000/bucket/b1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := s3control.NewHandler(s3control.NewInMemoryBackend())
			req := httptest.NewRequest(http.MethodPut, "/v20180820/bucket/b1", nil)
			req.Header.Set("X-Amz-Account-Id", tt.account)
			if tt.outpostID != "" {
				req.Header.Set("X-Amz-Outpost-Id", tt.outpostID)
			}

			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantInARN)
		})
	}
}
