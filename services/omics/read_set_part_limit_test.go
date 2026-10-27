package omics //nolint:testpackage // sets the unexported per-part limit.

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUploadReadSetPart_SizeLimit(t *testing.T) {
	t.Parallel()

	const limit = 64

	tests := []struct {
		name       string
		wantMsg    string
		size       int
		wantStatus int
	}{
		{name: "at limit", size: limit, wantStatus: http.StatusNotFound},
		{name: "over limit", size: limit + 1, wantStatus: http.StatusBadRequest, wantMsg: "maximum part size"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := NewHandler(NewInMemoryBackend("000000000000", "us-east-1"))
			h.maxPartBytes = limit

			req := httptest.NewRequest(
				http.MethodPut,
				"/sequencestore/1234567890/upload/missing/part?partNumber=1&partSource=SOURCE1",
				bytes.NewReader([]byte(strings.Repeat("a", tc.size))),
			)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

			assert.Equal(t, tc.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tc.wantMsg)
		})
	}
}
