package mediatailor_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDuplicateCreate_WireErrorType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "channel", path: "/channel/ch1"},
		{name: "source_location", path: "/sourceLocation/sl1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"HttpConfiguration": map[string]any{"BaseUrl": "https://example.com"},
			}

			first := doRequest(t, h, http.MethodPost, tt.path, body)
			assert.Equal(t, http.StatusOK, first.Code)

			rec := doRequest(t, h, http.MethodPost, tt.path, body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Equal(t, "BadRequestException", rec.Header().Get("X-Amzn-Errortype"))
		})
	}
}
