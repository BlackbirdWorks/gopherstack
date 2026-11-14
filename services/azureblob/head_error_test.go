package azureblob_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorResponses_CarryErrorCodeHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		wantCode string
		wantBody bool
	}{
		{"head missing blob", http.MethodHead, "/" + testAccount + "/box/missing", "BlobNotFound", false},
		{"get missing blob", http.MethodGet, "/" + testAccount + "/box/missing", "BlobNotFound", true},
		{"head missing container", http.MethodHead, "/" + testAccount + "/nobox/missing", "ContainerNotFound", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createContainer(t, h, "box")

			rec := doRequest(t, h, tt.method, tt.path, nil, nil)
			require.Equal(t, http.StatusNotFound, rec.Code)
			assert.Equal(t, tt.wantCode, rec.Header().Get("X-Ms-Error-Code"))

			if tt.wantBody {
				assert.Contains(t, rec.Body.String(), tt.wantCode)

				return
			}

			assert.Empty(t, rec.Body.String())
			assert.Empty(
				t,
				rec.Header().Get("Content-Length"),
				"a HEAD error must not advertise a body the SDK then tries to parse",
			)
		})
	}
}
