package polly_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_ListNextTokenOmittedWhenEmpty verifies the list endpoints omit
// NextToken from the response when there are no further pages (AWS omits it),
// rather than always emitting an empty NextToken key.
func TestParity_ListNextTokenOmittedWhenEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "list_speech_synthesis_tasks", path: "/v1/synthesisTasks"},
		{name: "list_lexicons", path: "/v1/lexicons"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := request(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			out := responseMap(t, rec)
			_, present := out["NextToken"]
			assert.False(t, present, "NextToken must be omitted when empty")
		})
	}
}
