package httputils_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func TestReadBodyLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		size    int
		limit   int64
		tooLong bool
	}{
		{name: "empty", size: 0, limit: 8},
		{name: "under", size: 7, limit: 8},
		{name: "at limit", size: 8, limit: 8},
		{name: "over limit", size: 9, limit: 8, tooLong: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			payload := strings.Repeat("a", tc.size)
			req := httptest.NewRequest(http.MethodPost, "/", io.NopCloser(strings.NewReader(payload)))
			got, err := httputils.ReadBodyLimit(req, tc.limit)

			if tc.tooLong {
				var mbe *http.MaxBytesError
				require.ErrorAs(t, err, &mbe)
				assert.Equal(t, tc.limit, mbe.Limit)

				return
			}

			require.NoError(t, err)
			assert.Len(t, got, tc.size)
		})
	}

	t.Run("nil body", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Body = nil
		got, err := httputils.ReadBodyLimit(req, 4)
		require.NoError(t, err)
		assert.Nil(t, got)
	})
}
