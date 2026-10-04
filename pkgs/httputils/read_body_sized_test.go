package httputils_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func TestReadBodyDeclaredLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		size     int
		declared int64
	}{
		{name: "exact", size: 5000, declared: 5000},
		{name: "declared_larger", size: 100, declared: 1 << 20},
		{name: "declared_smaller", size: 5000, declared: 10},
		{name: "unknown_length", size: 5000, declared: -1},
		{name: "empty", size: 0, declared: 0},
		{name: "huge_declared_tiny_body", size: 3, declared: 1 << 40},
		{name: "multi_grow", size: 3 << 20, declared: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			want := bytes.Repeat([]byte("a"), tt.size)
			req := httptest.NewRequest(http.MethodPost, "/", io.NopCloser(bytes.NewReader(want)))
			req.ContentLength = tt.declared

			got, err := httputils.ReadBody(req)
			require.NoError(t, err)
			assert.Equal(t, want, got)

			again, err := httputils.ReadBody(req)
			require.NoError(t, err)
			assert.Equal(t, want, again)
		})
	}
}

func TestReadBodyOverCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		declared int64
	}{
		{name: "honest", declared: httputils.MaxRequestBodyBytes + 1},
		{name: "understated", declared: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := bytes.Repeat([]byte("a"), int(httputils.MaxRequestBodyBytes)+1)
			req := httptest.NewRequest(http.MethodPost, "/", io.NopCloser(bytes.NewReader(body)))
			req.ContentLength = tt.declared

			_, err := httputils.ReadBody(req)

			var mbe *http.MaxBytesError

			require.ErrorAs(t, err, &mbe)
		})
	}
}
