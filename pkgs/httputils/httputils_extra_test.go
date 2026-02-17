package httputils_test

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"Gopherstack/pkgs/httputils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	errRead = errors.New("read error")
	errBoom = errors.New("boom")
)

type errReadCloser struct{}

func (errReadCloser) Read(_ []byte) (int, error) {
	return 0, errRead
}

func (errReadCloser) Close() error { return nil }

func TestReadBody_Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body io.ReadCloser
		name string
	}{
		{
			body: errReadCloser{},
			name: "read error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := &http.Request{Body: tt.body}
			_, err := httputils.ReadBody(req)
			require.Error(t, err)
		})
	}
}

func TestWriteError_NilLogger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err        error
		name       string
		statusCode int
	}{
		{
			err:        errBoom,
			name:       "nil logger",
			statusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/test", nil)

			httputils.WriteError(nil, rec, req, tt.err, tt.statusCode)
			assert.Equal(t, tt.statusCode, rec.Code)
		})
	}
}

func TestWriteJSON_ContentTypePreserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		want        string
	}{
		{
			name:        "preserve custom content type",
			contentType: "application/custom",
			want:        "application/custom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			rec.Header().Set("Content-Type", tt.contentType)

			httputils.WriteJSON(
				slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
				rec,
				http.StatusOK,
				map[string]string{"a": "b"},
			)
			assert.Equal(t, tt.want, rec.Header().Get("Content-Type"))
		})
	}
}

func TestDrainBody_Closes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data string
	}{
		{
			name: "closes body",
			data: "data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			closed := false
			body := io.NopCloser(bytes.NewReader([]byte(tt.data)))
			wrapped := &closeWatcher{ReadCloser: body, closed: &closed}

			req := &http.Request{Body: wrapped}
			httputils.DrainBody(req)

			assert.True(t, closed, "expected body to be closed")
		})
	}
}

type closeWatcher struct {
	io.ReadCloser

	closed *bool
}

func (c *closeWatcher) Close() error {
	*c.closed = true

	return c.ReadCloser.Close()
}
