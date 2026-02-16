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

	req := &http.Request{Body: errReadCloser{}}
	_, err := httputils.ReadBody(req)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestWriteError_NilLogger(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)

	httputils.WriteError(nil, rec, req, errBoom, http.StatusBadRequest)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestWriteJSON_ContentTypePreserved(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	rec.Header().Set("Content-Type", "application/custom")

	httputils.WriteJSON(
		slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		rec,
		http.StatusOK,
		map[string]string{"a": "b"},
	)
	if got := rec.Header().Get("Content-Type"); got != "application/custom" {
		t.Fatalf("expected Content-Type preserved, got %q", got)
	}
}

func TestDrainBody_Closes(t *testing.T) {
	t.Parallel()

	closed := false
	body := io.NopCloser(bytes.NewReader([]byte("data")))
	wrapped := &closeWatcher{ReadCloser: body, closed: &closed}

	req := &http.Request{Body: wrapped}
	httputils.DrainBody(req)

	if !closed {
		t.Fatalf("expected body to be closed")
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
