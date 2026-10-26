package gcpworkflows

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

const (
	callbackSegment = "/callbacks/"
	apiPrefix       = "/v1/"
	maxCallbackBody = 512 << 10
)

var errNoListener = errors.New("the Workflows listener is not running")

func (b *InMemoryBackend) createCallback(execName, method string) (string, error) {
	b.mu.Lock("createCallback")
	defer b.mu.Unlock()

	if b.pushBase == "" {
		return "", errNoListener
	}

	path := apiPrefix + execName + callbackSegment + strings.ReplaceAll(uuid.NewString(), "-", "")
	b.callbacks[path] = &callback{method: method, exec: execName, hit: make(chan map[string]any, 1)}

	return b.pushBase + path, nil
}

func (b *InMemoryBackend) awaitCallback(ctx context.Context, url string) (map[string]any, error) {
	path := url
	if i := strings.Index(url, apiPrefix); i >= 0 {
		path = url[i:]
	}

	b.mu.RLock("awaitCallback")
	cb, ok := b.callbacks[path]
	b.mu.RUnlock()

	if !ok {
		return nil, wfinterp.NewError("the callback endpoint no longer exists", wfinterp.TagOperationError)
	}

	select {
	case req := <-cb.hit:
		b.mu.Lock("awaitCallback")
		delete(b.callbacks, path)
		b.mu.Unlock()

		return req, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// CallbackHandler serves the endpoints events.create_callback_endpoint hands out.
func (b *InMemoryBackend) CallbackHandler() http.Handler {
	return http.HandlerFunc(b.serveCallback)
}

// IsCallbackPath reports whether path addresses a callback endpoint.
func IsCallbackPath(path string) bool {
	return strings.HasPrefix(path, apiPrefix+"projects/") && strings.Contains(path, callbackSegment)
}

func writeJSONError(w http.ResponseWriter, code int, status, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).
		Encode(map[string]any{"error": map[string]any{"code": code, "message": msg, "status": status}})
}

func (b *InMemoryBackend) serveCallback(w http.ResponseWriter, r *http.Request) {
	b.mu.RLock("serveCallback")
	cb, ok := b.callbacks[r.URL.Path]
	b.mu.RUnlock()

	switch {
	case !ok:
		writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "The callback endpoint was not found or has expired.")

		return
	case r.Method != cb.method:
		w.Header().Set("Allow", cb.method)
		writeJSONError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "The callback expects "+cb.method+".")

		return
	}

	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxCallbackBody))
	if err != nil {
		writeJSONError(w, http.StatusRequestEntityTooLarge, "INVALID_ARGUMENT", "The request body is too large.")

		return
	}

	headers := make(map[string]any, len(r.Header))
	for k, v := range r.Header {
		headers[k] = strings.Join(v, ",")
	}

	query := make(map[string]any, len(r.URL.Query()))
	for k, v := range r.URL.Query() {
		query[k] = strings.Join(v, ",")
	}

	req := map[string]any{
		"method": r.Method, "headers": headers, "query": query, "url": r.URL.Path, "body": callbackBody(r, raw),
	}

	select {
	case cb.hit <- req:
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}"))
	default:
		writeJSONError(w, http.StatusConflict, "ALREADY_EXISTS", "The callback was already received.")
	}
}

func callbackBody(r *http.Request, raw []byte) any {
	if len(raw) == 0 {
		return nil
	}

	if strings.Contains(r.Header.Get("Content-Type"), "json") {
		if v, err := wfinterp.ParseJSON(raw); err == nil {
			return v
		}
	}

	if strings.HasPrefix(r.Header.Get("Content-Type"), "text/") {
		return string(raw)
	}

	return raw
}
