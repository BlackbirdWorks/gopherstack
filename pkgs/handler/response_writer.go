package handler

import "net/http"

// ResponseWriter wraps [http.ResponseWriter] and tracks the HTTP status code.
// Use this when you need to inspect the status after WriteHeader is called.
type ResponseWriter struct {
	http.ResponseWriter

	statusCode int
}

// NewResponseWriter creates a ResponseWriter that wraps the given [http.ResponseWriter].
func NewResponseWriter(w http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{
		ResponseWriter: w,
	}
}

// WriteHeader writes the status code and delegates to the wrapped ResponseWriter.
func (w *ResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.ResponseWriter.WriteHeader(code)
}

// Write delegates to the wrapped ResponseWriter.
// If WriteHeader has not been called, the status code defaults to [http.StatusOK]
// consistent with the behaviour of the standard library's ResponseWriter.
func (w *ResponseWriter) Write(b []byte) (int, error) {
	if w.statusCode == 0 {
		w.WriteHeader(http.StatusOK)
	}

	return w.ResponseWriter.Write(b)
}

// StatusCode returns the HTTP status code that was written.
func (w *ResponseWriter) StatusCode() int {
	return w.statusCode
}
