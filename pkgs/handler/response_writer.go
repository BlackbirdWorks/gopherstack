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
		statusCode:     http.StatusOK, // default per HTTP spec
	}
}

// WriteHeader writes the status code and delegates to the wrapped ResponseWriter.
func (w *ResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// Write sets status to [http.StatusOK] if not already set, then delegates to wrapped ResponseWriter.
func (w *ResponseWriter) Write(b []byte) (int, error) {
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}

	return w.ResponseWriter.Write(b)
}

// StatusCode returns the HTTP status code that was written.
func (w *ResponseWriter) StatusCode() int {
	return w.statusCode
}
