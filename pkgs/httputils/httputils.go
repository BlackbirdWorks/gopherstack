package httputils

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"strconv"
)

// ReadBody reads the request body and returns it as a byte slice.
// It handles cases where r.Body might be nil (e.g. in some test environments).
func ReadBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}

	return io.ReadAll(r.Body)
}

// DrainBody reads and discards the request body.
// This is important for HTTP keep-alive, as the server needs to know
// the request body has been fully consumed before reusing the connection.
func DrainBody(r *http.Request) {
	if r.Body != nil {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
	}
}

// WriteError writes an error response with the given status code.
// It drains the request body to ensure connection reuse.
func WriteError(logger *slog.Logger, w http.ResponseWriter, r *http.Request, err error, code int) {
	DrainBody(r)
	if logger != nil {
		logger.Error("request failed", "error", err, "code", code, "path", r.URL.Path)
	}
	http.Error(w, err.Error(), code)
}

// WriteJSON writes a JSON response with the given status code.
func WriteJSON(logger *slog.Logger, w http.ResponseWriter, code int, payload any) {
	response, err := json.Marshal(payload)
	if err != nil {
		if logger != nil {
			logger.Error("failed to marshal JSON response", "error", err)
		}
		http.Error(w, "internal server error", http.StatusInternalServerError)

		return
	}

	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	w.WriteHeader(code)
	if _, wErr := w.Write(response); wErr != nil && logger != nil {
		logger.Error("failed to write JSON response", "error", wErr)
	}
}

// WriteXML writes an XML response with the given status code.
// The full body is buffered before writing so that Content-Length can be set,
// allowing the HTTP client to know when the body is fully received.
func WriteXML(logger *slog.Logger, w http.ResponseWriter, code int, payload any) {
	var buf bytes.Buffer
	buf.WriteString(xml.Header)

	encoder := xml.NewEncoder(&buf)
	if err := encoder.Encode(payload); err != nil {
		if logger != nil {
			logger.Error("failed to marshal XML response", "error", err)
		}
		http.Error(w, "internal server error", http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	w.WriteHeader(code)
	if _, err := buf.WriteTo(w); err != nil && logger != nil {
		logger.Error("failed to write XML response", "error", err)
	}
}
