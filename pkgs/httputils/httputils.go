package httputils

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"hash/crc32"
	"io"
	"log/slog"
	"net/http"
	"strconv"
)

// ReadBody reads the request body and returns it as a byte slice.
// It handles cases where r.Body might be nil (e.g. in some test environments).
// It re-seeds the request body so it can be read multiple times.
func ReadBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	// Re-seed the body so it can be read again
	r.Body = io.NopCloser(bytes.NewReader(body))

	return body, nil
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
	w.WriteHeader(code)
	if _, wErr := w.Write(response); wErr != nil && logger != nil {
		logger.Error("failed to write JSON response", "error", wErr)
	}
}

// WriteXML writes an XML response with the given status code.
// The full body is buffered before writing it to the response.
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
	w.WriteHeader(code)
	if _, err := buf.WriteTo(w); err != nil && logger != nil {
		logger.Error("failed to write XML response", "error", err)
	}
}

// WriteDynamoDBResponse writes a DynamoDB-style JSON response with CRC32 checksum.
func WriteDynamoDBResponse(logger *slog.Logger, w http.ResponseWriter, code int, payload any) {
	response, err := json.Marshal(payload)
	if err != nil {
		if logger != nil {
			logger.Error("failed to marshal DynamoDB response", "error", err)
		}
		http.Error(w,
			`{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"internal server error"}`,
			http.StatusInternalServerError)

		return
	}

	checksum := crc32.ChecksumIEEE(response)
	w.Header().Set("X-Amz-Crc32", strconv.FormatUint(uint64(checksum), 10))
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(code)
	if _, wErr := w.Write(response); wErr != nil && logger != nil {
		logger.Error("failed to write DynamoDB response", "error", wErr)
	}
}

// WriteS3ErrorResponse writes an S3-compatible XML error response.
func WriteS3ErrorResponse(logger *slog.Logger, w http.ResponseWriter, r *http.Request, s3Err any, code int) {
	DrainBody(r)
	WriteXML(logger, w, code, s3Err)
}
