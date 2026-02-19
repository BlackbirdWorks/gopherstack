package handler

import (
	"context"
	"encoding/json"
	"hash/crc32"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

// WriteJSON marshals the payload to JSON, sets standard headers, and writes the response.
// Sets Content-Type to "application/json" and Content-Length.
func WriteJSON(w http.ResponseWriter, code int, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(code)

	_, err = w.Write(body)

	return err
}

// WriteJSONWithChecksum marshals the payload to JSON, adds DynamoDB-style CRC32 checksum header,
// and writes the response. The checksum is required by the DynamoDB wire protocol.
// Sets Content-Type to "application/x-amz-json-1.0" and X-Amz-Crc32.
func WriteJSONWithChecksum(w http.ResponseWriter, code int, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	checksum := crc32.ChecksumIEEE(body)
	w.Header().Set("X-Amz-Crc32", strconv.FormatUint(uint64(checksum), 10))
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(code)

	_, err = w.Write(body)

	return err
}

// WriteError writes an error response with structured logging.
// Uses the logger to record the error with context.
func WriteError(logger *slog.Logger, w http.ResponseWriter, code int, message string, err error) {
	if err != nil {
		logger.DebugContext(context.TODO(), message, "error", err)
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(message))
}

// EchoError is a helper for Echo handlers to write errors with proper logging.
func EchoError(logger *slog.Logger, c *echo.Context, code int, message string, err error) error {
	if err != nil {
		logger.DebugContext(c.Request().Context(), message, "error", err)
	}

	return c.String(code, message)
}
