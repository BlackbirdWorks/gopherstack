package service

import (
	"bytes"
	"encoding/json"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

// JSONErrorMessageMiddleware mirrors a lone "message" or "Message" key in JSON
// error bodies to the other casing. Schema-serde SDK clients match the modeled key
// case-sensitively and otherwise surface an empty ErrorMessage().
func JSONErrorMessageMiddleware() Middleware {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			resp, ok := c.Response().(*echo.Response)
			if !ok {
				return next(c)
			}

			w := &jsonErrorMirrorWriter{ResponseWriter: resp.ResponseWriter, passthrough: resp.ResponseWriter}
			resp.ResponseWriter = w
			err := next(c)
			resp.ResponseWriter = w.ResponseWriter
			w.flush()

			return err
		}
	}
}

type jsonErrorMirrorWriter struct {
	http.ResponseWriter
	passthrough io.Writer
	body        bytes.Buffer
	status      int
	buffering   bool
}

func (w *jsonErrorMirrorWriter) WriteHeader(code int) {
	if w.status != 0 {
		return
	}

	w.status = code

	if code >= httpErrorStatusThreshold && strings.Contains(w.Header().Get("Content-Type"), "json") {
		w.buffering = true

		return
	}

	w.ResponseWriter.WriteHeader(code)
}

func (w *jsonErrorMirrorWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}

	if w.buffering {
		return w.body.Write(b)
	}

	return w.passthrough.Write(b)
}

func (w *jsonErrorMirrorWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *jsonErrorMirrorWriter) flush() {
	if !w.buffering {
		return
	}

	w.buffering = false
	out := mirrorMessageKey(w.body.Bytes())

	h := w.Header()
	h.Del("Content-Length")

	if h.Get("X-Amz-Crc32") != "" {
		h.Set("X-Amz-Crc32", strconv.FormatUint(uint64(crc32.ChecksumIEEE(out)), 10))
	}

	w.ResponseWriter.WriteHeader(w.status)
	_, _ = w.ResponseWriter.Write(out)
}

func mirrorMessageKey(body []byte) []byte {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		return body
	}

	lower, hasLower := obj["message"]
	upper, hasUpper := obj["Message"]

	switch {
	case hasLower == hasUpper:
		return body
	case hasLower:
		if len(lower) == 0 || lower[0] != '"' {
			return body
		}
		obj["Message"] = lower
	default:
		if len(upper) == 0 || upper[0] != '"' {
			return body
		}
		obj["message"] = upper
	}

	out, err := json.Marshal(obj)
	if err != nil {
		return body
	}

	return out
}
