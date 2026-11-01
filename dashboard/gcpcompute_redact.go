package dashboard

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	computeMetaKey    = "key"
	computeMetaValue  = "value"
	computeMetaLength = "valueLength"
	computeMetaHidden = "redacted"
	computeMetaField  = "metadata"
	computeMetaItems  = "items"
)

// serveRedacted runs the upstream request and hides every metadata value in a successful JSON answer. Startup scripts
// and SSH keys live in metadata, so only an explicit reveal of one key that is not SSH-related keeps its value.
func serveRedacted(c *echo.Context, inner *echo.Echo, fwd *http.Request, reveal string) error {
	rec := httptest.NewRecorder()
	inner.ServeHTTP(rec, fwd)

	body := rec.Body.Bytes()
	if rec.Code == http.StatusOK {
		body = redactComputeMetadata(body, reveal)
	}

	h := c.Response().Header()
	for k, v := range rec.Header() {
		if k != "Content-Length" {
			h[k] = v
		}
	}

	h.Set("Content-Length", strconv.Itoa(len(body)))
	c.Response().WriteHeader(rec.Code)

	_, err := c.Response().Write(body)

	return err
}

func computeRevealable(key string) bool {
	return key != "" && !strings.Contains(strings.ToLower(key), "ssh")
}

func redactComputeMetadata(body []byte, reveal string) []byte {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	var doc any
	if err := dec.Decode(&doc); err != nil {
		return body
	}

	redactWalk(doc, reveal)

	out, err := json.Marshal(doc)
	if err != nil {
		return body
	}

	return out
}

func redactWalk(v any, reveal string) {
	switch t := v.(type) {
	case map[string]any:
		if meta, ok := t[computeMetaField].(map[string]any); ok {
			redactItems(meta, reveal)
		}

		for _, child := range t {
			redactWalk(child, reveal)
		}
	case []any:
		for _, child := range t {
			redactWalk(child, reveal)
		}
	}
}

func redactItems(meta map[string]any, reveal string) {
	items, ok := meta[computeMetaItems].([]any)
	if !ok {
		return
	}

	for _, it := range items {
		m, isMap := it.(map[string]any)
		if !isMap {
			continue
		}

		val, _ := m[computeMetaValue].(string)
		m[computeMetaLength] = len(val)

		if key, _ := m[computeMetaKey].(string); key == reveal && computeRevealable(key) {
			continue
		}

		m[computeMetaValue] = ""
		m[computeMetaHidden] = true
	}
}
