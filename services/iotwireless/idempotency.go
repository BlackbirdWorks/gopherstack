package iotwireless

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"maps"
	"net/http"
	"net/url"
	"sync"

	"github.com/labstack/echo/v5"
)

const maxIdempotencyEntries = 1024

const (
	opCreateDeviceProfile   = "CreateDeviceProfile"
	opCreateFuotaTask       = "CreateFuotaTask"
	opCreateServiceProfile  = "CreateServiceProfile"
	opCreateWirelessDevice  = "CreateWirelessDevice"
	opCreateWirelessGateway = "CreateWirelessGateway"
)

func isIdempotentOp(op string) bool {
	switch op {
	case opCreateDestination, opCreateDeviceProfile, opCreateFuotaTask, opCreateMulticastGroup,
		opCreateNetworkAnalyzerConfiguration, opCreateServiceProfile, opCreateWirelessDevice,
		opCreateWirelessGateway, opCreateWirelessGatewayTaskDefinition,
		opStartSingleWirelessDeviceImportTask, opStartWirelessDeviceImportTask:
		return true
	default:
		return false
	}
}

type idempotentResult struct {
	contentType string
	body        []byte
	params      [sha256.Size]byte
	status      int
}

// idempotencyCache replays the first successful response for an op+token and
// rejects the same token with different parameters. Bounded FIFO, not persisted.
type idempotencyCache struct {
	entries map[string]idempotentResult
	order   []string
	mu      sync.Mutex
}

func (c *idempotencyCache) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = nil
	c.order = nil
}

func (c *idempotencyCache) put(key string, res idempotentResult) {
	if c.entries == nil {
		c.entries = make(map[string]idempotentResult)
	}

	c.entries[key] = res
	c.order = append(c.order, key)

	if len(c.order) > maxIdempotencyEntries {
		delete(c.entries, c.order[0])
		c.order = c.order[1:]
	}
}

// captureWriter buffers a handler's response so it can be cached and replayed.
type captureWriter struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (w *captureWriter) Header() http.Header { return w.header }

func (w *captureWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}

	return w.body.Write(p)
}

func (w *captureWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

// requestTokenAndParams returns the ClientRequestToken and a digest of every
// other body member.
func requestTokenAndParams(body []byte) (string, [sha256.Size]byte, bool) {
	var members map[string]any
	if err := json.Unmarshal(body, &members); err != nil {
		return "", [sha256.Size]byte{}, false
	}

	token, _ := members["ClientRequestToken"].(string)
	if token == "" {
		return "", [sha256.Size]byte{}, false
	}

	delete(members, "ClientRequestToken")
	canonical, err := json.Marshal(members)
	if err != nil {
		return "", [sha256.Size]byte{}, false
	}

	return token, sha256.Sum256(canonical), true
}

// dispatchIdempotent honours ClientRequestToken: same parameters replay the
// first response, different parameters are a 409 ConflictException.
func (h *Handler) dispatchIdempotent(
	c *echo.Context, op, resource string, body []byte, query url.Values,
) error {
	if !isIdempotentOp(op) {
		return h.dispatch(c, op, resource, body, query)
	}

	token, params, ok := requestTokenAndParams(body)
	if !ok {
		return h.dispatch(c, op, resource, body, query)
	}

	h.idem.mu.Lock()
	defer h.idem.mu.Unlock()

	key := op + "\x00" + token
	if prev, hit := h.idem.entries[key]; hit {
		if prev.params != params {
			return writeError(c, http.StatusConflict, "ClientRequestToken was already used with different parameters")
		}

		c.Response().Header().Set("Content-Type", prev.contentType)
		c.Response().WriteHeader(prev.status)
		_, _ = c.Response().Write(prev.body)

		return nil
	}

	orig := c.Response()
	cw := &captureWriter{header: http.Header{}}
	c.SetResponse(cw)
	err := h.dispatch(c, op, resource, body, query)
	c.SetResponse(orig)

	maps.Copy(orig.Header(), cw.header)

	orig.WriteHeader(cw.status)
	_, _ = orig.Write(cw.body.Bytes())

	if err == nil && cw.status >= http.StatusOK && cw.status < http.StatusMultipleChoices {
		h.idem.put(key, idempotentResult{
			status:      cw.status,
			contentType: cw.header.Get("Content-Type"),
			body:        cw.body.Bytes(),
			params:      params,
		})
	}

	return err
}
