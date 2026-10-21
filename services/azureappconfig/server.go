package azureappconfig

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

var apiVersions = map[string]bool{ //nolint:gochecknoglobals // supported api-version set, read-only
	"1.0": true, "2022-11-01-preview": true, "2023-10-01": true, "2023-11-01": true, "2024-09-01": true,
}

// request is one parsed data-plane call.
type request struct {
	c     *echo.Context
	r     *http.Request
	q     url.Values
	store string
	segs  []string
	body  []byte
}

func lowerQuery(raw url.Values) url.Values {
	out := make(url.Values, len(raw))
	for k, v := range raw {
		out[strings.ToLower(k)] = append(out[strings.ToLower(k)], v...)
	}

	return out
}

func hostLabel(r *http.Request) string {
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	if label, _, ok := strings.Cut(host, "."); ok && net.ParseIP(host) == nil {
		return label
	}

	return ""
}

func readBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		return nil, err
	}

	if len(body) > maxBodyBytes {
		return nil, ErrTooLarge
	}

	return body, nil
}

// resolveStore picks the store: /stores/{name} path prefix, host label of an existing store,
// the store owning the HMAC credential, else the default store.
func (h *Handler) resolveStore(r *http.Request, prefix string) (string, error) {
	b := h.Backend

	if prefix != "" {
		return prefix, h.ensure(prefix)
	}

	if label := hostLabel(r); label != "" && b.HasStore(label) {
		return label, nil
	}

	if id := credentialID(r); id != "" {
		if owner, _, ok := b.KeyOwner(id); ok {
			return owner, nil
		}
	}

	return DefaultStoreName, h.ensure(DefaultStoreName)
}

func (h *Handler) ensure(name string) error {
	if h.Backend.HasStore(name) {
		return nil
	}

	if !h.Settings.AutoCreate {
		return ErrStoreNotFound
	}

	return h.Backend.EnsureStore(name)
}

func (h *Handler) newRequest(c *echo.Context) (*request, error) {
	r := c.Request()
	prefix, path := splitStorePrefix(r.URL.EscapedPath())
	q := lowerQuery(r.URL.Query())

	body, err := readBody(r)
	if err != nil {
		return nil, err
	}

	store, err := h.resolveStore(r, prefix)
	if err != nil {
		return &request{c: c, r: r, store: store}, err
	}

	req := &request{c: c, r: r, q: q, store: store, segs: routeSegments(path), body: body}

	if h.Settings.ValidateHMAC {
		owner, authErr := h.Backend.authorize(r, body)
		if authErr == nil && owner != "" && !strings.EqualFold(owner, store) {
			authErr = ErrUnauthorized
		}

		if authErr != nil {
			return req, authErr
		}
	}

	if !apiVersions[q.Get("api-version")] {
		return req, ErrInvalidArgument
	}

	return req, nil
}

func (req *request) syncHeader(h *Handler) {
	req.c.Response().Header().Set("Sync-Token", h.Backend.SyncToken(req.store))
}

func (h *Handler) fail(req *request, err error) error {
	status, body := problemBody(err)

	if req.store != "" {
		req.syncHeader(h)
	}

	if status == http.StatusUnauthorized {
		req.c.Response().Header().Set("WWW-Authenticate", "HMAC-SHA256, Bearer")
	}

	req.c.Response().Header().Set("Content-Type", mediaProblem)

	return req.c.JSONBlob(status, mustJSON(body))
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}

	return b
}

func (h *Handler) respond(req *request, status int, media string, v any) error {
	req.syncHeader(h)
	req.c.Response().Header().Set("Content-Type", media)

	return req.c.JSONBlob(status, mustJSON(v))
}

func (h *Handler) respondEmpty(req *request, status int) error {
	req.syncHeader(h)

	return req.c.NoContent(status)
}

// Handler returns the echo handler of the HTTPS listener.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		c.Response().Header().Set("Server", "Microsoft-HTTPAPI/2.0")

		req, err := h.newRequest(c)
		if err != nil {
			if req == nil {
				req = &request{c: c, r: c.Request()}
			}

			return h.fail(req, err)
		}

		return h.dispatch(req)
	}
}

func (h *Handler) dispatch(req *request) error {
	if len(req.segs) == 0 {
		return h.fail(req, ErrKeyNotFound)
	}

	var err error

	switch strings.ToLower(req.segs[0]) {
	case "kv":
		err = h.routeKV(req)
	case "keys":
		err = h.routeKeys(req)
	case "labels":
		err = h.routeLabels(req)
	case "revisions":
		err = h.routeRevisions(req)
	case "locks":
		err = h.routeLocks(req)
	case "snapshots":
		err = h.routeSnapshots(req)
	case "operations":
		err = h.routeOperations(req)
	default:
		err = ErrKeyNotFound
	}

	if err != nil {
		return h.fail(req, err)
	}

	return nil
}

func methodNotAllowed() error { return ErrInvalidArgument }

func parseTime(v string) (time.Time, error) {
	if t, err := http.ParseTime(v); err == nil {
		return t.UTC(), nil
	}

	return time.Parse(time.RFC3339Nano, v)
}

// pointInTime resolves Accept-Datetime, the "at" parameter, or an "after" value that is a timestamp.
func (req *request) pointInTime() (time.Time, error) {
	for _, v := range []string{req.r.Header.Get("Accept-Datetime"), req.q.Get("at"), req.q.Get("after")} {
		if v == "" {
			continue
		}

		t, err := parseTime(v)
		if err == nil {
			return t, nil
		}

		if v != req.q.Get("after") {
			return time.Time{}, ErrInvalidArgument
		}
	}

	return time.Time{}, nil
}

func (req *request) precondition() Precondition {
	return Precondition{
		IfMatch:     unquoteETag(req.r.Header.Get("If-Match")),
		IfNoneMatch: unquoteETag(req.r.Header.Get("If-None-Match")),
	}
}

func decodeBody(body []byte, out any) error {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}

	if err := json.Unmarshal(body, out); err != nil {
		return ErrInvalidArgument
	}

	return nil
}
