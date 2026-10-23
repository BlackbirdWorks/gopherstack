package azureeventgrid

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	opPublishEvents        = "PublishEvents"
	opPublishCloudEvents   = "PublishCloudEvents"
	opSendEvents           = "SendEvents"
	opReceiveEvents        = "ReceiveEvents"
	opAcknowledgeEvents    = "AcknowledgeEvents"
	opReleaseEvents        = "ReleaseEvents"
	opRejectEvents         = "RejectEvents"
	opRenewEventLocks      = "RenewEventLocks"
	opValidateSubscription = "ValidateSubscription"
	unknownOperation       = "Unknown"

	defaultWaitSeconds = 60
	maxWaitSeconds     = 120
	maxLockTokens      = 100
	maxLockBodyBytes   = 64 << 10
)

type routeKind int

const (
	kindUnknown routeKind = iota
	kindPublish
	kindValidate
	kindNsPublish
	kindPull
)

type route struct {
	topic  string
	sub    string
	action string
	kind   routeKind
}

func splitSegments(p string) []string {
	var out []string

	for s := range strings.SplitSeq(p, "/") {
		if s != "" {
			out = append(out, s)
		}
	}

	return out
}

func parseRoute(method, path string) route {
	segs := splitSegments(path)

	switch {
	case len(segs) == 2 && strings.EqualFold(segs[0], "api") && strings.EqualFold(segs[1], "events"):
		return route{kind: kindPublish}
	case len(segs) == 1 && segs[0] == "validate" && method == http.MethodGet:
		return route{kind: kindValidate}
	case len(segs) == 2 && segs[0] == "topics":
		if name, action, ok := strings.Cut(segs[1], ":"); ok && action == "publish" {
			return route{kind: kindNsPublish, topic: name}
		}
	case len(segs) == 4 && segs[0] == "topics" && segs[2] == "eventsubscriptions":
		if name, action, ok := strings.Cut(segs[3], ":"); ok {
			return route{kind: kindPull, topic: segs[1], sub: name, action: action}
		}
	}

	return route{}
}

func operationOf(r *http.Request) string {
	rt := parseRoute(r.Method, r.URL.Path)

	switch rt.kind {
	case kindPublish:
		if strings.Contains(r.Header.Get("Content-Type"), "cloudevents") {
			return opPublishCloudEvents
		}

		return opPublishEvents
	case kindValidate:
		return opValidateSubscription
	case kindNsPublish:
		return opSendEvents
	case kindPull:
		return pullOps()[rt.action]
	case kindUnknown:
	}

	return unknownOperation
}

func pullOps() map[string]string {
	return map[string]string{
		"receive": opReceiveEvents, "acknowledge": opAcknowledgeEvents, "release": opReleaseEvents,
		"reject": opRejectEvents, "renewLock": opRenewEventLocks,
	}
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

func writeError(c *echo.Context, status int, code, message string) error {
	return c.JSON(status, map[string]any{"error": map[string]any{"code": code, "message": message}})
}

func errorStatus(err error) (int, string) {
	switch {
	case errors.Is(err, ErrUnauthorized):
		return http.StatusUnauthorized, "Unauthorized"
	case errors.Is(err, ErrTopicNotFound), errors.Is(err, ErrNamespaceTopicNotFound),
		errors.Is(err, ErrPullSubNotFound), errors.Is(err, ErrNamespaceNotFound):
		return http.StatusNotFound, outcomeNotFound
	case errors.Is(err, ErrTooManyEvents):
		return http.StatusRequestEntityTooLarge, "RequestEntityTooLarge"
	}

	return http.StatusBadRequest, outcomeBadRequest
}

func writeErr(c *echo.Context, err error) error {
	status, code := errorStatus(err)

	return writeError(c, status, code, err.Error())
}

func readBody(c *echo.Context, limit int64) ([]byte, error) {
	body, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, limit))
	if err != nil {
		return nil, ErrTooManyEvents
	}

	return body, nil
}

// Handler returns the echo handler of the HTTPS listener.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		c.Response().Header().Set("Server", "Microsoft-HTTPAPI/2.0")

		r := c.Request()
		rt := parseRoute(r.Method, r.URL.Path)

		switch {
		case rt.kind == kindValidate:
			return h.handleValidate(c)
		case r.Method != http.MethodPost:
			return writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowed", "Only POST is supported.")
		case rt.kind == kindPublish:
			return h.handlePublish(c)
		case rt.kind == kindNsPublish:
			return h.handleNsPublish(c, rt)
		case rt.kind == kindPull:
			return h.handlePull(c, rt)
		}

		return writeError(c, http.StatusNotFound, outcomeNotFound, "The requested URI does not represent any resource.")
	}
}

func (h *Handler) handleValidate(c *echo.Context) error {
	if !h.Backend.ConfirmValidation(c.Request().URL.Query().Get("id")) {
		return writeError(c, http.StatusNotFound, outcomeNotFound, "The validation code is unknown or already used.")
	}

	return c.String(http.StatusOK, "Webhook successfully validated as a subscription endpoint.")
}

func (h *Handler) handlePublish(c *echo.Context) error {
	r := c.Request()

	name, err := h.Backend.ResolveTopic(hostLabel(r), r, h.Settings.ValidateSAS)
	if err != nil {
		return writeErr(c, err)
	}

	info, err := h.Backend.Topic(name)
	if err != nil {
		return writeErr(c, err)
	}

	body, err := readBody(c, MaxRequestBytes)
	if err != nil {
		return writeErr(c, err)
	}

	events, err := ParseEvents(body, info.InputSchema)
	if err != nil {
		return writeErr(c, err)
	}

	if err = h.Backend.Publish(name, events); err != nil {
		return writeErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleNsPublish(c *echo.Context, rt route) error {
	r := c.Request()

	ns, err := h.Backend.ResolveNamespace(hostLabel(r), rt.topic, r, h.Settings.ValidateSAS)
	if err != nil {
		return writeErr(c, err)
	}

	body, err := readBody(c, MaxRequestBytes)
	if err != nil {
		return writeErr(c, err)
	}

	events, err := ParseEvents(body, SchemaCloudEvent)
	if err != nil {
		return writeErr(c, err)
	}

	if err = h.Backend.PublishNamespace(ns, rt.topic, events); err != nil {
		return writeErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func queryInt(r *http.Request, key string, def int) int {
	if v, err := strconv.Atoi(r.URL.Query().Get(key)); err == nil {
		return v
	}

	return def
}

func (h *Handler) handlePull(c *echo.Context, rt route) error {
	r := c.Request()

	ns, err := h.Backend.ResolveNamespace(hostLabel(r), rt.topic, r, h.Settings.ValidateSAS)
	if err != nil {
		return writeErr(c, err)
	}

	if rt.action == "receive" {
		return h.handleReceive(c, ns, rt)
	}

	if _, known := pullOps()[rt.action]; !known {
		return writeError(c, http.StatusNotFound, outcomeNotFound, "Unknown action.")
	}

	body, err := readBody(c, maxLockBodyBytes)
	if err != nil {
		return writeErr(c, err)
	}

	var in struct {
		LockTokens []string `json:"lockTokens"`
	}

	if err = json.Unmarshal(body, &in); err != nil || len(in.LockTokens) == 0 || len(in.LockTokens) > maxLockTokens {
		return writeError(c, http.StatusBadRequest, outcomeBadRequest, "lockTokens must hold 1 to 100 tokens.")
	}

	ok, failed, err := h.applyLockOp(rt, ns, in.LockTokens, queryInt(r, "releaseDelayInSeconds", 0))
	if err != nil {
		return writeErr(c, err)
	}

	return c.JSON(http.StatusOK, lockResult(ok, failed))
}

func (h *Handler) applyLockOp(rt route, ns string, tokens []string, delaySec int) ([]string, []LockFailure, error) {
	b := h.Backend

	switch rt.action {
	case "acknowledge":
		return b.Acknowledge(ns, rt.topic, rt.sub, tokens)
	case "release":
		return b.Release(ns, rt.topic, rt.sub, tokens, time.Duration(max(delaySec, 0))*time.Second)
	case "reject":
		return b.Reject(ns, rt.topic, rt.sub, tokens)
	}

	return b.RenewLock(ns, rt.topic, rt.sub, tokens)
}

func lockResult(ok []string, failed []LockFailure) map[string]any {
	fails := make([]map[string]any, 0, len(failed))

	for _, f := range failed {
		fails = append(fails, map[string]any{
			"lockToken": f.LockToken, "errorCode": f.Code, "errorDescription": f.Message,
			"error": map[string]any{"code": f.Code, "message": f.Message},
		})
	}

	if ok == nil {
		ok = []string{}
	}

	return map[string]any{"succeededLockTokens": ok, "failedLockTokens": fails}
}

func (h *Handler) handleReceive(c *echo.Context, ns string, rt route) error {
	r := c.Request()
	wait := min(max(queryInt(r, "maxWaitTime", defaultWaitSeconds), 0), maxWaitSeconds)

	got, err := h.Backend.Receive(
		r.Context(),
		ns,
		rt.topic,
		rt.sub,
		queryInt(r, "maxEvents", 1),
		time.Duration(wait)*time.Second,
	)
	if err != nil {
		return writeErr(c, err)
	}

	value := make([]map[string]any, 0, len(got))

	for _, g := range got {
		value = append(value, map[string]any{
			"brokerProperties": map[string]any{"lockToken": g.LockToken, "deliveryCount": g.DeliveryCount},
			"event":            g.Event,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"value": value})
}
