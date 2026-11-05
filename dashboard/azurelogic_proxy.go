package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	azureLogicPrefix       = "/dashboard/api/v1/azure/logic"
	azureLogicAccessPrefix = "/dashboard/api/v1/azure/logicaccess"
	logicPxNamespace       = "microsoft.logic"
	logicPxARMNamespace    = "Microsoft.Logic"
	logicPxAPIVersion      = "2019-05-01"
	logicPxWorkflows       = "workflows"
	logicPxTriggers        = "triggers"
	logicPxRuns            = "runs"
	logicPxActions         = "actions"
	logicPxDataPrefix      = "/_logic/workflows/"
	logicPxRGList          = 7
	logicPxRestItem        = 2
	logicPxRestTrigger     = 3
	logicPxRestAction      = 4
	logicPxMaxInvoke       = 1 << 16
	logicPxMaxReply        = 256 << 10
	logicPxMaxContent      = 1 << 20
	logicPxKeyURI          = "uri"
	logicPxKeyValue        = "value"
	logicPxKindInputs      = "inputs"
	logicPxKindOutputs     = "outputs"
)

var (
	logicPxSubRe  = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)
	logicPxNameRe = regexp.MustCompile(`^[A-Za-z0-9._()-]{1,128}$`)
)

// logicPxQueryKeys are the only list parameters forwarded to the emulator.
//
//nolint:gochecknoglobals // immutable allow-list
var logicPxQueryKeys = []string{"$top", "$skiptoken", "$filter"}

// registerAzureLogicRoutes exposes Microsoft.Logic workflows by dispatching in-process to the ARM handler. The
// generic proxy reaches only workflow reads, enable/disable, trigger and run reads, recurrence trigger runs and run
// cancellation, with the api-version pinned and the signed content links removed from every answer. The routes below
// the access prefix are the only ones that touch signed URLs, and each does so server side: the callback URL is
// returned only by an explicit POST, a trigger invocation and a content read use the signed URL without ever
// returning it.
func (h *DashboardHandler) registerAzureLogicRoutes() {
	h.registerARMProxy(armProxy{
		name: "Azure Logic Apps", prefix: azureLogicPrefix,
		queryKeys: logicPxQueryKeys, version: logicPxAPIVersion,
		methods: []string{http.MethodGet, http.MethodPost},
		allowed: logicProxyAllowed,
		body:    logicProxyBody,
		view:    logicProxyView,
	})

	var inner *echo.Echo

	if h.config.AzureARMOps != nil {
		inner = echo.New()
		inner.Any("/*", h.config.AzureARMOps.Handler())
	}

	a := logicAccess{inner: inner}
	guard := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if inner == nil {
				return azureACIFail(c, http.StatusServiceUnavailable, errServiceUnavailable, "Azure Logic Apps is not enabled")
			}

			return next(c)
		}
	}

	const ids = "/:sub/:rg/:wf"

	h.SubRouter.Add(http.MethodPost, azureLogicAccessPrefix+"/callback"+ids+"/:trigger", guard(a.callback))
	h.SubRouter.Add(http.MethodPost, azureLogicAccessPrefix+"/invoke"+ids+"/:trigger", guard(a.invoke))
	h.SubRouter.Add(http.MethodGet, azureLogicAccessPrefix+"/content"+ids+"/:run/trigger/:kind", guard(a.content))
	h.SubRouter.Add(http.MethodGet, azureLogicAccessPrefix+"/content"+ids+"/:run/action/:action/:kind",
		guard(a.content))
}

func logicProxyAllowed(method string, p []string) bool {
	if len(p) == logicPxRGList {
		return method == http.MethodGet && p[0] == armPxSubsSeg && p[2] == armPxRGsSeg && p[4] == armPxProvSeg &&
			p[5] == logicPxNamespace && p[6] == logicPxWorkflows
	}

	typ, name, rest, ok := armScope(p, logicPxNamespace)
	if !ok || typ != logicPxWorkflows || name == "" {
		return false
	}

	return logicProxyRest(method, rest)
}

func logicProxyRest(method string, rest []string) bool {
	get := method == http.MethodGet
	post := method == http.MethodPost

	switch len(rest) {
	case 0:
		return get
	case 1:
		return (get && (rest[0] == logicPxTriggers || rest[0] == logicPxRuns)) ||
			(post && (rest[0] == "enable" || rest[0] == "disable"))
	case logicPxRestItem:
		return get && (rest[0] == logicPxTriggers || rest[0] == logicPxRuns)
	case logicPxRestTrigger:
		return (post && rest[0] == logicPxTriggers && rest[2] == "run") ||
			(post && rest[0] == logicPxRuns && rest[2] == "cancel") ||
			(get && rest[0] == logicPxRuns && rest[2] == logicPxActions)
	case logicPxRestAction:
		return get && rest[0] == logicPxRuns && rest[2] == logicPxActions
	}

	return false
}


// logicProxyBody sends the actions that take no input an empty object.
func logicProxyBody(method string, _ []string, _ []byte) ([]byte, bool) {
	if method == http.MethodPost {
		return []byte("{}"), true
	}

	return nil, true
}

// logicProxyView drops the signed URI of every content link; the page reads content through the access routes.
func logicProxyView(doc any) {
	switch t := doc.(type) {
	case []any:
		for _, x := range t {
			logicProxyView(x)
		}
	case map[string]any:
		for k, x := range t {
			if m, ok := x.(map[string]any); ok && (k == "inputsLink" || k == "outputsLink" || k == "contentLink") {
				delete(m, logicPxKeyURI)

				continue
			}

			logicProxyView(x)
		}
	}
}

type logicAccess struct{ inner *echo.Echo }

type logicIDs struct{ sub, rg, wf string }

func (i logicIDs) armPath(rest ...string) string {
	parts := []string{
		"", "subscriptions", url.PathEscape(i.sub), "resourceGroups", url.PathEscape(i.rg), "providers",
		logicPxARMNamespace, logicPxWorkflows, url.PathEscape(i.wf),
	}

	for _, r := range rest {
		parts = append(parts, url.PathEscape(r))
	}

	return strings.Join(parts, "/")
}

func logicParams(c *echo.Context, names ...string) (logicIDs, []string, bool) {
	ids := logicIDs{sub: c.Param("sub"), rg: c.Param("rg"), wf: c.Param("wf")}
	if !logicPxSubRe.MatchString(ids.sub) || !logicPxNameRe.MatchString(ids.rg) || !logicPxNameRe.MatchString(ids.wf) {
		return ids, nil, false
	}

	extra := make([]string, 0, len(names))

	for _, n := range names {
		v := c.Param(n)
		if !logicPxNameRe.MatchString(v) {
			return ids, nil, false
		}

		extra = append(extra, v)
	}

	return ids, extra, true
}

func (a logicAccess) do(ctx context.Context, method, path, query string, body []byte) *httptest.ResponseRecorder {
	target := path
	if query != "" {
		target += "?" + query
	}

	req := httptest.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	a.inner.ServeHTTP(rec, req)

	return rec
}

func (a logicAccess) arm(ctx context.Context, method, path string) *httptest.ResponseRecorder {
	var body []byte
	if method == http.MethodPost {
		body = []byte("{}")
	}

	return a.do(ctx, method, path, "api-version="+logicPxAPIVersion, body)
}

// signed resolves the callback URL of a Request trigger as a data plane path and query; it is never sent out.
func (a logicAccess) signed(ctx context.Context, ids logicIDs, trigger string) (map[string]any, string, string, int) {
	rec := a.arm(ctx, http.MethodPost, ids.armPath(logicPxTriggers, trigger, "listCallbackUrl"))
	if rec.Code != http.StatusOK {
		return nil, "", "", rec.Code
	}

	var cb map[string]any
	if json.Unmarshal(rec.Body.Bytes(), &cb) != nil {
		return nil, "", "", http.StatusBadGateway
	}

	path, query, ok := logicDataTarget(logicStr(cb[logicPxKeyValue]))
	if !ok {
		return nil, "", "", http.StatusBadGateway
	}

	return cb, path, query, http.StatusOK
}

// logicDataTarget keeps only the path and query of a signed URL and requires the data plane prefix.
func logicDataTarget(raw string) (string, string, bool) {
	u, err := url.Parse(raw)
	if err != nil || !strings.HasPrefix(u.Path, logicPxDataPrefix) {
		return "", "", false
	}

	return u.Path, u.RawQuery, true
}

func logicStr(v any) string {
	s, _ := v.(string)

	return s
}

func logicNoStore(c *echo.Context) {
	c.Response().Header().Set("Cache-Control", "no-store")
}

// callback returns the signed callback URL of one trigger. It is the only route that sends a signature out.
func (a logicAccess) callback(c *echo.Context) error {
	ids, extra, ok := logicParams(c, "trigger")
	if !ok {
		return azureACIFail(c, http.StatusBadRequest, "InvalidRequestUri", "invalid Logic Apps request")
	}

	cb, _, _, status := a.signed(c.Request().Context(), ids, extra[0])
	if cb == nil {
		return azureACIFail(c, status, "CallbackUnavailable", "the trigger has no callback URL")
	}

	logicNoStore(c)

	return c.JSON(http.StatusOK, cb)
}

// invoke calls a Request trigger through its signed URL and returns only the result of the call.
func (a logicAccess) invoke(c *echo.Context) error {
	ids, extra, ok := logicParams(c, "trigger")
	if !ok {
		return azureACIFail(c, http.StatusBadRequest, "InvalidRequestUri", "invalid Logic Apps request")
	}

	body, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, logicPxMaxInvoke))
	if err != nil || (len(bytes.TrimSpace(body)) > 0 && !json.Valid(body)) {
		return azureACIFail(c, http.StatusBadRequest, "InvalidRequestContent", "the trigger body must be JSON")
	}

	ctx := c.Request().Context()

	cb, path, query, status := a.signed(ctx, ids, extra[0])
	if cb == nil {
		return azureACIFail(c, status, "CallbackUnavailable", "the trigger has no callback URL")
	}

	method := strings.ToUpper(logicStr(cb["method"]))
	if method == "" {
		method = http.MethodPost
	}

	rec := a.do(ctx, method, path, query, bytes.TrimSpace(body))
	reply := rec.Body.Bytes()

	if len(reply) > logicPxMaxReply {
		reply = reply[:logicPxMaxReply]
	}

	logicNoStore(c)

	return c.JSON(http.StatusOK, map[string]any{
		"status": rec.Code, "contentType": rec.Header().Get("Content-Type"),
		"runId": rec.Header().Get("X-Ms-Workflow-Run-Id"), "body": string(reply),
	})
}

// content reads the stored inputs or outputs of a trigger or an action of one run through its signed link.
func (a logicAccess) content(c *echo.Context) error {
	names := []string{"run", "kind"}
	if c.Param("action") != "" {
		names = []string{"run", "action", "kind"}
	}

	ids, extra, ok := logicParams(c, names...)
	if !ok || (extra[len(extra)-1] != logicPxKindInputs && extra[len(extra)-1] != logicPxKindOutputs) {
		return azureACIFail(c, http.StatusBadRequest, "InvalidRequestUri", "invalid Logic Apps request")
	}

	ctx := c.Request().Context()
	run, kind := extra[0], extra[len(extra)-1]

	forTrigger := len(extra) == logicPxRestItem
	armPath := ids.armPath(logicPxRuns, run)

	if !forTrigger {
		armPath = ids.armPath(logicPxRuns, run, logicPxActions, extra[1])
	}

	rec := a.arm(ctx, http.MethodGet, armPath)

	if rec.Code != http.StatusOK {
		return azureACIFail(c, rec.Code, "ContentUnavailable", "the run or action was not found")
	}

	var doc map[string]any
	if json.Unmarshal(rec.Body.Bytes(), &doc) != nil {
		return azureACIFail(c, http.StatusBadGateway, "ContentUnavailable", "unreadable run")
	}

	props, _ := doc["properties"].(map[string]any)
	if forTrigger {
		props, _ = props["trigger"].(map[string]any)
	}

	link, _ := props[kind+"Link"].(map[string]any)

	path, query, ok := logicDataTarget(logicStr(link[logicPxKeyURI]))
	if !ok {
		return azureACIFail(c, http.StatusNotFound, "ContentNotFound", "the "+kind+" are not stored")
	}

	data := a.do(ctx, http.MethodGet, path, query, nil)
	if data.Code != http.StatusOK {
		return azureACIFail(c, data.Code, "ContentUnavailable", "the stored "+kind+" could not be read")
	}

	out := data.Body.Bytes()
	if len(out) > logicPxMaxContent {
		return azureACIFail(c, http.StatusRequestEntityTooLarge, "ContentTooLarge", "the stored "+kind+" are too large")
	}

	logicNoStore(c)

	return c.JSONBlob(http.StatusOK, out)
}

