package azurearm

import (
	"errors"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	logicSubIdx       = 1
	logicRGIdx        = 3
	logicNsIdx        = 5
	logicTypeIdx      = 6
	logicWfIdx        = 7
	logicRestIdx      = 8
	logicLocSegs      = 10
	logicIdxA         = 2
	logicIdxB         = 3
	logicIdxC         = 4
	logicIdxWfKeyword = 8
	logicIdxLocWf     = 9
	logicIdxValidate  = 10
	logicSubListSegs  = 5
	logicRGListSegs   = 7
)

type logicReq struct {
	c    *echo.Context
	lp   *LogicProvider
	ref  logicRef3
	base string
	rest []string
}

type logicListKind int

const (
	logicListNone logicListKind = iota
	logicListRG
	logicListSub
)

type logicPath struct {
	ref  logicRef3
	rest []string
	list logicListKind
}

func parseLogicPath(segs []string) (logicPath, bool) {
	if len(segs) < logicSubListSegs || !strings.EqualFold(segs[0], subscriptionsSegment) {
		return logicPath{}, false
	}

	switch {
	case len(segs) == logicSubListSegs && segAt(segs, logicIdxA, providersSegment) &&
		segAt(segs, logicIdxB, namespaceMicrosoftLogic) && segAt(segs, logicIdxC, logicWorkflowsType):
		return logicPath{ref: logicRef3{sub: segs[logicSubIdx]}, list: logicListSub}, true
	case len(segs) < logicRGListSegs || !segAt(segs, logicIdxA, resourceGroupsSegment) ||
		!segAt(segs, logicIdxC, providersSegment) || !segAt(segs, logicNsIdx, namespaceMicrosoftLogic):
		return logicPath{}, false
	}

	lp := logicPath{ref: logicRef3{sub: segs[logicSubIdx], rg: segs[logicRGIdx]}}

	if !segAt(segs, logicTypeIdx, logicWorkflowsType) {
		return logicPath{}, false
	}

	if len(segs) == logicRGListSegs {
		lp.list = logicListRG

		return lp, true
	}

	lp.ref.wf = segs[logicWfIdx]
	lp.rest = segs[logicRestIdx:]

	return lp, true
}

func isLogicRoute(segs []string) bool {
	if _, ok := parseLogicPath(segs); ok {
		return true
	}

	return isLogicLocationValidate(segs)
}

func isLogicLocationValidate(segs []string) bool {
	return len(segs) == logicLocSegs+1 && isSubscriptionScoped(segs) && segAt(segs, logicIdxA, resourceGroupsSegment) &&
		segAt(segs, logicIdxC, providersSegment) && segAt(segs, logicNsIdx, namespaceMicrosoftLogic) &&
		segAt(segs, logicTypeIdx, "locations") && segAt(segs, logicIdxWfKeyword, logicWorkflowsType) &&
		segAt(segs, logicIdxValidate, "validate")
}

//nolint:gochecknoglobals // static route table, read-only after init
var logicRoutes = []armRoute{{
	match: func(segs []string, _ *http.Request) bool { return isLogicRoute(segs) },
	handle: func(h *Handler, c *echo.Context, segs []string) error {
		return h.handleLogic(c, segs)
	},
}}

func (h *Handler) logicProvider() (*LogicProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftLogic)
	if !ok {
		return nil, false
	}

	lp, ok := p.(*LogicProvider)

	return lp, ok
}

func logicShape(rest []string) string {
	parts := make([]string, len(rest))

	for i, s := range rest {
		if i%2 == 1 {
			parts[i] = "*"
		} else {
			parts[i] = strings.ToLower(s)
		}
	}

	return strings.Join(parts, "/")
}

func (h *Handler) handleLogic(c *echo.Context, segs []string) error {
	lp, ok := h.logicProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	r := c.Request()
	q := logicReq{c: c, lp: lp, base: h.baseURLFor(r)}

	if isLogicLocationValidate(segs) {
		q.ref = logicRef3{sub: segs[logicSubIdx], rg: segs[logicRGIdx], wf: segs[logicIdxLocWf]}

		return h.logicValidate(q)
	}

	path, _ := parseLogicPath(segs)
	q.ref, q.rest = path.ref, path.rest

	if path.list != logicListNone {
		if r.Method != http.MethodGet {
			return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not allowed.")
		}

		return h.logicListWorkflows(q, path.list)
	}

	fn, ok := logicHandlers()[r.Method+":"+logicShape(q.rest)]
	if !ok {
		return h.writeError(c, http.StatusNotFound, "NotFound", "The operation is not supported by this emulator.")
	}

	return fn(h, q)
}

type pageOf = page.Page[map[string]any]

type logicHandlerFunc func(h *Handler, q logicReq) error

func logicHandlers() map[string]logicHandlerFunc {
	return map[string]logicHandlerFunc{
		"GET:":                     (*Handler).logicGetWorkflow,
		"PUT:":                     (*Handler).logicPutWorkflow,
		"PATCH:":                   (*Handler).logicPatchWorkflow,
		"DELETE:":                  (*Handler).logicDeleteWorkflow,
		"POST:enable":              func(h *Handler, q logicReq) error { return h.logicSetState(q, logicStateEnabled) },
		"POST:disable":             func(h *Handler, q logicReq) error { return h.logicSetState(q, logicStateDisabled) },
		"POST:regenerateaccesskey": (*Handler).logicRegenerate,
		"POST:listcallbackurl":     func(h *Handler, q logicReq) error { return h.logicCallback(q, "") },
		"POST:validate":            (*Handler).logicValidate,
		"GET:versions":             (*Handler).logicListVersions,
		"GET:versions/*":           (*Handler).logicGetVersion,
		"POST:versions/*/triggers/*/listcallbackurl": func(h *Handler, q logicReq) error {
			return h.logicCallback(q, q.rest[3])
		},
		"GET:triggers":                    (*Handler).logicListTriggers,
		"GET:triggers/*":                  (*Handler).logicGetTrigger,
		"POST:triggers/*/listcallbackurl": func(h *Handler, q logicReq) error { return h.logicCallback(q, q.rest[1]) },
		"POST:triggers/*/run":             (*Handler).logicRunTrigger,
		"POST:triggers/*/reset":           (*Handler).logicResetTrigger,
		"POST:triggers/*/setstate":        (*Handler).logicSetTriggerState,
		"GET:triggers/*/schemas/*":        (*Handler).logicTriggerSchema,
		"GET:triggers/*/histories":        (*Handler).logicListHistories,
		"GET:triggers/*/histories/*":      (*Handler).logicGetHistory,
		"GET:runs":                        (*Handler).logicListRuns,
		"GET:runs/*":                      (*Handler).logicGetRun,
		"POST:runs/*/cancel":              (*Handler).logicCancelRun,
		"GET:runs/*/actions":              (*Handler).logicListActions,
		"GET:runs/*/actions/*":            (*Handler).logicGetAction,
	}
}

func (h *Handler) logicPutWorkflow(q logicReq) error {
	body, err := decodeJSONBody(q.c.Request())
	if err != nil {
		return h.writeAPIError(q.c, err)
	}

	id := ResourceID{
		SubscriptionID: q.ref.sub, ResourceGroup: q.ref.rg, Namespace: namespaceMicrosoftLogic,
		Types: []string{logicWorkflowsType}, Names: []string{q.ref.wf},
	}

	out, created, err := q.lp.putWorkflow(id, body, false, q.base)
	if err != nil {
		return h.writeAPIError(q.c, err)
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}

	return h.writeJSON(q.c, status, out)
}

func (h *Handler) logicPatchWorkflow(q logicReq) error {
	body, err := decodeJSONBody(q.c.Request())
	if err != nil {
		return h.writeAPIError(q.c, err)
	}

	id := ResourceID{
		SubscriptionID: q.ref.sub, ResourceGroup: q.ref.rg, Namespace: namespaceMicrosoftLogic,
		Types: []string{logicWorkflowsType}, Names: []string{q.ref.wf},
	}

	out, _, err := q.lp.putWorkflow(id, body, true, q.base)
	if err != nil {
		return h.writeAPIError(q.c, err)
	}

	return h.writeJSON(q.c, http.StatusOK, out)
}

func (h *Handler) logicGetWorkflow(q logicReq) error {
	out, err := q.lp.read(q.ref, func(w *logicWorkflow) (any, error) { return q.lp.workflowBody(w, q.base), nil })

	return h.logicReply(q, out, err)
}

func (h *Handler) logicReply(q logicReq, out any, err error) error {
	if err != nil {
		return h.writeAPIError(q.c, err)
	}

	return h.writeJSON(q.c, http.StatusOK, out)
}

func (h *Handler) logicDeleteWorkflow(q logicReq) error {
	id := ResourceID{
		SubscriptionID: q.ref.sub, ResourceGroup: q.ref.rg, Namespace: namespaceMicrosoftLogic,
		Types: []string{logicWorkflowsType}, Names: []string{q.ref.wf},
	}

	if err := q.lp.Delete(q.c.Request().Context(), id); err != nil {
		if errors.Is(err, ErrLogicWorkflowNotFound) {
			return q.c.NoContent(http.StatusNoContent)
		}

		return h.writeAPIError(q.c, err)
	}

	return q.c.NoContent(http.StatusOK)
}

func (h *Handler) logicSetState(q logicReq, state string) error {
	if err := q.lp.setState(q.ref, state); err != nil {
		return h.writeAPIError(q.c, err)
	}

	return q.c.NoContent(http.StatusOK)
}

func (h *Handler) logicRegenerate(q logicReq) error {
	body, err := decodeJSONBody(q.c.Request())
	if err != nil {
		return h.writeAPIError(q.c, err)
	}

	if err = q.lp.regenerateKey(q.ref, stringOf(body["keyType"])); err != nil {
		return h.writeAPIError(q.c, err)
	}

	return q.c.NoContent(http.StatusOK)
}

func (h *Handler) logicCallback(q logicReq, trigger string) error {
	body, err := decodeJSONBody(q.c.Request())
	if err != nil {
		return h.writeAPIError(q.c, err)
	}

	out, err := q.lp.callbackFor(q.ref, trigger, q.base, body)

	return h.logicReply(q, out, err)
}

func (h *Handler) logicValidate(q logicReq) error {
	body, err := decodeJSONBody(q.c.Request())
	if err != nil {
		return h.writeAPIError(q.c, err)
	}

	if err = q.lp.validateWorkflow(q.ref.wf, body); err != nil {
		return h.writeAPIError(q.c, err)
	}

	return q.c.NoContent(http.StatusOK)
}

func (h *Handler) logicQuery(q logicReq) (logicQuery, error) {
	v := q.c.Request().URL.Query()
	out := logicQuery{token: v.Get("$skiptoken")}

	if t := v.Get("$top"); t != "" {
		n, err := strconv.Atoi(t)
		if err != nil || n < 1 {
			return out, logicBadRequest("InvalidRequestContent", "The $top value '%s' is not valid.", t)
		}

		out.top = n
	}

	f, err := parseLogicFilter(v.Get("$filter"))
	out.filter = f

	return out, err
}

func (h *Handler) logicNextLink(q logicReq, token string) string {
	if token == "" {
		return ""
	}

	r := q.c.Request()
	vals := url.Values{}

	maps.Copy(vals, r.URL.Query())

	vals.Set("$skiptoken", token)

	return q.base + r.URL.Path + "?" + vals.Encode()
}

func (h *Handler) logicWriteList(q logicReq, items []map[string]any, next string) error {
	out := map[string]any{fieldValue: items}
	if link := h.logicNextLink(q, next); link != "" {
		out["nextLink"] = link
	}

	return h.writeJSON(q.c, http.StatusOK, out)
}

func (h *Handler) logicListWorkflows(q logicReq, kind logicListKind) error {
	lq, err := h.logicQuery(q)
	if err != nil {
		return h.writeAPIError(q.c, err)
	}

	rg := q.ref.rg
	if kind == logicListSub {
		rg = ""
	}

	q.lp.mu.RLock("listWorkflows")
	defer q.lp.mu.RUnlock()

	var all []map[string]any

	for _, w := range q.lp.sortedLocked(q.ref.sub, rg) {
		if filterMatches(lq.filter, logicFilterable{status: w.State}) {
			all = append(all, q.lp.workflowBody(w, q.base))
		}
	}

	pg := pageLogic(all, lq)

	return h.logicWriteList(q, pg.Data, pg.Next)
}

func (h *Handler) logicList(q logicReq, items func(w *logicWorkflow, lq logicQuery) ([]map[string]any, error)) error {
	lq, err := h.logicQuery(q)
	if err != nil {
		return h.writeAPIError(q.c, err)
	}

	out, err := q.lp.read(q.ref, func(w *logicWorkflow) (any, error) {
		all, ierr := items(w, lq)
		if ierr != nil {
			return nil, ierr
		}

		return pageLogic(all, lq), nil
	})
	if err != nil {
		return h.writeAPIError(q.c, err)
	}

	pg, _ := out.(pageOf)

	return h.logicWriteList(q, pg.Data, pg.Next)
}
