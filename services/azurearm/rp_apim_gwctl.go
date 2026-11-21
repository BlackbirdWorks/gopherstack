package azurearm

import (
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	apimKindRequest      = "request"
	apimKindResponse     = "response"
	apimKindSubscription = "subscription"
	apimCChoose          = "choose"
	apimCWhen            = "when"
	apimCOtherwise       = "otherwise"
	apimCSetVariable     = "set-variable"
	apimCCondition       = "condition"
	apimCID              = "id"
	apimMaxVars          = 256
	apimMaxVarBytes      = 1 << 20
	apimReasonExpr       = "ExpressionValueEvaluationFailure"
	apimSourceConfig     = "configuration"
	apimScopeGlobal      = "global"
	apimScopeProduct     = "product"
	apimScopeAPI         = "api"
	apimScopeOp          = "operation"
	apimPathSeparator    = `\`
)

// gwLastError is context.LastError of the error-handling reference.
type gwLastError struct {
	Source, Reason, Message, Scope, Section, Path, PolicyID string
}

type apimExprObj struct{ kind string }

func (g *gwState) now() time.Time { return g.plan.now() }

// subVal expands named values and evaluates a policy expression; evaluation failures are recorded in evalErr.
func (g *gwState) subVal(s string) any {
	if body, ok, _ := apimIsExprText(s); ok {
		n, err := apimParseExpr(body)
		if err == nil {
			var v any

			if v, err = apimEvalExpr(n, g, &apimExprCounter{}); err == nil {
				return v
			}
		}

		if g.evalErr == nil {
			g.evalErr = err
		}

		return nil
	}

	return apimNamedRefRE.ReplaceAllStringFunc(s, func(m string) string {
		return g.plan.named[strings.ToLower(m[2:len(m)-2])]
	})
}

func (g *gwState) boolAttr(n *polNode, attr string, def bool) bool {
	raw, ok := n.Attrs[attr]
	if !ok {
		return def
	}

	switch v := g.subVal(raw).(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), cmpTrue)
	}

	return def
}

func (g *gwState) intAttr(n *polNode, attr string, def int64) int64 {
	raw, ok := n.Attrs[attr]
	if !ok {
		return def
	}

	switch v := g.subVal(raw).(type) {
	case int64:
		return v
	case string:
		if i, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
			return i
		}
	}

	if g.evalErr == nil {
		g.evalErr = apimExprFail("the attribute '%s' is not an integer", attr)
	}

	return def
}

func (g *gwState) member(obj any, name string) (any, error) {
	switch o := obj.(type) {
	case apimExprRoot:
		return g.rootMember(name)
	case apimExprObj:
		return g.objMember(o.kind, name)
	case *apimExprResponse:
		return o.member(name)
	case *apimExprJWT:
		return o.member(name)
	case apimExprBody:
		return nil, apimExprFail("Body.%s is not supported", name)
	}

	return nil, apimExprFail("the member %s was accessed on a null or unsupported value", name)
}

func (g *gwState) rootMember(name string) (any, error) {
	switch name {
	case apxRequest:
		return apimExprObj{apimKindRequest}, nil
	case apxResponse:
		if g.status == 0 {
			return nil, nil //nolint:nilnil // context.Response is null before a response exists
		}

		return apimExprObj{apimKindResponse}, nil
	case apxVariables:
		return &apimExprDict{get: func(k string) (any, bool) {
			v, ok := g.vars[k]

			return v, ok
		}}, nil
	case apxSubscription:
		return g.nullable(g.plan.subID != "", apimKindSubscription), nil
	case apxProduct:
		return g.nullable(g.plan.productID != "", apimScopeProduct), nil
	case apxUser:
		return g.nullable(g.plan.userID != "", "user"), nil
	case apxLastError:
		return g.nullable(g.lastErr != nil, "lasterror"), nil
	case apxAPI:
		return apimExprObj{"api"}, nil
	case apxOperation:
		return g.nullable(g.plan.op != nil, "operation"), nil
	case apxTimestamp:
		return g.start().UTC(), nil
	case apxRequestID:
		return g.requestID(), nil
	}

	return nil, apimExprFail("context.%s is not supported", name)
}

func (g *gwState) nullable(ok bool, kind string) any {
	if !ok {
		return nil
	}

	return apimExprObj{kind}
}

func (g *gwState) objMember(kind, name string) (any, error) {
	var (
		v  any
		ok bool
	)

	switch kind {
	case apimKindRequest:
		return g.requestMember(name)
	case apimKindResponse:
		v, ok = g.responseMember(name)
	case apimKindSubscription:
		v, ok = g.plan.subID, name == apxID || name == apxName
	case apimScopeProduct:
		v, ok = g.productMember(name)
	case "user":
		v, ok = g.plan.userID, name == apxID
	case "api":
		v, ok = g.apiMember(name)
	case "operation":
		v, ok = g.operationMember(name)
	case "lasterror":
		return g.lastErrorMember(name)
	}

	if !ok {
		return nil, apimExprFail("%s.%s is not supported", kind, name)
	}

	return v, nil
}

func (g *gwState) responseMember(name string) (any, bool) {
	switch name {
	case apxStatusCode:
		return int64(g.status), true
	case apxStatusReason:
		return http.StatusText(g.status), true
	case apxHeaders:
		return apimHeaderDict(g.respHeader), true
	}

	return nil, false
}

func (g *gwState) productMember(name string) (any, bool) {
	switch name {
	case apxID:
		return g.plan.productID, true
	case apxName:
		return g.plan.productName, true
	}

	return nil, false
}

func (g *gwState) apiMember(name string) (any, bool) {
	switch name {
	case apxID:
		return g.plan.apiID, true
	case apxName:
		return strOf(g.plan.api, apimCDisplayName), true
	case apxPath:
		return strOf(g.plan.api, apimCPath), true
	}

	return nil, false
}

func (g *gwState) operationMember(name string) (any, bool) {
	switch name {
	case apxID:
		return g.plan.opID, true
	case apxName:
		return strOf(g.plan.op, apimCDisplayName), true
	case apxMethod:
		return strings.ToUpper(strOf(g.plan.op, apimCMethod)), true
	case apxURLTemplate:
		return strOf(g.plan.op, apimCUrlTemplate), true
	}

	return nil, false
}

func (g *gwState) lastErrorMember(name string) (any, error) {
	e := g.lastErr

	switch name {
	case apxSource:
		return e.Source, nil
	case apxReason:
		return e.Reason, nil
	case apxMessage:
		return e.Message, nil
	case apxScope:
		return e.Scope, nil
	case apxSection:
		return e.Section, nil
	case apxPath:
		return e.Path, nil
	case apxPolicyID:
		return e.PolicyID, nil
	}

	return nil, apimExprFail("LastError.%s is not supported", name)
}

func (g *gwState) requestMember(name string) (any, error) {
	switch name {
	case apxMethod:
		return g.r.Method, nil
	case apxIPAddress:
		return hostOnly(g.r.RemoteAddr), nil
	case apxHeaders:
		return apimHeaderDict(g.header), nil
	case apxMatchedParameters:
		return &apimExprDict{get: func(k string) (any, bool) {
			for pk, v := range g.plan.params {
				if strings.EqualFold(pk, k) {
					return v, true
				}
			}

			return nil, false
		}}, nil
	case apxURL, apxOriginalURL:
		host, port := g.r.Host, int64(0)
		if h, p, ok := strings.Cut(host, ":"); ok {
			host = h
			port, _ = strconv.ParseInt(p, 10, 64)
		}

		scheme := acaScaleHTTP
		if g.r.TLS != nil {
			scheme = acaSchemeHTTPS
		}

		u := &apimExprURL{path: g.plan.path, host: host, scheme: scheme, port: port}
		if name == apxURL {
			u.q, u.query = g.query, "?"+g.query.Encode()
		} else {
			u.q, u.query = g.r.URL.Query(), "?"+g.r.URL.RawQuery
		}

		return u, nil
	}

	return nil, apimExprFail("Request.%s is not supported", name)
}

func apimHeaderDict(h http.Header) *apimExprDict {
	return &apimExprDict{get: func(k string) (any, bool) {
		v := h.Values(textproto.CanonicalMIMEHeaderKey(k))

		return v, len(v) > 0
	}}
}

func (g *gwState) start() time.Time {
	if g.t0.IsZero() {
		g.t0 = g.now()
	}

	return g.t0
}

func (g *gwState) requestID() string {
	if g.reqID == "" {
		g.reqID = apimNewGUID()
	}

	return g.reqID
}

// failR records an error response and its LastError and ends the current section.
func (g *gwState) failR(status int, reason, msg string) {
	if g.errored {
		return
	}

	body, _ := json.Marshal(map[string]any{apimCStatusCode: status, fieldMessage: msg}, jsonx.V1())
	g.setResponse(status, http.Header{apimCContentType: {"application/json"}}, body)
	g.short, g.errored = true, true

	e := &gwLastError{Reason: reason, Message: msg, Section: g.section, Path: g.path}
	if n := g.cur; n != nil {
		e.Source, e.PolicyID = n.Name, n.Attrs[apimCID]
		e.Scope = apimScopeName(n.Scope)
	} else {
		e.Source = apimSourceConfig
	}

	g.lastErr = e
}

func apimScopeName(scope string) string {
	_, idx, _ := strings.Cut(scope, "|")

	switch idx {
	case "0":
		return apimScopeGlobal
	case "1":
		return apimScopeProduct
	case "2":
		return apimScopeAPI
	case "3":
		return apimScopeOp
	}

	return ""
}

func (g *gwState) raiseEval() {
	err := g.evalErr
	g.evalErr = nil

	msg := "Expression evaluation failed."
	if err != nil {
		msg += " " + strings.TrimPrefix(err.Error(), errAPIMExprRuntime.Error()+": ")
	}

	if errors.Is(err, errAPIMExprSyntax) {
		msg = "Expression could not be compiled. " + err.Error()
	}

	g.failR(http.StatusInternalServerError, apimReasonExpr, msg)
}

// execList runs statements in order; it stops at a short-circuit, an error or a cache hit (inbound).
func (g *gwState) execList(nodes []*polNode, section, path string) {
	for _, n := range nodes {
		g.cur, g.section = n, section

		g.execNode(n, section, path)

		if g.evalErr != nil {
			g.cur, g.section = n, section
			g.raiseEval()
		}

		if g.short || g.errored || (g.cacheHit && section == polInbound) {
			return
		}
	}
}

func (g *gwState) execNode(n *polNode, section, path string) {
	switch n.Name {
	case apimCChoose:
		g.choose(n, section, path)
	case apimCSetVariable:
		g.setVariable(n)
	case apimCSendRequest, apimCSendOneWay:
		g.sendRequest(n)
	case apimCCacheLookupValue, apimCCacheStoreValue, apimCCacheRemoveValue:
		g.cacheValue(n)
	default:
		if section == polInbound || section == polBackend {
			g.execRequest(n)
		} else {
			g.execResponse(n)
		}
	}
}

func (g *gwState) choose(n *polNode, section, path string) {
	whens := 0

	for _, w := range n.Kids {
		if w.Name != apimCWhen {
			continue
		}

		whens++

		g.cur = w
		cond := g.boolAttr(w, apimCCondition, false)

		if g.evalErr != nil {
			return
		}

		if cond {
			g.execList(w.Kids, section, path+apimPathSeparator+apimCWhen+"["+strconv.Itoa(whens)+"]")

			return
		}
	}

	if o := n.kid(apimCOtherwise); o != nil {
		g.execList(o.Kids, section, path+apimPathSeparator+apimCOtherwise+"[1]")
	}
}

func (g *gwState) setVariable(n *polNode) {
	v := g.subVal(n.Attrs[fieldValue])

	if g.evalErr != nil {
		return
	}

	if s, ok := v.(string); ok && len(s) > apimMaxVarBytes {
		g.evalErr = apimExprFail("the variable value is too large")

		return
	}

	name := n.Attrs[fieldName]
	if _, exists := g.vars[name]; !exists && len(g.vars) >= apimMaxVars {
		g.evalErr = apimExprFail("too many context variables")

		return
	}

	g.vars[name] = v
}

// onError runs the on-error section after an error; the error response stays unless a policy replaces it.
func (g *gwState) onError(docs []*polNode) {
	g.short, g.errored = false, true

	for _, n := range expandSection(docs, polOnError) {
		g.cur, g.section = n, polOnError
		g.execErrNode(n)

		if g.evalErr != nil {
			g.evalErr = nil

			return
		}

		if g.short {
			return
		}
	}
}

func (g *gwState) execErrNode(n *polNode) {
	if g.respHeader == nil {
		g.respHeader = http.Header{}
	}

	g.errored = true

	g.execNodeInError(n)
}

func (g *gwState) execNodeInError(n *polNode) {
	g.cur = n

	switch n.Name {
	case apimCChoose:
		g.chooseErr(n)
	default:
		g.execNode(n, polOnError, "")
	}
}

func (g *gwState) chooseErr(n *polNode) {
	for _, w := range n.Kids {
		if w.Name == apimCWhen && g.boolAttr(w, apimCCondition, false) {
			for _, k := range w.Kids {
				g.execNodeInError(k)

				if g.short || g.evalErr != nil {
					return
				}
			}

			return
		}
	}

	if o := n.kid(apimCOtherwise); o != nil {
		for _, k := range o.Kids {
			g.execNodeInError(k)

			if g.short || g.evalErr != nil {
				return
			}
		}
	}
}
