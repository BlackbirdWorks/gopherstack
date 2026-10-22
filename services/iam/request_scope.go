package iam

import (
	"cmp"
	"encoding/json"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// RequestScope lets a service override how the middleware authorises one request,
// for requests whose resource and condition keys are not derivable from the path.
type RequestScope struct {
	// Extra holds service condition keys (e.g. lambda:FunctionUrlAuthType).
	Extra map[string]string
	// Deny writes the service's own 403; nil falls back to the generic AccessDenied.
	Deny func(c *echo.Context) error
	// Action overrides the extracted IAM action when non-empty.
	Action string
	// ResourceARN is the resource whose policy is consulted.
	ResourceARN string
	// Anonymous authorises on the resource policy alone, for any caller (no signature needed).
	Anonymous bool
	// MatchPrincipal restricts resource-policy statements to those naming the caller.
	MatchPrincipal bool
}

// RequestScoper is implemented by an ActionExtractor that scopes requests.
type RequestScoper interface {
	IAMRequestScope(r *http.Request) (RequestScope, bool)
}

func requestScopeFromProviders(r *http.Request, extractors []ActionExtractor) (RequestScope, bool) {
	for _, ae := range extractors {
		if rs, ok := ae.(RequestScoper); ok {
			if scope, scoped := rs.IAMRequestScope(r); scoped {
				return scope, true
			}
		}
	}

	return RequestScope{}, false
}

// override swaps in the scope's resource, action and condition keys when it names a resource.
func (s RequestScope) override(
	resourceARN, action string, condCtx ConditionContext,
) (string, string, ConditionContext) {
	if s.ResourceARN == "" {
		return resourceARN, action, condCtx
	}

	return s.ResourceARN, cmp.Or(s.Action, action), s.apply(condCtx)
}

func (s RequestScope) apply(condCtx ConditionContext) ConditionContext {
	if len(s.Extra) == 0 {
		return condCtx
	}

	extra := make(map[string]string, len(condCtx.Extra)+len(s.Extra))
	maps.Copy(extra, condCtx.Extra)

	for k, v := range s.Extra {
		extra[strings.ToLower(k)] = v
	}

	condCtx.Extra = extra

	return condCtx
}

func (s RequestScope) deny(c *echo.Context, action, resource string) error {
	if s.Deny != nil {
		return s.Deny(c)
	}

	return writeAccessDenied(c, action, resource)
}

// enforceAnonymousScope authorises a request on the resource policy alone: only
// statements whose Principal is "*" apply.
func enforceAnonymousScope(c *echo.Context, next echo.HandlerFunc, cfg EnforcementConfig, scope RequestScope) error {
	r := c.Request()
	ctx := r.Context()
	docs := canonicalDocs(collectResourcePolicies(ctx, cfg.ResourceProviders, scope.ResourceARN))
	condCtx := scope.apply(ConditionContext{SourceIP: extractClientIP(r), SecureTransport: secureTransportValue(r)})
	resource := arn.Canonical(scope.ResourceARN)

	if EvaluatePolicies(filterPrincipalDocs(docs, condCtx), scope.Action, resource, condCtx) == EvalAllow {
		return next(c)
	}

	logger.Load(ctx).InfoContext(ctx, "IAM enforcement: anonymous access denied",
		"action", scope.Action, "resource", resource)

	if cfg.Audit {
		return next(c)
	}

	return scope.deny(c, scope.Action, resource)
}

// decideScoped is decideRequest plus principal matching and the cross-account rule:
// across accounts both policy types must allow.
func decideScoped(
	policyDocs, boundaryDocs, resourceDocs []string,
	action, resource string, condCtx ConditionContext, scope RequestScope,
) requestDecision {
	if scope.MatchPrincipal {
		resourceDocs = filterPrincipalDocs(resourceDocs, condCtx)
	}

	dec := decideRequest(policyDocs, boundaryDocs, resourceDocs, action, resource, condCtx)
	if !dec.allow || !crossAccount(resource, condCtx) {
		return dec
	}

	if EvaluatePolicies(policyDocs, action, resource, condCtx) == EvalAllow &&
		EvaluatePolicies(resourceDocs, action, resource, condCtx) == EvalAllow {
		return dec
	}

	return requestDecision{reason: reasonImplicitDeny, match: dec.match}
}

func crossAccount(resource string, condCtx ConditionContext) bool {
	acct, ok := arnAccountID(resource)

	return ok && condCtx.PrincipalAccount != "" && acct != condCtx.PrincipalAccount
}

// filterPrincipalDocs drops resource-policy statements whose Principal does not name the caller.
func filterPrincipalDocs(docs []string, condCtx ConditionContext) []string {
	out := make([]string, 0, len(docs))

	for _, d := range docs {
		out = append(out, filterPrincipalDoc(d, condCtx))
	}

	return out
}

func filterPrincipalDoc(doc string, condCtx ConditionContext) string {
	var raw map[string]any
	if json.Unmarshal([]byte(doc), &raw) != nil {
		return doc
	}

	var kept []any

	for _, st := range statementList(raw["Statement"]) {
		if m, ok := st.(map[string]any); ok && principalMatches(m["Principal"], condCtx) {
			kept = append(kept, st)
		}
	}

	raw["Statement"] = kept

	out, err := json.Marshal(raw)
	if err != nil {
		return doc
	}

	return string(out)
}

func statementList(v any) []any {
	switch s := v.(type) {
	case []any:
		return s
	case map[string]any:
		return []any{s}
	}

	return nil
}

func principalMatches(p any, condCtx ConditionContext) bool {
	switch v := p.(type) {
	case string:
		return v == "*"
	case map[string]any:
		return awsPrincipalMatches(v["AWS"], condCtx)
	}

	return false
}

func awsPrincipalMatches(v any, condCtx ConditionContext) bool {
	var list []any

	switch a := v.(type) {
	case string:
		list = []any{a}
	case []any:
		list = a
	}

	for _, e := range list {
		if s, ok := e.(string); ok && principalEntryMatches(s, condCtx) {
			return true
		}
	}

	return false
}

func principalEntryMatches(entry string, condCtx ConditionContext) bool {
	if entry == "*" {
		return true
	}

	caller := condCtx.PrincipalARN
	if caller == "" {
		return false
	}

	if strings.HasSuffix(entry, ":root") {
		acct, _ := arnAccountID(entry)

		return acct != "" && acct == condCtx.PrincipalAccount
	}

	if !strings.HasPrefix(entry, "arn:") {
		return entry == condCtx.PrincipalAccount
	}

	return entry == caller || sessionMatchesRole(entry, caller)
}

// sessionMatchesRole reports whether caller is an assumed-role session of the role entry names.
func sessionMatchesRole(entry, caller string) bool {
	_, rolePart, ok := strings.Cut(entry, ":role/")
	if !ok {
		return false
	}

	roleName := rolePart[strings.LastIndex(rolePart, "/")+1:]
	_, sess, found := strings.Cut(caller, ":assumed-role/")

	return found && strings.HasPrefix(sess, roleName+"/") && strings.Contains(caller, entryAccount(entry))
}

func entryAccount(entry string) string {
	acct, _ := arnAccountID(entry)

	return ":" + acct + ":"
}
