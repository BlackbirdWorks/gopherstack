package lambdaweb

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

const invokeAction = "lambda:" + opInvokeEndpoint

type caller struct {
	arn     string
	account string
}

// accessKeyOf returns the access key id from a SigV4 or SigV4a Authorization header.
func accessKeyOf(auth string) string {
	_, after, ok := strings.Cut(auth, "Credential=")
	if !ok {
		return ""
	}

	cred, _, _ := strings.Cut(after, ",")
	id, _, _ := strings.Cut(strings.TrimSpace(cred), "/")

	return id
}

func (b *InMemoryBackend) callerOf(r *http.Request, auth, account string) caller {
	b.mu.RLock("callerOf")
	res := b.principals
	b.mu.RUnlock()

	if res != nil {
		if key := accessKeyOf(auth); key != "" {
			if p, ok := res.ResolvePrincipal(r.Context(), key, r.Header.Get("X-Amz-Security-Token")); ok && p != nil {
				return callerFrom(p)
			}
		}
	}

	return caller{arn: rootARN(account), account: account}
}

func rootARN(account string) string { return arn.Build("iam", "", account, "root") }

func callerFrom(p *awsmeta.Principal) caller { return caller{arn: p.Arn, account: p.AccountID} }

// policyAllowsInvoke evaluates the function's resource-based policy for an IamAuth invoke. An explicit Deny always
// refuses; an Allow admits; no match admits a same-account caller (identity-based permission is enforced by
// --enforce-iam) and refuses a cross-account one, which needs both policies.
func (b *InMemoryBackend) policyAllowsInvoke(r *http.Request, ep *Endpoint, auth string) bool {
	b.mu.RLock("policyAllowsInvoke")

	var policy string
	if f, ok := b.functions.Get(functionKey(ep.Region, ep.FunctionName)); ok {
		policy = f.Policy
	}

	b.mu.RUnlock()

	c := b.callerOf(r, auth, ep.AccountID)
	same := c.account == ep.AccountID

	if policy == "" {
		return same
	}

	filtered := statementsFor(policy, c)
	if filtered == "" {
		return same
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	switch iam.EvaluatePolicies([]string{filtered}, invokeAction, ep.ARN, iam.ConditionContext{
		PrincipalARN: c.arn, PrincipalAccount: c.account, RequestedRegion: ep.Region, SourceIP: host,
		SecureTransport: boolString(r.TLS != nil),
	}) {
	case iam.EvalExplicitDeny:
		return false
	case iam.EvalAllow:
		return true
	case iam.EvalImplicitDeny:
		return same
	}

	return same
}

func boolString(v bool) string {
	if v {
		return "true"
	}

	return "false"
}

// statementsFor keeps the statements whose Principal (or NotPrincipal) covers c, since the IAM evaluator ignores
// the Principal element.
func statementsFor(policy string, c caller) string {
	var doc struct {
		Version   string         `json:"Version,omitempty"`
		Statement jsontext.Value `json:"Statement"`
	}

	if json.Unmarshal([]byte(policy), &doc, jsonx.V1()) != nil {
		return ""
	}

	var stmts []map[string]jsontext.Value

	if json.Unmarshal(doc.Statement, &stmts, jsonx.V1()) != nil {
		var one map[string]jsontext.Value
		if json.Unmarshal(doc.Statement, &one, jsonx.V1()) != nil {
			return ""
		}

		stmts = []map[string]jsontext.Value{one}
	}

	kept := make([]map[string]jsontext.Value, 0, len(stmts))

	for _, s := range stmts {
		if statementCovers(s, c) {
			kept = append(kept, s)
		}
	}

	out, err := json.Marshal(map[string]any{"Version": doc.Version, "Statement": kept}, jsonx.V1())
	if err != nil {
		return ""
	}

	return string(out)
}

func statementCovers(s map[string]jsontext.Value, c caller) bool {
	if raw, ok := s["Principal"]; ok {
		return principalListed(raw, c)
	}

	if raw, ok := s["NotPrincipal"]; ok {
		return !principalListed(raw, c)
	}

	return false
}

func principalListed(raw jsontext.Value, c caller) bool {
	var star string
	if json.Unmarshal(raw, &star, jsonx.V1()) == nil {
		return star == "*"
	}

	var m map[string]jsontext.Value
	if json.Unmarshal(raw, &m, jsonx.V1()) != nil {
		return false
	}

	for _, id := range stringsOf(m["AWS"]) {
		if id == "*" || id == c.arn || id == c.account || id == rootARN(c.account) {
			return true
		}
	}

	return false
}

func stringsOf(raw jsontext.Value) []string {
	var one string
	if json.Unmarshal(raw, &one, jsonx.V1()) == nil {
		return []string{one}
	}

	var many []string
	_ = json.Unmarshal(raw, &many, jsonx.V1())

	return many
}
