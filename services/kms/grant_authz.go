package kms

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	gopherarn "github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

const (
	arnAccountField = 4
	arnMinFields    = 6
	actionRetire    = "RetireGrant"
)

// arnAccount returns the account segment of an ARN, or "".
func arnAccount(a string) string {
	parts := strings.SplitN(a, ":", arnMinFields)
	if len(parts) < arnMinFields {
		return ""
	}

	return parts[arnAccountField]
}

// principalMatchesARN reports whether caller is the principal named by target:
// the identical ARN, the account root of the caller's account (account
// delegation), or the IAM role behind an assumed-role session.
func principalMatchesARN(caller *awsmeta.Principal, target string) bool {
	if target == "" {
		return false
	}

	if caller.Arn == target {
		return true
	}

	if target == gopherarn.Build("iam", "", caller.AccountID, "root") {
		return true
	}

	_, rest, ok := strings.Cut(caller.Arn, ":assumed-role/")
	if !ok {
		return false
	}

	roleName, _, _ := strings.Cut(rest, "/")
	if arnAccount(target) != caller.AccountID {
		return false
	}

	return strings.Contains(target, ":role/") && strings.HasSuffix(target, "/"+roleName)
}

func callerAccount(ctx context.Context, p *awsmeta.Principal) string {
	if p != nil && p.AccountID != "" {
		return p.AccountID
	}

	return awsmeta.Account(ctx)
}

// authorizeGrantRetire enforces who may retire g: its RetiringPrincipal, its
// GranteePrincipal when RetireGrant is a grant operation, or any principal of
// the issuing account (kms@v1.61 api_op_RetireGrant.go). No resolved caller
// means no identity to check, so the call is permitted.
func authorizeGrantRetire(ctx context.Context, g *Grant) error {
	p := awsmeta.GetPrincipal(ctx)
	if p == nil {
		return nil
	}

	if callerAccount(ctx, p) == g.IssuingAccount && g.IssuingAccount != "" {
		return nil
	}

	if principalMatchesARN(p, g.RetiringPrincipal) {
		return nil
	}

	if slices.Contains(g.Operations, actionRetire) && principalMatchesARN(p, g.GranteePrincipal) {
		return nil
	}

	return fmt.Errorf("%w: caller %s is not authorized to retire grant %s", ErrAccessDenied, p.Arn, g.GrantID)
}

// authorizeKeyAction permits a caller of the key's own account, or a
// cross-account caller named by an Allow statement of the key policy
// (and not denied). Callers without a resolved principal are permitted.
func (b *InMemoryBackend) authorizeKeyAction(ctx context.Context, region, keyID, action string) error {
	p := awsmeta.GetPrincipal(ctx)
	if p == nil || callerAccount(ctx, p) == b.accountID {
		return nil
	}

	policy, ok := b.policiesStore(region)[keyID]
	if ok && keyPolicyAllows(policy, p, action) {
		return nil
	}

	return fmt.Errorf("%w: caller %s is not authorized to perform kms:%s on this key", ErrAccessDenied, p.Arn, action)
}

type policyStatement struct {
	Principal any    `json:"Principal"`
	Action    any    `json:"Action"`
	Condition any    `json:"Condition"`
	Effect    string `json:"Effect"`
}

func stringList(v any) []string {
	switch t := v.(type) {
	case string:
		return []string{t}
	case []any:
		out := make([]string, 0, len(t))
		for _, e := range t {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}

		return out
	}

	return nil
}

func statementNamesPrincipal(s *policyStatement, p *awsmeta.Principal) bool {
	if s.Principal == "*" {
		return true
	}

	m, ok := s.Principal.(map[string]any)
	if !ok {
		return false
	}

	for _, name := range stringList(m["AWS"]) {
		if name == "*" || name == p.AccountID || principalMatchesARN(p, name) {
			return true
		}
	}

	return false
}

func statementCoversAction(s *policyStatement, action string) bool {
	for _, a := range stringList(s.Action) {
		if a == "*" || strings.EqualFold(a, "kms:*") || strings.EqualFold(a, "kms:"+action) {
			return true
		}
	}

	return false
}

func keyPolicyAllows(policy string, p *awsmeta.Principal, action string) bool {
	var doc struct {
		Statement json.RawMessage `json:"Statement"`
	}
	if json.Unmarshal([]byte(policy), &doc) != nil {
		return false
	}

	var stmts []policyStatement
	if json.Unmarshal(doc.Statement, &stmts) != nil {
		var one policyStatement
		if json.Unmarshal(doc.Statement, &one) != nil {
			return false
		}

		stmts = []policyStatement{one}
	}

	allowed := false

	for i := range stmts {
		s := &stmts[i]
		if !statementNamesPrincipal(s, p) || !statementCoversAction(s, action) {
			continue
		}

		switch {
		case strings.EqualFold(s.Effect, "Deny") && s.Condition == nil:
			return false
		case strings.EqualFold(s.Effect, "Allow") && s.Condition == nil:
			allowed = true
		}
	}

	return allowed
}
