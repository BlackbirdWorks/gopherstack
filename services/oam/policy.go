package oam

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

const (
	actionCreateLink = "oam:CreateLink"
	actionUpdateLink = "oam:UpdateLink"
)

type rawPolicy struct {
	Version   string          `json:"Version,omitempty"`
	Statement json.RawMessage `json:"Statement"`
}

func decodeStatements(raw json.RawMessage) ([]map[string]any, bool) {
	trimmed := strings.TrimSpace(string(raw))

	switch {
	case strings.HasPrefix(trimmed, "{"):
		var one map[string]any
		if json.Unmarshal(raw, &one) != nil {
			return nil, false
		}

		return []map[string]any{one}, true
	case strings.HasPrefix(trimmed, "["):
		var many []map[string]any
		if json.Unmarshal(raw, &many) != nil {
			return nil, false
		}

		return many, true
	default:
		return nil, false
	}
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

func validateSinkPolicy(policy string) error {
	var p rawPolicy
	if err := json.Unmarshal([]byte(policy), &p); err != nil {
		return fail(ErrInvalidParameter, "Policy is not valid JSON")
	}

	stmts, ok := decodeStatements(p.Statement)
	if !ok || len(stmts) == 0 {
		return fail(ErrInvalidParameter, "Policy must contain at least one Statement")
	}

	for _, st := range stmts {
		if effect, _ := st["Effect"].(string); effect != "Allow" {
			return fail(ErrInvalidParameter, "Policy statements must have Effect Allow")
		}

		if st["Principal"] == nil {
			return fail(ErrInvalidParameter, "Policy statements must have a Principal")
		}

		actions := stringList(st["Action"])
		if len(actions) == 0 {
			return fail(ErrInvalidParameter, "Policy statements must have an Action")
		}

		for _, a := range actions {
			if a != "*" && !strings.HasPrefix(a, "oam:") {
				return fail(ErrInvalidParameter, "Action %q is not an OAM action", a)
			}
		}
	}

	return nil
}

func principalCovers(principal any, account string) bool {
	root := arn.BuildGlobal("iam", "", account, "root")

	candidates := stringList(principal)
	if m, ok := principal.(map[string]any); ok {
		candidates = stringList(m["AWS"])
	}

	return slices.ContainsFunc(candidates, func(c string) bool {
		return c == "*" || c == account || c == root || strings.HasPrefix(c, "arn:aws:iam::"+account+":")
	})
}

// sinkPolicyAllows evaluates the sink policy for a caller account performing action with the given resource types.
func (b *InMemoryBackend) sinkPolicyAllowsLocked(
	sink *Sink,
	callerAccount, action string,
	resourceTypes []string,
) bool {
	if !sink.HasPolicy {
		return false
	}

	var p rawPolicy
	if json.Unmarshal([]byte(sink.Policy), &p) != nil {
		return false
	}

	stmts, ok := decodeStatements(p.Statement)
	if !ok {
		return false
	}

	applicable := make([]map[string]any, 0, len(stmts))

	for _, st := range stmts {
		if principalCovers(st["Principal"], callerAccount) {
			applicable = append(applicable, st)
		}
	}

	if len(applicable) == 0 {
		return false
	}

	doc, err := json.Marshal(map[string]any{"Version": p.Version, "Statement": applicable})
	if err != nil {
		return false
	}

	extra := map[string]string{"oam:resourcetypes": strings.Join(resourceTypes, ",")}

	if b.accounts != nil {
		if info, found := b.accounts.AccountInfo(callerAccount); found && info.OrgID != "" {
			extra["aws:principalorgid"] = info.OrgID
		}
	}

	ctx := iam.ConditionContext{
		PrincipalAccount: callerAccount,
		PrincipalARN:     arn.BuildGlobal("iam", "", callerAccount, "root"),
		Extra:            extra,
	}

	return iam.EvaluatePolicies([]string{string(doc)}, action, sink.ARN, ctx) == iam.EvalAllow
}
