package xray

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"path"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

type lockoutStatement struct {
	Effect       string         `json:"Effect"`
	Principal    jsontext.Value `json:"Principal"`
	Action       jsontext.Value `json:"Action"`
	Condition    jsontext.Value `json:"Condition"`
	NotPrincipal jsontext.Value `json:"NotPrincipal"`
	NotAction    jsontext.Value `json:"NotAction"`
}

// policyLocksOutCaller reports whether policyDocument has an unconditional Deny of xray:PutResourcePolicy that
// matches callerARN, which would make the policy unmanageable (LockoutPreventionException).
func policyLocksOutCaller(policyDocument, callerARN string) bool {
	if callerARN == "" {
		return false
	}

	var doc struct {
		Statement jsontext.Value `json:"Statement"`
	}
	if json.Unmarshal([]byte(policyDocument), &doc, jsonx.V1()) != nil {
		return false
	}

	var stmts []lockoutStatement
	if json.Unmarshal(doc.Statement, &stmts, jsonx.V1()) != nil {
		var one lockoutStatement
		if json.Unmarshal(doc.Statement, &one, jsonx.V1()) != nil {
			return false
		}

		stmts = []lockoutStatement{one}
	}

	for i := range stmts {
		s := &stmts[i]
		if s.Effect != "Deny" || len(s.Condition) > 0 || len(s.NotPrincipal) > 0 || len(s.NotAction) > 0 {
			continue
		}

		if matchesAny(s.Action, "xray:putresourcepolicy", true) && principalMatches(s.Principal, callerARN) {
			return true
		}
	}

	return false
}

func stringList(raw jsontext.Value) []string {
	var one string
	if json.Unmarshal(raw, &one, jsonx.V1()) == nil {
		return []string{one}
	}

	var many []string
	_ = json.Unmarshal(raw, &many, jsonx.V1())

	return many
}

func matchesAny(raw jsontext.Value, target string, fold bool) bool {
	for _, p := range stringList(raw) {
		if fold {
			p = strings.ToLower(p)
		}

		if ok, _ := path.Match(p, target); ok {
			return true
		}
	}

	return false
}

func principalMatches(raw jsontext.Value, callerARN string) bool {
	var wrapper struct {
		AWS jsontext.Value `json:"AWS"`
	}

	if json.Unmarshal(raw, &wrapper, jsonx.V1()) == nil && len(wrapper.AWS) > 0 {
		raw = wrapper.AWS
	}

	for _, p := range stringList(raw) {
		if p == "*" || p == callerARN || (strings.HasSuffix(p, ":root") && sameAccountRoot(p, callerARN)) {
			return true
		}
	}

	return false
}

// sameAccountRoot reports whether rootARN (arn:aws:iam::<acct>:root) names callerARN's account.
func sameAccountRoot(rootARN, callerARN string) bool {
	rp, cp := strings.Split(rootARN, ":"), strings.Split(callerARN, ":")
	const accountIdx = 4

	return len(rp) > accountIdx && len(cp) > accountIdx && rp[accountIdx] != "" && rp[accountIdx] == cp[accountIdx]
}
