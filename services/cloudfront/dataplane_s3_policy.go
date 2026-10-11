package cloudfront

import (
	"encoding/json"
	"slices"
	"strings"
)

const (
	cloudFrontServicePrincipal = "cloudfront.amazonaws.com"
	oaiPrincipalPrefix         = "arn:aws:iam::cloudfront:user/CloudFront Origin Access Identity "
	actionS3GetObject          = "s3:GetObject"
	actionS3ListBucket         = "s3:ListBucket"
)

type accessKind int

const (
	accessAnonymous accessKind = iota
	accessService
	accessOAI
)

// s3Caller is the identity CloudFront presents to the S3 origin.
type s3Caller struct {
	oaiID       string
	canonicalID string
	sourceARN   string
	account     string
	kind        accessKind
}

type policyEffectKind int

const (
	effectNone policyEffectKind = iota
	effectAllow
	effectDeny
)

type policyDoc struct {
	Statement json.RawMessage `json:"Statement"`
}

type policyStatement struct {
	Principal any            `json:"Principal"`
	Action    any            `json:"Action"`
	Resource  any            `json:"Resource"`
	Condition map[string]any `json:"Condition"`
	Effect    string         `json:"Effect"`
}

func parseStatements(policyJSON string) []policyStatement {
	var doc policyDoc
	if err := json.Unmarshal([]byte(policyJSON), &doc); err != nil {
		return nil
	}

	var many []policyStatement
	if err := json.Unmarshal(doc.Statement, &many); err == nil {
		return many
	}

	var one policyStatement
	if err := json.Unmarshal(doc.Statement, &one); err == nil {
		return []policyStatement{one}
	}

	return nil
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

func anyGlob(patterns []string, s string, fold bool) bool {
	for _, p := range patterns {
		if fold {
			p, s = strings.ToLower(p), strings.ToLower(s)
		}

		if globMatchCS(p, s) {
			return true
		}
	}

	return false
}

func (c s3Caller) principalMatches(principal any) bool {
	if s, ok := principal.(string); ok {
		return s == "*"
	}

	m, ok := principal.(map[string]any)
	if !ok {
		return false
	}

	if anyEqual(stringList(m["AWS"]), "*") {
		return true
	}

	switch c.kind {
	case accessService:
		return anyEqual(stringList(m["Service"]), cloudFrontServicePrincipal)
	case accessOAI:
		return anyEqual(stringList(m["AWS"]), oaiPrincipalPrefix+c.oaiID) ||
			c.canonicalID != "" && anyEqual(stringList(m["CanonicalUser"]), c.canonicalID)
	case accessAnonymous:
	}

	return false
}

func anyEqual(list []string, want string) bool {
	return slices.Contains(list, want)
}

func (c s3Caller) contextValue(key string) (string, bool) {
	switch strings.ToLower(key) {
	case "aws:sourcearn":
		return c.sourceARN, c.kind == accessService
	case "aws:sourceaccount":
		return c.account, c.kind == accessService
	}

	return "", false
}

// conditionsHold evaluates the string and ARN operators; unsupported operators never hold.
func (c s3Caller) conditionsHold(cond map[string]any) bool {
	for op, body := range cond {
		keys, ok := body.(map[string]any)
		if !ok {
			return false
		}

		for key, want := range keys {
			got, present := c.contextValue(key)
			if !present {
				return false
			}

			wants := stringList(want)

			switch strings.ToLower(op) {
			case "stringequals", "arnequals":
				if !anyEqual(wants, got) {
					return false
				}
			case "stringlike", "arnlike":
				if !anyGlob(wants, got, false) {
					return false
				}
			default:
				return false
			}
		}
	}

	return true
}

func (s *policyStatement) covers(action, resource string) bool {
	return anyGlob(stringList(s.Action), action, true) && anyGlob(stringList(s.Resource), resource, false)
}

// evaluatePolicy returns the combined bucket-policy effect for caller on action/resource.
func evaluatePolicy(policyJSON string, c s3Caller, action, resource string) policyEffectKind {
	result := effectNone

	for _, st := range parseStatements(policyJSON) {
		if !st.covers(action, resource) || !c.principalMatches(st.Principal) || !c.conditionsHold(st.Condition) {
			continue
		}

		switch strings.ToLower(st.Effect) {
		case "deny":
			return effectDeny
		case "allow":
			result = effectAllow
		}
	}

	return result
}
