package cognitoidentity

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

const (
	mappingTypeToken = "Token"
	mappingTypeRules = "Rules"
	resolutionDeny   = "Deny"

	claimPreferredRole = "cognito:preferred_role"
	claimRoles         = "cognito:roles"
)

func errNoRole(poolID, why string) error {
	return fmt.Errorf("%w: identity pool %q: %s", ErrInvalidIdentityPoolConfiguration, poolID, why)
}

// resolveRole picks the IAM role an identity assumes: a matching role mapping first, then the
// pool's authenticated or unauthenticated role.
func resolveRole(poolID string, roles *IdentityRoles, logins map[string]string) (string, error) {
	if roles == nil {
		return "", errNoRole(poolID, "no IAM roles configured")
	}

	if len(logins) == 0 {
		if roles.UnauthenticatedRoleARN == "" {
			return "", errNoRole(poolID, "no IAM role configured for this identity's auth state")
		}

		return roles.UnauthenticatedRoleARN, nil
	}

	providers := make([]string, 0, len(logins))
	for p := range logins {
		providers = append(providers, p)
	}

	sort.Strings(providers)

	for _, provider := range providers {
		claims, _ := parseLoginClaims(logins[provider])

		mapping, ok := findRoleMapping(roles.RoleMappings, provider, claims)
		if !ok {
			continue
		}

		role, matched := applyRoleMapping(mapping, claims)
		if matched {
			return role, nil
		}

		if mapping.AmbiguousRoleResolution == resolutionDeny {
			return "", errNoRole(poolID, "role mapping did not resolve a role and AmbiguousRoleResolution is Deny")
		}

		break
	}

	if roles.AuthenticatedRoleARN == "" {
		return "", errNoRole(poolID, "no IAM role configured for this identity's auth state")
	}

	return roles.AuthenticatedRoleARN, nil
}

// findRoleMapping matches a login provider to its mapping; user-pool keys carry a ":clientId" suffix.
func findRoleMapping(
	mappings map[string]RoleMapping,
	provider string,
	claims map[string]any,
) (RoleMapping, bool) {
	if m, ok := mappings[provider]; ok {
		return m, true
	}

	keys := make([]string, 0, len(mappings))
	for k := range mappings {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, key := range keys {
		clientID, found := strings.CutPrefix(key, provider+":")
		if !found {
			continue
		}

		aud, _ := claims["aud"].(string)
		cid, _ := claims["client_id"].(string)

		if (aud == "" && cid == "") || aud == clientID || cid == clientID {
			return mappings[key], true
		}
	}

	return RoleMapping{}, false
}

func applyRoleMapping(m RoleMapping, claims map[string]any) (string, bool) {
	switch m.Type {
	case mappingTypeToken:
		return roleFromTokenClaims(claims)
	case mappingTypeRules:
		if m.RulesConfiguration == nil {
			return "", false
		}

		for _, rule := range m.RulesConfiguration.Rules {
			if ruleMatches(rule, claims) {
				return rule.RoleARN, true
			}
		}
	}

	return "", false
}

func roleFromTokenClaims(claims map[string]any) (string, bool) {
	if role, _ := claims[claimPreferredRole].(string); role != "" {
		return role, true
	}

	if roles := claimStrings(claims[claimRoles]); len(roles) == 1 {
		return roles[0], true
	}

	return "", false
}

// claimStrings flattens a claim into its string values; a scalar yields one value.
func claimStrings(v any) []string {
	switch c := v.(type) {
	case string:
		return []string{c}
	case []any:
		out := make([]string, 0, len(c))

		for _, e := range c {
			out = append(out, fmt.Sprint(e))
		}

		return out
	case nil:
		return nil
	default:
		return []string{fmt.Sprint(c)}
	}
}

func ruleMatches(rule MappingRule, claims map[string]any) bool {
	values := claimStrings(claims[rule.Claim])
	if len(values) == 0 {
		return false
	}

	switch rule.MatchType {
	case "Equals":
		return slices.Contains(values, rule.Value)
	case "NotEqual":
		return !slices.Contains(values, rule.Value)
	case "Contains":
		return slices.ContainsFunc(values, func(v string) bool { return strings.Contains(v, rule.Value) })
	case "StartsWith":
		return slices.ContainsFunc(values, func(v string) bool { return strings.HasPrefix(v, rule.Value) })
	}

	return false
}
