package bedrockagentcorecontrol

import (
	"regexp"
	"slices"
	"unicode/utf8"
)

const (
	maxScopeMapping  = 50
	maxClaimString   = 255
	maxPrivateEPOver = 5
	claimValueString = "STRING"
	claimValueArray  = "STRING_ARRAY"
	opEquals         = "EQUALS"
)

var (
	discoveryPattern = regexp.MustCompile(
		`^[^\s?#]+/\.well-known/(openid-configuration|oauth-authorization-server)(/[^\s?#]+)?$`)
	scopePattern = regexp.MustCompile(`^[\x21\x23-\x5B\x5D-\x7E]+$`)
	claimPattern = regexp.MustCompile(`^[A-Za-z0-9_.\-:]+$`)
)

// validateJWTMembers applies the member constraints of CustomJWTAuthorizerConfiguration and its claim types
// (API_CustomJWTAuthorizerConfiguration, API_CustomClaimValidationType, API_AuthorizingClaimMatchValueType).
func validateJWTMembers(jwt map[string]any) error {
	for _, key := range []string{"allowedAudience", "allowedClients", "allowedScopes", "customClaims"} {
		if v, ok := jwt[key].([]any); ok && len(v) == 0 {
			return invalid("%s must contain at least one item", key)
		}
	}

	scopes := stringsOf(jwt["allowedScopes"])
	for _, sc := range scopes {
		if err := checkToken("allowedScopes entry", sc, scopePattern); err != nil {
			return err
		}
	}

	if err := validateScopeMapping(objOf(jwt, "advertisedScopeMapping"), scopes); err != nil {
		return err
	}

	if certs, ok := jwt["certificateConfigurations"].([]any); ok && len(certs) != 1 {
		return invalid("certificateConfigurations must contain exactly one item")
	}

	if over, ok := jwt["privateEndpointOverrides"].([]any); ok && len(over) > maxPrivateEPOver {
		return invalid("privateEndpointOverrides can contain at most %d items", maxPrivateEPOver)
	}

	for _, c := range listOf(jwt, "customClaims") {
		if err := validateCustomClaim(mapOf(c)); err != nil {
			return err
		}
	}

	return nil
}

func checkToken(what, v string, pattern *regexp.Regexp) error {
	if n := utf8.RuneCountInString(v); n < 1 || n > maxClaimString || !pattern.MatchString(v) {
		return invalid("%s %q must be 1-%d characters matching %s", what, v, maxClaimString, pattern.String())
	}

	return nil
}

func validateScopeMapping(m map[string]any, allowed []string) error {
	if len(m) > maxScopeMapping {
		return invalid("advertisedScopeMapping can contain at most %d entries", maxScopeMapping)
	}

	for k, v := range m {
		if err := checkToken("advertisedScopeMapping key", k, scopePattern); err != nil {
			return err
		}

		val, _ := v.(string)
		if err := checkToken("advertisedScopeMapping value", val, scopePattern); err != nil {
			return err
		}

		if !slices.Contains(allowed, k) {
			return invalid("advertisedScopeMapping key %q must be one of allowedScopes", k)
		}
	}

	return nil
}

func validateCustomClaim(c map[string]any) error {
	if err := checkToken("inboundTokenClaimName", strOf(c, "inboundTokenClaimName"), claimPattern); err != nil {
		return err
	}

	if t := strOf(c, "inboundTokenClaimValueType"); t != claimValueString && t != claimValueArray {
		return invalid("inboundTokenClaimValueType must be %s or %s", claimValueString, claimValueArray)
	}

	rule := objOf(c, "authorizingClaimMatchValue")
	if !slices.Contains([]string{opEquals, opContains, opContainsAny}, strOf(rule, "claimMatchOperator")) {
		return invalid("claimMatchOperator must be %s, %s or %s", opEquals, opContains, opContainsAny)
	}

	val := objOf(rule, "claimMatchValue")
	if _, ok := exactlyOne(val, []string{"matchValueString", "matchValueStringList"}); !ok {
		return invalid("claimMatchValue must set exactly one of matchValueString and matchValueStringList")
	}

	return nil
}
