package gcpcompute

import (
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collSecPolicy    = "securityPolicies"
	spTypeArmor      = "CLOUD_ARMOR"
	spTypeEdge       = "CLOUD_ARMOR_EDGE"
	spTypeNetwork    = "CLOUD_ARMOR_NETWORK"
	defaultRulePrio  = 2147483647
	versionedSrcIPs  = "SRC_IPS_V1"
	maxSrcIPRanges   = 10
	maxKeyConfigs    = 3
	wildcard         = "*"
	defaultRuleDesc  = "Default rule, higher priority overrides it"
	actionAllow      = "allow"
	actionRateBan    = "rate_based_ban"
	actionThrottle   = "throttle"
	actionRedirect   = "redirect"
	ruleKind         = "compute#securityPolicyRule"
	redirectExternal = "EXTERNAL_302"
	redirectRecaptch = "GOOGLE_RECAPTCHA"
)

var (
	denyActionRE   = regexp.MustCompile(`^deny\((403|404|502)\)$`)
	exceedDenyRE   = regexp.MustCompile(`^deny\((403|404|429|502)\)$`)
	secPolicyTypes = map[string]bool{spTypeArmor: true, spTypeEdge: true, spTypeNetwork: true}          //nolint:gochecknoglobals // lookup
	jsonParsings   = map[string]bool{"DISABLED": true, "STANDARD": true, "STANDARD_WITH_GRAPHQL": true} //nolint:gochecknoglobals // lookup
	logLevels      = map[string]bool{"NORMAL": true, "VERBOSE": true}                                   //nolint:gochecknoglobals // lookup
	enforceKeys    = map[string]bool{                                                                   //nolint:gochecknoglobals // read-only lookup table
		"ALL": true, "ASN": true, "HTTP_COOKIE": true, "HTTP_HEADER": true, "HTTP_PATH": true, "IP": true,
		"REGION_CODE": true, "SNI": true, "TLS_JA3_FINGERPRINT": true, "TLS_JA4_FINGERPRINT": true,
		"USER_IP": true, "XFF_IP": true,
	}
)

func secPolicySpec() lbSpec[computeapi.SecurityPolicy] {
	return lbSpec[computeapi.SecurityPolicy]{
		coll: collSecPolicy, kind: "compute#securityPolicy", noun: "security_policy",
		m: func(b *InMemoryBackend) map[string]*computeapi.SecurityPolicy { return b.secPolicies },
		meta: func(v *computeapi.SecurityPolicy) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				fp: &v.Fingerprint, id: &v.Id,
			}
		},
		prep: prepSecurityPolicy,
		view: func(v *computeapi.SecurityPolicy) {
			for _, r := range v.Rules {
				r.ForceSendFields = []string{"Priority", "Preview"}
			}
		},
	}
}

func defaultRule(policyType string) *computeapi.SecurityPolicyRule {
	r := &computeapi.SecurityPolicyRule{
		Kind: ruleKind, Action: actionAllow, Priority: defaultRulePrio, Description: defaultRuleDesc,
	}
	if policyType == spTypeNetwork {
		r.NetworkMatch = &computeapi.SecurityPolicyRuleNetworkMatcher{}
	} else {
		r.Match = &computeapi.SecurityPolicyRuleMatcher{
			VersionedExpr: versionedSrcIPs,
			Config:        &computeapi.SecurityPolicyRuleMatcherConfig{SrcIpRanges: []string{wildcard}},
		}
	}

	return r
}

func prepSecurityPolicy(_ *InMemoryBackend, _ lbCtx, in *computeapi.SecurityPolicy, _ map[string]any) *APIError {
	if in.Type == "" {
		in.Type = spTypeArmor
	}

	if !secPolicyTypes[in.Type] {
		return errField("resource.type", in.Type, "Must be CLOUD_ARMOR, CLOUD_ARMOR_EDGE or CLOUD_ARMOR_NETWORK.")
	}

	if err := validLabels(in.Labels); err != nil {
		return err
	}

	if in.LabelFingerprint == "" {
		in.LabelFingerprint = labelFP(in.Labels)
	}

	if err := validSecurityPolicyOptions(in); err != nil {
		return err
	}

	if len(in.Rules) == 0 {
		in.Rules = []*computeapi.SecurityPolicyRule{defaultRule(in.Type)}
	}

	seen := map[int64]bool{}

	for i, r := range in.Rules {
		if err := validateRule(in.Type, r, "resource.rules["+strconv.Itoa(i)+"]"); err != nil {
			return err
		}

		if seen[r.Priority] {
			return errInvalid("Invalid value for field 'resource.rules[%d].priority': '%d'. "+
				"Multiple rules have the same priority.", i, r.Priority)
		}

		seen[r.Priority] = true
	}

	if !seen[defaultRulePrio] {
		return errInvalid("Invalid value for field 'resource.rules': the policy must contain a default rule " +
			"with priority 2147483647 that matches everything.")
	}

	slices.SortFunc(in.Rules, func(a, b *computeapi.SecurityPolicyRule) int {
		switch {
		case a.Priority < b.Priority:
			return -1
		case a.Priority > b.Priority:
			return 1
		}

		return 0
	})

	return nil
}

func validSecurityPolicyOptions(in *computeapi.SecurityPolicy) *APIError {
	if o := in.AdvancedOptionsConfig; o != nil {
		if o.JsonParsing != "" && !jsonParsings[o.JsonParsing] {
			return errField("resource.advancedOptionsConfig.jsonParsing", o.JsonParsing, "")
		}

		if o.LogLevel != "" && !logLevels[o.LogLevel] {
			return errField("resource.advancedOptionsConfig.logLevel", o.LogLevel, "")
		}

		switch strings.ToUpper(o.RequestBodyInspectionSize) {
		case "", "8KB", "16KB", "32KB", "48KB", "64KB":
		default:
			return errField("resource.advancedOptionsConfig.requestBodyInspectionSize",
				o.RequestBodyInspectionSize, "Must be 8KB, 16KB, 32KB, 48KB or 64KB.")
		}
	}

	return nil
}

func validateRule(policyType string, r *computeapi.SecurityPolicyRule, field string) *APIError {
	if r == nil {
		return errRequired(field)
	}

	r.Kind = ruleKind

	if r.Priority < 0 || r.Priority > defaultRulePrio {
		return errField(field+".priority", strconv.FormatInt(r.Priority, 10),
			"Priority must be between 0 and 2147483647.")
	}

	if err := validateRuleAction(r, field); err != nil {
		return err
	}

	if err := validateRuleMatch(policyType, r, field); err != nil {
		return err
	}

	if r.Priority == defaultRulePrio {
		return validateDefaultRule(policyType, r, field)
	}

	return nil
}

func validateRuleAction(r *computeapi.SecurityPolicyRule, field string) *APIError {
	switch {
	case r.Action == "":
		return errRequired(field + ".action")
	case r.Action == actionAllow, denyActionRE.MatchString(r.Action):
	case r.Action == actionRateBan, r.Action == actionThrottle, r.Action == actionRedirect:
	default:
		return errField(field+".action", r.Action,
			"Must be allow, deny(403), deny(404), deny(502), rate_based_ban, throttle or redirect.")
	}

	limited := r.Action == actionRateBan || r.Action == actionThrottle
	if limited != (r.RateLimitOptions != nil) {
		if limited {
			return errRequired(field + ".rateLimitOptions")
		}

		return errField(field+".rateLimitOptions", "", "Only valid with the rate_based_ban and throttle actions.")
	}

	if (r.Action == actionRedirect) != (r.RedirectOptions != nil) {
		if r.Action == actionRedirect {
			return errRequired(field + ".redirectOptions")
		}

		return errField(field+".redirectOptions", "", "Only valid with the redirect action.")
	}

	if r.RedirectOptions != nil {
		if err := validRedirect(r.RedirectOptions, field+".redirectOptions"); err != nil {
			return err
		}
	}

	if r.RateLimitOptions != nil {
		return validRateLimit(r.Action, r.RateLimitOptions, field+".rateLimitOptions")
	}

	return nil
}

func validRedirect(o *computeapi.SecurityPolicyRuleRedirectOptions, field string) *APIError {
	switch o.Type {
	case redirectExternal:
		if o.Target == "" {
			return errRequired(field + ".target")
		}
	case redirectRecaptch:
		if o.Target != "" {
			return errField(field+".target", o.Target, "Cannot be specified for GOOGLE_RECAPTCHA.")
		}
	default:
		return errField(field+".type", o.Type, "Must be EXTERNAL_302 or GOOGLE_RECAPTCHA.")
	}

	return nil
}

func validRateLimit(action string, o *computeapi.SecurityPolicyRuleRateLimitOptions, field string) *APIError {
	if o.RateLimitThreshold == nil || o.RateLimitThreshold.Count < 1 || o.RateLimitThreshold.IntervalSec < 1 {
		return errField(field+".rateLimitThreshold", "", "count and intervalSec must be positive.")
	}

	if o.ConformAction == "" {
		o.ConformAction = actionAllow
	}

	if o.ConformAction != actionAllow {
		return errField(field+".conformAction", o.ConformAction, "The only valid value is allow.")
	}

	if o.ExceedAction == "" {
		return errRequired(field + ".exceedAction")
	}

	if o.ExceedAction != actionRedirect && !exceedDenyRE.MatchString(o.ExceedAction) {
		return errField(field+".exceedAction", o.ExceedAction, "Must be deny(403), deny(404), deny(429), deny(502) or redirect.")
	}

	if (o.ExceedAction == actionRedirect) != (o.ExceedRedirectOptions != nil) {
		return errField(field+".exceedRedirectOptions", "", "Required with, and only valid for, the redirect exceed action.")
	}

	if o.ExceedRedirectOptions != nil {
		if err := validRedirect(o.ExceedRedirectOptions, field+".exceedRedirectOptions"); err != nil {
			return err
		}
	}

	if action != actionRateBan && (o.BanDurationSec != 0 || o.BanThreshold != nil) {
		return errField(field+".banDurationSec", "", "banDurationSec and banThreshold are only valid with rate_based_ban.")
	}

	return validEnforceKey(o, field)
}

func validEnforceKey(o *computeapi.SecurityPolicyRuleRateLimitOptions, field string) *APIError {
	if len(o.EnforceOnKeyConfigs) > 0 && o.EnforceOnKey != "" {
		return errField(field+".enforceOnKey", o.EnforceOnKey, "Cannot be combined with enforceOnKeyConfigs.")
	}

	if len(o.EnforceOnKeyConfigs) > maxKeyConfigs {
		return errField(field+".enforceOnKeyConfigs", "", "At most 3 configs are allowed.")
	}

	if o.EnforceOnKey != "" && !enforceKeys[o.EnforceOnKey] {
		return errField(field+".enforceOnKey", o.EnforceOnKey, "")
	}

	named := o.EnforceOnKey == "HTTP_HEADER" || o.EnforceOnKey == "HTTP_COOKIE"
	if o.EnforceOnKeyName != "" && !named {
		return errField(field+".enforceOnKeyName", o.EnforceOnKeyName, "Only valid for HTTP_HEADER and HTTP_COOKIE.")
	}

	return nil
}

func validateRuleMatch(policyType string, r *computeapi.SecurityPolicyRule, field string) *APIError {
	if policyType == spTypeNetwork {
		if r.Match != nil {
			return errField(field+".match", "", "Network security policies use networkMatch.")
		}

		if r.NetworkMatch == nil {
			r.NetworkMatch = &computeapi.SecurityPolicyRuleNetworkMatcher{}
		}

		return validSrcRanges(r.NetworkMatch.SrcIpRanges, field+".networkMatch.srcIpRanges")
	}

	if r.NetworkMatch != nil {
		return errField(field+".networkMatch", "", "Only valid in CLOUD_ARMOR_NETWORK policies.")
	}

	m := r.Match
	if m == nil {
		return errRequired(field + ".match")
	}

	hasExpr := m.Expr != nil
	if hasExpr == (m.VersionedExpr != "") {
		return errInvalid("Invalid value for field '%s.match': exactly one of versionedExpr and expr must be set.", field)
	}

	if hasExpr {
		return validExpr(m.Expr, field+".match.expr")
	}

	if m.VersionedExpr != versionedSrcIPs {
		return errField(field+".match.versionedExpr", m.VersionedExpr, "Must be SRC_IPS_V1.")
	}

	if m.Config == nil {
		return errRequired(field + ".match.config")
	}

	return validSrcRanges(m.Config.SrcIpRanges, field+".match.config.srcIpRanges")
}

func validSrcRanges(ranges []string, field string) *APIError {
	if len(ranges) > maxSrcIPRanges {
		return errField(field, "", "At most 10 source IP ranges are allowed.")
	}

	for _, s := range ranges {
		if s == wildcard {
			continue
		}

		if _, err := netip.ParsePrefix(s); err == nil {
			continue
		}

		if _, err := netip.ParseAddr(s); err != nil {
			return errField(field, s, "Not a valid IP address range.")
		}
	}

	return nil
}

func validExpr(e *computeapi.Expr, field string) *APIError {
	expr := strings.TrimSpace(e.Expression)
	if expr == "" {
		return errRequired(field + ".expression")
	}

	depth, quote := 0, rune(0)

	for _, c := range expr {
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
		}

		if depth < 0 {
			break
		}
	}

	if depth != 0 || quote != 0 {
		return errField(field+".expression", e.Expression, "Syntax error in the expression.")
	}

	return nil
}

func validateDefaultRule(policyType string, r *computeapi.SecurityPolicyRule, field string) *APIError {
	if policyType == spTypeNetwork {
		return nil
	}

	m := r.Match
	if m.VersionedExpr != versionedSrcIPs || m.Config == nil ||
		!slices.Equal(m.Config.SrcIpRanges, []string{wildcard}) {
		return errField(field+".match", "", "The default rule (priority 2147483647) must match all source IP ranges (*).")
	}

	return nil
}

// resolveBSSecurityPolicyLocked validates a backend service's Cloud Armor attachment and stores its selfLink.
func (b *InMemoryBackend) resolveBSSecurityPolicyLocked(c lbCtx, bs *computeapi.BackendService) *APIError {
	if bs.SecurityPolicy == "" {
		return nil
	}

	if c.scope != scopeGlobal {
		return errField("resource.securityPolicy", bs.SecurityPolicy,
			"Regional backend services use regional security policies, which are not supported.")
	}

	if bs.LoadBalancingScheme != schemeExternal && bs.LoadBalancingScheme != schemeExternalManaged {
		return errField("resource.securityPolicy", bs.SecurityPolicy,
			"Security policies attach only to EXTERNAL and EXTERNAL_MANAGED backend services.")
	}

	self, err := lbResolve(b, secPolicySpec(), c, "resource.securityPolicy", bs.SecurityPolicy)
	if err != nil {
		return err
	}

	if p := lbFind(b, secPolicySpec(), self); p != nil && p.Type != spTypeArmor {
		return errField("resource.securityPolicy", bs.SecurityPolicy,
			"Only CLOUD_ARMOR security policies attach to backend services.")
	}

	bs.SecurityPolicy = self

	return nil
}
