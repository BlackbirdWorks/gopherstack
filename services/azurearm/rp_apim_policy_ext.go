package azurearm

import (
	"encoding/base64"
	"errors"
	"math"
	"slices"
	"strings"
)

const (
	apimCThumbprint      = "thumbprint"
	apimCUsername        = "username"
	apimCRespVar         = "response-variable-name"
	apimCPasswordAttr    = "password"
	apimCCertID          = "certificate-id"
	apimCIssuer          = "issuer"
	apimCMode            = "mode"
	apimCBodyAttr        = "body"
	apimCAudience        = "audience"
	apimCExternal        = "external"
	apimModeCopy         = "copy"
	apimCIncludeFragment = "include-fragment"
	apimSectionAny       = "*"
	apimFragLineShift    = 16
	apimMaxVariableName  = 128
	apimProductPolicyIdx = 2
)

// extPolSpec holds the statements of the control-flow, throttling, caching, send and JWT policies.
func extPolSpec(name string) (polSpec, bool) {
	if sp, ok := extPolSpecFlow(name); ok {
		return sp, true
	}

	return extPolSpecCache(name)
}

func extPolSpecFlow(name string) (polSpec, bool) {
	switch name {
	case apimCChoose:
		return polSpec{kids: []string{apimCWhen, apimCOtherwise}, sections: allSections()}, true
	case apimCSetVariable:
		return polSpec{
			attrs: []string{fieldName, fieldValue}, required: []string{fieldName, fieldValue}, sections: allSections(),
		}, true
	case apimCSendRequest:
		return polSpec{
			attrs:    []string{apimCMode, apimCRespVar, apimCTimeout, "ignore-error"},
			required: []string{apimCRespVar},
			kids:     []string{apimCSetURL, apimCSetMethod, apimCSetHeader, apimCSetBody, apimCAuthCertificate},
			sections: allSections(),
		}, true
	case apimCSendOneWay:
		return polSpec{
			attrs: []string{apimCMode, apimCTimeout},
			kids:  []string{apimCSetURL, apimCSetMethod, apimCSetHeader, apimCSetBody}, sections: allSections(),
		}, true
	case apimCRateLimitByKey:
		return polSpec{
			attrs: []string{
				apimCCalls, apimCRenewalPeriod, apimCIncCondition, apimCIncCount, apimCCounterKey,
				"retry-after-header-name", "retry-after-variable-name", "remaining-calls-header-name",
				"remaining-calls-variable-name", "total-calls-header-name",
			},
			required: []string{apimCCalls, apimCRenewalPeriod, apimCCounterKey},
			sections: []string{polInbound},
		}, true
	case apimCQuota:
		return polSpec{
			attrs: []string{apimCCalls, apimCBandwidth, apimCRenewalPeriod}, required: []string{apimCRenewalPeriod},
			kids: []string{apimCAPIElem}, sections: []string{polInbound},
		}, true
	case apimCQuotaByKey:
		return polSpec{
			attrs: []string{
				apimCCalls, apimCBandwidth, apimCRenewalPeriod, apimCIncCondition, apimCIncCount, apimCCounterKey,
				apimCFirstPeriod,
			},
			required: []string{apimCRenewalPeriod, apimCCounterKey}, sections: []string{polInbound},
		}, true
	}

	return polSpec{}, false
}

func extPolSpecCache(name string) (polSpec, bool) {
	switch name {
	case apimCCacheLookup:
		return polSpec{
			attrs: []string{
				"vary-by-developer", "vary-by-developer-groups", apimCCachingType, apimCDownstream, "must-revalidate",
				"allow-private-response-caching",
			},
			kids: []string{apimCVaryHeader, apimCVaryQuery}, sections: []string{polInbound},
		}, true
	case apimCCacheStore:
		return polSpec{
			attrs: []string{apimCDuration, "cache-response"}, required: []string{apimCDuration},
			sections: []string{polOutbound},
		}, true
	case apimCCacheLookupValue:
		return polSpec{
			attrs:    []string{apimCKey, apimCDefaultValue, apimCVariableName, apimCCachingType},
			required: []string{apimCKey, apimCVariableName}, sections: allSections(),
		}, true
	case apimCCacheStoreValue:
		return polSpec{
			attrs:    []string{apimCKey, fieldValue, apimCDuration, apimCCachingType},
			required: []string{apimCKey, fieldValue, apimCDuration}, sections: allSections(),
		}, true
	case apimCCacheRemoveValue:
		return polSpec{
			attrs: []string{apimCKey, apimCCachingType}, required: []string{apimCKey}, sections: allSections(),
		}, true
	case apimCValidateJWT:
		return polSpec{
			attrs: []string{
				"header-name", "query-parameter-name", "token-value", "failed-validation-httpcode",
				"failed-validation-error-message", "require-expiration-time", "require-scheme", "require-signed-tokens",
				"clock-skew", "output-token-variable-name",
			},
			kids:     []string{apimCOpenIDConfig, apimCSigningKeys, apimCAudiences, apimCIssuers, apimCRequiredClaims},
			sections: []string{polInbound},
		}, true
	case apimCIncludeFragment:
		return polSpec{attrs: []string{"fragment-id"}, required: []string{"fragment-id"}, sections: allSections()}, true
	case apimCAuthCertificate:
		return polSpec{
			attrs:    []string{apimCThumbprint, apimCCertID, apimCBodyAttr, apimCPasswordAttr},
			sections: []string{polInbound},
		}, true
	case apimCAuthBasic:
		return polSpec{
			attrs: []string{apimCUsername, apimCPasswordAttr}, required: []string{apimCUsername, apimCPasswordAttr},
			sections: []string{polInbound},
		}, true
	}

	return polSpec{}, false
}

func extNestedSpec(name string) (polSpec, bool) {
	switch name {
	case apimCAuthCertificate:
		return polSpec{attrs: []string{apimCThumbprint, apimCCertID, apimCBodyAttr, apimCPasswordAttr}}, true
	case apimCSetURL, apimCSetMethod, apimCVaryHeader, apimCVaryQuery, apimCAudience, apimCIssuer:
		return polSpec{}, true
	case apimCWhen:
		return polSpec{attrs: []string{apimCCondition}, required: []string{apimCCondition}}, true
	case apimCOtherwise:
		return polSpec{}, true
	case apimCAPIElem:
		return polSpec{
			attrs:    []string{fieldName, apimCID, apimCCalls, apimCBandwidth, apimCRenewalPeriod},
			required: []string{apimCRenewalPeriod}, kids: []string{apimCOperationElem},
		}, true
	case apimCOperationElem:
		return polSpec{
			attrs: []string{
				fieldName,
				apimCID,
				apimCCalls,
				apimCBandwidth,
				apimCRenewalPeriod,
			}, required: []string{apimCRenewalPeriod},
		}, true
	case apimCOpenIDConfig:
		return polSpec{attrs: []string{apimCUrl, "validate-connectivity"}, required: []string{apimCUrl}}, true
	case apimCSigningKeys:
		return polSpec{kids: []string{apimCKey}}, true
	case apimCKey:
		return polSpec{attrs: []string{apimCID, "n", "e", apimCCertID}}, true
	case apimCAudiences:
		return polSpec{kids: []string{apimCAudience}}, true
	case apimCIssuers:
		return polSpec{kids: []string{apimCIssuer}}, true
	case apimCRequiredClaims:
		return polSpec{kids: []string{"claim"}}, true
	case "claim":
		return polSpec{
			attrs: []string{fieldName, "match", "separator"}, required: []string{fieldName}, kids: []string{fieldValue},
		}, true
	}

	return polSpec{}, false
}

func intOrExpr(n *polNode, attr string, lo, hi int) error {
	if v, has := n.Attrs[attr]; has {
		if _, ok, _ := apimIsExprText(v); ok {
			return nil
		}
	}

	return posInt(n, attr, lo, hi)
}

func boolOrExpr(n *polNode, attr string) error {
	v, has := n.Attrs[attr]
	if !has {
		return nil
	}

	if _, ok, _ := apimIsExprText(v); ok || v == cmpTrue || v == apimCFalse {
		return nil
	}

	return polErr(n, "The attribute '%s' must be true, false or an expression.", attr)
}

func oneOfOrExpr(n *polNode, attr string, allowed ...string) error {
	v, has := n.Attrs[attr]
	if !has {
		return nil
	}

	if _, ok, _ := apimIsExprText(v); ok || slices.Contains(allowed, v) {
		return nil
	}

	return polErr(n, "The attribute '%s' must be one of %s.", attr, strings.Join(allowed, ", "))
}

func validateExt(s *apimService, n *polNode) error {
	switch n.Name {
	case apimCIncludeFragment:
		if s.Generic[apimFragmentsType][entKey("", n.Attrs["fragment-id"])] == nil {
			return polErr(n, "The policy fragment '%s' does not exist.", n.Attrs["fragment-id"])
		}
	case apimCRateLimitByKey:
		return validateRateLimitByKey(n)
	case apimCQuota, apimCQuotaByKey:
		return validateQuota(n)
	case apimCSetVariable:
		if v := n.Attrs[fieldName]; v == "" || len(v) > apimMaxVariableName {
			return polErr(n, "The variable name must be 1 to %d characters.", apimMaxVariableName)
		}
	case apimCAuthCertificate:
		return validateAuthCert(s, n)
	case apimCSendRequest, apimCSendOneWay:
		if ac := n.kid(apimCAuthCertificate); ac != nil {
			if err := validateAuthCert(s, ac); err != nil {
				return err
			}
		}

		return validateSend(n)
	case apimCValidateJWT:
		return validateJWTPolicy(s, n)
	default:
		return validateExtCache(n)
	}

	return nil
}

func validateExtCache(n *polNode) error {
	switch n.Name {
	case apimCCacheLookup:
		return validateCacheLookup(n)
	case apimCCacheStore:
		if err := boolOrExpr(n, "cache-response"); err != nil {
			return err
		}

		return intOrExpr(n, apimCDuration, 0, math.MaxInt32)
	case apimCCacheLookupValue, apimCCacheStoreValue, apimCCacheRemoveValue:
		if err := oneOfOrExpr(n, apimCCachingType, "internal", apimCExternal, "prefer-external"); err != nil {
			return err
		}

		return intOrExpr(n, apimCDuration, 1, math.MaxInt32)
	}

	return nil
}

func validateRateLimitByKey(n *polNode) error {
	if err := intOrExpr(n, apimCCalls, 1, math.MaxInt32); err != nil {
		return err
	}

	if err := intOrExpr(n, apimCRenewalPeriod, 1, apimMaxRenewalSecs); err != nil {
		return err
	}

	return intOrExpr(n, apimCIncCount, 1, math.MaxInt32)
}

func validateQuota(n *polNode) error {
	if n.Name == apimCQuota && !strings.HasPrefix(strings.TrimPrefix(n.Scope, "svc|"), "products/") {
		return polErr(n, "The quota policy can be used at the product scope only.")
	}

	for _, e := range append([]*polNode{n}, n.Kids...) {
		if err := validateQuotaElement(e); err != nil {
			return err
		}

		for _, o := range e.Kids {
			if err := validateQuotaElement(o); err != nil {
				return err
			}
		}
	}

	if n.Name == apimCQuotaByKey {
		if err := intOrExpr(n, apimCIncCount, 1, math.MaxInt32); err != nil {
			return err
		}

		if v := n.Attrs[apimCRenewalPeriod]; v != "0" {
			if err := posInt(n, apimCRenewalPeriod, apimMinKeyedRenewal, math.MaxInt32); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateQuotaElement(e *polNode) error {
	if e.Name == apimCAPIElem || e.Name == apimCOperationElem {
		if e.Attrs[apimCID] == "" && e.Attrs[fieldName] == "" {
			return polErr(e, "Either 'name' or 'id' is required.")
		}
	}

	_, hasCalls := e.Attrs[apimCCalls]
	_, hasBW := e.Attrs[apimCBandwidth]

	if !hasCalls && !hasBW {
		return polErr(e, "Either 'calls' or 'bandwidth' must be specified.")
	}

	if err := posInt(e, apimCCalls, 1, math.MaxInt32); err != nil {
		return err
	}

	if err := posInt(e, apimCBandwidth, 1, math.MaxInt32); err != nil {
		return err
	}

	return posInt(e, apimCRenewalPeriod, 0, math.MaxInt32)
}

func validateCacheLookup(n *polNode) error {
	bools := []string{
		"vary-by-developer",
		"vary-by-developer-groups",
		"must-revalidate",
		"allow-private-response-caching",
	}

	for _, b := range bools {
		if err := boolOrExpr(n, b); err != nil {
			return err
		}
	}

	if v := n.Attrs[apimCCachingType]; v != "" &&
		!slices.Contains([]string{"internal", apimCExternal, "prefer-external"}, v) {
		return polErr(n, "The attribute 'caching-type' must be internal, external or prefer-external.")
	}

	return oneOfOrExpr(n, apimCDownstream, "none", "private", "public")
}

func validateSend(n *polNode) error {
	if err := oneOfOrExpr(n, apimCMode, "new", apimModeCopy); err != nil {
		return err
	}

	if v := n.Attrs["ignore-error"]; v != "" && v != cmpTrue && v != apimCFalse {
		return polErr(n, "The attribute 'ignore-error' must be true or false.")
	}

	if err := intOrExpr(n, apimCTimeout, 1, int(apimForwardTimeout.Seconds())); err != nil {
		return err
	}

	if n.Attrs[apimCMode] == apimModeCopy {
		return nil
	}

	for _, k := range []string{apimCSetURL, apimCSetMethod} {
		if n.kid(k) == nil || strings.TrimSpace(n.kid(k).Text) == "" {
			return polErr(n, "The <%s> element is required unless mode is copy.", k)
		}
	}

	if u := strings.TrimSpace(n.kid(apimCSetURL).Text); !strings.Contains(u, "@") && !strings.Contains(u, "{{") &&
		!validAbsURL(u, acaScaleHTTP, acaSchemeHTTPS) {
		return polErr(n.kid(apimCSetURL), "The set-url must be an absolute http or https URL.")
	}

	if m := strings.TrimSpace(n.kid(apimCSetMethod).Text); !strings.Contains(m, "@") &&
		!apimSendMethodOK(strings.ToUpper(m)) {
		return polErr(n.kid(apimCSetMethod), "The set-method must be GET, POST, PUT, PATCH, DELETE, HEAD or OPTIONS.")
	}

	return nil
}

func validateJWTPolicy(s *apimService, n *polNode) error {
	sources := 0

	for _, a := range []string{"header-name", "query-parameter-name", "token-value"} {
		if n.Attrs[a] != "" {
			sources++
		}
	}

	if sources != 1 {
		return polErr(n, "Exactly one of header-name, query-parameter-name and token-value is required.")
	}

	if err := intOrExpr(n, "failed-validation-httpcode", apimMinStatus, apimMaxStatus); err != nil {
		return err
	}

	if err := intOrExpr(n, "clock-skew", 0, apimJWTMaxClockSkew); err != nil {
		return err
	}

	for _, b := range []string{"require-expiration-time", "require-signed-tokens"} {
		if err := boolOrExpr(n, b); err != nil {
			return err
		}
	}

	if n.kid(apimCOpenIDConfig) == nil && n.kid(apimCSigningKeys) == nil &&
		n.Attrs["require-signed-tokens"] != apimCFalse {
		return polErr(n, "An openid-config or issuer-signing-keys element is required to validate signed tokens.")
	}

	return validateJWTElements(s, n)
}

func validateJWTElements(s *apimService, n *polNode) error {
	for _, oc := range n.Kids {
		if oc.Name != apimCOpenIDConfig {
			continue
		}

		u := oc.Attrs[apimCUrl]
		if !strings.Contains(u, "@") && !strings.Contains(u, "{{") && !validAbsURL(u, acaScaleHTTP, acaSchemeHTTPS) {
			return polErr(oc, "The openid-config url must be an absolute http or https URL.")
		}
	}

	if sk := n.kid(apimCSigningKeys); sk != nil {
		for _, k := range sk.Kids {
			if err := validateSigningKey(s, k); err != nil {
				return err
			}
		}
	}

	if rc := n.kid(apimCRequiredClaims); rc != nil {
		for _, c := range rc.Kids {
			if m := c.Attrs["match"]; m != "" && m != "all" && m != apimMatchAny {
				return polErr(c, "The attribute 'match' must be all or any.")
			}
		}
	}

	return nil
}

func validateSigningKey(s *apimService, k *polNode) error {
	_, hasN := k.Attrs["n"]
	_, hasE := k.Attrs["e"]

	if id := k.Attrs[apimCCertID]; id != "" {
		if s.Generic[apimCertificatesType][entKey("", id)] == nil {
			return polErr(k, "The certificate '%s' does not exist.", id)
		}

		return nil
	}

	if hasN != hasE {
		return polErr(k, "The attributes 'n' and 'e' must be given together.")
	}

	if hasN {
		if _, ok := apimRSAKey(k.Attrs["n"], k.Attrs["e"]); !ok {
			return polErr(k, "The modulus and exponent must be Base64url-encoded RSA public key parts.")
		}

		return nil
	}

	txt := strings.TrimSpace(k.Text)
	if txt == "" {
		return polErr(k, "The key needs Base64 text or the n and e attributes.")
	}

	if strings.Contains(txt, "{{") || strings.Contains(txt, "@") {
		return nil
	}

	if b, err := base64.StdEncoding.DecodeString(txt); err != nil || len(b) == 0 {
		return polErr(k, "The key is not valid Base64.")
	}

	return nil
}

// validateFragment checks that a policy fragment is a <fragment> of supported statements without nested includes.
func validateFragment(s *apimService, doc string) error {
	root, err := parsePolicy(doc, "svc|fragment")
	if err != nil {
		return err
	}

	if root.Name != "fragment" {
		return polErr(root, "The root element of a policy fragment must be <fragment>.")
	}

	for _, st := range root.Kids {
		if st.Name == apimCIncludeFragment || st.Name == polBase {
			return polErr(st, "The element is not allowed in a policy fragment.")
		}

		if err = validateStatement(s, st, apimSectionAny); err != nil {
			return err
		}
	}

	return nil
}

func (s *apimService) fragmentMap() map[string]string {
	out := make(map[string]string, len(s.Generic[apimFragmentsType]))
	for _, e := range s.Generic[apimFragmentsType] {
		out[strings.ToLower(e.ID)] = strOf(e.Props, fieldValue)
	}

	return out
}

var errAPIMFragment = errors.New("policy fragment not found")

// inlineFragments replaces include-fragment statements by the statements of the fragment, one level deep.
func (g *gwState) inlineFragments(n *polNode) error {
	var kids []*polNode

	for _, k := range n.Kids {
		if k.Name != apimCIncludeFragment {
			if err := g.inlineFragments(k); err != nil {
				return err
			}

			kids = append(kids, k)

			continue
		}

		doc, ok := g.plan.fragments[strings.ToLower(k.Attrs["fragment-id"])]
		if !ok {
			return errAPIMFragment
		}

		root, err := parsePolicy(doc, k.Scope)
		if err != nil {
			return err
		}

		for _, c := range root.Kids {
			retag(c, k)
			kids = append(kids, c)
		}
	}

	n.Kids = kids

	return nil
}

func retag(n, inc *polNode) {
	n.Scope, n.Line = inc.Scope, inc.Line<<apimFragLineShift|n.Line

	for _, k := range n.Kids {
		retag(k, inc)
	}
}
