package cognitoidp

import (
	"crypto/rsa"
	"slices"
	"time"
)

// samlExpect is what a Response must be bound to. An empty requestID means an unsolicited
// (IdP-initiated) response, which must carry no InResponseTo at all.
type samlExpect struct {
	sp        *rsa.PrivateKey
	poolID    string
	acs       string
	requestID string
}

// samlResult is a fully validated assertion.
type samlResult struct {
	validUntil   time.Time
	claims       map[string]any
	assertionID  string
	nameID       string
	sessionIndex string
}

// validateSAMLResponse verifies the XML signature against the pinned IdP certificates, then the
// issuer, destination, request binding, audience and validity window, using only signed content.
func validateSAMLResponse(raw []byte, md *samlMetadata, exp *samlExpect, now time.Time) (*samlResult, error) {
	root, err := parseXML(raw)
	if err != nil {
		return nil, samlErrorf("%v", err)
	}

	if !root.is(nsSAMLProtocol, "Response") {
		return nil, samlErrorf("root element is not a SAML Response")
	}

	signed, err := signedElements(root, md.keys)
	if err != nil {
		return nil, err
	}

	if slices.Contains(signed, root) {
		if err = checkSignedResponse(root, exp); err != nil {
			return nil, err
		}
	}

	assertion, err := responseAssertion(root, signed, md, exp)
	if err != nil {
		return nil, err
	}

	return assertionClaims(assertion, md, exp, now)
}

// responseAssertion returns the single assertion (decrypting it when encrypted) covered by a verified signature.
func responseAssertion(root *xnode, signed []*xnode, md *samlMetadata, exp *samlExpect) (*xnode, error) {
	plain := root.childrenNamed(nsSAMLAssertion, "Assertion")
	encrypted := root.childrenNamed(nsSAMLAssertion, "EncryptedAssertion")

	if len(plain)+len(encrypted) != 1 {
		return nil, samlErrorf("response must contain exactly one assertion")
	}

	if len(plain) == 1 {
		return coveredAssertion(plain[0], signed, root)
	}

	a, err := decryptAssertion(encrypted[0], exp.sp)
	if err != nil {
		return nil, err
	}

	inner, err := signedElements(a, md.keys)
	if err != nil {
		return nil, err
	}

	return coveredAssertion(a, append(inner, signed...), root)
}

// coveredAssertion accepts a only when a verified signature covers it or the whole Response.
func coveredAssertion(a *xnode, signed []*xnode, root *xnode) (*xnode, error) {
	if len(signed) == 0 {
		return nil, sigErrorf("document is not signed")
	}

	if !slices.Contains(signed, a) && !slices.Contains(signed, root) {
		return nil, samlErrorf("assertion is not covered by a valid signature")
	}

	return a, nil
}

// checkSignedResponse validates the Response-level attributes, which are only trustworthy when the
// Response element itself is covered by a verified signature.
func checkSignedResponse(root *xnode, exp *samlExpect) error {
	if root.attr("Destination") != exp.acs {
		return samlErrorf("response destination does not match this endpoint")
	}

	if root.attr("InResponseTo") != exp.requestID {
		return samlErrorf("response is not a reply to the pending request")
	}

	status := root.child(nsSAMLProtocol, "Status")
	if status == nil || status.child(nsSAMLProtocol, "StatusCode") == nil ||
		status.child(nsSAMLProtocol, "StatusCode").attr("Value") != samlStatusOK {
		return samlErrorf("identity provider returned a non-success status")
	}

	return nil
}

func assertionClaims(a *xnode, md *samlMetadata, exp *samlExpect, now time.Time) (*samlResult, error) {
	if iss := a.child(nsSAMLAssertion, "Issuer"); iss == nil || iss.textContent() != md.entityID {
		return nil, samlErrorf("assertion issuer does not match the identity provider")
	}

	if a.attr(attrNameID) == "" {
		return nil, samlErrorf("assertion has no ID")
	}

	subj := a.child(nsSAMLAssertion, "Subject")
	if subj == nil {
		return nil, samlErrorf("assertion has no subject")
	}

	if err := checkConfirmation(subj, exp, now); err != nil {
		return nil, err
	}

	validUntil, err := checkConditions(a, exp.poolID, now)
	if err != nil {
		return nil, err
	}

	nameID := subj.child(nsSAMLAssertion, "NameID")
	if nameID == nil || nameID.textContent() == "" {
		return nil, samlErrorf("assertion has no NameID")
	}

	res := &samlResult{
		claims: attributeClaims(a), assertionID: a.attr(attrNameID), nameID: nameID.textContent(),
		validUntil: validUntil, sessionIndex: authnSessionIndex(a),
	}
	res.claims[claimSub] = res.nameID
	res.claims[claimNameID] = res.nameID

	return res, nil
}

func attributeClaims(a *xnode) map[string]any {
	claims := map[string]any{}

	stmt := a.child(nsSAMLAssertion, "AttributeStatement")
	if stmt == nil {
		return claims
	}

	for _, attr := range stmt.childrenNamed(nsSAMLAssertion, "Attribute") {
		if vals := attr.childrenNamed(nsSAMLAssertion, "AttributeValue"); len(vals) > 0 && attr.attr("Name") != "" {
			claims[attr.attr("Name")] = vals[0].textContent()
		}
	}

	return claims
}

func authnSessionIndex(a *xnode) string {
	if stmt := a.child(nsSAMLAssertion, "AuthnStatement"); stmt != nil {
		return stmt.attr("SessionIndex")
	}

	return ""
}

func checkConfirmation(subj *xnode, exp *samlExpect, now time.Time) error {
	for _, sc := range subj.childrenNamed(nsSAMLAssertion, "SubjectConfirmation") {
		if sc.attr("Method") != samlConfirmation {
			continue
		}

		data := sc.child(nsSAMLAssertion, "SubjectConfirmationData")
		if data == nil || data.attr("Recipient") != exp.acs || data.attr("InResponseTo") != exp.requestID {
			continue
		}

		notAfter, err := time.Parse(time.RFC3339, data.attr("NotOnOrAfter"))
		if err != nil || !now.Before(notAfter.Add(samlClockSkew)) {
			continue
		}

		return nil
	}

	return samlErrorf("assertion has no valid bearer subject confirmation")
}

// checkConditions enforces the validity window and audience and returns the assertion's NotOnOrAfter.
func checkConditions(a *xnode, poolID string, now time.Time) (time.Time, error) {
	cond := a.child(nsSAMLAssertion, "Conditions")
	if cond == nil {
		return time.Time{}, samlErrorf("assertion has no conditions")
	}

	notAfter, err := time.Parse(time.RFC3339, cond.attr("NotOnOrAfter"))
	if err != nil || !now.Before(notAfter.Add(samlClockSkew)) {
		return time.Time{}, samlErrorf("assertion has expired")
	}

	if nb := cond.attr("NotBefore"); nb != "" {
		notBefore, perr := time.Parse(time.RFC3339, nb)
		if perr != nil || now.Add(samlClockSkew).Before(notBefore) {
			return time.Time{}, samlErrorf("assertion is not yet valid")
		}
	}

	return notAfter, checkAudience(cond, samlSPPrefix+poolID)
}

func checkAudience(cond *xnode, want string) error {
	restrictions := cond.childrenNamed(nsSAMLAssertion, "AudienceRestriction")
	if len(restrictions) == 0 {
		return samlErrorf("assertion audience is not %s", want)
	}

	for _, ar := range restrictions {
		if !slices.ContainsFunc(ar.childrenNamed(nsSAMLAssertion, "Audience"), func(a *xnode) bool {
			return a.textContent() == want
		}) {
			return samlErrorf("assertion audience is not %s", want)
		}
	}

	return nil
}
