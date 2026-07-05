package sts

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
)

// clockSkewLeeway is the tolerance applied to JWT/SAML temporal claims so that
// small clock differences between the IdP and the emulator do not reject
// otherwise-valid tokens. It mirrors the leeway most AWS SDKs and IdPs apply.
const clockSkewLeeway = 60 * time.Second

// jwtFullPartCount is the number of segments in a complete JWT (header.payload.signature).
const jwtFullPartCount = 3

// validateWebIdentityToken validates the self-consistent temporal and structural
// claims of an OIDC web-identity JWT without verifying its cryptographic
// signature (which would require the external IdP's keys). It rejects:
//   - structurally malformed JWTs (three non-empty segments whose payload cannot
//     be decoded) with ErrInvalidIdentityToken;
//   - tokens whose "exp" is in the past with ErrExpiredToken;
//   - tokens whose "nbf" is in the future (not yet valid) or whose "iat" is in
//     the future (issued later than now) with ErrInvalidIdentityToken.
//
// Opaque, non-JWT tokens (no '.' separators) are accepted, matching the mock's
// long-standing permissive behaviour for callers that pass simple test tokens.
func validateWebIdentityToken(token string) error {
	if err := checkJWTWellFormed(token); err != nil {
		return err
	}

	claims := parseJWTPayloadClaims(token)
	if claims == nil {
		// Not JWT-shaped (or a fixture opaque token): nothing to validate.
		return nil
	}

	if err := validateJWTNotExpired(token); err != nil {
		return err
	}

	now := time.Now().UTC()

	if nbf, ok, err := jwtTimeClaim(claims, jwtClaimNbf); err != nil {
		return err
	} else if ok && now.Add(clockSkewLeeway).Before(nbf) {
		return fmt.Errorf(
			"%w: JWT nbf claim %s is in the future",
			ErrInvalidIdentityToken, nbf.Format(time.RFC3339),
		)
	}

	if iat, ok, err := jwtTimeClaim(claims, jwtClaimIat); err != nil {
		return err
	} else if ok && iat.After(now.Add(clockSkewLeeway)) {
		return fmt.Errorf(
			"%w: JWT iat claim %s is in the future",
			ErrInvalidIdentityToken, iat.Format(time.RFC3339),
		)
	}

	return nil
}

// checkJWTWellFormed rejects tokens that clearly intend to be JWTs (exactly three
// non-empty dot-separated segments) but whose payload segment cannot be decoded
// into JSON claims. Tokens with fewer segments are treated as opaque and pass.
func checkJWTWellFormed(token string) error {
	parts := strings.Split(token, ".")
	if len(parts) != jwtFullPartCount {
		return nil
	}

	if slices.Contains(parts, "") {
		return nil
	}

	if parseJWTPayloadClaims(token) == nil {
		return fmt.Errorf("%w: JWT payload could not be decoded", ErrInvalidIdentityToken)
	}

	return nil
}

// validateTradeInTokenExpiry checks the self-consistent "exp" claim of a
// JWT-shaped GetDelegatedAccessToken TradeInToken, without verifying its
// cryptographic signature (the external issuer's signing keys are not
// available to the emulator — the same constraint that applies to
// WebIdentityToken and SAMLAssertion validation above). Opaque, non-JWT tokens
// (fixtures without JWT structure) are accepted unchanged, matching the mock's
// existing permissive handling of such test tokens. A JWT-shaped token whose
// exp claim has already passed is rejected with ErrExpiredTradeInToken, which
// the handler maps to the real AWS ExpiredTradeInTokenException error code.
func validateTradeInTokenExpiry(token string) error {
	parts := strings.Split(token, ".")
	if len(parts) != jwtFullPartCount || slices.Contains(parts, "") {
		// Not JWT-shaped: nothing to validate.
		return nil
	}

	claims := parseJWTPayloadClaims(token)
	if claims == nil {
		return nil
	}

	exp, ok, err := jwtTimeClaim(claims, jwtClaimExp)
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}

	if time.Now().UTC().After(exp) {
		return fmt.Errorf(
			"%w: TradeInToken exp claim is %s",
			ErrExpiredTradeInToken, exp.Format(time.RFC3339),
		)
	}

	return nil
}

// jwtTimeClaim extracts a NumericDate JWT claim (seconds since the Unix epoch)
// as a UTC time. The second result reports presence; an error is returned only
// when the claim is present but not a parseable number.
func jwtTimeClaim(claims map[string]any, key string) (time.Time, bool, error) {
	v, ok := claims[key]
	if !ok {
		return time.Time{}, false, nil
	}

	switch n := v.(type) {
	case float64:
		return time.Unix(int64(n), 0).UTC(), true, nil
	case json.Number:
		secs, err := n.Int64()
		if err != nil {
			return time.Time{}, false, fmt.Errorf(
				"%w: JWT %s claim is not a valid timestamp", ErrInvalidIdentityToken, key,
			)
		}

		return time.Unix(secs, 0).UTC(), true, nil
	default:
		return time.Time{}, false, fmt.Errorf(
			"%w: JWT %s claim is not a numeric date", ErrInvalidIdentityToken, key,
		)
	}
}

// samlConditionWindow holds the temporal window extracted from a SAML assertion.
type samlConditionWindow struct {
	notBefore    time.Time
	notOnOrAfter time.Time
	hasBefore    bool
	hasAfter     bool
}

// validateSAMLTemporalConditions decodes a base64-encoded SAML assertion and
// enforces the temporal validity window declared in its <Conditions> element
// (and any <SubjectConfirmationData> NotOnOrAfter). Assertions whose window has
// not yet begun or has already elapsed are rejected with ErrInvalidSAMLAssertion,
// which the handler maps to the AWS InvalidIdentityToken error. Assertions that
// declare no temporal window (e.g. minimal test fixtures) are accepted.
func validateSAMLTemporalConditions(assertion string) error {
	decoded, ok := decodeSAMLAssertion(assertion)
	if !ok {
		// Base64 validity is checked separately by validateSAMLAssertion.
		return nil
	}

	window, err := extractSAMLWindow(decoded)
	if err != nil {
		return err
	}

	now := time.Now().UTC()

	if window.hasBefore && now.Add(clockSkewLeeway).Before(window.notBefore) {
		return fmt.Errorf(
			"%w: assertion NotBefore %s is in the future",
			ErrInvalidSAMLAssertion, window.notBefore.Format(time.RFC3339),
		)
	}

	if window.hasAfter && !now.Add(-clockSkewLeeway).Before(window.notOnOrAfter) {
		return fmt.Errorf(
			"%w: assertion NotOnOrAfter %s has passed",
			ErrInvalidSAMLAssertion, window.notOnOrAfter.Format(time.RFC3339),
		)
	}

	return nil
}

// decodeSAMLAssertion decodes a standard- or URL-base64-encoded SAML assertion.
func decodeSAMLAssertion(assertion string) ([]byte, bool) {
	if decoded, err := base64.StdEncoding.DecodeString(assertion); err == nil {
		return decoded, true
	}

	if decoded, err := base64.URLEncoding.DecodeString(assertion); err == nil {
		return decoded, true
	}

	return nil, false
}

// extractSAMLWindow scans a decoded SAML assertion for the temporal attributes
// on <Conditions> and <SubjectConfirmationData>, ignoring XML namespaces. The
// tightest window is retained: the latest NotBefore and the earliest NotOnOrAfter.
func extractSAMLWindow(decoded []byte) (samlConditionWindow, error) {
	var window samlConditionWindow

	dec := xml.NewDecoder(strings.NewReader(string(decoded)))

	for {
		tok, err := dec.Token()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				// Not well-formed XML: shape is validated elsewhere; do not
				// enforce a temporal window on unparseable content.
				window = samlConditionWindow{}
			}

			break
		}

		start, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}

		local := start.Name.Local
		if local != "Conditions" && local != "SubjectConfirmationData" {
			continue
		}

		if applyErr := applySAMLAttrs(&window, start.Attr); applyErr != nil {
			return samlConditionWindow{}, applyErr
		}
	}

	return window, nil
}

// applySAMLAttrs folds the NotBefore/NotOnOrAfter attributes of one element into
// the running window, keeping the tightest bounds.
func applySAMLAttrs(window *samlConditionWindow, attrs []xml.Attr) error {
	for _, attr := range attrs {
		switch attr.Name.Local {
		case "NotBefore":
			t, err := parseSAMLTime(attr.Value)
			if err != nil {
				return err
			}

			if !window.hasBefore || t.After(window.notBefore) {
				window.notBefore = t
				window.hasBefore = true
			}
		case "NotOnOrAfter":
			t, err := parseSAMLTime(attr.Value)
			if err != nil {
				return err
			}

			if !window.hasAfter || t.Before(window.notOnOrAfter) {
				window.notOnOrAfter = t
				window.hasAfter = true
			}
		}
	}

	return nil
}

// parseSAMLTime parses a SAML timestamp (RFC 3339 / ISO 8601 with 'Z' or offset).
func parseSAMLTime(value string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05"} {
		if t, err := time.Parse(layout, value); err == nil {
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf(
		"%w: unparseable SAML timestamp %q", ErrInvalidSAMLAssertion, value,
	)
}
