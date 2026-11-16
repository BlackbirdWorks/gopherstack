package gcpidentitytoolkit

import (
	"encoding/base64"
	"encoding/json/v2"
	"encoding/xml"
	"net/url"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const maxAssertionBytes = 1 << 20

// idpAssertion is the identity an IdP credential asserts.
type idpAssertion struct {
	raw           map[string]any
	subject       string
	email         string
	name          string
	picture       string
	givenName     string
	familyName    string
	idToken       string
	accessToken   string
	emailVerified bool
}

func idpInvalid(detail string) *APIError { return idtk("INVALID_IDP_RESPONSE", detail) }

func claimString(m map[string]any, key string) string {
	s, _ := m[key].(string)

	return s
}

func claimBool(m map[string]any, key string) bool {
	switch v := m[key].(type) {
	case bool:
		return v
	case string:
		return v == "true"
	}

	return false
}

// decodeClaims reads the claims of an unsigned-or-signed JWT or of a bare JSON object; signatures are not verified.
func decodeClaims(tok string) (map[string]any, bool) {
	var out map[string]any

	if len(tok) > maxAssertionBytes {
		return nil, false
	}

	if strings.HasPrefix(strings.TrimSpace(tok), "{") {
		return out, json.Unmarshal([]byte(tok), &out, jsonx.V1()) == nil
	}

	parts := strings.Split(tok, ".")
	if len(parts) < 2 || len(tok) > maxAssertionBytes {
		return nil, false
	}

	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return nil, false
	}

	return out, json.Unmarshal(raw, &out, jsonx.V1()) == nil
}

func audienceMatches(claims map[string]any, want string) bool {
	switch v := claims["aud"].(type) {
	case string:
		return v == want
	case []any:
		for _, a := range v {
			if s, ok := a.(string); ok && s == want {
				return true
			}
		}

		return false
	}

	return false
}

func expired(claims map[string]any, now time.Time) bool {
	exp, ok := claims["exp"].(float64)

	return ok && float64(now.Unix()) > exp
}

func assertionFromClaims(c map[string]any, idToken, accessToken string) (*idpAssertion, error) {
	sub := claimString(c, claimSub)
	if sub == "" {
		return nil, idpInvalid("The credential carries no subject.")
	}

	return &idpAssertion{
		raw: c, subject: sub, email: claimString(c, claimEmail), emailVerified: claimBool(c, "email_verified"),
		name: claimString(c, "name"), picture: claimString(c, "picture"), givenName: claimString(c, "given_name"),
		familyName: claimString(c, "family_name"), idToken: idToken, accessToken: accessToken,
	}, nil
}

// oauthAssertion reads the id_token (or a JSON access_token) of a post body; issuer and clientID are checked
// when non-empty.
func oauthAssertion(body url.Values, issuer, clientID string, now time.Time) (*idpAssertion, error) {
	idTok, access := body.Get("id_token"), body.Get("access_token")

	src := idTok
	if src == "" {
		src = access
	}

	if src == "" {
		return nil, idpInvalid("The post body carries no id_token or access_token.")
	}

	claims, ok := decodeClaims(src)
	if !ok {
		return nil, idpInvalid("The credential is not a decodable token.")
	}

	if issuer != "" && claimString(claims, "iss") != issuer {
		return nil, idpInvalid("The credential issuer does not match the provider configuration.")
	}

	if clientID != "" && issuer != "" && !audienceMatches(claims, clientID) {
		return nil, idpInvalid("The credential audience does not match the provider client id.")
	}

	if expired(claims, now) {
		return nil, idpInvalid("The credential has expired.")
	}

	return assertionFromClaims(claims, idTok, access)
}

type samlResponse struct {
	Issuer    string `xml:"Issuer"`
	Assertion struct {
		Issuer  string `xml:"Issuer"`
		Subject struct {
			NameID string `xml:"NameID"`
		} `xml:"Subject"`
		Conditions struct {
			NotOnOrAfter string `xml:"NotOnOrAfter,attr"`
			Audience     string `xml:"AudienceRestriction>Audience"`
		} `xml:"Conditions"`
		Attributes []struct {
			Name   string   `xml:"Name,attr"`
			Values []string `xml:"AttributeValue"`
		} `xml:"AttributeStatement>Attribute"`
	} `xml:"Assertion"`
}

func samlConditions(audience, notOnOrAfter, spEntity string, now time.Time) error {
	if audience != "" && audience != spEntity {
		return idpInvalid("The SAML audience does not match the configured spEntityId.")
	}

	if notOnOrAfter != "" {
		t, err := time.Parse(time.RFC3339, notOnOrAfter)
		if err != nil || !now.Before(t) {
			return idpInvalid("The SAML assertion has expired.")
		}
	}

	return nil
}

// samlAssertion decodes a base64 SAMLResponse; the XML signature is not verified.
func samlAssertion(body url.Values, idpEntity, spEntity string, now time.Time) (*idpAssertion, error) {
	enc := body.Get("SAMLResponse")
	if enc == "" {
		return nil, idpInvalid("The post body carries no SAMLResponse.")
	}

	raw, err := base64.StdEncoding.DecodeString(enc)
	if err != nil || len(raw) > maxAssertionBytes {
		return nil, idpInvalid("The SAMLResponse is not valid base64.")
	}

	var resp samlResponse
	if err = xml.Unmarshal(raw, &resp); err != nil {
		return nil, idpInvalid("The SAMLResponse is not valid XML.")
	}

	issuer := resp.Assertion.Issuer
	if issuer == "" {
		issuer = resp.Issuer
	}

	if issuer != idpEntity {
		return nil, idpInvalid("The SAML issuer does not match the configured idpEntityId.")
	}

	cond := resp.Assertion.Conditions
	if err = samlConditions(cond.Audience, cond.NotOnOrAfter, spEntity, now); err != nil {
		return nil, err
	}

	claims := map[string]any{claimSub: resp.Assertion.Subject.NameID}

	for _, a := range resp.Assertion.Attributes {
		if len(a.Values) > 0 {
			claims[a.Name] = a.Values[0]
		}
	}

	if _, ok := claims[claimEmail]; !ok && strings.Contains(resp.Assertion.Subject.NameID, "@") {
		claims[claimEmail] = resp.Assertion.Subject.NameID
	}

	claims["email_verified"] = true

	return assertionFromClaims(claims, "", "")
}
