package cognitoidp_test

import (
	"bytes"
	"compress/flate"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// rawQueryParts splits a raw query without decoding, keeping each value exactly as signed.
func rawQueryParts(raw string) map[string]string {
	out := map[string]string{}

	for kv := range strings.SplitSeq(raw, "&") {
		k, v, _ := strings.Cut(kv, "=")
		out[k] = v
	}

	return out
}

func verifyRedirectSig(t *testing.T, rawQuery string, pub *rsa.PublicKey) {
	t.Helper()

	p := rawQueryParts(rawQuery)
	signed := "SAMLRequest=" + p["SAMLRequest"] + "&RelayState=" + p["RelayState"] + "&SigAlg=" + p["SigAlg"]
	sigB64, err := url.QueryUnescape(p["Signature"])
	require.NoError(t, err)

	sig, err := base64.StdEncoding.DecodeString(sigB64)
	require.NoError(t, err)

	sum := sha256.Sum256([]byte(signed))
	require.NoError(t, rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], sig))

	alg, err := url.QueryUnescape(p["SigAlg"])
	require.NoError(t, err)
	assert.Equal(t, "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256", alg)
}

func TestSAMLSignedAuthnRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tweak      func(*samlIDP)
		extra      map[string]string
		name       string
		wantSigned bool
	}{
		{name: "redirect unsigned by default"},
		{
			name:       "redirect signed by provider setting",
			extra:      map[string]string{"RequestSigningAlgorithm": "rsa-sha256"},
			wantSigned: true,
		},
		{name: "redirect signed when idp wants it", tweak: func(i *samlIDP) { i.wantSigned = true }, wantSigned: true},
		{
			name: "post signed", tweak: func(i *samlIDP) { i.ssoPost = true },
			extra: map[string]string{"RequestSigningAlgorithm": "rsa-sha256"}, wantSigned: true,
		},
		{name: "post unsigned by default", tweak: func(i *samlIDP) { i.ssoPost = true }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMLEnvWith(t, false, tt.tweak, tt.extra)
			spKey, _, err := cognitoidp.PoolSAMLKeyForTest(env.backend, env.poolID)
			require.NoError(t, err)

			q := authorizeQuery(env.clientID, "")
			q.Set("identity_provider", fedProvider)
			resp, body := env.get(t, "/oauth2/authorize?"+q.Encode(), nil)

			if env.idp.ssoPost {
				require.Equal(t, http.StatusOK, resp.StatusCode)

				m := regexp.MustCompile(`name="SAMLRequest" value="([^"]+)"`).FindStringSubmatch(string(body))
				require.Len(t, m, 2)

				raw, derr := base64.StdEncoding.DecodeString(html.UnescapeString(m[1]))
				require.NoError(t, derr)

				if tt.wantSigned {
					require.NoError(t, cognitoidp.VerifyRootSignatureForTest(raw, &spKey.PublicKey))
					assert.Contains(t, string(raw), "AuthnRequest")
				} else {
					assert.NotContains(t, string(raw), "Signature")
				}

				return
			}

			require.Equal(t, http.StatusFound, resp.StatusCode)

			loc, perr := url.Parse(resp.Header.Get("Location"))
			require.NoError(t, perr)

			if tt.wantSigned {
				verifyRedirectSig(t, loc.RawQuery, &spKey.PublicKey)

				return
			}

			assert.NotContains(t, loc.RawQuery, "Signature=")
		})
	}
}

func inflate(t *testing.T, b64Deflated string) string {
	t.Helper()

	raw, err := base64.StdEncoding.DecodeString(b64Deflated)
	require.NoError(t, err)

	out, err := io.ReadAll(flate.NewReader(bytes.NewReader(raw)))
	require.NoError(t, err)

	return string(out)
}

func TestSAMLSPMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extra      map[string]string
		name       string
		wantSigned string
	}{
		{name: "unsigned requests", wantSigned: "false"},
		{
			name:       "signed requests",
			extra:      map[string]string{"RequestSigningAlgorithm": "rsa-sha256"},
			wantSigned: "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMLEnvWith(t, false, nil, tt.extra)
			_, certDER, err := cognitoidp.PoolSAMLKeyForTest(env.backend, env.poolID)
			require.NoError(t, err)

			resp, body := env.get(t, "/"+env.poolID+"/saml2/metadata", nil)
			require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
			assert.Equal(t, "application/samlmetadata+xml", resp.Header.Get("Content-Type"))

			var md struct {
				EntityID string `xml:"entityID,attr"`
				SP       struct {
					Signed string `xml:"AuthnRequestsSigned,attr"`
					Keys   []struct {
						Use  string `xml:"use,attr"`
						Cert string `xml:"KeyInfo>X509Data>X509Certificate"`
					} `xml:"KeyDescriptor"`
					ACS []struct {
						Location string `xml:"Location,attr"`
					} `xml:"AssertionConsumerService"`
					SLO []struct {
						Location string `xml:"Location,attr"`
					} `xml:"SingleLogoutService"`
				} `xml:"SPSSODescriptor"`
			}

			require.NoError(t, xml.Unmarshal(body, &md))
			assert.Equal(t, "urn:amazon:cognito:sp:"+env.poolID, md.EntityID)
			assert.Equal(t, tt.wantSigned, md.SP.Signed)
			require.Len(t, md.SP.Keys, 2)

			uses := []string{md.SP.Keys[0].Use, md.SP.Keys[1].Use}
			assert.ElementsMatch(t, []string{"signing", "encryption"}, uses)

			for _, k := range md.SP.Keys {
				assert.Equal(t, base64.StdEncoding.EncodeToString(certDER), k.Cert)
			}

			require.Len(t, md.SP.ACS, 1)
			assert.Equal(t, env.acs(), md.SP.ACS[0].Location)
			require.NotEmpty(t, md.SP.SLO)
			assert.Equal(t, env.srv.URL+"/saml2/logout", md.SP.SLO[0].Location)
		})
	}

	t.Run("unknown pool", func(t *testing.T) {
		t.Parallel()

		env := newSAMLEnv(t, false)
		resp, _ := env.get(t, "/us-east-1_nope/saml2/metadata", nil)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}
