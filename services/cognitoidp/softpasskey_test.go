package cognitoidp_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

const (
	authDataUP = 0x01
	authDataUV = 0x04
	authDataAT = 0x40
	coseKeyLen = 32
)

// softPasskey is a software WebAuthn authenticator: an ES256 credential that answers the
// registration and assertion ceremonies the way a browser's navigator.credentials would.
type softPasskey struct {
	key        *ecdsa.PrivateKey
	origin     string
	credID     []byte
	userHandle []byte
	signCount  uint32
	verified   bool
}

func newSoftPasskey(t *testing.T) *softPasskey {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	id := make([]byte, 32)
	_, err = rand.Read(id)
	require.NoError(t, err)

	return &softPasskey{key: key, credID: id, verified: true}
}

func b64u(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func rpHash(rpID string) []byte {
	h := sha256.Sum256([]byte(rpID))

	return h[:]
}

func (p *softPasskey) clientData(t *testing.T, typ, challenge, rpID string) []byte {
	t.Helper()

	origin := p.origin
	if origin == "" {
		origin = "https://" + rpID
	}

	raw, err := json.Marshal(map[string]any{
		"type": typ, "challenge": challenge, "origin": origin, "crossOrigin": false,
	})
	require.NoError(t, err)

	return raw
}

func (p *softPasskey) flags(extra byte) byte {
	f := byte(authDataUP) | extra
	if p.verified {
		f |= authDataUV
	}

	return f
}

// register answers CredentialCreationOptions with a RegistrationResponseJSON ("none" attestation).
func (p *softPasskey) register(t *testing.T, opts map[string]any) map[string]any {
	t.Helper()

	rpID := opts["rp"].(map[string]any)["id"].(string)
	challenge := opts["challenge"].(string)

	handle, err := base64.RawURLEncoding.DecodeString(opts["user"].(map[string]any)["id"].(string))
	require.NoError(t, err)

	p.userHandle = handle

	pub, err := p.key.PublicKey.Bytes()
	require.NoError(t, err)

	x, y := pub[1:1+coseKeyLen], pub[1+coseKeyLen:]

	cose, err := cbor.Marshal(map[int]any{1: 2, 3: -7, -1: 1, -2: x, -3: y})
	require.NoError(t, err)

	authData := append([]byte{}, rpHash(rpID)...)
	authData = append(authData, p.flags(authDataAT))
	authData = binary.BigEndian.AppendUint32(authData, p.signCount)
	authData = append(authData, make([]byte, 16)...)
	authData = binary.BigEndian.AppendUint16(authData, uint16(len(p.credID)))
	authData = append(authData, p.credID...)
	authData = append(authData, cose...)

	attObj, err := cbor.Marshal(map[string]any{"fmt": "none", "attStmt": map[string]any{}, "authData": authData})
	require.NoError(t, err)

	return map[string]any{
		"id": b64u(p.credID), "rawId": b64u(p.credID), "type": "public-key",
		"authenticatorAttachment": "platform",
		"response": map[string]any{
			"clientDataJSON":    b64u(p.clientData(t, "webauthn.create", challenge, rpID)),
			"attestationObject": b64u(attObj),
			"transports":        []string{"internal"},
		},
		"clientExtensionResults": map[string]any{},
	}
}

// assert answers CREDENTIAL_REQUEST_OPTIONS with an AuthenticationResponseJSON string.
func (p *softPasskey) assert(t *testing.T, requestOptions string) string {
	t.Helper()

	var opts struct {
		Challenge string `json:"challenge"`
		RPID      string `json:"rpId"`
	}
	require.NoError(t, json.Unmarshal([]byte(requestOptions), &opts))

	p.signCount++

	authData := append([]byte{}, rpHash(opts.RPID)...)
	authData = append(authData, p.flags(0))
	authData = binary.BigEndian.AppendUint32(authData, p.signCount)

	clientData := p.clientData(t, "webauthn.get", opts.Challenge, opts.RPID)
	cdHash := sha256.Sum256(clientData)

	digest := sha256.Sum256(append(append([]byte{}, authData...), cdHash[:]...))
	sig, err := ecdsa.SignASN1(rand.Reader, p.key, digest[:])
	require.NoError(t, err)

	out, err := json.Marshal(map[string]any{
		"id": b64u(p.credID), "rawId": b64u(p.credID), "type": "public-key",
		"response": map[string]any{
			"authenticatorData": b64u(authData), "clientDataJSON": b64u(clientData),
			"signature": b64u(sig), "userHandle": b64u(p.userHandle),
		},
		"clientExtensionResults": map[string]any{},
	})
	require.NoError(t, err)

	return string(out)
}

// enablePasskeys configures the pool's passkey relying party for backend-level tests.
func enablePasskeys(t *testing.T, b *cognitoidp.InMemoryBackend, poolID string) {
	t.Helper()

	require.NoError(t, b.SetUserPoolMfaConfigFull(poolID, cognitoidp.UserPoolMfaFullConfig{
		WebAuthnConfiguration: map[string]any{"RelyingPartyId": passkeyRPID},
	}))
}

// registerPasskey runs the full registration ceremony against the backend for accessToken's user.
func registerPasskey(t *testing.T, b *cognitoidp.InMemoryBackend, accessToken string) *softPasskey {
	t.Helper()

	opts, err := b.StartWebAuthnRegistration(accessToken)
	require.NoError(t, err)

	pk := newSoftPasskey(t)
	_, err = b.CompleteWebAuthnRegistration(accessToken, pk.register(t, opts))
	require.NoError(t, err)

	return pk
}

// registerPasskeyHandler is registerPasskey over the JSON handler.
func registerPasskeyHandler(t *testing.T, h *cognitoidp.Handler, accessToken string) *softPasskey {
	t.Helper()

	rec := doCognitoRequest(t, h, "StartWebAuthnRegistration", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var start struct {
		Options map[string]any `json:"CredentialCreationOptions"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &start))

	pk := newSoftPasskey(t)
	rec = doCognitoRequest(t, h, "CompleteWebAuthnRegistration", map[string]any{
		"AccessToken": accessToken, "Credential": pk.register(t, start.Options),
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	return pk
}
