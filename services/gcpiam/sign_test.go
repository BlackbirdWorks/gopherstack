package gcpiam_test

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func systemPublicKey(t *testing.T, e *env, sa *adminpb.ServiceAccount, keyID string) []byte {
	t.Helper()

	got, err := e.client.GetServiceAccountKey(t.Context(), &adminpb.GetServiceAccountKeyRequest{
		Name: sa.GetName() + "/keys/" + keyID, PublicKeyType: adminpb.ServiceAccountPublicKeyType_TYPE_X509_PEM_FILE,
	})
	require.NoError(t, err)

	return got.GetPublicKeyData()
}

func TestSignBlobVerifiesWithSystemKey(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sa := e.account(t, "sign-sa")
	data := []byte("payload to sign")

	keyID, sig, err := e.signBlob(t, sa.GetName(), data)
	require.NoError(t, err)
	assert.Regexp(t, `^[0-9a-f]{40}$`, keyID)

	pub := parsePublicKey(t, &adminpb.ServiceAccountKey{PublicKeyData: systemPublicKey(t, e, sa, keyID)})
	require.NoError(t, verifySHA256(pub, data, sig))
	require.Error(t, verifySHA256(pub, []byte("tampered"), sig))

	again, _, err := e.signBlob(t, sa.GetName(), data)
	require.NoError(t, err)
	assert.Equal(t, keyID, again, "the system-managed key is stable")
}

func TestSignJwt(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sa := e.account(t, "jwt-sa")
	now := e.clk.now().Unix()

	keyID, token, err := e.signJwt(t, sa.GetName(), `{"sub":"someone","iat":1}`)
	require.NoError(t, err)

	parts := strings.Split(token, ".")
	require.Len(t, parts, 3)

	enc := base64.RawURLEncoding
	hdr, err := enc.DecodeString(parts[0])
	require.NoError(t, err)

	var header map[string]string
	require.NoError(t, json.Unmarshal(hdr, &header))
	assert.Equal(t, map[string]string{"alg": "RS256", "typ": "JWT", "kid": keyID}, header)

	body, err := enc.DecodeString(parts[1])
	require.NoError(t, err)

	var claims map[string]any
	require.NoError(t, json.Unmarshal(body, &claims))
	assert.Equal(t, "someone", claims["sub"])
	assert.InDelta(t, float64(now+3600), claims["exp"], 0, "exp defaults to one hour")

	sig, err := enc.DecodeString(parts[2])
	require.NoError(t, err)

	pub := parsePublicKey(t, &adminpb.ServiceAccountKey{PublicKeyData: systemPublicKey(t, e, sa, keyID)})
	require.NoError(t, verifySHA256(pub, []byte(parts[0]+"."+parts[1]), sig))
}

func TestSignErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sa := e.account(t, "sign-err-sa")
	now := e.clk.now().Unix()

	jwt := func(payload string) error {
		_, _, err := e.signJwt(t, sa.GetName(), payload)

		return err
	}

	requireCode(t, codes.InvalidArgument, jwt("not json"))
	requireCode(t, codes.InvalidArgument, jwt("[1]"))
	requireCode(t, codes.InvalidArgument, jwt(`{"exp":"soon"}`))
	requireCode(t, codes.InvalidArgument, jwt(`{"exp":`+itoa(now-10)+`}`))
	requireCode(t, codes.InvalidArgument, jwt(`{"exp":`+itoa(now+13*3600)+`}`))
	require.NoError(t, jwt(`{"exp":`+itoa(now+11*3600)+`}`))

	require.NoError(
		t,
		e.client.DisableServiceAccount(t.Context(), &adminpb.DisableServiceAccountRequest{Name: sa.GetName()}),
	)
	requireCode(t, codes.FailedPrecondition, jwt(`{}`))

	_, _, err := e.signBlob(t, sa.GetName(), []byte("x"))
	requireCode(t, codes.FailedPrecondition, err)

	_, _, err = e.signBlob(t, project+"/serviceAccounts/ghost@test-project.iam.gserviceaccount.com", nil)
	requireCode(t, codes.NotFound, err)
}

//nolint:staticcheck // SignBlob is deprecated upstream but part of the served API
func (e *env) signBlob(t *testing.T, name string, data []byte) (string, []byte, error) {
	t.Helper()

	resp, err := e.client.SignBlob(t.Context(), &adminpb.SignBlobRequest{Name: name, BytesToSign: data})
	if err != nil {
		return "", nil, err
	}

	return resp.GetKeyId(), resp.GetSignature(), nil
}

//nolint:staticcheck // SignJwt is deprecated upstream but part of the served API
func (e *env) signJwt(t *testing.T, name, payload string) (string, string, error) {
	t.Helper()

	resp, err := e.client.SignJwt(t.Context(), &adminpb.SignJwtRequest{Name: name, Payload: payload})
	if err != nil {
		return "", "", err
	}

	return resp.GetKeyId(), resp.GetSignedJwt(), nil
}

func itoa(n int64) string {
	b, _ := json.Marshal(n)

	return string(b)
}
