package ocivault_test

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/keymanagement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pkcs8(t *testing.T, k any) []byte {
	t.Helper()

	der, err := x509.MarshalPKCS8PrivateKey(k)
	require.NoError(t, err)

	return der
}

func (ce *cryptoEnv) wrappingKey(t *testing.T) *rsa.PublicKey {
	t.Helper()

	r, err := ce.m.GetWrappingKey(t.Context(), keymanagement.GetWrappingKeyRequest{})
	require.NoError(t, err)
	assert.Equal(t, *ce.v.WrappingkeyId, *r.Id)
	assert.Equal(t, *ce.v.Id, *r.VaultId)
	assert.Equal(t, keymanagement.WrappingKeyLifecycleStateEnabled, r.LifecycleState)

	pub, ok := pubOf(t, *r.PublicKey).(*rsa.PublicKey)
	require.True(t, ok)

	return pub
}

func (ce *cryptoEnv) importKey(
	t *testing.T,
	ks *keymanagement.KeyShape,
	wrapped []byte,
	alg keymanagement.WrappedImportKeyWrappingAlgorithmEnum,
) (keymanagement.Key, error) {
	t.Helper()

	r, err := ce.m.ImportKey(t.Context(), keymanagement.ImportKeyRequest{
		CompartmentId: new(testCompartment), DisplayName: new("imported"), KeyShape: ks,
		WrappedImportKey: &keymanagement.WrappedImportKey{KeyMaterial: new(b64(wrapped)), WrappingAlgorithm: alg}})

	return r.Key, err
}

func TestImportKeyMaterial(t *testing.T) {
	t.Parallel()

	ce := newCryptoEnv(t)
	wk := ce.wrappingKey(t)

	rsaPriv, kerr := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, kerr)

	ecPriv, kerr := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	require.NoError(t, kerr)

	aesMat := make([]byte, 32)
	_, _ = rand.Read(aesMat)

	t.Run("aes via oaep aes kwp", func(t *testing.T) {
		t.Parallel()

		wrapped, cerr := wrapRSAAES(wk, aesMat)
		require.NoError(t, cerr)

		key, cerr := ce.importKey(t, aes256(), wrapped, keymanagement.WrappedImportKeyWrappingAlgorithmAesSha256)
		require.NoError(t, cerr)

		ver, cerr := ce.m.GetKeyVersion(
			t.Context(),
			keymanagement.GetKeyVersionRequest{KeyId: key.Id, KeyVersionId: key.CurrentKeyVersion},
		)
		require.NoError(t, cerr)
		assert.Equal(t, keymanagement.KeyVersionOriginExternal, ver.Origin)

		enc, cerr := ce.encrypt(t, key, "imported secret", nil, "")
		require.NoError(t, cerr)

		assert.Equal(t, "imported secret", openSealed(t, aesMat, *enc.Ciphertext))
	})

	t.Run("aes via oaep sha256", func(t *testing.T) {
		t.Parallel()

		wrapped, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, wk, aesMat, nil)
		require.NoError(t, err)

		_, err = ce.importKey(t, aes256(), wrapped, keymanagement.WrappedImportKeyWrappingAlgorithmSha256)
		require.NoError(t, err)
	})

	t.Run("rsa signs like the original", func(t *testing.T) {
		t.Parallel()

		wrapped, err := wrapRSAAES(wk, pkcs8(t, rsaPriv))
		require.NoError(t, err)

		key, err := ce.importKey(
			t,
			shape(keymanagement.KeyShapeAlgorithmRsa, 256, ""),
			wrapped,
			keymanagement.WrappedImportKeyWrappingAlgorithmAesSha256,
		)
		require.NoError(t, err)

		msg := []byte("hello")
		digest := sha256.Sum256(msg)

		sig, err := rsa.SignPKCS1v15(rand.Reader, rsaPriv, crypto.SHA256, digest[:])
		require.NoError(t, err)
		assert.True(t, ce.verify(t, key, *key.CurrentKeyVersion, "SHA_256_RSA_PKCS1_V1_5", "RAW", msg, sig))

		ver, err := ce.m.GetKeyVersion(
			t.Context(),
			keymanagement.GetKeyVersionRequest{KeyId: key.Id, KeyVersionId: key.CurrentKeyVersion},
		)
		require.NoError(t, err)
		pub, ok := pubOf(t, *ver.PublicKey).(*rsa.PublicKey)
		require.True(t, ok)
		assert.True(t, pub.Equal(&rsaPriv.PublicKey))
	})

	t.Run("ecdsa import", func(t *testing.T) {
		t.Parallel()

		wrapped, err := wrapRSAAES(wk, pkcs8(t, ecPriv))
		require.NoError(t, err)

		key, err := ce.importKey(
			t,
			shape(keymanagement.KeyShapeAlgorithmEcdsa, 48, keymanagement.KeyShapeCurveIdP384),
			wrapped,
			keymanagement.WrappedImportKeyWrappingAlgorithmAesSha256,
		)
		require.NoError(t, err)

		msg := []byte("hello")
		d := sha256.Sum256(msg)
		sig, err := ecdsa.SignASN1(rand.Reader, ecPriv, d[:])
		require.NoError(t, err)
		assert.True(t, ce.verify(t, key, *key.CurrentKeyVersion, "ECDSA_SHA_256", "RAW", msg, sig))
	})
}

func TestImportKeyRejects(t *testing.T) {
	t.Parallel()

	ce := newCryptoEnv(t)
	wk := ce.wrappingKey(t)

	short := make([]byte, 16)
	wrappedShort, kerr := wrapRSAAES(wk, short)
	require.NoError(t, kerr)

	other, kerr := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, kerr)

	wrappedForeign, kerr := wrapRSAAES(&other.PublicKey, make([]byte, 32))
	require.NoError(t, kerr)

	aesAlg := keymanagement.WrappedImportKeyWrappingAlgorithmAesSha256
	rsaShape := shape(keymanagement.KeyShapeAlgorithmRsa, 256, "")

	tests := []struct {
		shape   *keymanagement.KeyShape
		name    string
		alg     keymanagement.WrappedImportKeyWrappingAlgorithmEnum
		wrapped []byte
	}{
		{name: "length mismatch", shape: aes256(), wrapped: wrappedShort, alg: aesAlg},
		{name: "foreign wrapping key", shape: aes256(), wrapped: wrappedForeign, alg: aesAlg},
		{name: "garbage", shape: aes256(), wrapped: []byte("garbage-garbage"), alg: aesAlg},
		{name: "rsa shape with aes bytes", shape: rsaShape, wrapped: wrappedShort, alg: aesAlg},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, cerr := ce.importKey(t, tt.shape, tt.wrapped, tt.alg)
			assert.Equal(t, "InvalidParameter", svcErr(t, cerr).GetCode())
		})
	}
}

func TestImportWithoutWrappingKeyIssued(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	v := e.mkVault(t, "nowrap")
	ce := &cryptoEnv{m: e.mgmt(t, v), c: e.crypto(t, v), e: e, v: v}

	_, err := ce.importKey(t, aes256(), []byte("x"), keymanagement.WrappedImportKeyWrappingAlgorithmSha256)
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())
}

func TestImportKeyVersion(t *testing.T) {
	t.Parallel()

	ce := newCryptoEnv(t)
	wk := ce.wrappingKey(t)

	mat := make([]byte, 32)
	_, _ = rand.Read(mat)

	wrapped, err := wrapRSAAES(wk, mat)
	require.NoError(t, err)

	key, err := ce.importKey(t, aes256(), wrapped, keymanagement.WrappedImportKeyWrappingAlgorithmAesSha256)
	require.NoError(t, err)

	mat2 := make([]byte, 32)
	_, _ = rand.Read(mat2)

	wrapped2, err := wrapRSAAES(wk, mat2)
	require.NoError(t, err)

	v2, err := ce.m.ImportKeyVersion(t.Context(), keymanagement.ImportKeyVersionRequest{
		KeyId: key.Id,
		WrappedImportKey: &keymanagement.WrappedImportKey{
			KeyMaterial: new(
				b64(wrapped2),
			),
			WrappingAlgorithm: keymanagement.WrappedImportKeyWrappingAlgorithmAesSha256,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, keymanagement.KeyVersionOriginExternal, v2.Origin)

	enc, err := ce.encrypt(t, key, "v2", nil, "")
	require.NoError(t, err)
	assert.Equal(t, *v2.Id, *enc.KeyVersionId)
}

func unwrapExported(t *testing.T, priv *rsa.PrivateKey, alg string, enc string) []byte {
	t.Helper()

	blob := unb64(t, enc)

	var (
		out []byte
		err error
	)

	if alg == wrapAlgAES {
		out, err = unwrapRSAAES(priv, blob)
	} else {
		out, err = rsa.DecryptOAEP(sha256.New(), nil, priv, blob, nil)
	}

	require.NoError(t, err)

	return out
}

func TestExportKey(t *testing.T) {
	t.Parallel()

	ce := newCryptoEnv(t)

	wrapPriv, kerr := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, kerr)

	wrapPub := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: mustPKIX(t, &wrapPriv.PublicKey)}))

	type exportAlg = keymanagement.ExportKeyDetailsAlgorithmEnum

	export := func(key keymanagement.Key, alg exportAlg, pub string) (keymanagement.ExportedKeyData, error) {
		r, cerr := ce.c.ExportKey(t.Context(), keymanagement.ExportKeyRequest{
			KeyId: key.Id, Algorithm: alg, PublicKey: new(pub)})

		return r.ExportedKeyData, cerr
	}

	t.Run("rsa private key exports and signs", func(t *testing.T) {
		t.Parallel()

		key := ce.e.mkKey(
			t,
			ce.m,
			"exp",
			shape(keymanagement.KeyShapeAlgorithmRsa, 256, ""),
			keymanagement.CreateKeyDetailsProtectionModeSoftware,
		)
		exp, err := export(key, keymanagement.ExportKeyDetailsAlgorithmAesSha256, wrapPub)
		require.NoError(t, err)
		assert.Equal(t, *exp.KeyVersionId, *key.CurrentKeyVersion)
		assert.Equal(t, keymanagement.ExportedKeyDataAlgorithmAesSha256, exp.Algorithm)

		mat := unwrapExported(t, wrapPriv, wrapAlgAES, *exp.EncryptedKey)
		parsed, err := x509.ParsePKCS8PrivateKey(mat)
		require.NoError(t, err)

		rk, ok := parsed.(*rsa.PrivateKey)
		require.True(t, ok)

		msg := []byte("exported")
		digest := sha256.Sum256(msg)
		sig, err := rsa.SignPKCS1v15(rand.Reader, rk, crypto.SHA256, digest[:])
		require.NoError(t, err)
		assert.True(t, ce.verify(t, key, *key.CurrentKeyVersion, "SHA_256_RSA_PKCS1_V1_5", "RAW", msg, sig))
	})

	t.Run("aes with plain oaep", func(t *testing.T) {
		t.Parallel()

		key := ce.e.mkKey(t, ce.m, "exp-aes", aes256(), keymanagement.CreateKeyDetailsProtectionModeSoftware)
		enc, err := ce.encrypt(t, key, "round trip", nil, "")
		require.NoError(t, err)

		exp, err := export(key, keymanagement.ExportKeyDetailsAlgorithmSha256, wrapPub)
		require.NoError(t, err)

		mat := unwrapExported(t, wrapPriv, wrapAlgSHA256, *exp.EncryptedKey)
		assert.Len(t, mat, 32)

		assert.Equal(t, "round trip", openSealed(t, mat, *enc.Ciphertext))
	})

	t.Run("rejections", func(t *testing.T) {
		t.Parallel()

		hsm := ce.e.mkKey(t, ce.m, "hsm", aes256(), keymanagement.CreateKeyDetailsProtectionModeHsm)
		soft := ce.e.mkKey(t, ce.m, "soft", aes256(), keymanagement.CreateKeyDetailsProtectionModeSoftware)

		_, err := export(hsm, keymanagement.ExportKeyDetailsAlgorithmSha256, wrapPub)
		assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

		_, err = export(soft, keymanagement.ExportKeyDetailsAlgorithmSha256, "not a pem")
		assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())
	})
}

func mustPKIX(t *testing.T, pub any) []byte {
	t.Helper()

	der, err := x509.MarshalPKIXPublicKey(pub)
	require.NoError(t, err)

	return der
}
