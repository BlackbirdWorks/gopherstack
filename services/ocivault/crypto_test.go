package ocivault_test

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/keymanagement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func unb64(t *testing.T, s string) []byte {
	t.Helper()

	b, err := base64.StdEncoding.DecodeString(s)
	require.NoError(t, err)

	return b
}

func pubOf(t *testing.T, pemStr string) any {
	t.Helper()

	blk, _ := pem.Decode([]byte(pemStr))
	require.NotNil(t, blk)

	pub, err := x509.ParsePKIXPublicKey(blk.Bytes)
	require.NoError(t, err)

	return pub
}

type cryptoEnv struct {
	m keymanagement.KmsManagementClient
	c keymanagement.KmsCryptoClient
	e *env
	v keymanagement.Vault
}

func newCryptoEnv(t *testing.T) *cryptoEnv {
	t.Helper()

	e := newEnv(t, envOpts{})
	v := e.mkVault(t, "crypto")

	return &cryptoEnv{m: e.mgmt(t, v), c: e.crypto(t, v), e: e, v: v}
}

func (ce *cryptoEnv) encrypt(
	t *testing.T,
	key keymanagement.Key,
	pt string,
	aad map[string]string,
	alg keymanagement.EncryptDataDetailsEncryptionAlgorithmEnum,
) (keymanagement.EncryptedData, error) {
	t.Helper()

	r, err := ce.c.Encrypt(t.Context(), keymanagement.EncryptRequest{
		KeyId: key.Id, Plaintext: new(b64([]byte(pt))), AssociatedData: aad, EncryptionAlgorithm: alg})

	return r.EncryptedData, err
}

func (ce *cryptoEnv) decrypt(
	t *testing.T,
	key keymanagement.Key,
	ct string,
	aad map[string]string,
	alg keymanagement.DecryptDataDetailsEncryptionAlgorithmEnum,
) (keymanagement.DecryptedData, error) {
	t.Helper()

	r, err := ce.c.Decrypt(t.Context(), keymanagement.DecryptRequest{
		KeyId: key.Id, Ciphertext: new(ct), AssociatedData: aad, EncryptionAlgorithm: alg})

	return r.DecryptedData, err
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	t.Parallel()

	ce := newCryptoEnv(t)

	tests := []struct {
		shape *keymanagement.KeyShape
		aad   map[string]string
		alg   keymanagement.EncryptDataDetailsEncryptionAlgorithmEnum
		dalg  keymanagement.DecryptDataDetailsEncryptionAlgorithmEnum
		name  string
	}{
		{name: "aes128", shape: shape(keymanagement.KeyShapeAlgorithmAes, 16, "")},
		{
			name:  "aes192 aad",
			shape: shape(keymanagement.KeyShapeAlgorithmAes, 24, ""),
			aad:   map[string]string{"k": "v", "a": "b"},
		},
		{name: "aes256 aad", shape: aes256(), aad: map[string]string{"ctx": "prod"}},
		{name: "rsa2048 default", shape: shape(keymanagement.KeyShapeAlgorithmRsa, 256, "")},
		{
			name:  "rsa3072 sha256",
			shape: shape(keymanagement.KeyShapeAlgorithmRsa, 384, ""),
			alg:   keymanagement.EncryptDataDetailsEncryptionAlgorithmRsaOaepSha256,
			dalg:  keymanagement.DecryptDataDetailsEncryptionAlgorithmRsaOaepSha256,
		},
		{
			name:  "rsa2048 sha1",
			shape: shape(keymanagement.KeyShapeAlgorithmRsa, 256, ""),
			alg:   keymanagement.EncryptDataDetailsEncryptionAlgorithmRsaOaepSha1,
			dalg:  keymanagement.DecryptDataDetailsEncryptionAlgorithmRsaOaepSha1,
		},
		{
			name:  "rsa4096",
			shape: shape(keymanagement.KeyShapeAlgorithmRsa, 512, ""),
			alg:   keymanagement.EncryptDataDetailsEncryptionAlgorithmRsaOaepSha256,
			dalg:  keymanagement.DecryptDataDetailsEncryptionAlgorithmRsaOaepSha256,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key := ce.e.mkKey(t, ce.m, tt.name, tt.shape, "")
			enc, err := ce.encrypt(t, key, "attack at dawn", tt.aad, tt.alg)
			require.NoError(t, err)
			assert.Equal(t, *key.Id, *enc.KeyId)
			assert.Equal(t, *key.CurrentKeyVersion, *enc.KeyVersionId)
			assert.NotContains(t, unb64(t, *enc.Ciphertext), []byte("attack"))

			dec, err := ce.decrypt(t, key, *enc.Ciphertext, tt.aad, tt.dalg)
			require.NoError(t, err)
			assert.Equal(t, "attack at dawn", string(unb64(t, *dec.Plaintext)))
			assert.NotEmpty(t, *dec.PlaintextChecksum)

			if tt.shape.Algorithm == keymanagement.KeyShapeAlgorithmAes {
				_, err = ce.decrypt(t, key, *enc.Ciphertext, map[string]string{"wrong": "aad"}, "")
				assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
			}

			other := ce.e.mkKey(t, ce.m, "other", tt.shape, "")
			_, err = ce.decrypt(t, other, *enc.Ciphertext, tt.aad, tt.dalg)
			assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())
		})
	}
}

func TestEncryptValidation(t *testing.T) {
	t.Parallel()

	ce := newCryptoEnv(t)
	aesKey := ce.e.mkKey(t, ce.m, "aes", aes256(), "")
	ecKey := ce.e.mkKey(
		t,
		ce.m,
		"ec",
		shape(keymanagement.KeyShapeAlgorithmEcdsa, 32, keymanagement.KeyShapeCurveIdP256),
		"",
	)
	disabled := ce.e.mkKey(t, ce.m, "dis", aes256(), "")
	_, err := ce.m.DisableKey(t.Context(), keymanagement.DisableKeyRequest{KeyId: disabled.Id})
	require.NoError(t, err)

	big := string(make([]byte, 4097))

	tests := []struct {
		key      keymanagement.Key
		name     string
		plain    string
		rawPlain *string
		wantCode string
		status   int
	}{
		{key: aesKey, name: "too large", plain: big, wantCode: "InvalidParameter", status: http.StatusBadRequest},
		{
			key:      aesKey,
			name:     "not base64",
			rawPlain: new("***"),
			wantCode: "InvalidParameter",
			status:   http.StatusBadRequest,
		},
		{
			key:      ecKey,
			name:     "ecdsa cannot encrypt",
			plain:    "x",
			wantCode: "InvalidParameter",
			status:   http.StatusBadRequest,
		},
		{key: disabled, name: "disabled key", plain: "x", wantCode: "IncorrectState", status: http.StatusConflict},
		{
			key:      keymanagement.Key{Id: new("ocid1.key.oc1..nope")},
			name:     "unknown key",
			plain:    "x",
			wantCode: "NotAuthorizedOrNotFound",
			status:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pt := new(b64([]byte(tt.plain)))
			if tt.rawPlain != nil {
				pt = tt.rawPlain
			}

			_, cerr := ce.c.Encrypt(t.Context(), keymanagement.EncryptRequest{
				KeyId: tt.key.Id, Plaintext: pt,
			})
			se := svcErr(t, cerr)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.wantCode, se.GetCode())
		})
	}
}

func TestDecryptAfterRotation(t *testing.T) {
	t.Parallel()

	ce := newCryptoEnv(t)

	tests := []struct {
		shape *keymanagement.KeyShape
		name  string
	}{
		{aes256(), "aes"},
		{shape(keymanagement.KeyShapeAlgorithmRsa, 256, ""), "rsa"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key := ce.e.mkKey(t, ce.m, tt.name, tt.shape, "")
			old, err := ce.encrypt(t, key, "before rotation", nil, "")
			require.NoError(t, err)

			v2, err := ce.m.CreateKeyVersion(t.Context(), keymanagement.CreateKeyVersionRequest{KeyId: key.Id})
			require.NoError(t, err)

			fresh, err := ce.encrypt(t, key, "after rotation", nil, "")
			require.NoError(t, err)
			assert.Equal(t, *v2.Id, *fresh.KeyVersionId)
			assert.Equal(t, *key.CurrentKeyVersion, *old.KeyVersionId)

			for _, c := range []struct {
				enc  keymanagement.EncryptedData
				want string
			}{{old, "before rotation"}, {fresh, "after rotation"}} {
				dec, cerr := ce.decrypt(t, key, *c.enc.Ciphertext, nil, "")
				require.NoError(t, cerr)
				assert.Equal(t, c.want, string(unb64(t, *dec.Plaintext)))
				assert.Equal(t, *c.enc.KeyVersionId, *dec.KeyVersionId)
			}
		})
	}
}

func TestRSAInteropWithLocalOAEP(t *testing.T) {
	t.Parallel()

	ce := newCryptoEnv(t)
	key := ce.e.mkKey(t, ce.m, "rsa", shape(keymanagement.KeyShapeAlgorithmRsa, 256, ""), "")
	ver, err := ce.m.GetKeyVersion(
		t.Context(),
		keymanagement.GetKeyVersionRequest{KeyId: key.Id, KeyVersionId: key.CurrentKeyVersion},
	)
	require.NoError(t, err)

	pub, ok := pubOf(t, *ver.PublicKey).(*rsa.PublicKey)
	require.True(t, ok)

	ct, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, []byte("from outside"), nil)
	require.NoError(t, err)

	dec, err := ce.decrypt(t, key, b64(ct), nil, "")
	require.NoError(t, err)
	assert.Equal(t, "from outside", string(unb64(t, *dec.Plaintext)))
}

func TestGenerateDataEncryptionKey(t *testing.T) {
	t.Parallel()

	ce := newCryptoEnv(t)

	tests := []struct {
		shape *keymanagement.KeyShape
		name  string
		dek   int
		plain bool
	}{
		{aes256(), "aes with plaintext", 32, true},
		{aes256(), "aes ciphertext only", 16, false},
		{shape(keymanagement.KeyShapeAlgorithmRsa, 256, ""), "rsa wraps dek", 24, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key := ce.e.mkKey(t, ce.m, tt.name, tt.shape, "")
			aad := map[string]string{"app": "x"}
			if tt.shape.Algorithm == keymanagement.KeyShapeAlgorithmRsa {
				aad = nil
			}

			resp, err := ce.c.GenerateDataEncryptionKey(t.Context(), keymanagement.GenerateDataEncryptionKeyRequest{
				KeyId: key.Id, KeyShape: shape(keymanagement.KeyShapeAlgorithmAes, tt.dek, ""),
				IncludePlaintextKey: new(tt.plain), AssociatedData: aad,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, *resp.Ciphertext)

			if !tt.plain {
				assert.Nil(t, resp.Plaintext)

				return
			}

			dek := unb64(t, *resp.Plaintext)
			assert.Len(t, dek, tt.dek)

			dec, err := ce.decrypt(t, key, *resp.Ciphertext, aad, "")
			require.NoError(t, err)
			assert.Equal(t, dek, unb64(t, *dec.Plaintext))
		})
	}

	key := ce.e.mkKey(t, ce.m, "k", aes256(), "")
	_, err := ce.c.GenerateDataEncryptionKey(t.Context(), keymanagement.GenerateDataEncryptionKeyRequest{
		KeyId: key.Id, IncludePlaintextKey: new(true), KeyShape: shape(keymanagement.KeyShapeAlgorithmAes, 7, ""),
	})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())
}

type rsaSpec struct {
	hash crypto.Hash
	pss  bool
}

//nolint:gochecknoglobals // read-only test table
var rsaAlgs = map[string]rsaSpec{
	"SHA_224_RSA_PKCS_PSS": {crypto.SHA224, true}, "SHA_256_RSA_PKCS_PSS": {crypto.SHA256, true},
	"SHA_384_RSA_PKCS_PSS": {crypto.SHA384, true}, "SHA_512_RSA_PKCS_PSS": {crypto.SHA512, true},
	"SHA_224_RSA_PKCS1_V1_5": {crypto.SHA224, false}, "SHA_256_RSA_PKCS1_V1_5": {crypto.SHA256, false},
	"SHA_384_RSA_PKCS1_V1_5": {crypto.SHA384, false}, "SHA_512_RSA_PKCS1_V1_5": {crypto.SHA512, false},
}

//nolint:gochecknoglobals // read-only test table
var ecAlgs = map[string]crypto.Hash{
	"ECDSA_SHA_256": crypto.SHA256, "ECDSA_SHA_384": crypto.SHA384, "ECDSA_SHA_512": crypto.SHA512,
}

func tamper(b []byte) []byte {
	out := append([]byte(nil), b...)
	out[0] ^= 0xff

	return out
}

func digestOf(h crypto.Hash, msg []byte) []byte {
	d := h.New()
	d.Write(msg)

	return d.Sum(nil)
}

func (ce *cryptoEnv) sign(
	t *testing.T,
	key keymanagement.Key,
	alg, mtype string,
	msg []byte,
) (keymanagement.SignedData, error) {
	t.Helper()

	r, err := ce.c.Sign(t.Context(), keymanagement.SignRequest{
		KeyId: key.Id, Message: new(b64(msg)), SigningAlgorithm: keymanagement.SignDataDetailsSigningAlgorithmEnum(alg),
		MessageType: keymanagement.SignDataDetailsMessageTypeEnum(mtype)})

	return r.SignedData, err
}

func (ce *cryptoEnv) verify(t *testing.T, key keymanagement.Key, ver, alg, mtype string, msg, sig []byte) bool {
	t.Helper()

	r, err := ce.c.Verify(t.Context(), keymanagement.VerifyRequest{
		KeyId: key.Id, KeyVersionId: new(ver), Message: new(b64(msg)), Signature: new(b64(sig)),
		SigningAlgorithm: keymanagement.VerifyDataDetailsSigningAlgorithmEnum(alg),
		MessageType:      keymanagement.VerifyDataDetailsMessageTypeEnum(mtype)})
	require.NoError(t, err)

	return *r.IsSignatureValid
}

func TestSignVerifyRSA(t *testing.T) {
	t.Parallel()

	ce := newCryptoEnv(t)

	for _, size := range []int{256, 384, 512} {
		key := ce.e.mkKey(t, ce.m, "rsa", shape(keymanagement.KeyShapeAlgorithmRsa, size, ""), "")
		ver, err := ce.m.GetKeyVersion(
			t.Context(),
			keymanagement.GetKeyVersionRequest{KeyId: key.Id, KeyVersionId: key.CurrentKeyVersion},
		)
		require.NoError(t, err)

		pub, ok := pubOf(t, *ver.PublicKey).(*rsa.PublicKey)
		require.True(t, ok)

		algs := rsaAlgs
		if size != 256 {
			algs = map[string]rsaSpec{
				"SHA_256_RSA_PKCS_PSS":   rsaAlgs["SHA_256_RSA_PKCS_PSS"],
				"SHA_512_RSA_PKCS1_V1_5": rsaAlgs["SHA_512_RSA_PKCS1_V1_5"],
			}
		}

		for alg, spec := range algs {
			for _, mtype := range []string{"RAW", "DIGEST"} {
				t.Run(alg+"/"+mtype, func(t *testing.T) {
					t.Parallel()

					msg := []byte("sign me")
					in := msg

					if mtype == "DIGEST" {
						in = digestOf(spec.hash, msg)
					}

					signed, cerr := ce.sign(t, key, alg, mtype, in)
					require.NoError(t, cerr)
					assert.Equal(t, *key.CurrentKeyVersion, *signed.KeyVersionId)

					sig := unb64(t, *signed.Signature)
					digest := digestOf(spec.hash, msg)

					if spec.pss {
						require.NoError(
							t,
							rsa.VerifyPSS(
								pub,
								spec.hash,
								digest,
								sig,
								&rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash},
							),
						)
					} else {
						require.NoError(t, rsa.VerifyPKCS1v15(pub, spec.hash, digest, sig))
					}

					assert.True(t, ce.verify(t, key, *signed.KeyVersionId, alg, mtype, in, sig))
					assert.False(t, ce.verify(t, key, *signed.KeyVersionId, alg, mtype, tamper(in), sig))
				})
			}
		}
	}
}

func TestSignVerifyECDSA(t *testing.T) {
	t.Parallel()

	ce := newCryptoEnv(t)

	curves := []struct {
		curve  keymanagement.KeyShapeCurveIdEnum
		length int
	}{
		{
			keymanagement.KeyShapeCurveIdP256,
			32,
		},
		{keymanagement.KeyShapeCurveIdP384, 48},
		{keymanagement.KeyShapeCurveIdP521, 66},
	}

	for _, c := range curves {
		key := ce.e.mkKey(t, ce.m, string(c.curve), shape(keymanagement.KeyShapeAlgorithmEcdsa, c.length, c.curve), "")
		ver, err := ce.m.GetKeyVersion(
			t.Context(),
			keymanagement.GetKeyVersionRequest{KeyId: key.Id, KeyVersionId: key.CurrentKeyVersion},
		)
		require.NoError(t, err)

		pub, ok := pubOf(t, *ver.PublicKey).(*ecdsa.PublicKey)
		require.True(t, ok)

		for alg, h := range ecAlgs {
			for _, mtype := range []string{"RAW", "DIGEST"} {
				t.Run(string(c.curve)+"/"+alg+"/"+mtype, func(t *testing.T) {
					t.Parallel()

					msg := []byte("sign me")
					in := msg

					if mtype == "DIGEST" {
						in = digestOf(h, msg)
					}

					signed, cerr := ce.sign(t, key, alg, mtype, in)
					require.NoError(t, cerr)

					sig := unb64(t, *signed.Signature)
					assert.True(t, ecdsa.VerifyASN1(pub, digestOf(h, msg), sig))
					assert.True(t, ce.verify(t, key, *signed.KeyVersionId, alg, mtype, in, sig))
					assert.False(t, ce.verify(t, key, *signed.KeyVersionId, alg, mtype, tamper(in), sig))
				})
			}
		}
	}
}

func TestSignValidation(t *testing.T) {
	t.Parallel()

	ce := newCryptoEnv(t)
	rsaKey := ce.e.mkKey(t, ce.m, "rsa", shape(keymanagement.KeyShapeAlgorithmRsa, 256, ""), "")
	ecKey := ce.e.mkKey(
		t,
		ce.m,
		"ec",
		shape(keymanagement.KeyShapeAlgorithmEcdsa, 32, keymanagement.KeyShapeCurveIdP256),
		"",
	)
	aesKey := ce.e.mkKey(t, ce.m, "aes", aes256(), "")

	tests := []struct {
		key   keymanagement.Key
		name  string
		alg   string
		mtype string
		msg   []byte
	}{
		{rsaKey, "rsa key ecdsa alg", "ECDSA_SHA_256", "RAW", []byte("m")},
		{ecKey, "ecdsa key rsa alg", "SHA_256_RSA_PKCS_PSS", "RAW", []byte("m")},
		{aesKey, "aes cannot sign", "SHA_256_RSA_PKCS_PSS", "RAW", []byte("m")},
		{rsaKey, "digest wrong length", "SHA_256_RSA_PKCS_PSS", "DIGEST", []byte("short")},
		{rsaKey, "raw too large", "SHA_256_RSA_PKCS_PSS", "RAW", make([]byte, 4097)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ce.sign(t, tt.key, tt.alg, tt.mtype, tt.msg)
			se := svcErr(t, err)
			assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
			assert.Equal(t, "InvalidParameter", se.GetCode())
		})
	}
}

func TestSignOldVersionAfterRotation(t *testing.T) {
	t.Parallel()

	ce := newCryptoEnv(t)
	key := ce.e.mkKey(t, ce.m, "rsa", shape(keymanagement.KeyShapeAlgorithmRsa, 256, ""), "")
	signed, err := ce.sign(t, key, "SHA_256_RSA_PKCS_PSS", "RAW", []byte("m"))
	require.NoError(t, err)

	_, err = ce.m.CreateKeyVersion(t.Context(), keymanagement.CreateKeyVersionRequest{KeyId: key.Id})
	require.NoError(t, err)

	assert.True(
		t,
		ce.verify(
			t,
			key,
			*signed.KeyVersionId,
			"SHA_256_RSA_PKCS_PSS",
			"RAW",
			[]byte("m"),
			unb64(t, *signed.Signature),
		),
	)

	fresh, err := ce.sign(t, key, "SHA_256_RSA_PKCS_PSS", "RAW", []byte("m"))
	require.NoError(t, err)
	assert.NotEqual(t, *signed.KeyVersionId, *fresh.KeyVersionId)
	assert.False(
		t,
		ce.verify(t, key, *signed.KeyVersionId, "SHA_256_RSA_PKCS_PSS", "RAW", []byte("m"), unb64(t, *fresh.Signature)),
	)
}
