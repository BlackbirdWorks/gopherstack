package gcpkms_test

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"testing"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestEncryptDecrypt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		plaintext string
		aad       string
		decAAD    string
		wantCode  codes.Code
	}{
		{"plain", "hello kms", "", "", codes.OK},
		{"empty plaintext", "", "", "", codes.OK},
		{"aad match", "secret", "ctx-1", "ctx-1", codes.OK},
		{"aad mismatch", "secret", "ctx-1", "ctx-2", codes.InvalidArgument},
		{"aad missing on decrypt", "secret", "ctx-1", "", codes.InvalidArgument},
	}

	e := newEnv(t)
	key := e.key(t, e.ring(t, "r"), "k", kmspb.CryptoKey_ENCRYPT_DECRYPT,
		kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			enc, err := e.client.Encrypt(t.Context(), &kmspb.EncryptRequest{
				Name: key.GetName(), Plaintext: []byte(tt.plaintext), AdditionalAuthenticatedData: []byte(tt.aad),
				PlaintextCrc32C: crc([]byte(tt.plaintext)), AdditionalAuthenticatedDataCrc32C: crc([]byte(tt.aad)),
			})
			require.NoError(t, err)
			assert.Equal(t, key.GetName()+"/cryptoKeyVersions/1", enc.GetName())
			assert.True(t, enc.GetVerifiedPlaintextCrc32C())
			assert.True(t, enc.GetVerifiedAdditionalAuthenticatedDataCrc32C())
			assert.Equal(t, crc(enc.GetCiphertext()).GetValue(), enc.GetCiphertextCrc32C().GetValue())
			assert.Equal(t, kmspb.ProtectionLevel_SOFTWARE, enc.GetProtectionLevel())

			dec, err := e.client.Decrypt(t.Context(), &kmspb.DecryptRequest{
				Name: key.GetName(), Ciphertext: enc.GetCiphertext(), AdditionalAuthenticatedData: []byte(tt.decAAD),
			})
			require.Equal(t, tt.wantCode, status.Code(err))

			if tt.wantCode == codes.OK {
				assert.Equal(t, tt.plaintext, string(dec.GetPlaintext()))
				assert.True(t, dec.GetUsedPrimary())
				assert.Equal(t, crc(dec.GetPlaintext()).GetValue(), dec.GetPlaintextCrc32C().GetValue())
			}
		})
	}
}

func TestEncryptDecryptErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "r")
	key := e.key(t, ring, "k", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION)
	other := e.key(
		t,
		ring,
		"other",
		kmspb.CryptoKey_ENCRYPT_DECRYPT,
		kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION,
	)
	mac := e.key(t, ring, "mac", kmspb.CryptoKey_MAC, kmspb.CryptoKeyVersion_HMAC_SHA256)

	enc, err := e.client.Encrypt(t.Context(), &kmspb.EncryptRequest{Name: key.GetName(), Plaintext: []byte("x")})
	require.NoError(t, err)

	tampered := append([]byte(nil), enc.GetCiphertext()...)
	tampered[len(tampered)-1] ^= 0xff

	tests := []struct {
		call func() error
		name string
		want codes.Code
	}{
		{func() error {
			_, cerr := e.client.Encrypt(t.Context(), &kmspb.EncryptRequest{
				Name: key.GetName(), Plaintext: []byte("x"), PlaintextCrc32C: wrapperspb.Int64(1),
			})

			return cerr
		}, "plaintext crc mismatch", codes.InvalidArgument},
		{func() error {
			_, cerr := e.client.Encrypt(t.Context(), &kmspb.EncryptRequest{
				Name: key.GetName(), Plaintext: make([]byte, 64<<10+1),
			})

			return cerr
		}, "plaintext too large", codes.InvalidArgument},
		{func() error {
			_, cerr := e.client.Decrypt(t.Context(), &kmspb.DecryptRequest{Name: key.GetName(), Ciphertext: tampered})

			return cerr
		}, "tampered ciphertext", codes.InvalidArgument},
		{func() error {
			_, cerr := e.client.Decrypt(
				t.Context(),
				&kmspb.DecryptRequest{Name: key.GetName(), Ciphertext: []byte("junk")},
			)

			return cerr
		}, "garbage ciphertext", codes.InvalidArgument},
		{func() error {
			_, cerr := e.client.Decrypt(
				t.Context(),
				&kmspb.DecryptRequest{Name: other.GetName(), Ciphertext: enc.GetCiphertext()},
			)

			return cerr
		}, "ciphertext from another key", codes.InvalidArgument},
		{func() error {
			_, cerr := e.client.Encrypt(t.Context(), &kmspb.EncryptRequest{Name: mac.GetName(), Plaintext: []byte("x")})

			return cerr
		}, "wrong purpose", codes.InvalidArgument},
		{func() error {
			_, cerr := e.client.Encrypt(
				t.Context(),
				&kmspb.EncryptRequest{Name: ring + "/cryptoKeys/nope", Plaintext: []byte("x")},
			)

			return cerr
		}, "unknown key", codes.NotFound},
		{func() error {
			_, cerr := e.client.Encrypt(t.Context(), &kmspb.EncryptRequest{Name: "bad", Plaintext: []byte("x")})

			return cerr
		}, "bad name", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, status.Code(tt.call()))
		})
	}
}

func TestEncryptWithVersionName(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	key := e.key(
		t,
		e.ring(t, "r"),
		"k",
		kmspb.CryptoKey_ENCRYPT_DECRYPT,
		kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION,
	)

	v2, err := e.client.CreateCryptoKeyVersion(t.Context(), &kmspb.CreateCryptoKeyVersionRequest{Parent: key.GetName()})
	require.NoError(t, err)

	enc, err := e.client.Encrypt(t.Context(), &kmspb.EncryptRequest{Name: v2.GetName(), Plaintext: []byte("v2")})
	require.NoError(t, err)
	assert.Equal(t, v2.GetName(), enc.GetName())

	dec, err := e.client.Decrypt(
		t.Context(),
		&kmspb.DecryptRequest{Name: key.GetName(), Ciphertext: enc.GetCiphertext()},
	)
	require.NoError(t, err)
	assert.Equal(t, "v2", string(dec.GetPlaintext()))
	assert.False(t, dec.GetUsedPrimary(), "version 1 is still primary")

	_, err = e.client.UpdateCryptoKeyPrimaryVersion(t.Context(), &kmspb.UpdateCryptoKeyPrimaryVersionRequest{
		Name: key.GetName(), CryptoKeyVersionId: "2",
	})
	require.NoError(t, err)

	dec, err = e.client.Decrypt(
		t.Context(),
		&kmspb.DecryptRequest{Name: key.GetName(), Ciphertext: enc.GetCiphertext()},
	)
	require.NoError(t, err)
	assert.True(t, dec.GetUsedPrimary())
}

func parsePub(t *testing.T, pemStr string) crypto.PublicKey {
	t.Helper()

	blk, _ := pem.Decode([]byte(pemStr))
	require.NotNil(t, blk)

	pub, err := x509.ParsePKIXPublicKey(blk.Bytes)
	require.NoError(t, err)

	return pub
}

func TestAsymmetricSign(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify func(t *testing.T, pub crypto.PublicKey, data, sig []byte)
		name   string
		alg    kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm
	}{
		{
			name: "rsa pss 2048",
			alg:  kmspb.CryptoKeyVersion_RSA_SIGN_PSS_2048_SHA256,
			verify: func(t *testing.T, pub crypto.PublicKey, data, sig []byte) {
				t.Helper()
				sum := sha256.Sum256(data)
				require.NoError(t, rsa.VerifyPSS(pub.(*rsa.PublicKey), crypto.SHA256, sum[:], sig,
					&rsa.PSSOptions{SaltLength: sha256.Size}))
			},
		},
		{
			name: "rsa pkcs1 2048",
			alg:  kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_2048_SHA256,
			verify: func(t *testing.T, pub crypto.PublicKey, data, sig []byte) {
				t.Helper()
				sum := sha256.Sum256(data)
				require.NoError(t, rsa.VerifyPKCS1v15(pub.(*rsa.PublicKey), crypto.SHA256, sum[:], sig))
			},
		},
		{
			name: "ec p256",
			alg:  kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256,
			verify: func(t *testing.T, pub crypto.PublicKey, data, sig []byte) {
				t.Helper()
				sum := sha256.Sum256(data)
				assert.True(t, ecdsa.VerifyASN1(pub.(*ecdsa.PublicKey), sum[:], sig))
			},
		},
		{
			name: "ec p384",
			alg:  kmspb.CryptoKeyVersion_EC_SIGN_P384_SHA384,
			verify: func(t *testing.T, pub crypto.PublicKey, data, sig []byte) {
				t.Helper()
				sum := sha512.Sum384(data)
				assert.True(t, ecdsa.VerifyASN1(pub.(*ecdsa.PublicKey), sum[:], sig))
			},
		},
	}

	e := newEnv(t)
	ring := e.ring(t, "r")

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key := e.key(t, ring, fmt.Sprintf("sign%d", i), kmspb.CryptoKey_ASYMMETRIC_SIGN, tt.alg)
			name := key.GetName() + "/cryptoKeyVersions/1"

			pk, err := e.client.GetPublicKey(t.Context(), &kmspb.GetPublicKeyRequest{Name: name})
			require.NoError(t, err)
			assert.Equal(t, tt.alg, pk.GetAlgorithm())
			assert.Equal(t, crc([]byte(pk.GetPem())).GetValue(), pk.GetPemCrc32C().GetValue())
			pub := parsePub(t, pk.GetPem())

			data := []byte("message to sign")

			var digest *kmspb.Digest

			if tt.alg == kmspb.CryptoKeyVersion_EC_SIGN_P384_SHA384 {
				sum := sha512.Sum384(data)
				digest = &kmspb.Digest{Digest: &kmspb.Digest_Sha384{Sha384: sum[:]}}
			} else {
				sum := sha256.Sum256(data)
				digest = &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: sum[:]}}
			}

			resp, err := e.client.AsymmetricSign(t.Context(), &kmspb.AsymmetricSignRequest{
				Name: name, Digest: digest, DigestCrc32C: crc(digestBytes(digest)),
			})
			require.NoError(t, err)
			assert.True(t, resp.GetVerifiedDigestCrc32C())
			assert.Equal(t, crc(resp.GetSignature()).GetValue(), resp.GetSignatureCrc32C().GetValue())
			tt.verify(t, pub, data, resp.GetSignature())

			byData, err := e.client.AsymmetricSign(t.Context(), &kmspb.AsymmetricSignRequest{Name: name, Data: data})
			require.NoError(t, err)
			tt.verify(t, pub, data, byData.GetSignature())

			wrong := sha512.Sum512(data)
			_, err = e.client.AsymmetricSign(t.Context(), &kmspb.AsymmetricSignRequest{
				Name: name, Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha512{Sha512: wrong[:]}},
			})
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func digestBytes(d *kmspb.Digest) []byte {
	switch v := d.GetDigest().(type) {
	case *kmspb.Digest_Sha256:
		return v.Sha256
	case *kmspb.Digest_Sha384:
		return v.Sha384
	default:
		return nil
	}
}

func TestSignEd25519AndLargeRSA(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "r")

	ed := e.key(t, ring, "ed", kmspb.CryptoKey_ASYMMETRIC_SIGN, kmspb.CryptoKeyVersion_EC_SIGN_ED25519)
	edName := ed.GetName() + "/cryptoKeyVersions/1"

	pk, err := e.client.GetPublicKey(t.Context(), &kmspb.GetPublicKeyRequest{Name: edName})
	require.NoError(t, err)

	sig, err := e.client.AsymmetricSign(t.Context(), &kmspb.AsymmetricSignRequest{Name: edName, Data: []byte("hi")})
	require.NoError(t, err)
	assert.True(t, ed25519.Verify(parsePub(t, pk.GetPem()).(ed25519.PublicKey), []byte("hi"), sig.GetSignature()))

	sum := sha256.Sum256([]byte("hi"))
	_, err = e.client.AsymmetricSign(t.Context(), &kmspb.AsymmetricSignRequest{
		Name: edName, Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: sum[:]}},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	rk := e.key(t, ring, "rsa4096", kmspb.CryptoKey_ASYMMETRIC_SIGN, kmspb.CryptoKeyVersion_RSA_SIGN_PSS_4096_SHA512)
	rName := rk.GetName() + "/cryptoKeyVersions/1"

	der, err := e.client.GetPublicKey(t.Context(), &kmspb.GetPublicKeyRequest{
		Name: rName, PublicKeyFormat: kmspb.PublicKey_DER,
	})
	require.NoError(t, err)
	assert.Equal(t, kmspb.PublicKey_DER, der.GetPublicKeyFormat())

	pub, err := x509.ParsePKIXPublicKey(der.GetPublicKey().GetData())
	require.NoError(t, err)
	assert.Equal(t, 4096, pub.(*rsa.PublicKey).N.BitLen())

	sum512 := sha512.Sum512([]byte("hi"))
	rs, err := e.client.AsymmetricSign(t.Context(), &kmspb.AsymmetricSignRequest{
		Name: rName, Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha512{Sha512: sum512[:]}},
	})
	require.NoError(t, err)
	require.NoError(t, rsa.VerifyPSS(pub.(*rsa.PublicKey), crypto.SHA512, sum512[:], rs.GetSignature(),
		&rsa.PSSOptions{SaltLength: sha512.Size}))
}

func TestAsymmetricDecrypt(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	key := e.key(
		t,
		e.ring(t, "r"),
		"k",
		kmspb.CryptoKey_ASYMMETRIC_DECRYPT,
		kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_2048_SHA256,
	)
	name := key.GetName() + "/cryptoKeyVersions/1"

	pk, err := e.client.GetPublicKey(t.Context(), &kmspb.GetPublicKeyRequest{Name: name})
	require.NoError(t, err)

	ct, err := rsa.EncryptOAEP(
		sha256.New(),
		rand.Reader,
		parsePub(t, pk.GetPem()).(*rsa.PublicKey),
		[]byte("oaep"),
		nil,
	)
	require.NoError(t, err)

	resp, err := e.client.AsymmetricDecrypt(t.Context(), &kmspb.AsymmetricDecryptRequest{
		Name: name, Ciphertext: ct, CiphertextCrc32C: crc(ct),
	})
	require.NoError(t, err)
	assert.Equal(t, "oaep", string(resp.GetPlaintext()))
	assert.True(t, resp.GetVerifiedCiphertextCrc32C())

	_, err = e.client.AsymmetricDecrypt(
		t.Context(),
		&kmspb.AsymmetricDecryptRequest{Name: name, Ciphertext: []byte("bad")},
	)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = e.client.AsymmetricSign(t.Context(), &kmspb.AsymmetricSignRequest{Name: name, Data: []byte("x")})
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "wrong purpose")
}

func TestMac(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		alg  kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm
		size int
	}{
		{"sha224", kmspb.CryptoKeyVersion_HMAC_SHA224, 28},
		{"sha256", kmspb.CryptoKeyVersion_HMAC_SHA256, 32},
		{"sha384", kmspb.CryptoKeyVersion_HMAC_SHA384, 48},
		{"sha512", kmspb.CryptoKeyVersion_HMAC_SHA512, 64},
	}

	e := newEnv(t)
	ring := e.ring(t, "r")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key := e.key(t, ring, "mac-"+tt.name, kmspb.CryptoKey_MAC, tt.alg)
			name := key.GetName() + "/cryptoKeyVersions/1"
			data := []byte("authenticate me")

			signed, err := e.client.MacSign(t.Context(), &kmspb.MacSignRequest{
				Name: name, Data: data, DataCrc32C: crc(data),
			})
			require.NoError(t, err)
			assert.Len(t, signed.GetMac(), tt.size)
			assert.True(t, signed.GetVerifiedDataCrc32C())

			ok, err := e.client.MacVerify(t.Context(), &kmspb.MacVerifyRequest{
				Name: name, Data: data, Mac: signed.GetMac(), MacCrc32C: crc(signed.GetMac()),
			})
			require.NoError(t, err)
			assert.True(t, ok.GetSuccess())
			assert.True(t, ok.GetVerifiedSuccessIntegrity())
			assert.True(t, ok.GetVerifiedMacCrc32C())

			bad, err := e.client.MacVerify(t.Context(), &kmspb.MacVerifyRequest{
				Name: name, Data: []byte("tampered"), Mac: signed.GetMac(),
			})
			require.NoError(t, err)
			assert.False(t, bad.GetSuccess())
		})
	}
}

func TestRawEncryptDecrypt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		plaintext string
		aad       string
		wantIV    int
		alg       kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm
		wantTag   int32
		encCode   codes.Code
	}{
		{"gcm128", "raw gcm", "aad", 12, kmspb.CryptoKeyVersion_AES_128_GCM, 16, codes.OK},
		{"gcm256", "raw gcm", "", 12, kmspb.CryptoKeyVersion_AES_256_GCM, 16, codes.OK},
		{"cbc128", "0123456789abcdef", "", 16, kmspb.CryptoKeyVersion_AES_128_CBC, 0, codes.OK},
		{"cbc256", "0123456789abcdef0123456789abcdef", "", 16, kmspb.CryptoKeyVersion_AES_256_CBC, 0, codes.OK},
		{"cbc unaligned", "short", "", 16, kmspb.CryptoKeyVersion_AES_256_CBC, 0, codes.InvalidArgument},
		{"ctr128", "any length ok", "", 16, kmspb.CryptoKeyVersion_AES_128_CTR, 0, codes.OK},
		{"ctr256", "any length ok", "", 16, kmspb.CryptoKeyVersion_AES_256_CTR, 0, codes.OK},
		{"ctr rejects aad", "x", "aad", 16, kmspb.CryptoKeyVersion_AES_256_CTR, 0, codes.InvalidArgument},
	}

	e := newEnv(t)
	ring := e.ring(t, "r")

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key := e.key(t, ring, fmt.Sprintf("raw%d", i), kmspb.CryptoKey_RAW_ENCRYPT_DECRYPT, tt.alg)
			name := key.GetName() + "/cryptoKeyVersions/1"

			enc, err := e.client.RawEncrypt(t.Context(), &kmspb.RawEncryptRequest{
				Name: name, Plaintext: []byte(tt.plaintext), AdditionalAuthenticatedData: []byte(tt.aad),
				PlaintextCrc32C: crc([]byte(tt.plaintext)),
			})
			require.Equal(t, tt.encCode, status.Code(err))

			if err != nil {
				return
			}

			assert.Equal(t, tt.wantTag, enc.GetTagLength())
			assert.Len(t, enc.GetInitializationVector(), tt.wantIV)
			assert.True(t, enc.GetVerifiedPlaintextCrc32C())
			assert.NotEqual(t, tt.plaintext, string(enc.GetCiphertext()))

			dec, err := e.client.RawDecrypt(t.Context(), &kmspb.RawDecryptRequest{
				Name: name, Ciphertext: enc.GetCiphertext(), AdditionalAuthenticatedData: []byte(tt.aad),
				InitializationVector: enc.GetInitializationVector(), TagLength: enc.GetTagLength(),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.plaintext, string(dec.GetPlaintext()))

			if tt.wantTag > 0 {
				_, err = e.client.RawDecrypt(t.Context(), &kmspb.RawDecryptRequest{
					Name: name, Ciphertext: enc.GetCiphertext(), AdditionalAuthenticatedData: []byte("wrong"),
					InitializationVector: enc.GetInitializationVector(), TagLength: enc.GetTagLength(),
				})
				assert.Equal(t, codes.InvalidArgument, status.Code(err))
			}
		})
	}
}

func TestGenerateRandomBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		loc  string
		n    int32
		want codes.Code
	}{
		{"min", location, 8, codes.OK},
		{"max", location, 1024, codes.OK},
		{"too short", location, 7, codes.InvalidArgument},
		{"too long", location, 1025, codes.InvalidArgument},
		{"bad location", "nope", 16, codes.InvalidArgument},
	}

	e := newEnv(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := e.client.GenerateRandomBytes(t.Context(), &kmspb.GenerateRandomBytesRequest{
				Location: tt.loc, LengthBytes: tt.n, ProtectionLevel: kmspb.ProtectionLevel_HSM,
			})
			require.Equal(t, tt.want, status.Code(err))

			if tt.want == codes.OK {
				assert.Len(t, resp.GetData(), int(tt.n))
				assert.Equal(t, crc(resp.GetData()).GetValue(), resp.GetDataCrc32C().GetValue())
			}
		})
	}
}
