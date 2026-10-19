package gcpkms_test

import (
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"encoding/hex"
	"testing"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcpkms"
)

func TestRawPKCS1Sign(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		alg  kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm
		max  int
	}{
		{"2048", kmspb.CryptoKeyVersion_RSA_SIGN_RAW_PKCS1_2048, 245},
		{"3072", kmspb.CryptoKeyVersion_RSA_SIGN_RAW_PKCS1_3072, 373},
	}

	e := newEnv(t)
	ring := e.ring(t, "r")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key := e.key(t, ring, "raw"+tt.name, kmspb.CryptoKey_ASYMMETRIC_SIGN, tt.alg)
			name := key.GetName() + "/cryptoKeyVersions/1"

			pk, err := e.client.GetPublicKey(t.Context(), &kmspb.GetPublicKeyRequest{Name: name})
			require.NoError(t, err)
			pub := parsePub(t, pk.GetPem()).(*rsa.PublicKey)

			data := []byte("0123456789abcdef0123456789abcdef")
			resp, err := e.client.AsymmetricSign(t.Context(), &kmspb.AsymmetricSignRequest{
				Name: name, Data: data, DataCrc32C: crc(data),
			})
			require.NoError(t, err)
			require.NoError(t, rsa.VerifyPKCS1v15(pub, crypto.Hash(0), data, resp.GetSignature()))
			assert.True(t, resp.GetVerifiedDataCrc32C())

			big := make([]byte, tt.max+1)
			_, err = e.client.AsymmetricSign(t.Context(), &kmspb.AsymmetricSignRequest{Name: name, Data: big})
			assert.Equal(t, codes.InvalidArgument, status.Code(err), "too long")

			_, err = e.client.AsymmetricSign(t.Context(), &kmspb.AsymmetricSignRequest{
				Name: name, Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: make([]byte, 32)}},
			})
			assert.Equal(t, codes.InvalidArgument, status.Code(err), "digest rejected")

			_, err = e.client.AsymmetricSign(t.Context(), &kmspb.AsymmetricSignRequest{Name: name})
			assert.Equal(t, codes.InvalidArgument, status.Code(err), "empty")
		})
	}
}

func TestSHA1Algorithms(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "r")

	t.Run("hmac", func(t *testing.T) {
		t.Parallel()

		key := e.key(t, ring, "hmac1", kmspb.CryptoKey_MAC, kmspb.CryptoKeyVersion_HMAC_SHA1)
		name := key.GetName() + "/cryptoKeyVersions/1"

		signed, err := e.client.MacSign(t.Context(), &kmspb.MacSignRequest{Name: name, Data: []byte("d")})
		require.NoError(t, err)
		assert.Len(t, signed.GetMac(), sha1.Size)

		ok, err := e.client.MacVerify(t.Context(), &kmspb.MacVerifyRequest{
			Name: name, Data: []byte("d"), Mac: signed.GetMac(),
		})
		require.NoError(t, err)
		assert.True(t, ok.GetSuccess())
		assert.True(t, hmac.Equal(signed.GetMac(), signed.GetMac()))
	})

	t.Run("oaep", func(t *testing.T) {
		t.Parallel()

		key := e.key(t, ring, "oaep1", kmspb.CryptoKey_ASYMMETRIC_DECRYPT,
			kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_2048_SHA1)
		name := key.GetName() + "/cryptoKeyVersions/1"

		pk, err := e.client.GetPublicKey(t.Context(), &kmspb.GetPublicKeyRequest{Name: name})
		require.NoError(t, err)

		ct, err := rsa.EncryptOAEP(
			sha1.New(),
			rand.Reader,
			parsePub(t, pk.GetPem()).(*rsa.PublicKey),
			[]byte("hi"),
			nil,
		)
		require.NoError(t, err)

		resp, err := e.client.AsymmetricDecrypt(
			t.Context(),
			&kmspb.AsymmetricDecryptRequest{Name: name, Ciphertext: ct},
		)
		require.NoError(t, err)
		assert.Equal(t, "hi", string(resp.GetPlaintext()))
	})

	t.Run("sha1 import", func(t *testing.T) {
		t.Parallel()

		job, err := e.client.CreateImportJob(t.Context(), &kmspb.CreateImportJobRequest{
			Parent: ring, ImportJobId: "sha1job", ImportJob: &kmspb.ImportJob{
				ImportMethod:    kmspb.ImportJob_RSA_OAEP_3072_SHA1_AES_256,
				ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
			},
		})
		require.NoError(t, err)

		key := e.key(t, ring, "imp1", kmspb.CryptoKey_MAC, kmspb.CryptoKeyVersion_HMAC_SHA256,
			func(k *kmspb.CryptoKey) { k.ImportOnly = true })

		pub := parsePub(t, job.GetPublicKey().GetPem()).(*rsa.PublicKey)
		kek := make([]byte, 32)
		_, _ = rand.Read(kek)
		material := make([]byte, 32)
		_, _ = rand.Read(material)

		head, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, pub, kek, nil)
		require.NoError(t, err)
		kwp, err := gcpkms.KWPWrap(kek, material)
		require.NoError(t, err)

		v, err := e.client.ImportCryptoKeyVersion(t.Context(), &kmspb.ImportCryptoKeyVersionRequest{
			Parent: key.GetName(), Algorithm: kmspb.CryptoKeyVersion_HMAC_SHA256, ImportJob: job.GetName(),
			WrappedKeyMaterial: &kmspb.ImportCryptoKeyVersionRequest_RsaAesWrappedKey{
				RsaAesWrappedKey: append(head, kwp...),
			},
		})
		require.NoError(t, err)

		signed, err := e.client.MacSign(t.Context(), &kmspb.MacSignRequest{Name: v.GetName(), Data: []byte("x")})
		require.NoError(t, err)

		want := hmac.New(crypto.SHA256.New, material)
		want.Write([]byte("x"))
		assert.Equal(t, want.Sum(nil), signed.GetMac())
	})
}

func TestKWPRFC5649Vectors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, kek, key, wrapped string
	}{
		{
			"20 octets", "5840df6e29b02af1ab493b705bf16ea1ae8338f4dcc176a8",
			"c37b7e6492584340bed12207808941155068f738",
			"138bdeaa9b8fa7fc61f97742e72248ee5ae6ae5360d1ae6a5f54f373fa543b6a",
		},
		{
			"7 octets", "5840df6e29b02af1ab493b705bf16ea1ae8338f4dcc176a8",
			"466f7250617369", "afbeb0f07dfbf5419200f2ccb50bb24f",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			kek, _ := hex.DecodeString(tt.kek)
			key, _ := hex.DecodeString(tt.key)

			wrapped, err := gcpkms.KWPWrap(kek, key)
			require.NoError(t, err)
			assert.Equal(t, tt.wrapped, hex.EncodeToString(wrapped))

			out, err := gcpkms.KWPUnwrap(kek, wrapped)
			require.NoError(t, err)
			assert.Equal(t, key, out)
		})
	}
}
