package gcpkms_test

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"testing"
	"time"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/services/gcpkms"
)

func wrapKey(t *testing.T, method kmspb.ImportJob_ImportMethod, pubPEM string, key []byte) []byte {
	t.Helper()

	pub := parsePub(t, pubPEM).(*rsa.PublicKey)

	if method == kmspb.ImportJob_RSA_OAEP_3072_SHA256 || method == kmspb.ImportJob_RSA_OAEP_4096_SHA256 {
		ct, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, key, nil)
		require.NoError(t, err)

		return ct
	}

	kek := make([]byte, 32)
	_, err := rand.Read(kek)
	require.NoError(t, err)

	rsaPart, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, kek, nil)
	require.NoError(t, err)

	kwp, err := gcpkms.KWPWrap(kek, key)
	require.NoError(t, err)

	return append(rsaPart, kwp...)
}

func TestKeyWrapRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		size int
	}{
		{"8 bytes", 8}, {"unaligned 20", 20}, {"32 bytes", 32}, {"pkcs8-sized 138", 138},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			kek := make([]byte, 32)
			key := make([]byte, tt.size)
			_, _ = rand.Read(kek)
			_, _ = rand.Read(key)

			w, err := gcpkms.KWPWrap(kek, key)
			require.NoError(t, err)

			out, err := gcpkms.KWPUnwrap(kek, w)
			require.NoError(t, err)
			assert.Equal(t, key, out)

			w[len(w)-1] ^= 1
			_, err = gcpkms.KWPUnwrap(kek, w)
			assert.Error(t, err)
		})
	}
}

func TestImportSymmetric(t *testing.T) {
	t.Parallel()

	methods := []kmspb.ImportJob_ImportMethod{
		kmspb.ImportJob_RSA_OAEP_3072_SHA256_AES_256,
		kmspb.ImportJob_RSA_OAEP_3072_SHA256,
	}

	e := newEnv(t)
	ring := e.ring(t, "r")

	for _, method := range methods {
		t.Run(method.String(), func(t *testing.T) {
			t.Parallel()

			job, err := e.client.CreateImportJob(t.Context(), &kmspb.CreateImportJobRequest{
				Parent: ring, ImportJobId: "job-" + method.String(),
				ImportJob: &kmspb.ImportJob{ImportMethod: method, ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE},
			})
			require.NoError(t, err)
			assert.Equal(t, kmspb.ImportJob_ACTIVE, job.GetState())
			assert.NotEmpty(t, job.GetPublicKey().GetPem())
			assert.True(t, e.clk.now().Add(72*time.Hour).Equal(job.GetExpireTime().AsTime()))

			key := e.key(t, ring, "imp-"+method.String(), kmspb.CryptoKey_ENCRYPT_DECRYPT,
				kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION, func(k *kmspb.CryptoKey) { k.ImportOnly = true })
			assert.Nil(t, key.GetPrimary())

			material := make([]byte, 32)
			_, _ = rand.Read(material)

			v, err := e.client.ImportCryptoKeyVersion(t.Context(), &kmspb.ImportCryptoKeyVersionRequest{
				Parent: key.GetName(), Algorithm: kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION,
				ImportJob: job.GetName(),
				WrappedKeyMaterial: &kmspb.ImportCryptoKeyVersionRequest_RsaAesWrappedKey{
					RsaAesWrappedKey: wrapKey(t, method, job.GetPublicKey().GetPem(), material),
				},
			})
			require.NoError(t, err)
			assert.Equal(t, kmspb.CryptoKeyVersion_ENABLED, v.GetState())
			assert.Equal(t, job.GetName(), v.GetImportJob())
			assert.NotNil(t, v.GetImportTime())

			_, err = e.client.UpdateCryptoKeyPrimaryVersion(t.Context(), &kmspb.UpdateCryptoKeyPrimaryVersionRequest{
				Name: key.GetName(), CryptoKeyVersionId: "1",
			})
			require.NoError(t, err)

			enc, err := e.client.Encrypt(
				t.Context(),
				&kmspb.EncryptRequest{Name: key.GetName(), Plaintext: []byte("imported")},
			)
			require.NoError(t, err)

			// header is 1 tag byte + 8 byte version id, then a 12 byte nonce
			block, err := aes.NewCipher(material)
			require.NoError(t, err)
			gcm, err := cipher.NewGCM(block)
			require.NoError(t, err)

			body := enc.GetCiphertext()[9:]
			pt, err := gcm.Open(nil, body[:12], body[12:], nil)
			require.NoError(t, err)
			assert.Equal(t, "imported", string(pt), "ciphertext must be produced with the imported key")
		})
	}
}

func TestImportAsymmetricAndReimport(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "r")
	method := kmspb.ImportJob_RSA_OAEP_3072_SHA256_AES_256

	job, err := e.client.CreateImportJob(t.Context(), &kmspb.CreateImportJobRequest{
		Parent: ring, ImportJobId: "job", ImportJob: &kmspb.ImportJob{
			ImportMethod: method, ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
		},
	})
	require.NoError(t, err)

	key := e.key(t, ring, "ec", kmspb.CryptoKey_ASYMMETRIC_SIGN, kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256,
		func(k *kmspb.CryptoKey) {
			k.ImportOnly = true
			k.DestroyScheduledDuration = durationpb.New(24 * time.Hour)
		})

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	der, err := x509.MarshalPKCS8PrivateKey(priv)
	require.NoError(t, err)

	importReq := func(version string, alg kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm, material []byte) error {
		_, cerr := e.client.ImportCryptoKeyVersion(t.Context(), &kmspb.ImportCryptoKeyVersionRequest{
			Parent: key.GetName(), CryptoKeyVersion: version, Algorithm: alg, ImportJob: job.GetName(),
			WrappedKeyMaterial: &kmspb.ImportCryptoKeyVersionRequest_RsaAesWrappedKey{
				RsaAesWrappedKey: wrapKey(t, method, job.GetPublicKey().GetPem(), material),
			},
		})

		return cerr
	}

	require.NoError(t, importReq("", kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256, der))

	name := key.GetName() + "/cryptoKeyVersions/1"

	pk, err := e.client.GetPublicKey(t.Context(), &kmspb.GetPublicKeyRequest{Name: name})
	require.NoError(t, err)
	assert.Equal(t, &priv.PublicKey, parsePub(t, pk.GetPem()))

	sum := sha256.Sum256([]byte("m"))
	sig, err := e.client.AsymmetricSign(t.Context(), &kmspb.AsymmetricSignRequest{
		Name: name, Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: sum[:]}},
	})
	require.NoError(t, err)
	assert.True(t, ecdsa.VerifyASN1(&priv.PublicKey, sum[:], sig.GetSignature()))

	other, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	require.NoError(t, err)

	otherDER, err := x509.MarshalPKCS8PrivateKey(other)
	require.NoError(t, err)

	tests := []struct {
		call func() error
		name string
		want codes.Code
	}{
		{
			func() error { return importReq("", kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256, otherDER) },
			"curve mismatch",
			codes.InvalidArgument,
		},
		{
			func() error { return importReq("", kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256, []byte("junk")) },
			"not pkcs8",
			codes.InvalidArgument,
		},
		{
			func() error { return importReq("", kmspb.CryptoKeyVersion_HMAC_SHA256, der) },
			"alg purpose mismatch",
			codes.InvalidArgument,
		},
		{
			func() error { return importReq(name, kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256, der) },
			"reimport enabled",
			codes.FailedPrecondition,
		},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.want, status.Code(tt.call()), tt.name)
	}

	_, err = e.client.DestroyCryptoKeyVersion(t.Context(), &kmspb.DestroyCryptoKeyVersionRequest{Name: name})
	require.NoError(t, err)

	e.clk.advance(25 * time.Hour)

	require.NoError(t, importReq(name, kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256, der))

	again, err := e.client.GetCryptoKeyVersion(t.Context(), &kmspb.GetCryptoKeyVersionRequest{Name: name})
	require.NoError(t, err)
	assert.Equal(t, kmspb.CryptoKeyVersion_ENABLED, again.GetState())

	e.clk.advance(48 * time.Hour)

	assert.Equal(
		t,
		codes.FailedPrecondition,
		status.Code(importReq("", kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256, der)),
		"job expired after 3 days",
	)

	expired, err := e.client.GetImportJob(t.Context(), &kmspb.GetImportJobRequest{Name: job.GetName()})
	require.NoError(t, err)
	assert.Equal(t, kmspb.ImportJob_EXPIRED, expired.GetState())
}

func TestImportJobs(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "r")
	create := func(id string, m kmspb.ImportJob_ImportMethod, l kmspb.ProtectionLevel) error {
		_, err := e.client.CreateImportJob(t.Context(), &kmspb.CreateImportJobRequest{
			Parent: ring, ImportJobId: id, ImportJob: &kmspb.ImportJob{ImportMethod: m, ProtectionLevel: l},
		})

		return err
	}

	require.NoError(t, create("a", kmspb.ImportJob_RSA_OAEP_3072_SHA256, kmspb.ProtectionLevel_HSM))

	tests := []struct {
		call func() error
		name string
		want codes.Code
	}{
		{
			func() error { return create("a", kmspb.ImportJob_RSA_OAEP_3072_SHA256, kmspb.ProtectionLevel_HSM) },
			"duplicate",
			codes.AlreadyExists,
		},
		{func() error {
			return create("b", kmspb.ImportJob_IMPORT_METHOD_UNSPECIFIED, kmspb.ProtectionLevel_HSM)
		}, "unspecified method", codes.InvalidArgument},
		{
			func() error { return create("c", kmspb.ImportJob_RSA_OAEP_3072_SHA256, kmspb.ProtectionLevel_EXTERNAL) },
			"bad level",
			codes.InvalidArgument,
		},
		{func() error {
			_, err := e.client.GetImportJob(t.Context(), &kmspb.GetImportJobRequest{Name: ring + "/importJobs/zzz"})

			return err
		}, "get missing", codes.NotFound},
		{func() error {
			_, err := e.client.ListImportJobs(t.Context(), &kmspb.ListImportJobsRequest{Parent: ring}).Next()

			return err
		}, "list", codes.OK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, status.Code(tt.call()))
		})
	}
}
