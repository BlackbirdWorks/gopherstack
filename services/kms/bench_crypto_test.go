package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func benchKey(b *testing.B, in *kms.CreateKeyInput) (*kms.InMemoryBackend, string) {
	b.Helper()

	be := kms.NewInMemoryBackend()
	out, err := be.CreateKey(context.Background(), in)
	require.NoError(b, err)

	return be, out.KeyMetadata.KeyID
}

func BenchmarkKMSEncryptDecrypt(b *testing.B) {
	ctx := context.Background()
	be, id := benchKey(b, &kms.CreateKeyInput{})
	pt := []byte("hello world payload")
	ec := map[string]string{"a": "b", "c": "d"}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		enc, err := be.Encrypt(ctx, &kms.EncryptInput{KeyID: id, Plaintext: pt, EncryptionContext: ec})
		if err != nil {
			b.Fatal(err)
		}

		_, err = be.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: enc.CiphertextBlob, EncryptionContext: ec})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKMSGenerateDataKey(b *testing.B) {
	ctx := context.Background()
	be, id := benchKey(b, &kms.CreateKeyInput{})

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := be.GenerateDataKey(ctx, &kms.GenerateDataKeyInput{KeyID: id, KeySpec: "AES_256"}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKMSSignVerify(b *testing.B) {
	for _, tc := range []struct{ name, spec, alg string }{
		{"rsa2048", "RSA_2048", "RSASSA_PKCS1_V1_5_SHA_256"},
		{"ecc256", "ECC_NIST_P256", "ECDSA_SHA_256"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			be, id := benchKey(b, &kms.CreateKeyInput{KeySpec: tc.spec, KeyUsage: "SIGN_VERIFY"})
			msg := []byte("message to sign")

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				s, err := be.Sign(ctx, &kms.SignInput{KeyID: id, Message: msg, SigningAlgorithm: tc.alg})
				if err != nil {
					b.Fatal(err)
				}

				v, err := be.Verify(ctx, &kms.VerifyInput{
					KeyID: id, Message: msg, Signature: s.Signature, SigningAlgorithm: tc.alg,
				})
				if err != nil || !v.SignatureValid {
					b.Fatal(err)
				}
			}
		})
	}
}
