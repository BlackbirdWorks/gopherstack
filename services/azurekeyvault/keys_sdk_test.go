package azurekeyvault_test

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/asn1"
	"math/big"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newKeysClient(t *testing.T, s *testServer, vault string) *azkeys.Client {
	t.Helper()

	c, err := azkeys.NewClient(s.vaultURL(vault), fakeCredential{}, &azkeys.ClientOptions{
		ClientOptions:                        s.clientOptions(),
		DisableChallengeResourceVerification: true,
	})
	require.NoError(t, err)

	return c
}

func rsaPublic(t *testing.T, k *azkeys.JSONWebKey) *rsa.PublicKey {
	t.Helper()

	return &rsa.PublicKey{N: new(big.Int).SetBytes(k.N), E: int(new(big.Int).SetBytes(k.E).Int64())}
}

func TestKeys_RSA_SDK(t *testing.T) {
	t.Parallel()

	s := defaultServer(t)
	c := newKeysClient(t, s, "rsa")
	ctx := t.Context()

	created, err := c.CreateKey(ctx, "k", azkeys.CreateKeyParameters{
		Kty: new(azkeys.KeyTypeRSA), KeySize: new(int32(2048)),
	}, nil)
	require.NoError(t, err)

	pub := rsaPublic(t, created.Key)
	assert.Equal(t, 2048, pub.N.BitLen())
	assert.Equal(t, 65537, pub.E)

	version := created.Key.KID.Version()
	plain := []byte("attack at dawn")

	t.Run("oaep_roundtrip_and_external", func(t *testing.T) {
		t.Parallel()

		algs := []azkeys.EncryptionAlgorithm{azkeys.EncryptionAlgorithmRSAOAEP, azkeys.EncryptionAlgorithmRSAOAEP256}

		for _, alg := range algs {
			enc, eErr := c.Encrypt(ctx, "k", version, azkeys.KeyOperationParameters{Algorithm: &alg, Value: plain}, nil)
			require.NoError(t, eErr)

			dec, dErr := c.Decrypt(
				ctx,
				"k",
				version,
				azkeys.KeyOperationParameters{Algorithm: &alg, Value: enc.Result},
				nil,
			)
			require.NoError(t, dErr)
			assert.Equal(t, plain, dec.Result)
		}

		ct, eErr := rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, plain, nil)
		require.NoError(t, eErr)

		alg := azkeys.EncryptionAlgorithmRSAOAEP256
		dec, dErr := c.Decrypt(ctx, "k", version, azkeys.KeyOperationParameters{Algorithm: &alg, Value: ct}, nil)
		require.NoError(t, dErr)
		assert.Equal(t, plain, dec.Result)
	})

	t.Run("rsa15", func(t *testing.T) {
		t.Parallel()

		alg := azkeys.EncryptionAlgorithmRSA15
		enc, eErr := c.Encrypt(ctx, "k", version, azkeys.KeyOperationParameters{Algorithm: &alg, Value: plain}, nil)
		require.NoError(t, eErr)

		dec, dErr := c.Decrypt(
			ctx,
			"k",
			version,
			azkeys.KeyOperationParameters{Algorithm: &alg, Value: enc.Result},
			nil,
		)
		require.NoError(t, dErr)
		assert.Equal(t, plain, dec.Result)
	})

	t.Run("sign_verify", func(t *testing.T) {
		t.Parallel()

		digest := sha256.Sum256(plain)

		for _, alg := range []azkeys.SignatureAlgorithm{azkeys.SignatureAlgorithmRS256, azkeys.SignatureAlgorithmPS256} {
			sig, sErr := c.Sign(ctx, "k", version, azkeys.SignParameters{Algorithm: &alg, Value: digest[:]}, nil)
			require.NoError(t, sErr)

			if alg == azkeys.SignatureAlgorithmRS256 {
				require.NoError(t, rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], sig.Result))
			} else {
				require.NoError(t, rsa.VerifyPSS(pub, crypto.SHA256, digest[:], sig.Result,
					&rsa.PSSOptions{SaltLength: sha256.Size}))
			}

			ok, vErr := c.Verify(ctx, "k", version,
				azkeys.VerifyParameters{Algorithm: &alg, Digest: digest[:], Signature: sig.Result}, nil)
			require.NoError(t, vErr)
			assert.True(t, *ok.Value)

			bad := append([]byte(nil), sig.Result...)
			bad[0] ^= 0xff
			ok, vErr = c.Verify(ctx, "k", version,
				azkeys.VerifyParameters{Algorithm: &alg, Digest: digest[:], Signature: bad}, nil)
			require.NoError(t, vErr)
			assert.False(t, *ok.Value)
		}
	})

	t.Run("wrap_unwrap", func(t *testing.T) {
		t.Parallel()

		alg := azkeys.EncryptionAlgorithmRSAOAEP256
		cek := make([]byte, 32)
		_, _ = rand.Read(cek)

		w, wErr := c.WrapKey(ctx, "k", version, azkeys.KeyOperationParameters{Algorithm: &alg, Value: cek}, nil)
		require.NoError(t, wErr)

		u, uErr := c.UnwrapKey(ctx, "k", version, azkeys.KeyOperationParameters{Algorithm: &alg, Value: w.Result}, nil)
		require.NoError(t, uErr)
		assert.Equal(t, cek, u.Result)
	})

	t.Run("key_ops_enforced", func(t *testing.T) {
		t.Parallel()

		restricted, rErr := c.CreateKey(ctx, "signonly", azkeys.CreateKeyParameters{
			Kty: new(azkeys.KeyTypeRSA), KeyOps: []*azkeys.KeyOperation{new(azkeys.KeyOperationSign)},
		}, nil)
		require.NoError(t, rErr)

		alg := azkeys.EncryptionAlgorithmRSAOAEP
		_, eErr := c.Encrypt(ctx, "signonly", restricted.Key.KID.Version(),
			azkeys.KeyOperationParameters{Algorithm: &alg, Value: plain}, nil)
		require.Error(t, eErr)
		assert.Contains(t, eErr.Error(), "403")
	})

	t.Run("rotate_and_policy", func(t *testing.T) {
		t.Parallel()

		first, cErr := c.CreateKey(ctx, "rot", azkeys.CreateKeyParameters{Kty: new(azkeys.KeyTypeRSA)}, nil)
		require.NoError(t, cErr)

		rot, rErr := c.RotateKey(ctx, "rot", nil)
		require.NoError(t, rErr)
		assert.NotEqual(t, first.Key.KID.Version(), rot.Key.KID.Version())

		pager := c.NewListKeyPropertiesVersionsPager("rot", nil)
		n := 0

		for pager.More() {
			pg, pErr := pager.NextPage(ctx)
			require.NoError(t, pErr)

			n += len(pg.Value)
		}

		assert.Equal(t, 2, n)

		pol, pErr := c.UpdateKeyRotationPolicy(ctx, "rot", azkeys.KeyRotationPolicy{
			LifetimeActions: []*azkeys.LifetimeAction{{
				Action:  &azkeys.LifetimeActionType{Type: new(azkeys.KeyRotationPolicyActionRotate)},
				Trigger: &azkeys.LifetimeActionTrigger{TimeAfterCreate: new("P90D")},
			}},
			Attributes: &azkeys.KeyRotationPolicyAttributes{ExpiryTime: new("P2Y")},
		}, nil)
		require.NoError(t, pErr)
		assert.Equal(t, "P2Y", *pol.Attributes.ExpiryTime)

		got, gErr := c.GetKeyRotationPolicy(ctx, "rot", nil)
		require.NoError(t, gErr)
		require.Len(t, got.LifetimeActions, 1)
		assert.Equal(t, "P90D", *got.LifetimeActions[0].Trigger.TimeAfterCreate)
	})
}

func TestKeys_EC_SDK(t *testing.T) {
	t.Parallel()

	s := defaultServer(t)
	c := newKeysClient(t, s, "ec")
	ctx := t.Context()

	tests := []struct {
		ec     elliptic.Curve
		curve  azkeys.CurveName
		alg    azkeys.SignatureAlgorithm
		digest int
	}{
		{elliptic.P256(), azkeys.CurveNameP256, azkeys.SignatureAlgorithmES256, 32},
		{elliptic.P384(), azkeys.CurveNameP384, azkeys.SignatureAlgorithmES384, 48},
		{elliptic.P521(), azkeys.CurveNameP521, azkeys.SignatureAlgorithmES512, 64},
	}

	for _, tt := range tests {
		t.Run(string(tt.curve), func(t *testing.T) {
			t.Parallel()

			name := "ec-" + string(tt.curve)
			created, err := c.CreateKey(ctx, name, azkeys.CreateKeyParameters{
				Kty: new(azkeys.KeyTypeEC), Curve: new(tt.curve),
			}, nil)
			require.NoError(t, err)

			size := (tt.ec.Params().BitSize + 7) / 8
			point := append(append([]byte{4}, created.Key.X...), created.Key.Y...)
			require.Len(t, point, 1+2*size)

			pub, err := ecdsa.ParseUncompressedPublicKey(tt.ec, point)
			require.NoError(t, err)

			digest := make([]byte, tt.digest)
			_, _ = rand.Read(digest)

			alg := tt.alg
			sig, err := c.Sign(ctx, name, "", azkeys.SignParameters{Algorithm: &alg, Value: digest}, nil)
			require.NoError(t, err)
			require.Len(t, sig.Result, 2*size)

			der, err := asn1.Marshal(struct{ R, S *big.Int }{
				new(big.Int).SetBytes(sig.Result[:size]), new(big.Int).SetBytes(sig.Result[size:]),
			})
			require.NoError(t, err)
			assert.True(t, ecdsa.VerifyASN1(pub, digest, der), "signature verifies against the returned public key")

			ok, err := c.Verify(ctx, name, "",
				azkeys.VerifyParameters{Algorithm: &alg, Digest: digest, Signature: sig.Result}, nil)
			require.NoError(t, err)
			assert.True(t, *ok.Value)
		})
	}
}

func TestKeys_Lifecycle_SDK(t *testing.T) {
	t.Parallel()

	s := defaultServer(t)
	c := newKeysClient(t, s, "life")
	ctx := t.Context()

	t.Run("import_and_use", func(t *testing.T) {
		t.Parallel()

		priv, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		priv.Precompute()

		imp, err := c.ImportKey(ctx, "imp", azkeys.ImportKeyParameters{Key: &azkeys.JSONWebKey{
			Kty: new(azkeys.KeyTypeRSA),
			N:   priv.N.Bytes(), E: big.NewInt(int64(priv.E)).Bytes(), D: priv.D.Bytes(),
			P: priv.Primes[0].Bytes(), Q: priv.Primes[1].Bytes(),
			DP: priv.Precomputed.Dp.Bytes(), DQ: priv.Precomputed.Dq.Bytes(), QI: priv.Precomputed.Qinv.Bytes(),
		}}, nil)
		require.NoError(t, err)
		assert.Equal(t, priv.N.Bytes(), imp.Key.N)

		ct, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, &priv.PublicKey, []byte("secret"), nil)
		require.NoError(t, err)

		alg := azkeys.EncryptionAlgorithmRSAOAEP256
		dec, err := c.Decrypt(ctx, "imp", "", azkeys.KeyOperationParameters{Algorithm: &alg, Value: ct}, nil)
		require.NoError(t, err)
		assert.Equal(t, []byte("secret"), dec.Result)
	})

	t.Run("delete_recover_purge_backup", func(t *testing.T) {
		t.Parallel()

		_, err := c.CreateKey(ctx, "del", azkeys.CreateKeyParameters{Kty: new(azkeys.KeyTypeEC)}, nil)
		require.NoError(t, err)

		bk, err := c.BackupKey(ctx, "del", nil)
		require.NoError(t, err)

		del, err := c.DeleteKey(ctx, "del", nil)
		require.NoError(t, err)
		assert.NotNil(t, del.DeletedDate)

		_, err = c.GetKey(ctx, "del", "", nil)
		require.Error(t, err)

		_, err = c.RecoverDeletedKey(ctx, "del", nil)
		require.NoError(t, err)

		_, err = c.GetKey(ctx, "del", "", nil)
		require.NoError(t, err)

		_, err = c.DeleteKey(ctx, "del", nil)
		require.NoError(t, err)
		_, err = c.PurgeDeletedKey(ctx, "del", nil)
		require.NoError(t, err)

		_, err = c.RestoreKey(ctx, azkeys.RestoreKeyParameters{KeyBackup: bk.Value}, nil)
		require.NoError(t, err)

		_, err = c.GetKey(ctx, "del", "", nil)
		require.NoError(t, err)
	})

	t.Run("random_bytes", func(t *testing.T) {
		t.Parallel()

		r, err := c.GetRandomBytes(ctx, azkeys.GetRandomBytesParameters{Count: new(int32(24))}, nil)
		require.NoError(t, err)
		assert.Len(t, r.Value, 24)
	})
}
