package gcpsecretmanager_test

import (
	"testing"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/services/gcpkms"
	"github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
)

func newKMS(t *testing.T) *gcpkms.InMemoryBackend {
	t.Helper()

	b := gcpkms.NewInMemoryBackend()

	for _, loc := range []string{"global", "us-east1"} {
		_, err := b.CreateKeyRing(&kmspb.CreateKeyRingRequest{Parent: "projects/p/locations/" + loc, KeyRingId: "r"})
		require.NoError(t, err)

		_, err = b.CreateCryptoKey(&kmspb.CreateCryptoKeyRequest{
			Parent: "projects/p/locations/" + loc + "/keyRings/r", CryptoKeyId: "sym",
			CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
		})
		require.NoError(t, err)
	}

	_, err := b.CreateCryptoKey(&kmspb.CreateCryptoKeyRequest{
		Parent: "projects/p/locations/global/keyRings/r", CryptoKeyId: "sign",
		CryptoKey: &kmspb.CryptoKey{
			Purpose:         kmspb.CryptoKey_ASYMMETRIC_SIGN,
			VersionTemplate: &kmspb.CryptoKeyVersionTemplate{Algorithm: kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256},
		},
	})
	require.NoError(t, err)

	return b
}

func disableKey(t *testing.T, b *gcpkms.InMemoryBackend, key string) {
	t.Helper()

	_, err := b.UpdateCryptoKeyVersion(&kmspb.UpdateCryptoKeyVersionRequest{
		CryptoKeyVersion: &kmspb.CryptoKeyVersion{
			Name:  key + "/cryptoKeyVersions/1",
			State: kmspb.CryptoKeyVersion_DISABLED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}},
	})
	require.NoError(t, err)
}

func autoSecret(key string) *secretmanagerpb.Secret {
	return &secretmanagerpb.Secret{Replication: &secretmanagerpb.Replication{
		Replication: &secretmanagerpb.Replication_Automatic_{Automatic: &secretmanagerpb.Replication_Automatic{
			CustomerManagedEncryption: &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: key},
		}},
	}}
}

func userSecret(loc, key string) *secretmanagerpb.Secret {
	return &secretmanagerpb.Secret{Replication: &secretmanagerpb.Replication{
		Replication: &secretmanagerpb.Replication_UserManaged_{UserManaged: &secretmanagerpb.Replication_UserManaged{
			Replicas: []*secretmanagerpb.Replication_UserManaged_Replica{{
				Location:                  loc,
				CustomerManagedEncryption: &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: key},
			}},
		}},
	}}
}

func TestCreateSecretCMEK(t *testing.T) {
	t.Parallel()

	const (
		global = "projects/p/locations/global/keyRings/r/cryptoKeys/sym"
		east   = "projects/p/locations/us-east1/keyRings/r/cryptoKeys/sym"
	)

	tests := []struct {
		secret *secretmanagerpb.Secret
		name   string
		code   codes.Code
		wired  bool
	}{
		{name: "automatic global", secret: autoSecret(global), wired: true},
		{name: "automatic regional key", secret: autoSecret(east), wired: true, code: codes.InvalidArgument},
		{name: "user managed match", secret: userSecret("us-east1", east), wired: true},
		{
			name:   "user managed mismatch",
			secret: userSecret("us-east1", global),
			wired:  true,
			code:   codes.InvalidArgument,
		},
		{
			name: "missing key", wired: true, code: codes.InvalidArgument,
			secret: autoSecret("projects/p/locations/global/keyRings/r/cryptoKeys/none"),
		},
		{
			name: "signing key", wired: true, code: codes.InvalidArgument,
			secret: autoSecret("projects/p/locations/global/keyRings/r/cryptoKeys/sign"),
		},
		{name: "unwired missing key", secret: autoSecret(east)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := gcpsecretmanager.NewInMemoryBackend()
			if tt.wired {
				b.SetKeyChecker(newKMS(t))
			}

			_, err := b.CreateSecret(&secretmanagerpb.CreateSecretRequest{
				Parent: "projects/p", SecretId: "s", Secret: tt.secret,
			})
			if tt.code == codes.OK {
				require.NoError(t, err)

				return
			}

			require.Equal(t, tt.code, status.Code(err))
		})
	}
}

func TestCMEKKeyDisabledAfterCreate(t *testing.T) {
	t.Parallel()

	const key = "projects/p/locations/global/keyRings/r/cryptoKeys/sym"

	kb := newKMS(t)
	b := gcpsecretmanager.NewInMemoryBackend()
	b.SetKeyChecker(kb)

	_, err := b.CreateSecret(
		&secretmanagerpb.CreateSecretRequest{Parent: "projects/p", SecretId: "s", Secret: autoSecret(key)},
	)
	require.NoError(t, err)

	add := func() error {
		_, e := b.AddSecretVersion(&secretmanagerpb.AddSecretVersionRequest{
			Parent: "projects/p/secrets/s", Payload: &secretmanagerpb.SecretPayload{Data: []byte("x")},
		})

		return e
	}
	access := func() error {
		_, e := b.AccessSecretVersion("projects/p/secrets/s/versions/1")

		return e
	}

	require.NoError(t, add())
	require.NoError(t, access())

	disableKey(t, kb, key)

	require.Equal(t, codes.FailedPrecondition, status.Code(add()))
	require.Equal(t, codes.FailedPrecondition, status.Code(access()))
}
