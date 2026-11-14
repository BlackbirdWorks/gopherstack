package gcpkms_test

import (
	"testing"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
	"github.com/blackbirdworks/gopherstack/services/gcpkms"
)

func TestCheckKey(t *testing.T) {
	t.Parallel()

	const ring = "projects/p/locations/us-east1/keyRings/r"

	tests := []struct {
		setup    func(t *testing.T, b *gcpkms.InMemoryBackend)
		want     error
		name     string
		key      string
		location string
	}{
		{name: "ok", key: ring + "/cryptoKeys/sym", location: "us-east1"},
		{name: "any location", key: ring + "/cryptoKeys/sym"},
		{name: "missing", key: ring + "/cryptoKeys/none", want: cmek.ErrNotFound},
		{name: "wrong location", key: ring + "/cryptoKeys/sym", location: "global", want: cmek.ErrLocation},
		{name: "signing key", key: ring + "/cryptoKeys/sign", want: cmek.ErrPurpose},
		{
			name: "disabled", key: ring + "/cryptoKeys/sym", want: cmek.ErrDisabled,
			setup: func(t *testing.T, b *gcpkms.InMemoryBackend) {
				t.Helper()

				_, err := b.UpdateCryptoKeyVersion(&kmspb.UpdateCryptoKeyVersionRequest{
					CryptoKeyVersion: &kmspb.CryptoKeyVersion{
						Name: ring + "/cryptoKeys/sym/cryptoKeyVersions/1", State: kmspb.CryptoKeyVersion_DISABLED,
					},
					UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}},
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := gcpkms.NewInMemoryBackend()
			_, err := b.CreateKeyRing(&kmspb.CreateKeyRingRequest{
				Parent: "projects/p/locations/us-east1", KeyRingId: "r",
			})
			require.NoError(t, err)

			_, err = b.CreateCryptoKey(&kmspb.CreateCryptoKeyRequest{
				Parent: ring, CryptoKeyId: "sym", CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
			})
			require.NoError(t, err)

			_, err = b.CreateCryptoKey(&kmspb.CreateCryptoKeyRequest{
				Parent: ring, CryptoKeyId: "sign", CryptoKey: &kmspb.CryptoKey{
					Purpose: kmspb.CryptoKey_ASYMMETRIC_SIGN,
					VersionTemplate: &kmspb.CryptoKeyVersionTemplate{
						Algorithm: kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256,
					},
				},
			})
			require.NoError(t, err)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			err = b.CheckKey(tt.key, tt.location)
			if tt.want == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, tt.want)
		})
	}
}
