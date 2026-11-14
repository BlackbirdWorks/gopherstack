package gcplogging_test

import (
	"testing"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/services/gcpkms"
	"github.com/blackbirdworks/gopherstack/services/gcplogging"
)

const (
	keyEast = "projects/test-project/locations/us-east1/keyRings/r/cryptoKeys/sym"
	keyWest = "projects/test-project/locations/us-west1/keyRings/r/cryptoKeys/sym"
	keySign = "projects/test-project/locations/us-east1/keyRings/r/cryptoKeys/sign"
	keyNone = "projects/test-project/locations/us-east1/keyRings/r/cryptoKeys/none"
)

func newKMS(t *testing.T) *gcpkms.InMemoryBackend {
	t.Helper()

	b := gcpkms.NewInMemoryBackend()

	for _, l := range []string{"us-east1", "us-west1"} {
		_, err := b.CreateKeyRing(
			&kmspb.CreateKeyRingRequest{Parent: "projects/test-project/locations/" + l, KeyRingId: "r"},
		)
		require.NoError(t, err)

		_, err = b.CreateCryptoKey(&kmspb.CreateCryptoKeyRequest{
			Parent: "projects/test-project/locations/" + l + "/keyRings/r", CryptoKeyId: "sym",
			CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
		})
		require.NoError(t, err)
	}

	_, err := b.CreateCryptoKey(&kmspb.CreateCryptoKeyRequest{
		Parent: "projects/test-project/locations/us-east1/keyRings/r", CryptoKeyId: "sign",
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

func TestCmekKeyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		key      string
		location string
		disable  bool
		wired    bool
		code     codes.Code
	}{
		{name: "ok", key: keyEast, location: "us-east1", wired: true},
		{name: "missing", key: keyNone, location: "us-east1", wired: true, code: codes.InvalidArgument},
		{name: "other location", key: keyWest, location: "us-east1", wired: true, code: codes.InvalidArgument},
		{name: "wrong purpose", key: keySign, location: "us-east1", wired: true, code: codes.InvalidArgument},
		{
			name:     "disabled",
			key:      keyEast,
			location: "us-east1",
			wired:    true,
			disable:  true,
			code:     codes.FailedPrecondition,
		},
		{name: "global location", key: keyEast, location: "global", wired: true, code: codes.InvalidArgument},
		{name: "unwired", key: keyNone, location: "us-east1"},
	}

	for _, tt := range tests {
		t.Run(tt.name+" bucket", func(t *testing.T) {
			t.Parallel()

			kb := newKMS(t)
			b := gcplogging.NewInMemoryBackend()

			if tt.wired {
				b.SetKeyChecker(kb)
			}

			if tt.disable {
				disableKey(t, kb, tt.key)
			}

			_, err := b.CreateBucket(&loggingpb.CreateBucketRequest{
				Parent: proj + "/locations/" + tt.location, BucketId: "b",
				Bucket: &loggingpb.LogBucket{CmekSettings: &loggingpb.CmekSettings{KmsKeyName: tt.key}},
			})
			require.Equal(t, tt.code, status.Code(err))
		})
	}

	for _, tt := range tests {
		if tt.location == "global" {
			continue
		}

		t.Run(tt.name+" cmek settings", func(t *testing.T) {
			t.Parallel()

			kb := newKMS(t)
			b := gcplogging.NewInMemoryBackend()

			if tt.wired {
				b.SetKeyChecker(kb)
			}

			if tt.disable {
				disableKey(t, kb, tt.key)
			}

			_, err := b.UpdateCmekSettings(&loggingpb.UpdateCmekSettingsRequest{
				Name:         proj + "/cmekSettings",
				CmekSettings: &loggingpb.CmekSettings{KmsKeyName: tt.key},
			})

			want := tt.code
			if tt.name == "other location" {
				want = codes.OK
			}

			require.Equal(t, want, status.Code(err))
		})
	}
}
