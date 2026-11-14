package gcpredis_test

import (
	"testing"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"cloud.google.com/go/redis/apiv1/redispb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/services/gcpkms"
)

const (
	keyHome   = "projects/test-project/locations/us-central1/keyRings/r/cryptoKeys/sym"
	keySign   = "projects/test-project/locations/us-central1/keyRings/r/cryptoKeys/sign"
	keyNone   = "projects/test-project/locations/us-central1/keyRings/r/cryptoKeys/none"
	keyGlobal = "projects/test-project/locations/global/keyRings/r/cryptoKeys/sym"
)

func newKMS(t *testing.T) *gcpkms.InMemoryBackend {
	t.Helper()

	b := gcpkms.NewInMemoryBackend()

	for _, l := range []string{"us-central1", "global"} {
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
		Parent: "projects/test-project/locations/us-central1/keyRings/r", CryptoKeyId: "sign",
		CryptoKey: &kmspb.CryptoKey{
			Purpose:         kmspb.CryptoKey_ASYMMETRIC_SIGN,
			VersionTemplate: &kmspb.CryptoKeyVersionTemplate{Algorithm: kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256},
		},
	})
	require.NoError(t, err)

	return b
}

func TestCreateInstanceCMEK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		key   string
		code  codes.Code
		wired bool
	}{
		{name: "ok", key: keyHome, wired: true},
		{name: "missing", key: keyNone, wired: true, code: codes.InvalidArgument},
		{name: "global key", key: keyGlobal, wired: true, code: codes.InvalidArgument},
		{name: "wrong purpose", key: keySign, wired: true, code: codes.InvalidArgument},
		{name: "bad shape", key: "not-a-key", wired: true, code: codes.InvalidArgument},
		{name: "unwired bad shape", key: "not-a-key"},
		{name: "unwired", key: keyNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			if tt.wired {
				e.handler.Backend.SetKeyChecker(newKMS(t))
			}

			_, err := e.client.CreateInstance(t.Context(), &redispb.CreateInstanceRequest{
				Parent: loc, InstanceId: "inst",
				Instance: basic(func(i *redispb.Instance) { i.CustomerManagedKey = tt.key }),
			})
			require.Equal(t, tt.code, status.Code(err))
		})
	}
}

func TestDisabledKeySuspendsInstance(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	kb := newKMS(t)
	e.handler.Backend.SetKeyChecker(kb)

	e.create(t, "inst", basic(func(i *redispb.Instance) { i.CustomerManagedKey = keyHome }))

	setState := func(s kmspb.CryptoKeyVersion_CryptoKeyVersionState) {
		_, err := kb.UpdateCryptoKeyVersion(&kmspb.UpdateCryptoKeyVersionRequest{
			CryptoKeyVersion: &kmspb.CryptoKeyVersion{Name: keyHome + "/cryptoKeyVersions/1", State: s},
			UpdateMask:       &fieldmaskpb.FieldMask{Paths: []string{"state"}},
		})
		require.NoError(t, err)
	}

	get := func() *redispb.Instance {
		got, err := e.client.GetInstance(t.Context(), &redispb.GetInstanceRequest{Name: loc + "/instances/inst"})
		require.NoError(t, err)

		return got
	}

	setState(kmspb.CryptoKeyVersion_DISABLED)

	got := get()
	assert.Equal(t, redispb.Instance_STATE_UNSPECIFIED, got.GetState())
	assert.Equal(t, []redispb.Instance_SuspensionReason{redispb.Instance_CUSTOMER_MANAGED_KEY_ISSUE},
		got.GetSuspensionReasons())

	setState(kmspb.CryptoKeyVersion_ENABLED)

	got = get()
	assert.Equal(t, redispb.Instance_READY, got.GetState())
	assert.Empty(t, got.GetSuspensionReasons())
}
