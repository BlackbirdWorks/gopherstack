package gcpeventarc_test

import (
	"testing"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/services/gcpeventarc"
	"github.com/blackbirdworks/gopherstack/services/gcpkms"
)

const (
	keyRing  = "projects/" + project + "/locations/" + location + "/keyRings/r"
	symKey   = keyRing + "/cryptoKeys/sym"
	signKey  = keyRing + "/cryptoKeys/sign"
	missing  = keyRing + "/cryptoKeys/none"
	otherKey = "projects/" + project + "/locations/europe-west1/keyRings/r/cryptoKeys/sym"
)

func newKMS(t *testing.T) *gcpkms.InMemoryBackend {
	t.Helper()

	b := gcpkms.NewInMemoryBackend()

	for _, loc := range []string{location, "europe-west1"} {
		_, err := b.CreateKeyRing(&kmspb.CreateKeyRingRequest{
			Parent: "projects/" + project + "/locations/" + loc, KeyRingId: "r",
		})
		require.NoError(t, err)

		_, err = b.CreateCryptoKey(&kmspb.CreateCryptoKeyRequest{
			Parent: "projects/" + project + "/locations/" + loc + "/keyRings/r", CryptoKeyId: "sym",
			CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
		})
		require.NoError(t, err)
	}

	_, err := b.CreateCryptoKey(&kmspb.CreateCryptoKeyRequest{
		Parent: keyRing, CryptoKeyId: "sign", CryptoKey: &kmspb.CryptoKey{
			Purpose:         kmspb.CryptoKey_ASYMMETRIC_SIGN,
			VersionTemplate: &kmspb.CryptoKeyVersionTemplate{Algorithm: kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256},
		},
	})
	require.NoError(t, err)

	return b
}

func TestChannelCMEK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		key   string
		code  codes.Code
		wired bool
	}{
		{name: "ok", key: symKey, wired: true},
		{name: "missing", key: missing, wired: true, code: codes.InvalidArgument},
		{name: "other region", key: otherKey, wired: true, code: codes.InvalidArgument},
		{name: "wrong purpose", key: signKey, wired: true, code: codes.InvalidArgument},
		{name: "unwired", key: missing},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := gcpeventarc.NewInMemoryBackend()
			if tt.wired {
				b.SetKeyChecker(newKMS(t))
			}

			_, err := b.CreateChannel(&eventarcpb.CreateChannelRequest{
				Parent: parent, ChannelId: "chan",
				Channel: &eventarcpb.Channel{CryptoKeyName: tt.key},
			})
			require.Equal(t, tt.code, code(err))

			_, err = b.UpdateGoogleChannelConfig(&eventarcpb.UpdateGoogleChannelConfigRequest{
				GoogleChannelConfig: &eventarcpb.GoogleChannelConfig{
					Name: parent + "/googleChannelConfig", CryptoKeyName: tt.key,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"crypto_key_name"}},
			})
			require.Equal(t, tt.code, code(err))
		})
	}
}

func TestPublishFailsWhenChannelKeyDisabled(t *testing.T) {
	t.Parallel()

	kb := newKMS(t)
	b := gcpeventarc.NewInMemoryBackend()
	b.SetKeyChecker(kb)

	_, err := b.CreateChannel(&eventarcpb.CreateChannelRequest{
		Parent: parent, ChannelId: "chan", Channel: &eventarcpb.Channel{CryptoKeyName: symKey},
	})
	require.NoError(t, err)

	ev := []map[string]any{{
		"id": "1", "source": "//example.com", "specversion": "1.0", "type": customType,
	}}
	ch := parent + "/channels/chan"

	require.NoError(t, b.PublishToChannel(ch, ev))

	_, err = kb.UpdateCryptoKeyVersion(&kmspb.UpdateCryptoKeyVersionRequest{
		CryptoKeyVersion: &kmspb.CryptoKeyVersion{
			Name:  symKey + "/cryptoKeyVersions/1",
			State: kmspb.CryptoKeyVersion_DISABLED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}},
	})
	require.NoError(t, err)

	require.Equal(t, codes.FailedPrecondition, code(b.PublishToChannel(ch, ev)))
}

func TestMessageBusCMEK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		code codes.Code
	}{
		{name: "ok", key: symKey},
		{name: "missing", key: missing, code: codes.InvalidArgument},
		{name: "other region", key: otherKey, code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.handler.Backend.SetKeyChecker(newKMS(t))

			_, err := e.client.CreateMessageBus(t.Context(), &eventarcpb.CreateMessageBusRequest{
				Parent: parent, MessageBusId: "bus-one", MessageBus: &eventarcpb.MessageBus{CryptoKeyName: tt.key},
			})
			require.Equal(t, tt.code, code(err))
		})
	}
}
