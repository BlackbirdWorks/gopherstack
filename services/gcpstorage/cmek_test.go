package gcpstorage_test

import (
	"encoding/json/v2"
	"testing"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/gcpkms"
	"github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

const (
	cmekRing = "projects/p/locations/us-east1/keyRings/r"
	cmekSym  = cmekRing + "/cryptoKeys/sym"
	cmekSign = cmekRing + "/cryptoKeys/sign"
	cmekNone = cmekRing + "/cryptoKeys/none"
	cmekWest = "projects/p/locations/us-west1/keyRings/r/cryptoKeys/sym"
)

func newKMS(t *testing.T) *gcpkms.InMemoryBackend {
	t.Helper()

	b := gcpkms.NewInMemoryBackend()

	for _, l := range []string{"us-east1", "us-west1"} {
		_, err := b.CreateKeyRing(&kmspb.CreateKeyRingRequest{Parent: "projects/p/locations/" + l, KeyRingId: "r"})
		require.NoError(t, err)

		_, err = b.CreateCryptoKey(&kmspb.CreateCryptoKeyRequest{
			Parent: "projects/p/locations/" + l + "/keyRings/r", CryptoKeyId: "sym",
			CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
		})
		require.NoError(t, err)
	}

	_, err := b.CreateCryptoKey(&kmspb.CreateCryptoKeyRequest{
		Parent: cmekRing, CryptoKeyId: "sign", CryptoKey: &kmspb.CryptoKey{
			Purpose:         kmspb.CryptoKey_ASYMMETRIC_SIGN,
			VersionTemplate: &kmspb.CryptoKeyVersionTemplate{Algorithm: kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256},
		},
	})
	require.NoError(t, err)

	return b
}

func bucketPatch(t *testing.T, key string) gcpstorage.Patch {
	t.Helper()

	enc, err := json.Marshal(map[string]string{"defaultKmsKeyName": key}, jsonx.V1())
	require.NoError(t, err)

	name, _ := json.Marshal("bkt", jsonx.V1())
	loc, _ := json.Marshal("US-EAST1", jsonx.V1())

	return gcpstorage.Patch{"name": name, "location": loc, "encryption": enc}
}

func TestBucketDefaultKMSKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		wired   bool
		wantErr bool
	}{
		{name: "ok", key: cmekSym, wired: true},
		{name: "missing", key: cmekNone, wired: true, wantErr: true},
		{name: "other location", key: cmekWest, wired: true, wantErr: true},
		{name: "wrong purpose", key: cmekSign, wired: true, wantErr: true},
		{name: "unwired", key: cmekNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := gcpstorage.NewInMemoryBackend()
			if tt.wired {
				b.SetKMSChecker(newKMS(t))
			}

			_, err := b.CreateBucket("p", bucketPatch(t, tt.key))
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestObjectKMSKey(t *testing.T) {
	t.Parallel()

	reqKey := func(k string) gcpstorage.Patch {
		raw, _ := json.Marshal(k, jsonx.V1())

		return gcpstorage.Patch{"kmsKeyName": raw}
	}

	tests := []struct {
		patch   gcpstorage.Patch
		name    string
		bucket  string
		want    string
		wantErr bool
	}{
		{name: "request key", bucket: "", patch: reqKey(cmekSym), want: cmekSym + "/cryptoKeyVersions/1"},
		{name: "bucket default", bucket: cmekSym, want: cmekSym + "/cryptoKeyVersions/1"},
		{
			name:   "request overrides default",
			bucket: cmekSym,
			patch:  reqKey(cmekSym),
			want:   cmekSym + "/cryptoKeyVersions/1",
		},
		{name: "missing key", patch: reqKey(cmekNone), wantErr: true},
		{name: "other location", patch: reqKey(cmekWest), wantErr: true},
		{name: "wrong purpose", patch: reqKey(cmekSign), wantErr: true},
		{name: "no key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := gcpstorage.NewInMemoryBackend()
			b.SetKMSChecker(newKMS(t))

			bp := bucketPatch(t, tt.bucket)
			if tt.bucket == "" {
				delete(bp, "encryption")
			}

			_, err := b.CreateBucket("p", bp)
			require.NoError(t, err)

			o, err := b.PutObject("bkt", "obj", []byte("x"), tt.patch, gcpstorage.Preconds{})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, o.KMSKeyName)
		})
	}
}
