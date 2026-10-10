package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

type mrKeySet struct {
	b        *kms.InMemoryBackend
	primary  string
	replicas []string
}

func newImportedMultiRegionSet(t *testing.T, replicaRegions ...string) *mrKeySet {
	t.Helper()

	ctx := context.Background()
	b := kms.NewInMemoryBackend()

	created, err := b.CreateKey(ctx, &kms.CreateKeyInput{Origin: kms.KeyOriginExternal, MultiRegion: true})
	require.NoError(t, err)

	set := &mrKeySet{b: b, primary: created.KeyMetadata.Arn}

	_, err = b.ImportKeyMaterialWithResult(ctx, &kms.ImportKeyMaterialInput{
		KeyID: set.primary, KeyMaterial: materialBytes(1),
	})
	require.NoError(t, err)

	for _, region := range replicaRegions {
		rep, repErr := b.ReplicateKey(ctx, &kms.ReplicateKeyInput{KeyID: set.primary, ReplicaRegion: region})
		require.NoError(t, repErr)

		arn := rep.ReplicaKeyMetadata.Arn
		_, err = b.ImportKeyMaterialWithResult(ctx, &kms.ImportKeyMaterialInput{
			KeyID: arn, KeyMaterial: materialBytes(1),
		})
		require.NoError(t, err)

		set.replicas = append(set.replicas, arn)
	}

	return set
}

func (s *mrKeySet) states(t *testing.T, keyID string) map[string]string {
	t.Helper()

	out, err := s.b.ListKeyRotations(context.Background(), &kms.ListKeyRotationsInput{
		KeyID: keyID, IncludeKeyMaterial: "ALL_KEY_MATERIAL",
	})
	require.NoError(t, err)

	got := map[string]string{}
	for _, r := range out.Rotations {
		got[r.KeyMaterialID] = r.KeyMaterialState
	}

	return got
}

func TestMultiRegionImportedKeyRotation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	set := newImportedMultiRegionSet(t, "us-west-2", "eu-west-1")

	first := set.states(t, set.primary)
	require.Len(t, first, 1)

	var id1 string
	for id := range first {
		id1 = id
	}

	assert.Equal(t, first, set.states(t, set.replicas[0]), "the same material carries the same ID in every Region")

	imp, err := set.b.ImportKeyMaterialWithResult(ctx, &kms.ImportKeyMaterialInput{
		KeyID: set.primary, KeyMaterial: materialBytes(2), ImportType: "NEW_KEY_MATERIAL",
	})
	require.NoError(t, err)

	id2 := imp.KeyMaterialID
	assert.Equal(t, "PENDING_MULTI_REGION_IMPORT_AND_ROTATION", set.states(t, set.primary)[id2])

	_, err = set.b.RotateKeyOnDemand(ctx, &kms.RotateKeyOnDemandInput{KeyID: set.primary})
	require.ErrorIs(t, err, kms.ErrKeyInvalidState)

	_, err = set.b.ImportKeyMaterialWithResult(ctx, &kms.ImportKeyMaterialInput{
		KeyID: set.replicas[0], KeyMaterial: materialBytes(2), ImportType: "NEW_KEY_MATERIAL",
	})
	require.ErrorIs(t, err, kms.ErrUnsupportedOrigin)

	for i, replica := range set.replicas {
		_, err = set.b.ImportKeyMaterialWithResult(ctx, &kms.ImportKeyMaterialInput{
			KeyID: replica, KeyMaterial: materialBytes(2), ImportType: "EXISTING_KEY_MATERIAL", KeyMaterialID: id2,
		})
		require.NoError(t, err)
		assert.Equal(t, "PENDING_ROTATION", set.states(t, replica)[id2])

		want := "PENDING_MULTI_REGION_IMPORT_AND_ROTATION"
		if i == len(set.replicas)-1 {
			want = "PENDING_ROTATION"
		}

		assert.Equal(t, want, set.states(t, set.primary)[id2])
	}

	_, err = set.b.RotateKeyOnDemand(ctx, &kms.RotateKeyOnDemandInput{KeyID: set.replicas[0]})
	require.ErrorIs(t, err, kms.ErrUnsupportedOrigin)

	_, err = set.b.RotateKeyOnDemand(ctx, &kms.RotateKeyOnDemandInput{KeyID: set.primary})
	require.NoError(t, err)

	for _, key := range append([]string{set.primary}, set.replicas...) {
		got := set.states(t, key)
		assert.Equal(t, "CURRENT", got[id2], key)
		assert.Equal(t, "NON_CURRENT", got[id1], key)
	}
}

func TestMultiRegionImportedKeyRotation_PrimaryWithoutReplicas(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	set := newImportedMultiRegionSet(t)

	imp, err := set.b.ImportKeyMaterialWithResult(ctx, &kms.ImportKeyMaterialInput{
		KeyID: set.primary, KeyMaterial: materialBytes(2), ImportType: "NEW_KEY_MATERIAL",
	})
	require.NoError(t, err)

	assert.Equal(t, "PENDING_ROTATION", set.states(t, set.primary)[imp.KeyMaterialID])

	_, err = set.b.RotateKeyOnDemand(ctx, &kms.RotateKeyOnDemandInput{KeyID: set.primary})
	require.NoError(t, err)
	assert.Equal(t, "CURRENT", set.states(t, set.primary)[imp.KeyMaterialID])
}
