package ociartifacts_test

import (
	"testing"

	"github.com/oracle/oci-go-sdk/v65/artifacts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
)

func TestResolveImage(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	img := e.registry(t).PushImage(testNamespace+"/team/fn", "v1")
	digest := ocitest.Digest(img.Body)

	tests := []struct {
		name      string
		ref       string
		wantFound bool
		wantOwned bool
	}{
		{name: "tag", ref: testNamespace + "/team/fn:v1", wantFound: true, wantOwned: true},
		{name: "registry host", ref: "iad.ocir.io/" + testNamespace + "/team/fn:v1", wantFound: true, wantOwned: true},
		{name: "digest", ref: "iad.ocir.io/" + testNamespace + "/team/fn@" + digest, wantFound: true, wantOwned: true},
		{name: "unknown tag", ref: "iad.ocir.io/" + testNamespace + "/team/fn:v9", wantOwned: true},
		{name: "unknown repo", ref: testNamespace + "/team/none:v1", wantOwned: true},
		{name: "other namespace", ref: "iad.ocir.io/other/team/fn:v1"},
		{name: "other registry", ref: "docker.io/library/alpine:3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, found, owned := e.backend.ResolveImage(tt.ref)
			assert.Equal(t, tt.wantFound, found)
			assert.Equal(t, tt.wantOwned, owned)

			if tt.wantFound {
				assert.Equal(t, digest, got.Digest)
				assert.NotEmpty(t, got.ID)
			}
		})
	}
}

func TestImageSignatures(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	image := e.pushed(t)

	ref, found, _ := e.backend.ResolveImage(testNamespace + "/signed:v1")
	require.True(t, found)
	assert.Empty(t, e.backend.ImageSignatures(ref.ID))

	_, err := e.client.CreateContainerImageSignature(t.Context(), artifacts.CreateContainerImageSignatureRequest{
		CreateContainerImageSignatureDetails: sigDetails(image.Id),
	})
	require.NoError(t, err)

	sigs := e.backend.ImageSignatures(ref.ID)
	require.Len(t, sigs, 1)
	assert.Equal(t, testKey, sigs[0].KeyID)
	assert.Equal(t, testKeyVersion, sigs[0].KeyVersionID)
	assert.Equal(t, b64("signature"), sigs[0].Signature)
	assert.Empty(t, e.backend.ImageSignatures("ocid1.image.oc1..other"))
}
