package gcpcontainer_test

import (
	"testing"

	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestGetJSONWebKeys(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	src.ready(t, zone, stdCluster("c"))
	src.ready(t, zone, stdCluster("d"))

	get := func(e *env, name string) (*containerpb.Jwk, error) {
		resp, err := e.gc.GetJSONWebKeys(
			t.Context(),
			&containerpb.GetJSONWebKeysRequest{Parent: clusterPath(zone, name)},
		)
		if err != nil {
			return nil, err
		}

		require.Len(t, resp.GetKeys(), 1)

		return resp.GetKeys()[0], nil
	}

	first, err := get(src, "c")
	require.NoError(t, err)
	assert.Equal(t, "RSA", first.GetKty())
	assert.Equal(t, "RS256", first.GetAlg())
	assert.Equal(t, "sig", first.GetUse())
	assert.Equal(t, "AQAB", first.GetE())
	assert.NotEmpty(t, first.GetKid())
	assert.NotEmpty(t, first.GetN())

	again, err := get(src, "c")
	require.NoError(t, err)
	assert.Equal(t, first.GetKid(), again.GetKid())

	other, err := get(src, "d")
	require.NoError(t, err)
	assert.NotEqual(t, first.GetKid(), other.GetKid())

	dst := newEnv(t)
	require.NoError(t, dst.h.Restore(t.Context(), src.h.Snapshot(t.Context())))

	restored, err := get(dst, "c")
	require.NoError(t, err)
	assert.Equal(t, first.GetN(), restored.GetN())

	_, err = get(src, "ghost")
	assertCode(t, codes.NotFound, err, "")

	_, err = src.gc.GetJSONWebKeys(t.Context(), &containerpb.GetJSONWebKeysRequest{Parent: "bad"})
	assertCode(t, codes.InvalidArgument, err, "")
}
