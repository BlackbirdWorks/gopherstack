package cognitoidentity_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

func newTestBackend() *cognitoidentity.InMemoryBackend {
	return cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")
}

func TestInMemoryBackend_Region(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "eu-west-1")
	assert.Equal(t, "eu-west-1", b.Region())
}

func TestInMemoryBackend_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	pool, err := b.CreateIdentityPool(
		context.Background(),
		"persist-pool",
		true,
		false,
		"",
		nil,
		nil,
		map[string]string{
			"env": "prod",
		},
	)
	require.NoError(t, err)

	_, err = b.GetID(context.Background(), pool.IdentityPoolID, "000000000000", map[string]string{
		"accounts.google.com": "google-token",
	})
	require.NoError(t, err)

	_, err = b.SetPrincipalTagAttributeMap(context.Background(),
		pool.IdentityPoolID,
		"cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx",
		false,
		map[string]string{"sub": "user_id"},
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	restored, err := b2.DescribeIdentityPool(context.Background(), pool.IdentityPoolID)
	require.NoError(t, err)
	assert.Equal(t, "persist-pool", restored.IdentityPoolName)
	assert.Equal(t, "prod", restored.Tags["env"])

	result, err := b2.ListIdentities(context.Background(), pool.IdentityPoolID, 10, false, "")
	require.NoError(t, err)
	assert.Len(t, result.Identities, 1)

	mapping, err := b2.GetPrincipalTagAttributeMap(context.Background(),
		pool.IdentityPoolID,
		"cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx",
	)
	require.NoError(t, err)
	assert.Equal(t, "user_id", mapping.PrincipalTags["sub"])
}
