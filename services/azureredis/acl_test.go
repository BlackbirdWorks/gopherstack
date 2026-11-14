package azureredis_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureredis"
)

func TestEntraDataAccessPolicies(t *testing.T) {
	t.Parallel()

	const oid = "11111111-2222-3333-4444-555555555555"

	b, _ := newBackend(t)
	b.SetTokenValidator(func(token string) (string, bool) {
		after, ok := strings.CutPrefix(token, "token-for-")

		return after, ok
	})

	spec := basicSpec()
	spec.EntraEnabled = true
	spec.Assignments = []azureredis.AccessAssignment{{ObjectID: oid, Alias: "reader", Permissions: "+@read ~Az*"}}
	ep := ensure(t, b, "c1", spec)
	keys, err := b.Keys("c1")
	require.NoError(t, err)

	admin := dialTLS(t, ep, keys.Primary)
	require.NoError(t, admin.Set(t.Context(), "Azure", "v", 0).Err())
	require.NoError(t, admin.Set(t.Context(), "other", "v", 0).Err())

	c := dialTLSUser(t, ep, oid, "token-for-"+oid)

	v, err := c.Get(t.Context(), "Azure").Result()
	require.NoError(t, err)
	assert.Equal(t, "v", v)

	require.ErrorContains(t, c.Set(t.Context(), "Azure", "x", 0).Err(), "NOPERM")
	require.ErrorContains(t, c.Get(t.Context(), "other").Err(), "NOPERM")

	spec.Assignments[0].Permissions = "+@all allkeys"
	ensure(t, b, "c1", spec)
	require.NoError(t, c.Set(t.Context(), "other", "x", 0).Err())

	spec.Assignments = nil
	ensure(t, b, "c1", spec)
	require.ErrorContains(t, c.Get(t.Context(), "Azure").Err(), "NOPERM")
}
