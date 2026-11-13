package azurearm

import (
	"context"
	"encoding/json/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func msiTestID(sub, rg, name string) ResourceID {
	return ResourceID{
		SubscriptionID: sub, ResourceGroup: rg, Namespace: namespaceMicrosoftManagedIdentity,
		Types: []string{msiIdentitiesType}, Names: []string{name},
	}
}

func TestManagedIdentityRestoreOldSnapshot(t *testing.T) {
	t.Parallel()

	const old = `{"identities":{"rg-a/id-one":{"name":"id-one","resourceGroup":"rg-a","location":"westus",` +
		`"principalId":"p-1","clientId":"c-1","tags":{"k":"v"},"federated":[{"name":"fic-one",` +
		`"issuer":"https://issuer.example","subject":"s","audiences":["api://x"]}]}}}`

	var snap managedIdentitySnapshot
	require.NoError(t, json.Unmarshal([]byte(old), &snap, jsonx.V1()))

	p := NewManagedIdentityProvider("tenant")
	p.SetDefaultSubscription("sub-default")
	p.restore(&snap)

	got, err := p.Get(context.Background(), msiTestID("sub-default", "rg-a", "id-one"))
	require.NoError(t, err)

	props, _ := got[fieldProperties].(map[string]any)
	assert.Equal(t, "p-1", props["principalId"])
	assert.Equal(t, "c-1", props["clientId"])

	_, err = p.Get(context.Background(), msiTestID("sub-other", "rg-a", "id-one"))
	require.ErrorIs(t, err, ErrResourceNotFound)

	fed := msiTestID("sub-default", "rg-a", "id-one")
	fed.Types, fed.Names = []string{msiIdentitiesType, msiFederatedType}, []string{"id-one", "fic-one"}

	_, err = p.Get(context.Background(), fed)
	require.NoError(t, err, "children of a restored identity survive")

	_, principal, found := p.lookupUserAssigned(msiTestID("sub-default", "rg-a", "id-one").ARMID())
	assert.True(t, found)
	assert.Equal(t, "c-1", principal)

	out, err := json.Marshal(p.snapshot(), jsonx.V1())
	require.NoError(t, err)

	fields := []string{`"identities"`, `"name"`, `"resourceGroup"`, `"principalId"`, `"clientId"`, `"federated"`}

	for _, field := range fields {
		assert.Contains(t, string(out), field)
	}

	assert.Contains(t, string(out), `"subscription":"sub-default"`)
	assert.Contains(t, strings.ToLower(string(out)), "sub-default/rg-a/id-one")
}

func TestManagedIdentityRoundTripKeepsSubscriptions(t *testing.T) {
	t.Parallel()

	p := NewManagedIdentityProvider("tenant")
	p.SetDefaultSubscription("sub-default")

	body := map[string]any{fieldLocation: "westus"}

	for _, sub := range []string{"sub-a", "sub-b"} {
		_, err := p.Put(context.Background(), msiTestID(sub, "rg", "same-name"), body)
		require.NoError(t, err)
	}

	out, err := json.Marshal(p.snapshot(), jsonx.V1())
	require.NoError(t, err)

	var snap managedIdentitySnapshot
	require.NoError(t, json.Unmarshal(out, &snap, jsonx.V1()))

	q := NewManagedIdentityProvider("tenant")
	q.SetDefaultSubscription("sub-default")
	q.restore(&snap)

	for _, sub := range []string{"sub-a", "sub-b"} {
		_, err = q.Get(context.Background(), msiTestID(sub, "rg", "same-name"))
		require.NoError(t, err, sub)
	}

	_, err = q.Get(context.Background(), msiTestID("sub-default", "rg", "same-name"))
	require.ErrorIs(t, err, ErrResourceNotFound)
}
