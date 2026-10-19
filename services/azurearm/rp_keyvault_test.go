package azurearm_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

type fakeKeyVaults struct {
	vaults map[string]int
	calls  []string
}

func (f *fakeKeyVaults) CreateVault(name string, retentionDays int, _ bool) {
	f.calls = append(f.calls, "CreateVault:"+name)
	f.vaults[name] = retentionDays
}

func (f *fakeKeyVaults) DeleteVault(name string) {
	f.calls = append(f.calls, "DeleteVault:"+name)
	delete(f.vaults, name)
}

func kvID(t *testing.T, rg, tail string) azurearm.ResourceID {
	t.Helper()

	id, err := azurearm.ParseGenericResourcePath(
		"/subscriptions/" + azurearm.DefaultSubscriptionID + "/resourceGroups/" + rg +
			"/providers/Microsoft.KeyVault/vaults/" + tail)
	require.NoError(t, err)

	return id
}

func TestKeyVaultProvider_Lifecycle(t *testing.T) {
	t.Parallel()

	fake := &fakeKeyVaults{vaults: map[string]int{}}
	p := azurearm.NewKeyVaultProvider(azurearm.KeyVaultEndpointConfig{}, fake)
	ctx := azurearm.WithRequestHost(context.Background(), "gopher")

	body, err := p.Put(ctx, kvID(t, "rg", "kv1"), map[string]any{
		"location": "westus",
		"properties": map[string]any{
			"softDeleteRetentionInDays": float64(30),
			"enablePurgeProtection":     true,
			"accessPolicies":            []any{},
		},
	})
	require.NoError(t, err)

	props, ok := body["properties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "https://kv1.gopher:10004/", props["vaultUri"])
	assert.Equal(t, "Succeeded", props["provisioningState"])
	assert.Equal(t, true, props["enableSoftDelete"])
	assert.Equal(t, true, props["enablePurgeProtection"])
	assert.Equal(t, "Microsoft.KeyVault/vaults", body["type"])
	assert.Equal(t, 30, fake.vaults["kv1"], "data plane told the retention period")

	_, err = p.Get(ctx, kvID(t, "other", "kv1"))
	require.Error(t, err, "wrong resource group")

	_, err = p.Put(ctx, kvID(t, "other", "kv1"), map[string]any{})
	require.Error(t, err, "global name uniqueness")

	list, err := p.List(ctx, kvID(t, "rg", "x"))
	require.NoError(t, err)
	assert.Len(t, list, 1)

	require.NoError(t, p.Delete(ctx, kvID(t, "rg", "kv1")))
	assert.NotContains(t, fake.vaults, "kv1")
	require.Error(t, p.Delete(ctx, kvID(t, "rg", "kv1")))
}

func TestKeyVaultProvider_AccessPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
		in   []any
		want int
	}{
		{"add_new", "add", []any{map[string]any{"tenantId": "t", "objectId": "o2"}}, 2},
		{"add_existing_replaces", "add", []any{map[string]any{"tenantId": "t", "objectId": "o1"}}, 1},
		{"remove", "remove", []any{map[string]any{"tenantId": "T", "objectId": "O1"}}, 0},
		{
			"replace",
			"replace",
			[]any{map[string]any{"tenantId": "t", "objectId": "x"}, map[string]any{"tenantId": "t", "objectId": "y"}},
			2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := azurearm.NewKeyVaultProvider(azurearm.KeyVaultEndpointConfig{}, nil)
			ctx := context.Background()

			_, err := p.Put(ctx, kvID(t, "rg", "kv"), map[string]any{"properties": map[string]any{
				"accessPolicies": []any{map[string]any{"tenantId": "t", "objectId": "o1"}},
			}})
			require.NoError(t, err)

			out, err := p.Put(ctx, kvID(t, "rg", "kv/accessPolicies/"+tt.op), map[string]any{
				"properties": map[string]any{"accessPolicies": tt.in},
			})
			require.NoError(t, err)

			props, ok := out["properties"].(map[string]any)
			require.True(t, ok)
			assert.Len(t, props["accessPolicies"], tt.want)

			got, err := p.Get(ctx, kvID(t, "rg", "kv"))
			require.NoError(t, err)

			gotProps, ok := got["properties"].(map[string]any)
			require.True(t, ok)
			assert.Len(t, gotProps["accessPolicies"], tt.want)
		})
	}

	p := azurearm.NewKeyVaultProvider(azurearm.KeyVaultEndpointConfig{}, nil)
	_, err := p.Put(context.Background(), kvID(t, "rg", "kv/accessPolicies/add"), map[string]any{})
	require.Error(t, err, "vault must exist first")
}

func TestKeyVaultProvider_ResetAndGroupDelete(t *testing.T) {
	t.Parallel()

	fake := &fakeKeyVaults{vaults: map[string]int{}}
	p := azurearm.NewKeyVaultProvider(azurearm.KeyVaultEndpointConfig{Override: "kv.example:9"}, fake)
	ctx := context.Background()

	out, err := p.Put(ctx, kvID(t, "rg", "a"), map[string]any{})
	require.NoError(t, err)

	props, ok := out["properties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "https://a.kv.example:9/", props["vaultUri"])

	_, err = p.Put(ctx, kvID(t, "rg2", "b"), map[string]any{})
	require.NoError(t, err)

	p.DeleteResourcesInGroup(ctx, "RG")
	assert.NotContains(t, fake.vaults, "a")
	assert.Contains(t, fake.vaults, "b")

	p.Reset()

	list, err := p.List(ctx, kvID(t, "rg2", "x"))
	require.NoError(t, err)
	assert.Empty(t, list)
}
