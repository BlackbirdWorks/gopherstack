package ocivault_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/vault"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type replEnv struct {
	*secretEnv

	targetVault string
	targetKey   string
}

func newReplEnv(t *testing.T) *replEnv {
	t.Helper()

	se := newSecretEnv(t)
	tv := se.e.mkVault(t, "target")
	tk := se.e.mkKey(t, se.e.mgmt(t, tv), "target-key", aes256(), "")

	return &replEnv{secretEnv: se, targetVault: *tv.Id, targetKey: *tk.Id}
}

func (re *replEnv) config(forward bool) *vault.ReplicationConfig {
	return &vault.ReplicationConfig{
		IsWriteForwardEnabled: new(forward),
		ReplicationTargets: []vault.ReplicationTarget{{
			TargetKeyId: new(re.targetKey), TargetRegion: new("us-phoenix-1"), TargetVaultId: new(re.targetVault),
		}},
	}
}

func (re *replEnv) replica(t *testing.T, name string) vault.SecretSummary {
	t.Helper()

	list, err := re.e.vsecret.ListSecrets(t.Context(), vault.ListSecretsRequest{
		CompartmentId: new(testCompartment), VaultId: &re.targetVault, Name: new(name),
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	return list.Items[0]
}

func TestSecretReplication(t *testing.T) {
	t.Parallel()

	re := newReplEnv(t)
	ctx := t.Context()

	src := re.create(
		t,
		"replicated",
		"v1",
		func(d *vault.CreateSecretDetails) { d.ReplicationConfig = re.config(false) },
	)
	require.NotNil(t, src.ReplicationConfig)
	assert.False(t, *src.IsReplica)

	rep := re.replica(t, "replicated")
	assert.True(t, *rep.IsReplica)
	assert.Equal(t, re.targetKey, *rep.KeyId)
	assert.Equal(t, *re.v.Id, *rep.SourceRegionInformation.SourceVaultId)
	assert.Equal(t, *re.key.Id, *rep.SourceRegionInformation.SourceKeyId)

	b, err := re.bundle(t, rep.Id, nil)
	require.NoError(t, err)
	assert.Equal(t, "v1", bundleText(t, b))

	re.update(t, src.Id, vault.UpdateSecretDetails{SecretContent: content("v2")})

	b, err = re.bundle(t, rep.Id, nil)
	require.NoError(t, err)
	assert.Equal(t, "v2", bundleText(t, b), "new source versions reach the replica")

	_, err = re.e.vsecret.UpdateSecret(ctx, vault.UpdateSecretRequest{
		SecretId: rep.Id, Description: new("nope"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "replicas are read-only")

	re.update(t, src.Id, vault.UpdateSecretDetails{ReplicationConfig: &vault.ReplicationConfig{
		ReplicationTargets: []vault.ReplicationTarget{},
	}})

	list, err := re.e.vsecret.ListSecrets(ctx, vault.ListSecretsRequest{
		CompartmentId: new(testCompartment), VaultId: &re.targetVault,
	})
	require.NoError(t, err)
	assert.Empty(t, list.Items, "dropping the target removes the replica")
}

func TestSecretReplicationWriteForward(t *testing.T) {
	t.Parallel()

	re := newReplEnv(t)
	ctx := t.Context()

	src := re.create(t, "forwarded", "v1", func(d *vault.CreateSecretDetails) { d.ReplicationConfig = re.config(true) })
	rep := re.replica(t, "forwarded")

	_, err := re.e.vsecret.UpdateSecret(ctx, vault.UpdateSecretRequest{
		SecretId: rep.Id, SecretContent: content("from replica"),
	})
	require.NoError(t, err)

	b, err := re.bundle(t, src.Id, nil)
	require.NoError(t, err)
	assert.Equal(t, "from replica", bundleText(t, b), "the write reached the source")

	b, err = re.bundle(t, rep.Id, nil)
	require.NoError(t, err)
	assert.Equal(t, "from replica", bundleText(t, b))
}

func TestSecretReplicationRules(t *testing.T) {
	t.Parallel()

	re := newReplEnv(t)

	tests := []struct {
		mut  func(*vault.CreateSecretDetails)
		name string
	}{
		{name: "same vault", mut: func(d *vault.CreateSecretDetails) {
			c := re.config(false)
			c.ReplicationTargets[0].TargetVaultId = re.v.Id
			d.ReplicationConfig = c
		}},
		{name: "unknown vault", mut: func(d *vault.CreateSecretDetails) {
			c := re.config(false)
			c.ReplicationTargets[0].TargetVaultId = new("ocid1.vault.oc1.iad.nope")
			d.ReplicationConfig = c
		}},
		{name: "unknown key", mut: func(d *vault.CreateSecretDetails) {
			c := re.config(false)
			c.ReplicationTargets[0].TargetKeyId = new("ocid1.key.oc1.iad.nope")
			d.ReplicationConfig = c
		}},
		{name: "bad region", mut: func(d *vault.CreateSecretDetails) {
			c := re.config(false)
			c.ReplicationTargets[0].TargetRegion = new("moon")
			d.ReplicationConfig = c
		}},
		{name: "too many targets", mut: func(d *vault.CreateSecretDetails) {
			c := re.config(false)
			c.ReplicationTargets = append(c.ReplicationTargets, c.ReplicationTargets[0], c.ReplicationTargets[0],
				c.ReplicationTargets[0])
			d.ReplicationConfig = c
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := vault.CreateSecretDetails{
				CompartmentId: new(testCompartment), VaultId: re.v.Id, KeyId: re.key.Id,
				SecretName: new("rules-" + tt.name), SecretContent: content("x"),
			}
			tt.mut(&d)

			_, err := re.e.vsecret.CreateSecret(t.Context(), vault.CreateSecretRequest{CreateSecretDetails: d})
			assert.Contains(t, []int{http.StatusBadRequest, http.StatusNotFound}, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}
