package ocivault_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/keymanagement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const replicaRegion = "us-phoenix-1"

func (e *env) mkPrivateVault(t *testing.T) keymanagement.Vault {
	t.Helper()

	r, err := e.vaults.CreateVault(t.Context(), keymanagement.CreateVaultRequest{
		CompartmentId: new(testCompartment), DisplayName: new("private"),
		VaultType: keymanagement.CreateVaultDetailsVaultTypeVirtualPrivate,
	})
	require.NoError(t, err)
	e.settle()

	return r.Vault
}

func (e *env) replicate(t *testing.T, v keymanagement.Vault, region string) error {
	t.Helper()

	_, err := e.vaults.CreateVaultReplica(t.Context(), keymanagement.CreateVaultReplicaRequest{
		VaultId: v.Id, ReplicaRegion: new(region),
	})

	return err
}

func (e *env) replicas(t *testing.T, v keymanagement.Vault) []keymanagement.VaultReplicaSummary {
	t.Helper()

	r, err := e.vaults.ListVaultReplicas(t.Context(), keymanagement.ListVaultReplicasRequest{VaultId: v.Id})
	require.NoError(t, err)

	return r.Items
}

func TestVaultReplicaLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	v := e.mkPrivateVault(t)
	assert.True(t, *v.IsVaultReplicable)

	m := e.mgmt(t, v)
	pre := e.mkKey(t, m, "pre-existing", aes256(), "")

	require.NoError(t, e.replicate(t, v, replicaRegion))
	require.NoError(t, e.replicate(t, v, replicaRegion), "repeat is a no-op")

	items := e.replicas(t, v)
	require.Len(t, items, 1)
	assert.Equal(t, replicaRegion, *items[0].Region)
	assert.Equal(t, keymanagement.VaultReplicaSummaryStatusCreating, items[0].Status)
	assert.Contains(t, *items[0].ManagementEndpoint, "-"+replicaRegion+"-management.localhost:")

	e.settle()
	assert.Equal(t, keymanagement.VaultReplicaSummaryStatusCreated, e.replicas(t, v)[0].Status)

	got, err := e.vaults.GetVault(ctx, keymanagement.GetVaultRequest{VaultId: v.Id})
	require.NoError(t, err)
	require.NotNil(t, got.ReplicaDetails)

	synced, err := m.GetKey(ctx, keymanagement.GetKeyRequest{KeyId: pre.Id})
	require.NoError(t, err)
	require.NotNil(t, synced.ReplicaDetails, "keys that existed before the replica are replicated")

	cr, err := m.CreateKey(ctx, keymanagement.CreateKeyRequest{
		CompartmentId: new(testCompartment), DisplayName: new("after"), KeyShape: aes256(),
	})
	require.NoError(t, err)

	k, fresh := cr.Key, cr.Key
	require.NotNil(t, fresh.ReplicaDetails)
	status := func() keymanagement.ReplicaDetailsStatusEnum {
		r, serr := m.GetReplicationStatus(ctx, keymanagement.GetReplicationStatusRequest{
			ReplicationId: fresh.ReplicaDetails.ReplicationId,
		})
		require.NoError(t, serr)
		require.Len(t, r.ReplicaDetails, 1)
		assert.Equal(t, replicaRegion, *r.ReplicaDetails[0].Region)

		return r.ReplicaDetails[0].Status
	}

	assert.Equal(t, keymanagement.ReplicaDetailsStatusReplicating, status())
	e.settle()
	assert.Equal(t, keymanagement.ReplicaDetailsStatusReplicated, status())

	rm, err := keymanagement.NewKmsManagementClientWithConfigurationProvider(
		e.prov,
		*e.replicas(t, v)[0].ManagementEndpoint,
	)
	require.NoError(t, err)

	_, err = rm.GetKey(ctx, keymanagement.GetKeyRequest{KeyId: k.Id})
	require.NoError(t, err, "the replica endpoint serves reads")

	_, err = rm.CreateKey(ctx, keymanagement.CreateKeyRequest{
		CompartmentId: new(testCompartment), DisplayName: new("nope"), KeyShape: aes256(),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "replicas are read-only")

	rc, err := keymanagement.NewKmsCryptoClientWithConfigurationProvider(e.prov, *e.replicas(t, v)[0].CryptoEndpoint)
	require.NoError(t, err)

	_, err = rc.Encrypt(ctx, keymanagement.EncryptRequest{KeyId: k.Id, Plaintext: new(b64([]byte("x")))})
	require.NoError(t, err, "the replica endpoint serves crypto")

	_, err = e.vaults.ScheduleVaultDeletion(ctx, keymanagement.ScheduleVaultDeletionRequest{
		VaultId: v.Id, TimeOfDeletion: &common.SDKTime{Time: e.clock.Now().Add(8 * 24 * time.Hour)},
	})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())

	_, err = e.vaults.DeleteVaultReplica(ctx, keymanagement.DeleteVaultReplicaRequest{
		VaultId: v.Id, ReplicaRegion: new(replicaRegion),
	})
	require.NoError(t, err)
	assert.Equal(t, keymanagement.VaultReplicaSummaryStatusDeleting, e.replicas(t, v)[0].Status)

	e.settle()
	assert.Empty(t, e.replicas(t, v))

	_, err = e.vaults.ScheduleVaultDeletion(ctx, keymanagement.ScheduleVaultDeletionRequest{
		VaultId: v.Id, TimeOfDeletion: &common.SDKTime{Time: e.clock.Now().Add(8 * 24 * time.Hour)},
	})
	require.NoError(t, err)
}

func TestVaultReplicaRules(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	private := e.mkPrivateVault(t)
	def := e.mkVault(t, "default")

	require.NoError(t, e.replicate(t, private, replicaRegion))

	tests := []struct {
		call func() error
		name string
		code string
		http int
	}{
		{func() error { return e.replicate(t, def, replicaRegion) }, "default vault", "InvalidParameter", 400},
		{func() error { return e.replicate(t, private, "us-ashburn-1") }, "own region", "InvalidParameter", 400},
		{func() error { return e.replicate(t, private, "not a region") }, "bad region", "InvalidParameter", 400},
		{func() error { return e.replicate(t, private, "eu-frankfurt-1") }, "second region", "Conflict", 409},
		{func() error {
			_, err := e.vaults.DeleteVaultReplica(t.Context(), keymanagement.DeleteVaultReplicaRequest{
				VaultId:       private.Id,
				ReplicaRegion: new("eu-frankfurt-1"),
			})

			return err
		}, "delete unknown replica", "NotAuthorizedOrNotFound", 404},
		{func() error {
			_, err := e.vaults.ListVaultReplicas(t.Context(), keymanagement.ListVaultReplicasRequest{
				VaultId: new("ocid1.vault.oc1..none"),
			})

			return err
		}, "list unknown vault", "NotAuthorizedOrNotFound", 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			se := svcErr(t, tt.call())
			assert.Equal(t, tt.http, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}

	m := e.mgmt(t, private)
	_, err := m.GetReplicationStatus(t.Context(), keymanagement.GetReplicationStatusRequest{ReplicationId: new("nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}
