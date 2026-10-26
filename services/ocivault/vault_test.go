package ocivault_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/keymanagement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocivault"
)

func TestVaultLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()

	created := e.mkVault(t, "main")
	assert.Equal(t, keymanagement.VaultLifecycleStateCreating, created.LifecycleState)
	assert.True(t, strings.HasPrefix(*created.Id, "ocid1.vault.oc1."+oci.Region+"."))
	assert.Contains(t, *created.ManagementEndpoint, "-management.localhost:")
	assert.Contains(t, *created.CryptoEndpoint, "-crypto.localhost:")
	assert.NotEqual(t, *created.ManagementEndpoint, *created.CryptoEndpoint)
	assert.NotEmpty(t, *created.WrappingkeyId)

	got, err := e.vaults.GetVault(ctx, keymanagement.GetVaultRequest{VaultId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, keymanagement.VaultLifecycleStateActive, got.LifecycleState)
	assert.Equal(t, keymanagement.VaultVaultTypeDefault, got.VaultType)
	assert.NotNil(t, got.Etag)

	upd, err := e.vaults.UpdateVault(ctx, keymanagement.UpdateVaultRequest{
		VaultId: created.Id, IfMatch: got.Etag,
		DisplayName: new("renamed"), FreeformTags: map[string]string{"env": "test"},
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *upd.DisplayName)
	assert.Equal(t, "test", upd.FreeformTags["env"])

	_, err = e.vaults.UpdateVault(ctx, keymanagement.UpdateVaultRequest{
		VaultId: created.Id, IfMatch: got.Etag,
		DisplayName: new("stale"),
	})
	se := svcErr(t, err)
	assert.Equal(t, http.StatusPreconditionFailed, se.GetHTTPStatusCode())
	assert.Equal(t, "NoEtagMatch", se.GetCode())

	deadline := e.clock.Now().Add(10 * 24 * time.Hour)
	sched, err := e.vaults.ScheduleVaultDeletion(ctx, keymanagement.ScheduleVaultDeletionRequest{
		VaultId:        created.Id,
		TimeOfDeletion: &common.SDKTime{Time: deadline},
	})
	require.NoError(t, err)
	assert.Equal(t, keymanagement.VaultLifecycleStateSchedulingDeletion, sched.LifecycleState)

	got, err = e.vaults.GetVault(ctx, keymanagement.GetVaultRequest{VaultId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, keymanagement.VaultLifecycleStatePendingDeletion, got.LifecycleState)
	require.NotNil(t, got.TimeOfDeletion)
	assert.WithinDuration(t, deadline, got.TimeOfDeletion.Time, time.Second)

	cancel, err := e.vaults.CancelVaultDeletion(ctx, keymanagement.CancelVaultDeletionRequest{VaultId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, keymanagement.VaultLifecycleStateCancellingDeletion, cancel.LifecycleState)

	got, err = e.vaults.GetVault(ctx, keymanagement.GetVaultRequest{VaultId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, keymanagement.VaultLifecycleStateActive, got.LifecycleState)
	assert.Nil(t, got.TimeOfDeletion)
}

func TestVaultDeletionElapses(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	v := e.mkVault(t, "doomed")

	_, err := e.vaults.ScheduleVaultDeletion(ctx, keymanagement.ScheduleVaultDeletionRequest{VaultId: v.Id})
	require.NoError(t, err)

	e.clock.Advance(29 * 24 * time.Hour)

	got, err := e.vaults.GetVault(ctx, keymanagement.GetVaultRequest{VaultId: v.Id})
	require.NoError(t, err)
	assert.Equal(t, keymanagement.VaultLifecycleStatePendingDeletion, got.LifecycleState)

	e.clock.Advance(2 * 24 * time.Hour)

	got, err = e.vaults.GetVault(ctx, keymanagement.GetVaultRequest{VaultId: v.Id})
	require.NoError(t, err)
	assert.Equal(t, keymanagement.VaultLifecycleStateDeleted, got.LifecycleState)
}

func TestVaultErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	v := e.mkVault(t, "v")

	tests := []struct {
		call     func() error
		name     string
		wantCode string
		status   int
	}{
		{
			name: "missing vault", status: http.StatusNotFound, wantCode: "NotAuthorizedOrNotFound",
			call: func() error {
				_, err := e.vaults.GetVault(ctx, keymanagement.GetVaultRequest{VaultId: new("ocid1.vault.oc1..nope")})

				return err
			},
		},
		{
			name: "bad vault type", status: http.StatusBadRequest, wantCode: "InvalidParameter",
			call: func() error {
				_, err := e.vaults.CreateVault(ctx, keymanagement.CreateVaultRequest{
					CompartmentId: new(testCompartment), DisplayName: new("x"),
					VaultType: keymanagement.CreateVaultDetailsVaultTypeExternal,
				})

				return err
			},
		},
		{
			name: "deletion window too short", status: http.StatusBadRequest, wantCode: "InvalidParameter",
			call: func() error {
				_, err := e.vaults.ScheduleVaultDeletion(ctx, keymanagement.ScheduleVaultDeletionRequest{
					VaultId:        v.Id,
					TimeOfDeletion: &common.SDKTime{Time: e.clock.Now().Add(24 * time.Hour)},
				})

				return err
			},
		},
		{
			name: "cancel when active", status: http.StatusConflict, wantCode: "IncorrectState",
			call: func() error {
				_, err := e.vaults.CancelVaultDeletion(ctx, keymanagement.CancelVaultDeletionRequest{VaultId: v.Id})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			se := svcErr(t, tt.call())
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.wantCode, se.GetCode())
		})
	}
}

func TestListVaultsPagingAndSort(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()

	for _, n := range []string{"b", "a", "c"} {
		e.mkVault(t, n)
		e.clock.Advance(time.Second)
	}

	tests := []struct {
		name  string
		sort  keymanagement.ListVaultsSortByEnum
		order keymanagement.ListVaultsSortOrderEnum
		want  []string
	}{
		{"time desc default", "", "", []string{"c", "a", "b"}},
		{
			"time asc",
			keymanagement.ListVaultsSortByTimecreated,
			keymanagement.ListVaultsSortOrderAsc,
			[]string{"b", "a", "c"},
		},
		{
			"name asc",
			keymanagement.ListVaultsSortByDisplayname,
			keymanagement.ListVaultsSortOrderAsc,
			[]string{"a", "b", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got []string

			req := keymanagement.ListVaultsRequest{
				CompartmentId: new(testCompartment), Limit: new(2), SortBy: tt.sort, SortOrder: tt.order,
			}

			for {
				resp, err := e.vaults.ListVaults(ctx, req)
				require.NoError(t, err)

				for _, v := range resp.Items {
					got = append(got, *v.DisplayName)
				}

				if resp.OpcNextPage == nil {
					break
				}

				req.Page = resp.OpcNextPage
			}

			assert.Equal(t, tt.want, got)
		})
	}

	other, err := e.vaults.ListVaults(
		ctx,
		keymanagement.ListVaultsRequest{CompartmentId: new("ocid1.compartment.oc1..other")},
	)
	require.NoError(t, err)
	assert.Empty(t, other.Items)
}

func TestVaultUsageAndCompartmentMove(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	v := e.mkVault(t, "u")
	m := e.mgmt(t, v)

	e.mkKey(t, m, "soft", aes256(), keymanagement.CreateKeyDetailsProtectionModeSoftware)
	e.mkKey(t, m, "hsm", aes256(), keymanagement.CreateKeyDetailsProtectionModeHsm)

	usage, err := e.vaults.GetVaultUsage(ctx, keymanagement.GetVaultUsageRequest{VaultId: v.Id})
	require.NoError(t, err)
	assert.Equal(t, 2, *usage.KeyCount)
	assert.Equal(t, 1, *usage.SoftwareKeyCount)
	assert.Equal(t, 2, *usage.KeyVersionCount)

	_, err = e.vaults.ChangeVaultCompartment(ctx, keymanagement.ChangeVaultCompartmentRequest{
		VaultId:       v.Id,
		CompartmentId: new("ocid1.compartment.oc1..moved"),
	})
	require.NoError(t, err)

	got, err := e.vaults.GetVault(ctx, keymanagement.GetVaultRequest{VaultId: v.Id})
	require.NoError(t, err)
	assert.Equal(t, "ocid1.compartment.oc1..moved", *got.CompartmentId)
}
