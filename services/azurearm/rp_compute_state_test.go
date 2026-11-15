package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeSDK_SnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	src := newCmpStack(t)
	uai := src.userIdentity(t, "id-one")
	vm := linuxVM(src.newNIC(t, "nic1"))
	vm.Identity = vmIdentity(armcompute.ResourceIdentityTypeSystemAssignedUserAssigned, *uai.ID)
	vm.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{
		{
			Lun:          new(int32(0)),
			Name:         new("data0"),
			CreateOption: new(armcompute.DiskCreateOptionTypesEmpty),
			DiskSizeGB:   new(int32(8)),
		},
	}
	created := src.mustVM(t, "vm1", vm)

	pl, err := src.cf.NewVirtualMachinesClient().BeginDeallocate(t.Context(), netRG, "vm1", nil)
	require.NoError(t, waitPoller(t, pl, err))

	_, err = src.putScaleSet(t, "vmss1", src.scaleSet(t, 2))
	require.NoError(t, err)

	_, err = src.cf.NewAvailabilitySetsClient().CreateOrUpdate(t.Context(), netRG, "as1", armcompute.AvailabilitySet{
		Location: new("westus"), SKU: &armcompute.SKU{Name: new("Aligned")},
	}, nil)
	require.NoError(t, err)

	snap := src.handler.Snapshot(t.Context())

	dst := newCmpStack(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	got, err := dst.cf.NewVirtualMachinesClient().Get(t.Context(), netRG, "vm1", nil)
	require.NoError(t, err)
	assert.Equal(t, *created.Properties.VMID, *got.Properties.VMID)
	assert.Equal(t, *created.Identity.PrincipalID, *got.Identity.PrincipalID)
	assert.Equal(t, "deallocated", dst.power(t, "vm1"))

	state, managedBy := dst.diskState(t, "data0")
	assert.Equal(t, "Reserved", state)
	assert.Equal(t, *created.ID, managedBy)

	nic, err := dst.net.NewInterfacesClient().Get(t.Context(), netRG, "nic1", nil)
	require.NoError(t, err)
	assert.Equal(t, *created.ID, *nic.Properties.VirtualMachine.ID, "the NIC back-reference persists")

	assert.Len(t, dst.instances(t), 2)

	_, err = dst.cf.NewAvailabilitySetsClient().Get(t.Context(), netRG, "as1", nil)
	require.NoError(t, err)

	dst.handler.Reset()

	_, err = dst.cf.NewVirtualMachinesClient().Get(t.Context(), netRG, "vm1", nil)
	require.Error(t, err, "reset clears Microsoft.Compute")
}

func TestComputeSDK_ResourceGroupCascade(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	s.rawDo(t, http.MethodPut, "/subscriptions/"+aciSub+"/resourcegroups/rg-vm", `{"location":"westus"}`)

	vm := linuxVM(s.newNIC(t, "nic1"))
	vm.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{
		{
			Lun:          new(int32(0)),
			Name:         new("data0"),
			CreateOption: new(armcompute.DiskCreateOptionTypesEmpty),
			DiskSizeGB:   new(int32(8)),
		},
	}
	created, err := s.putVM(t, "rg-vm", "vm1", vm)
	require.NoError(t, err)

	osName := *created.Properties.StorageProfile.OSDisk.Name

	resp := s.rawDo(t, http.MethodDelete, "/subscriptions/"+aciSub+"/resourcegroups/rg-vm?api-version=2021-04-01", "")
	require.Less(t, resp.StatusCode, 300)

	_, err = s.cf.NewVirtualMachinesClient().Get(t.Context(), "rg-vm", "vm1", nil)
	require.Error(t, err)

	for _, name := range []string{osName, "data0"} {
		_, err = s.cf.NewDisksClient().Get(t.Context(), "rg-vm", name, nil)
		require.Error(t, err, name)
	}

	nic, err := s.net.NewInterfacesClient().Get(t.Context(), netRG, "nic1", nil)
	require.NoError(t, err)
	assert.Nil(t, nic.Properties.VirtualMachine, "the NIC of a removed machine is released")

	delPoller, err := s.net.NewInterfacesClient().BeginDelete(t.Context(), netRG, "nic1", nil)
	require.NoError(t, waitPoller(t, delPoller, err))
}

func TestComputeSDK_ReapplyReimageSpot(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	c := s.cf.NewVirtualMachinesClient()

	s.mustVM(t, "reg", linuxVM(s.newNIC(t, "nic-reg")))

	pl, perr := c.BeginReapply(t.Context(), netRG, "reg", nil)
	require.NoError(t, waitPoller(t, pl, perr))

	pd, perr := c.BeginDeallocate(t.Context(), netRG, "reg", nil)
	require.NoError(t, waitPoller(t, pd, perr))

	pi, perr := c.BeginReimage(t.Context(), netRG, "reg", nil)
	err := waitPoller(t, pi, perr)
	require.Error(t, err)
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err), "a deallocated machine cannot be reimaged")

	_, err = c.SimulateEviction(t.Context(), netRG, "reg", nil)
	require.Error(t, err)
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err), "only Spot machines can be evicted")

	bad := linuxVM(s.newNIC(t, "nic-bad"))
	bad.Properties.EvictionPolicy = new(armcompute.VirtualMachineEvictionPolicyTypesDelete)
	_, err = s.putVM(t, netRG, "bad", bad)
	require.Error(t, err, "evictionPolicy needs Spot priority")

	for _, tc := range []struct {
		name, policy, wantPower string
	}{{"spot-dealloc", "Deallocate", "deallocated"}, {"spot-delete", "Delete", ""}} {
		vm := linuxVM(s.newNIC(t, "nic-"+tc.name))
		vm.Properties.Priority = new(armcompute.VirtualMachinePriorityTypesSpot)
		vm.Properties.EvictionPolicy = new(armcompute.VirtualMachineEvictionPolicyTypes(tc.policy))
		vm.Properties.BillingProfile = &armcompute.BillingProfile{MaxPrice: new(float64(-1))}
		s.mustVM(t, tc.name, vm)

		_, err = c.SimulateEviction(t.Context(), netRG, tc.name, nil)
		require.NoError(t, err)

		if tc.wantPower != "" {
			assert.Equal(t, tc.wantPower, s.power(t, tc.name))

			continue
		}

		_, err = c.Get(t.Context(), netRG, tc.name, nil)
		require.Error(t, err, "Delete policy removes the machine")
	}
}
