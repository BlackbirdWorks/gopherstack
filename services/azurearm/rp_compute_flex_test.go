package azurearm_test

import (
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *cmpStack) flexSet(t *testing.T, capacity int64) armcompute.VirtualMachineScaleSet {
	t.Helper()

	ss := s.scaleSet(t, capacity)
	ss.Properties.OrchestrationMode = new(armcompute.OrchestrationModeFlexible)
	ss.Properties.PlatformFaultDomainCount = new(int32(1))
	ss.Properties.UpgradePolicy = nil
	ss.Properties.VirtualMachineProfile.NetworkProfile.NetworkAPIVersion = new(
		armcompute.NetworkAPIVersionTwoThousandTwenty1101,
	)

	return ss
}

func firstIPConfig(ss *armcompute.VirtualMachineScaleSet) *armcompute.VirtualMachineScaleSetIPConfiguration {
	cfg := ss.Properties.VirtualMachineProfile.NetworkProfile.NetworkInterfaceConfigurations[0]

	return cfg.Properties.IPConfigurations[0]
}

func (s *cmpStack) vmsIn(t *testing.T, vmss string) []*armcompute.VirtualMachine {
	t.Helper()

	filter := "virtualMachineScaleSet/id eq '/subscriptions/" + aciSub + "/resourceGroups/" + netRG +
		"/providers/Microsoft.Compute/virtualMachineScaleSets/" + vmss + "'"
	pager := s.cf.NewVirtualMachinesClient().
		NewListPager(netRG, &armcompute.VirtualMachinesClientListOptions{Filter: &filter})

	var out []*armcompute.VirtualMachine

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		out = append(out, page.Value...)
	}

	return out
}

func (s *cmpStack) nicNames(t *testing.T) []string {
	t.Helper()

	pager := s.net.NewInterfacesClient().NewListPager(netRG, nil)

	var out []string

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		for _, n := range page.Value {
			out = append(out, *n.Name)
		}
	}

	return out
}

func (s *cmpStack) diskCount(t *testing.T) int {
	t.Helper()

	pager := s.cf.NewDisksClient().NewListByResourceGroupPager(netRG, nil)
	n := 0

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		n += len(page.Value)
	}

	return n
}

func TestComputeSDK_FlexibleScaleOut(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	ss, err := s.putScaleSet(t, "vmss1", s.flexSet(t, 3))
	require.NoError(t, err)
	assert.Equal(t, "Flexible", string(*ss.Properties.OrchestrationMode))
	assert.EqualValues(t, 1, *ss.Properties.PlatformFaultDomainCount)
	assert.Nil(t, ss.Properties.VirtualMachineProfile.OSProfile.AdminPassword)

	vms := s.vmsIn(t, "vmss1")
	require.Len(t, vms, 3)

	names := map[string]bool{}

	for _, vm := range vms {
		assert.True(t, strings.HasPrefix(*vm.Name, "vmss1_"), *vm.Name)
		assert.Equal(t, *ss.ID, *vm.Properties.VirtualMachineScaleSet.ID)
		assert.Equal(t, "Standard_B2s", string(*vm.Properties.HardwareProfile.VMSize))
		assert.True(t, strings.HasPrefix(*vm.Properties.OSProfile.ComputerName, "web"))
		assert.Nil(t, vm.Properties.OSProfile.AdminPassword)
		assert.Equal(t, "azureuser", *vm.Properties.OSProfile.AdminUsername)
		require.Len(t, vm.Properties.NetworkProfile.NetworkInterfaces, 1)

		names[*vm.Name] = true
	}

	assert.Len(t, names, 3)
	assert.Len(t, s.nicNames(t), 3)
	assert.Equal(t, 3, s.diskCount(t))

	for _, n := range s.nicNames(t) {
		nic, nerr := s.net.NewInterfacesClient().Get(t.Context(), netRG, n, nil)
		require.NoError(t, nerr)
		require.NotNil(t, nic.Properties.VirtualMachine)
		assert.True(t, names[strings.TrimSuffix(n, "-nic")], n)
		assert.Equal(t, s.subnetID, *nic.Properties.IPConfigurations[0].Properties.Subnet.ID)
	}

	insts := s.instances(t)
	require.Len(t, insts, 3, "the scale set VM list names the member machines")

	scaleIn := s.flexSet(t, 1)
	_, err = s.putScaleSet(t, "vmss1", scaleIn)
	require.NoError(t, err)
	assert.Len(t, s.vmsIn(t, "vmss1"), 1)
	assert.Len(t, s.nicNames(t), 1, "NICs follow their machine")
	assert.Equal(t, 1, s.diskCount(t), "OS disks follow their machine")

	_, err = s.putScaleSet(t, "vmss1", s.flexSet(t, 3))
	require.NoError(t, err)

	after := map[string]bool{}
	for _, vm := range s.vmsIn(t, "vmss1") {
		after[*vm.Name] = true
	}

	assert.Len(t, after, 3)

	for n := range names {
		delete(after, n)
	}

	assert.Len(t, after, 2, "names of removed machines are not reused")

	dl, err := s.cf.NewVirtualMachineScaleSetsClient().BeginDelete(t.Context(), netRG, "vmss1", nil)
	require.NoError(t, waitPoller(t, dl, err))

	assert.Empty(t, s.vmsIn(t, "vmss1"))
	assert.Empty(t, s.nicNames(t))
	assert.Zero(t, s.diskCount(t))
}

func TestComputeSDK_FlexibleValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(*armcompute.VirtualMachineScaleSet)
		name     string
		wantCode string
	}{
		{name: "defaults fault domain to max spreading", mutate: func(ss *armcompute.VirtualMachineScaleSet) {
			ss.Properties.PlatformFaultDomainCount = nil
		}},
		{name: "regional three fault domains", mutate: func(ss *armcompute.VirtualMachineScaleSet) {
			ss.Properties.PlatformFaultDomainCount = new(int32(3))
		}},
		{
			name:     "regional five fault domains",
			wantCode: "InvalidParameter",
			mutate: func(ss *armcompute.VirtualMachineScaleSet) {
				ss.Properties.PlatformFaultDomainCount = new(int32(5))
			},
		},
		{name: "zero fault domains", wantCode: "InvalidParameter", mutate: func(ss *armcompute.VirtualMachineScaleSet) {
			ss.Properties.PlatformFaultDomainCount = new(int32(0))
		}},
		{
			name:     "zonal needs one fault domain",
			wantCode: "InvalidParameter",
			mutate: func(ss *armcompute.VirtualMachineScaleSet) {
				ss.Zones = []*string{new("1"), new("2")}
				ss.Properties.PlatformFaultDomainCount = new(int32(2))
			},
		},
		{name: "zonal with max spreading", mutate: func(ss *armcompute.VirtualMachineScaleSet) {
			ss.Zones = []*string{new("1"), new("2")}
		}},
		{
			name:     "overprovision unsupported",
			wantCode: "InvalidParameter",
			mutate: func(ss *armcompute.VirtualMachineScaleSet) {
				ss.Properties.Overprovision = new(true)
			},
		},
		{
			name:     "capacity needs a profile",
			wantCode: "InvalidParameter",
			mutate: func(ss *armcompute.VirtualMachineScaleSet) {
				ss.Properties.VirtualMachineProfile = nil
			},
		},
		{name: "empty scale set", mutate: func(ss *armcompute.VirtualMachineScaleSet) {
			ss.Properties.VirtualMachineProfile = nil
			ss.SKU = nil
		}},
		{
			name:     "public ip configuration",
			wantCode: "NotImplemented",
			mutate: func(ss *armcompute.VirtualMachineScaleSet) {
				ipc := firstIPConfig(ss)
				ipc.Properties.PublicIPAddressConfiguration = &armcompute.VirtualMachineScaleSetPublicIPAddressConfiguration{
					Name: new("pip"),
				}
			},
		},
		{
			name:     "unknown subnet",
			wantCode: "InvalidResourceReference",
			mutate: func(ss *armcompute.VirtualMachineScaleSet) {
				ipc := firstIPConfig(ss)
				ipc.Properties.Subnet.ID = new(*ipc.Properties.Subnet.ID + "-ghost")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			ss := s.flexSet(t, 2)
			tt.mutate(&ss)

			got, err := s.putScaleSet(t, "vmss1", ss)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))
				assert.Empty(t, s.vmsIn(t, "vmss1"), "a refused write leaves no machines behind")
				assert.Empty(t, s.nicNames(t))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "Flexible", string(*got.Properties.OrchestrationMode))
		})
	}
}

func TestComputeSDK_FlexibleModeImmutable(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	_, err := s.putScaleSet(t, "vmss1", s.scaleSet(t, 1))
	require.NoError(t, err)

	_, err = s.putScaleSet(t, "vmss1", s.flexSet(t, 1))
	require.Error(t, err)
	assert.Equal(t, "PropertyChangeNotAllowed", sdkErrCode(err))

	bad := s.scaleSet(t, 1)
	bad.Properties.OrchestrationMode = new(armcompute.OrchestrationMode("Mixed"))
	_, err = s.putScaleSet(t, "vmss2", bad)
	require.Error(t, err)
	assert.Equal(t, "InvalidParameter", sdkErrCode(err))
}

func TestComputeSDK_FlexibleZones(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	ss := s.flexSet(t, 4)
	ss.Zones = []*string{new("1"), new("2")}
	_, err := s.putScaleSet(t, "vmss1", ss)
	require.NoError(t, err)

	count := map[string]int{}

	for _, vm := range s.vmsIn(t, "vmss1") {
		require.Len(t, vm.Zones, 1)
		count[*vm.Zones[0]]++
	}

	assert.Equal(t, map[string]int{"1": 2, "2": 2}, count, "machines are spread across the zones")
}

func TestComputeSDK_FlexibleAttachVM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(*testing.T, *cmpStack, *armcompute.VirtualMachine)
		name     string
		vmss     string
		wantCode string
	}{
		{name: "attach", vmss: "flex", mutate: func(*testing.T, *cmpStack, *armcompute.VirtualMachine) {}},
		{
			name: "attach with fault domain",
			vmss: "flex",
			mutate: func(_ *testing.T, _ *cmpStack, vm *armcompute.VirtualMachine) {
				vm.Properties.PlatformFaultDomain = new(int32(0))
			},
		},
		{name: "fault domain out of range", vmss: "flex", wantCode: "InvalidParameter",
			mutate: func(_ *testing.T, _ *cmpStack, vm *armcompute.VirtualMachine) {
				vm.Properties.PlatformFaultDomain = new(int32(1))
			}},
		{
			name:     "uniform scale set",
			vmss:     "uni",
			wantCode: "InvalidParameter",
			mutate:   func(*testing.T, *cmpStack, *armcompute.VirtualMachine) {},
		},
		{
			name:     "missing scale set",
			vmss:     "ghost",
			wantCode: "ResourceNotFound",
			mutate:   func(*testing.T, *cmpStack, *armcompute.VirtualMachine) {},
		},
		{
			name:     "other location",
			vmss:     "flex",
			wantCode: "InvalidParameter",
			mutate: func(_ *testing.T, _ *cmpStack, vm *armcompute.VirtualMachine) {
				vm.Location = new("eastus")
			},
		},
		{
			name:     "availability set",
			vmss:     "flex",
			wantCode: "InvalidParameter",
			mutate: func(t *testing.T, s *cmpStack, vm *armcompute.VirtualMachine) {
				t.Helper()

				set, err := s.cf.NewAvailabilitySetsClient().
					CreateOrUpdate(t.Context(), netRG, "as1", armcompute.AvailabilitySet{
						Location: new("westus"), SKU: &armcompute.SKU{Name: new("Aligned")},
					}, nil)
				require.NoError(t, err)

				vm.Properties.AvailabilitySet = &armcompute.SubResource{ID: set.ID}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			flex, err := s.putScaleSet(t, "flex", s.flexSet(t, 0))
			require.NoError(t, err)
			_, err = s.putScaleSet(t, "uni", s.scaleSet(t, 0))
			require.NoError(t, err)

			vm := linuxVM(s.newNIC(t, "nic1"))
			id := *flex.ID
			if tt.vmss != "flex" {
				id = strings.Replace(id, "/flex", "/"+tt.vmss, 1)
			}

			vm.Properties.VirtualMachineScaleSet = &armcompute.SubResource{ID: new(id)}
			tt.mutate(t, s, &vm)

			_, err = s.putVM(t, netRG, "vm1", vm)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			listed := s.vmsIn(t, "flex")
			require.Len(t, listed, 1)
			assert.Equal(t, "vm1", *listed[0].Name)

			dl, err := s.cf.NewVirtualMachineScaleSetsClient().BeginDelete(t.Context(), netRG, "flex", nil)
			require.NoError(t, waitPoller(t, dl, err))

			got, err := s.cf.NewVirtualMachinesClient().Get(t.Context(), netRG, "vm1", nil)
			require.NoError(t, err, "a machine added by hand outlives the scale set")
			assert.Nil(t, got.Properties.VirtualMachineScaleSet)
		})
	}
}

func TestComputeSDK_VMListFilter(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)

	pager := s.cf.NewVirtualMachinesClient().NewListPager(netRG,
		&armcompute.VirtualMachinesClientListOptions{Filter: new("name eq 'x'")})
	_, err := pager.NextPage(t.Context())
	require.Error(t, err)
	assert.Equal(t, "InvalidParameter", sdkErrCode(err))
}

func TestComputeSDK_FlexibleExtensionProfile(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	ss := s.flexSet(t, 2)
	ss.Properties.VirtualMachineProfile.ExtensionProfile = &armcompute.VirtualMachineScaleSetExtensionProfile{
		Extensions: []*armcompute.VirtualMachineScaleSetExtension{{
			Name: new("cse"),
			Properties: &armcompute.VirtualMachineScaleSetExtensionProperties{
				Publisher: new("Microsoft.Azure.Extensions"), Type: new("CustomScript"), TypeHandlerVersion: new("2.1"),
				ProtectedSettings: map[string]any{"commandToExecute": extSecret},
			},
		}},
	}

	_, err := s.putScaleSet(t, "vmss1", ss)
	require.NoError(t, err)

	for _, vm := range s.vmsIn(t, "vmss1") {
		got, gerr := s.cf.NewVirtualMachineExtensionsClient().Get(t.Context(), netRG, *vm.Name, "cse", nil)
		require.NoError(t, gerr)
		assert.Equal(t, "CustomScript", *got.Properties.Type)
		assert.Nil(t, got.Properties.ProtectedSettings)
	}

	bad := s.flexSet(t, 1)
	bad.Properties.VirtualMachineProfile.ExtensionProfile = &armcompute.VirtualMachineScaleSetExtensionProfile{
		Extensions: []*armcompute.VirtualMachineScaleSetExtension{{
			Name:       new("broken"),
			Properties: &armcompute.VirtualMachineScaleSetExtensionProperties{Publisher: new("x")},
		}},
	}
	_, err = s.putScaleSet(t, "vmss2", bad)
	require.Error(t, err)
	assert.Empty(t, s.vmsIn(t, "vmss2"), "a failing extension rolls the machine back")
}

func TestComputeSDK_FlexibleBatchActions(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	_, err := s.putScaleSet(t, "vmss1", s.flexSet(t, 3))
	require.NoError(t, err)

	vms := s.vmsIn(t, "vmss1")
	target := *vms[0].Name
	c := s.cf.NewVirtualMachineScaleSetsClient()

	off, err := c.BeginPowerOff(
		t.Context(),
		netRG,
		"vmss1",
		&armcompute.VirtualMachineScaleSetsClientBeginPowerOffOptions{
			VMInstanceIDs: &armcompute.VirtualMachineScaleSetVMInstanceIDs{InstanceIDs: []*string{&target}},
		},
	)
	require.NoError(t, waitPoller(t, off, err))
	assert.Equal(t, "stopped", s.power(t, target))
	assert.Equal(t, "running", s.power(t, *vms[1].Name))

	del, err := c.BeginDeleteInstances(
		t.Context(),
		netRG,
		"vmss1",
		armcompute.VirtualMachineScaleSetVMInstanceRequiredIDs{
			InstanceIDs: []*string{&target},
		},
		nil,
	)
	require.NoError(t, waitPoller(t, del, err))
	assert.Len(t, s.vmsIn(t, "vmss1"), 2)
	assert.Len(t, s.nicNames(t), 2)

	bogus := "nope"
	off, err = c.BeginPowerOff(
		t.Context(),
		netRG,
		"vmss1",
		&armcompute.VirtualMachineScaleSetsClientBeginPowerOffOptions{
			VMInstanceIDs: &armcompute.VirtualMachineScaleSetVMInstanceIDs{InstanceIDs: []*string{&bogus}},
		},
	)
	err = waitPoller(t, off, err)
	require.Error(t, err)
	assert.Equal(t, "InvalidParameter", sdkErrCode(err))
}

func TestComputeSDK_FlexibleInstanceDelete(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	_, err := s.putScaleSet(t, "vmss1", s.flexSet(t, 1))
	require.NoError(t, err)

	name := *s.vmsIn(t, "vmss1")[0].Name

	dl, err := s.cf.NewVirtualMachineScaleSetVMsClient().BeginDelete(t.Context(), netRG, "vmss1", name, nil)
	err = waitPoller(t, dl, err)
	require.Error(t, err)
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err))

	got, err := s.cf.NewVirtualMachineScaleSetVMsClient().Get(t.Context(), netRG, "vmss1", name, nil)
	require.NoError(t, err)
	assert.Equal(t, name, *got.InstanceID)
}

func TestComputeSDK_FlexibleSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newCmpStack(t)
	_, err := src.putScaleSet(t, "vmss1", src.flexSet(t, 2))
	require.NoError(t, err)

	dst := newCmpStack(t)
	require.NoError(t, dst.handler.Restore(t.Context(), src.handler.Snapshot(t.Context())))

	assert.Len(t, dst.vmsIn(t, "vmss1"), 2)
	assert.Len(t, dst.nicNames(t), 2)

	_, err = dst.putScaleSet(t, "vmss1", dst.flexSet(t, 3))
	require.NoError(t, err)
	assert.Len(t, dst.vmsIn(t, "vmss1"), 3, "the instance counter survives a restore")
}
