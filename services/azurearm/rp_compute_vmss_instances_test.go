package azurearm_test

import (
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *cmpStack) instanceNICs(t *testing.T, instance string) []*armnetwork.Interface {
	t.Helper()

	var out []*armnetwork.Interface

	pager := s.net.NewInterfacesClient().NewListVirtualMachineScaleSetVMNetworkInterfacesPager(
		netRG, "vmss1", instance, nil)

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		out = append(out, page.Value...)
	}

	return out
}

func instanceIDs(list []*armcompute.VirtualMachineScaleSetVM) []string {
	out := make([]string, 0, len(list))
	for _, in := range list {
		out = append(out, *in.InstanceID)
	}

	return out
}

func TestComputeSDK_UniformInstanceInterfaces(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	_, err := s.putScaleSet(t, "vmss1", s.scaleSet(t, 2))
	require.NoError(t, err)

	all := s.net.NewInterfacesClient().NewListVirtualMachineScaleSetNetworkInterfacesPager(netRG, "vmss1", nil)
	page, err := all.NextPage(t.Context())
	require.NoError(t, err)
	require.Len(t, page.Value, 2)

	nics := s.instanceNICs(t, "1")
	require.Len(t, nics, 1)
	assert.Equal(t, "nic", *nics[0].Name)
	assert.Contains(t, *nics[0].ID, "/virtualMachineScaleSets/vmss1/virtualMachines/1/networkInterfaces/nic")
	assert.Contains(t, *nics[0].Properties.VirtualMachine.ID, "/virtualMachineScaleSets/vmss1/virtualMachines/1")

	ip := nics[0].Properties.IPConfigurations[0].Properties
	assert.NotEmpty(t, *ip.PrivateIPAddress)

	nicsClient := s.net.NewInterfacesClient()

	one, err := nicsClient.GetVirtualMachineScaleSetNetworkInterface(t.Context(), netRG, "vmss1", "1", "nic", nil)
	require.NoError(t, err)
	assert.Equal(t, *nics[0].Properties.ResourceGUID, *one.Properties.ResourceGUID)

	vm, err := s.cf.NewVirtualMachineScaleSetVMsClient().Get(t.Context(), netRG, "vmss1", "1", nil)
	require.NoError(t, err)
	assert.Equal(t, *nics[0].ID, *vm.Properties.NetworkProfile.NetworkInterfaces[0].ID)

	top := s.net.NewInterfacesClient().NewListPager(netRG, nil)
	topPage, err := top.NextPage(t.Context())
	require.NoError(t, err)
	assert.Empty(t, topPage.Value, "instance interfaces are not top-level network interfaces")

	_, err = nicsClient.GetVirtualMachineScaleSetNetworkInterface(t.Context(), netRG, "vmss1", "9", "nic", nil)
	require.Error(t, err)

	pl, err := s.net.NewSubnetsClient().BeginDelete(t.Context(), netRG, "vnet1", "s1", nil)
	err = waitPoller(t, pl, err)
	require.Error(t, err)
	assert.Equal(t, "InUseSubnetCannotBeDeleted", sdkErrCode(err), "a scale set keeps its subnet in use")

	_, err = s.putScaleSet(t, "vmss1", s.scaleSet(t, 1))
	require.NoError(t, err)

	page, err = nicsClient.NewListVirtualMachineScaleSetNetworkInterfacesPager(netRG, "vmss1", nil).
		NextPage(t.Context())
	require.NoError(t, err)
	assert.Len(t, page.Value, 1, "scale-in removes the interface of the removed instance")

	del, err := s.cf.NewVirtualMachineScaleSetsClient().BeginDelete(t.Context(), netRG, "vmss1", nil)
	require.NoError(t, waitPoller(t, del, err))

	pl, err = s.net.NewSubnetsClient().BeginDelete(t.Context(), netRG, "vnet1", "s1", nil)
	require.NoError(t, waitPoller(t, pl, err), "deleting the scale set frees the subnet")
}

func TestComputeSDK_UniformInterfacesPersist(t *testing.T) {
	t.Parallel()

	src := newCmpStack(t)
	_, err := src.putScaleSet(t, "vmss1", src.scaleSet(t, 2))
	require.NoError(t, err)

	dst := newCmpStack(t)
	require.NoError(t, dst.handler.Restore(t.Context(), src.handler.Snapshot(t.Context())))

	nics := dst.instanceNICs(t, "0")
	require.Len(t, nics, 1)

	_, err = dst.putScaleSet(t, "vmss1", dst.scaleSet(t, 3))
	require.NoError(t, err)
	assert.Len(t, dst.instanceNICs(t, "2"), 1)
}

func TestComputeSDK_ScaleInPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		rule  string
		zones []string
		want  []string
	}{
		{name: "default newest first", rule: "", want: []string{"0", "1", "2"}},
		{name: "newest", rule: "NewestVM", want: []string{"0", "1", "2"}},
		{name: "oldest", rule: "OldestVM", want: []string{"3", "4", "5"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			ss := s.scaleSet(t, 6)
			ss.Properties.ScaleInPolicy = &armcompute.ScaleInPolicy{}

			if tt.rule != "" {
				ss.Properties.ScaleInPolicy.Rules = []*armcompute.VirtualMachineScaleSetScaleInRules{
					new(armcompute.VirtualMachineScaleSetScaleInRules(tt.rule)),
				}
			}

			for _, z := range tt.zones {
				ss.Zones = append(ss.Zones, new(z))
			}

			_, err := s.putScaleSet(t, "vmss1", ss)
			require.NoError(t, err)

			shrunk := ss
			shrunk.SKU = &armcompute.SKU{Name: new("Standard_B2s"), Capacity: new(int64(3))}
			_, err = s.putScaleSet(t, "vmss1", shrunk)
			require.NoError(t, err)

			assert.ElementsMatch(t, tt.want, instanceIDs(s.instances(t)))
		})
	}
}

func TestComputeSDK_ScaleInZoneBalance(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	ss := s.scaleSet(t, 6)
	ss.Zones = []*string{new("1"), new("2")}

	_, err := s.putScaleSet(t, "vmss1", ss)
	require.NoError(t, err)

	vms := s.cf.NewVirtualMachineScaleSetVMsClient()

	for _, id := range []string{"1", "3"} {
		pl, derr := vms.BeginDelete(t.Context(), netRG, "vmss1", id, nil)
		require.NoError(t, waitPoller(t, pl, derr))
	}

	shrunk := s.scaleSet(t, 3)
	shrunk.Zones = ss.Zones

	_, err = s.putScaleSet(t, "vmss1", shrunk)
	require.NoError(t, err)

	got := s.instances(t)
	assert.ElementsMatch(t, []string{"0", "2", "5"}, instanceIDs(got), "the zone holding three instances loses one")

	zones := map[string]string{}
	for _, in := range got {
		zones[*in.InstanceID] = *in.Zones[0]
	}

	assert.Equal(t, map[string]string{"0": "1", "2": "1", "5": "2"}, zones)
}

func TestComputeSDK_ScaleInPolicyInvalid(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	ss := s.scaleSet(t, 1)
	ss.Properties.ScaleInPolicy = &armcompute.ScaleInPolicy{
		Rules: []*armcompute.VirtualMachineScaleSetScaleInRules{
			new(armcompute.VirtualMachineScaleSetScaleInRules("Random")),
		},
	}

	_, err := s.putScaleSet(t, "vmss1", ss)
	require.Error(t, err)
	assert.Equal(t, "InvalidParameter", sdkErrCode(err))
}

func TestComputeSDK_UniformInstanceUpdateAndProtection(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	_, err := s.putScaleSet(t, "vmss1", s.scaleSet(t, 3))
	require.NoError(t, err)

	vms := s.cf.NewVirtualMachineScaleSetVMsClient()
	cur, err := vms.Get(t.Context(), netRG, "vmss1", "2", nil)
	require.NoError(t, err)

	cur.Tags = map[string]*string{"role": new("canary")}
	cur.Properties.ProtectionPolicy = &armcompute.VirtualMachineScaleSetVMProtectionPolicy{
		ProtectFromScaleIn: new(true),
	}

	pl, err := vms.BeginUpdate(t.Context(), netRG, "vmss1", "2", cur.VirtualMachineScaleSetVM, nil)
	require.NoError(t, waitPoller(t, pl, err))

	got, err := vms.Get(t.Context(), netRG, "vmss1", "2", nil)
	require.NoError(t, err)
	assert.Equal(t, "canary", *got.Tags["role"])
	assert.True(t, *got.Properties.ProtectionPolicy.ProtectFromScaleIn)

	_, err = s.putScaleSet(t, "vmss1", s.scaleSetCapacity(t, 2))
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"0", "2"}, instanceIDs(s.instances(t)), "the protected instance survives scale-in")

	_, err = s.putScaleSet(t, "vmss1", s.scaleSetCapacity(t, 1))
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"2"}, instanceIDs(s.instances(t)))

	_, err = s.putScaleSet(t, "vmss1", s.scaleSetCapacity(t, 0))
	require.Error(t, err)
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err))

	bad := got.VirtualMachineScaleSetVM
	bad.Properties.HardwareProfile = &armcompute.HardwareProfile{
		VMSize: new(armcompute.VirtualMachineSizeTypes("Standard_D2s_v3")),
	}

	_, err = vms.BeginUpdate(t.Context(), netRG, "vmss1", "2", bad, nil)
	require.Error(t, err)
	assert.Equal(t, "InvalidParameter", sdkErrCode(err))
}

func (s *cmpStack) scaleSetCapacity(t *testing.T, capacity int64) armcompute.VirtualMachineScaleSet {
	t.Helper()

	return s.scaleSet(t, capacity)
}

type vmsClient = armcompute.VirtualMachineScaleSetVMsClient

func TestComputeSDK_UniformInstanceActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(*testing.T, *vmsClient) error
		name string
		want string
	}{
		{name: "power off", want: "PowerState/stopped", call: func(t *testing.T, c *vmsClient) error {
			t.Helper()

			pl, err := c.BeginPowerOff(t.Context(), netRG, "vmss1", "1", nil)

			return waitPoller(t, pl, err)
		}},
		{name: "deallocate", want: "PowerState/deallocated", call: func(t *testing.T, c *vmsClient) error {
			t.Helper()

			pl, err := c.BeginDeallocate(t.Context(), netRG, "vmss1", "1", nil)

			return waitPoller(t, pl, err)
		}},
		{name: "restart", want: "PowerState/running", call: func(t *testing.T, c *vmsClient) error {
			t.Helper()

			pl, err := c.BeginRestart(t.Context(), netRG, "vmss1", "1", nil)

			return waitPoller(t, pl, err)
		}},
		{name: "reimage", want: "PowerState/running", call: func(t *testing.T, c *vmsClient) error {
			t.Helper()

			pl, err := c.BeginReimage(t.Context(), netRG, "vmss1", "1", nil)

			return waitPoller(t, pl, err)
		}},
		{name: "redeploy", want: "PowerState/running", call: func(t *testing.T, c *vmsClient) error {
			t.Helper()

			pl, err := c.BeginRedeploy(t.Context(), netRG, "vmss1", "1", nil)

			return waitPoller(t, pl, err)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			_, err := s.putScaleSet(t, "vmss1", s.scaleSet(t, 2))
			require.NoError(t, err)

			vms := s.cf.NewVirtualMachineScaleSetVMsClient()
			require.NoError(t, tt.call(t, vms))

			got, err := vms.Get(t.Context(), netRG, "vmss1", "1", nil)
			require.NoError(t, err)

			var power string

			for _, st := range got.Properties.InstanceView.Statuses {
				if strings.HasPrefix(*st.Code, "PowerState/") {
					power = *st.Code
				}
			}

			assert.Equal(t, tt.want, power)

			other, err := vms.Get(t.Context(), netRG, "vmss1", "0", nil)
			require.NoError(t, err)
			assert.True(t, *other.Properties.LatestModelApplied)
		})
	}
}

func TestComputeSDK_RollingUpgrades(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	rolling := func(sku string) armcompute.VirtualMachineScaleSet {
		ss := s.scaleSet(t, 3)
		ss.Properties.UpgradePolicy.Mode = new(armcompute.UpgradeModeRolling)
		ss.Properties.UpgradePolicy.RollingUpgradePolicy = &armcompute.RollingUpgradePolicy{
			MaxBatchInstancePercent: new(int32(50)),
		}
		ss.Properties.VirtualMachineProfile.StorageProfile.ImageReference.SKU = new(sku)

		return ss
	}

	got, err := s.putScaleSet(t, "vmss1", rolling("server"))
	require.NoError(t, err)

	pol := got.Properties.UpgradePolicy.RollingUpgradePolicy
	assert.EqualValues(t, 50, *pol.MaxBatchInstancePercent)
	assert.EqualValues(t, 20, *pol.MaxUnhealthyInstancePercent, "documented default")
	assert.Equal(t, "PT0S", *pol.PauseTimeBetweenBatches)

	rc := s.cf.NewVirtualMachineScaleSetRollingUpgradesClient()

	_, err = rc.GetLatest(t.Context(), netRG, "vmss1", nil)
	require.Error(t, err, "no upgrade has run yet")

	_, err = s.putScaleSet(t, "vmss1", rolling("server-arm"))
	require.NoError(t, err)

	latest, err := rc.GetLatest(t.Context(), netRG, "vmss1", nil)
	require.NoError(t, err)
	assert.Equal(t, armcompute.RollingUpgradeStatusCodeCompleted, *latest.Properties.RunningStatus.Code)
	assert.EqualValues(t, 3, *latest.Properties.Progress.SuccessfulInstanceCount)
	assert.EqualValues(t, 50, *latest.Properties.Policy.MaxBatchInstancePercent)

	for _, in := range s.instances(t) {
		assert.True(t, *in.Properties.LatestModelApplied)
	}

	pl, err := rc.BeginCancel(t.Context(), netRG, "vmss1", nil)
	err = waitPoller(t, pl, err)
	require.Error(t, err)
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err))

	manual := s.scaleSet(t, 3)
	manual.Properties.VirtualMachineProfile.StorageProfile.ImageReference.SKU = new("server-x")
	manual.Properties.UpgradePolicy.Mode = new(armcompute.UpgradeModeManual)
	_, err = s.putScaleSet(t, "vmss1", manual)
	require.NoError(t, err)

	start, err := rc.BeginStartOSUpgrade(t.Context(), netRG, "vmss1", nil)
	require.NoError(t, waitPoller(t, start, err))

	for _, in := range s.instances(t) {
		assert.True(t, *in.Properties.LatestModelApplied)
	}

	bad := rolling("server")
	bad.Properties.UpgradePolicy.RollingUpgradePolicy.MaxBatchInstancePercent = new(int32(0))
	_, err = s.putScaleSet(t, "vmss1", bad)
	require.Error(t, err)
	assert.Equal(t, "InvalidParameter", sdkErrCode(err))
}

func TestComputeSDK_FlexibleScaleInPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rule       armcompute.VirtualMachineScaleSetScaleInRules
		wantOldest bool
	}{
		{name: "default", rule: armcompute.VirtualMachineScaleSetScaleInRulesDefault},
		{name: "oldest", rule: armcompute.VirtualMachineScaleSetScaleInRulesOldestVM, wantOldest: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			ss := s.flexSet(t, 2)
			ss.Properties.ScaleInPolicy = &armcompute.ScaleInPolicy{
				Rules: []*armcompute.VirtualMachineScaleSetScaleInRules{new(tt.rule)},
			}

			_, err := s.putScaleSet(t, "vmss1", ss)
			require.NoError(t, err)

			before := s.vmsIn(t, "vmss1")
			require.Len(t, before, 2)

			shrunk := s.flexSet(t, 1)
			shrunk.Properties.ScaleInPolicy = ss.Properties.ScaleInPolicy
			_, err = s.putScaleSet(t, "vmss1", shrunk)
			require.NoError(t, err)

			after := s.vmsIn(t, "vmss1")
			require.Len(t, after, 1)

			keep := *before[1].Name
			if !tt.wantOldest {
				keep = *before[0].Name
			}

			assert.Equal(t, keep, *after[0].Name)
		})
	}
}

func TestComputeSDK_ScaleInZonesThenRule(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	ss := s.scaleSet(t, 6)
	ss.Zones = []*string{new("1"), new("2")}
	ss.Properties.ScaleInPolicy = &armcompute.ScaleInPolicy{
		Rules: []*armcompute.VirtualMachineScaleSetScaleInRules{
			new(armcompute.VirtualMachineScaleSetScaleInRulesOldestVM),
		},
	}

	_, err := s.putScaleSet(t, "vmss1", ss)
	require.NoError(t, err)

	vms := s.cf.NewVirtualMachineScaleSetVMsClient()

	for _, id := range []string{"2", "4"} {
		pl, derr := vms.BeginDelete(t.Context(), netRG, "vmss1", id, nil)
		require.NoError(t, waitPoller(t, pl, derr))
	}

	shrunk := ss
	shrunk.SKU = &armcompute.SKU{Name: new("Standard_B2s"), Capacity: new(int64(3))}
	_, err = s.putScaleSet(t, "vmss1", shrunk)
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"0", "3", "5"}, instanceIDs(s.instances(t)),
		"zone 2 holds three instances, so its oldest goes even though instance 0 is the oldest overall")
}

func TestComputeSDK_ScaleInPrioritizeUnhealthy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want []string
		flag bool
	}{
		{name: "off removes the newest", want: []string{"0", "1", "2"}},
		{name: "on removes the stopped instance first", flag: true, want: []string{"0", "2", "3"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			ss := s.scaleSet(t, 4)
			ss.Properties.ScaleInPolicy = &armcompute.ScaleInPolicy{PrioritizeUnhealthyVMs: &tt.flag}

			_, err := s.putScaleSet(t, "vmss1", ss)
			require.NoError(t, err)

			pl, err := s.cf.NewVirtualMachineScaleSetVMsClient().BeginPowerOff(t.Context(), netRG, "vmss1", "1", nil)
			require.NoError(t, waitPoller(t, pl, err))

			shrunk := ss
			shrunk.SKU = &armcompute.SKU{Name: new("Standard_B2s"), Capacity: new(int64(3))}
			_, err = s.putScaleSet(t, "vmss1", shrunk)
			require.NoError(t, err)

			assert.ElementsMatch(t, tt.want, instanceIDs(s.instances(t)))
		})
	}
}
