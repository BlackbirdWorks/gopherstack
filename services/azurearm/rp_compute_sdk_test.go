package azurearm_test

import (
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	cmpTestPass = "Str0ng!Passw0rd1"
	cmpTestKey  = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC7 test@host"
)

type cmpStack struct {
	*netStack
	cf       *armcompute.ClientFactory
	subnetID string
}

func newCmpStack(t *testing.T) *cmpStack {
	t.Helper()

	ns := newNetStack(t)

	cf, err := armcompute.NewClientFactory(aciSub, aciCred{}, &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: ns.url, Audience: "https://management.core.windows.net/"},
		}},
		Transport: ns.hc,
		Retry:     policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	v := ns.vnet(t, "vnet1", []string{"10.0.0.0/16"}, map[string]string{"s1": "10.0.1.0/24"})

	return &cmpStack{netStack: ns, cf: cf, subnetID: *v.Properties.Subnets[0].ID}
}

func (s *cmpStack) newNIC(t *testing.T, name string) string {
	t.Helper()

	n, err := s.nic(t, name, s.subnetID, armnetwork.IPAllocationMethodDynamic, "", nil)
	require.NoError(t, err)

	return *n.ID
}

func linuxVM(nicID string) armcompute.VirtualMachine {
	return armcompute.VirtualMachine{
		Location: new("westus"),
		Properties: &armcompute.VirtualMachineProperties{
			HardwareProfile: &armcompute.HardwareProfile{
				VMSize: new(armcompute.VirtualMachineSizeTypes("Standard_B2s")),
			},
			StorageProfile: &armcompute.StorageProfile{
				ImageReference: &armcompute.ImageReference{
					Publisher: new(
						"Canonical",
					), Offer: new("ubuntu-24_04-lts"), SKU: new("server"), Version: new("latest"),
				},
				OSDisk: &armcompute.OSDisk{CreateOption: new(armcompute.DiskCreateOptionTypesFromImage)},
			},
			OSProfile: &armcompute.OSProfile{
				ComputerName: new("vmhost"), AdminUsername: new("azureuser"), AdminPassword: new(cmpTestPass),
			},
			NetworkProfile: &armcompute.NetworkProfile{
				NetworkInterfaces: []*armcompute.NetworkInterfaceReference{{ID: new(nicID)}},
			},
		},
	}
}

func (s *cmpStack) putVM(
	t *testing.T,
	rg, name string,
	vm armcompute.VirtualMachine,
) (armcompute.VirtualMachine, error) {
	t.Helper()

	poller, err := s.cf.NewVirtualMachinesClient().BeginCreateOrUpdate(t.Context(), rg, name, vm, nil)
	if err != nil {
		return armcompute.VirtualMachine{}, err
	}

	res, err := poller.PollUntilDone(t.Context(), pollFast())

	return res.VirtualMachine, err
}

func (s *cmpStack) mustVM(t *testing.T, name string, vm armcompute.VirtualMachine) armcompute.VirtualMachine {
	t.Helper()

	got, err := s.putVM(t, netRG, name, vm)
	require.NoError(t, err)

	return got
}

func computePath(rest string) string {
	return "/subscriptions/" + aciSub + "/resourceGroups/" + netRG + "/providers/Microsoft.Compute/" + rest
}

func (s *cmpStack) power(t *testing.T, name string) string {
	t.Helper()

	iv, err := s.cf.NewVirtualMachinesClient().InstanceView(t.Context(), netRG, name, nil)
	require.NoError(t, err)

	for _, st := range iv.Statuses {
		if code := *st.Code; strings.HasPrefix(code, "PowerState/") {
			return strings.TrimPrefix(code, "PowerState/")
		}
	}

	return ""
}

func (s *cmpStack) diskState(t *testing.T, name string) (string, string) {
	t.Helper()

	d, err := s.cf.NewDisksClient().Get(t.Context(), netRG, name, nil)
	require.NoError(t, err)

	mb := ""
	if d.ManagedBy != nil {
		mb = *d.ManagedBy
	}

	return string(*d.Properties.DiskState), mb
}

func TestComputeSDK_VMLifecycle(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	nicID := s.newNIC(t, "nic1")
	vms := s.cf.NewVirtualMachinesClient()

	vm := s.mustVM(t, "vm1", linuxVM(nicID))
	p := vm.Properties
	assert.Equal(t, "Standard_B2s", string(*p.HardwareProfile.VMSize))
	assert.NotEmpty(t, *p.VMID)
	assert.Equal(t, "Succeeded", *p.ProvisioningState)
	assert.Nil(t, p.OSProfile.AdminPassword, "secrets are never echoed")
	assert.Equal(t, "azureuser", *p.OSProfile.AdminUsername)
	assert.Equal(t, "vmhost", *p.OSProfile.ComputerName)

	osDisk := p.StorageProfile.OSDisk
	require.NotNil(t, osDisk.ManagedDisk.ID)
	assert.Equal(t, "Premium_LRS", string(*osDisk.ManagedDisk.StorageAccountType))
	assert.EqualValues(t, 30, *osDisk.DiskSizeGB)
	assert.Equal(t, "Linux", string(*osDisk.OSType))

	state, managedBy := s.diskState(t, *osDisk.Name)
	assert.Equal(t, "Attached", state)
	assert.Equal(t, *vm.ID, managedBy)

	nic, err := s.net.NewInterfacesClient().Get(t.Context(), netRG, "nic1", nil)
	require.NoError(t, err)
	assert.Equal(t, *vm.ID, *nic.Properties.VirtualMachine.ID)
	assert.True(t, *nic.Properties.Primary)

	assert.Equal(t, "running", s.power(t, "vm1"))

	delPoller, err := s.net.NewInterfacesClient().BeginDelete(t.Context(), netRG, "nic1", nil)
	if err == nil {
		_, err = delPoller.PollUntilDone(t.Context(), pollFast())
	}

	require.Error(t, err)
	assert.Equal(t, "NicInUse", sdkErrCode(err))

	expanded, err := vms.Get(t.Context(), netRG, "vm1", &armcompute.VirtualMachinesClientGetOptions{
		Expand: new(armcompute.InstanceViewTypesInstanceView),
	})
	require.NoError(t, err)
	require.NotNil(t, expanded.Properties.InstanceView)
	assert.NotEmpty(t, expanded.Properties.InstanceView.Statuses)

	got, err := vms.Get(t.Context(), netRG, "vm1", nil)
	require.NoError(t, err)
	assert.Nil(t, got.Properties.InstanceView)
	assert.Equal(t, *vm.Properties.VMID, *got.Properties.VMID)
}

func TestComputeSDK_VMActions(t *testing.T) {
	t.Parallel()

	steps := []struct {
		action    string
		wantPower string
		wantCode  string
	}{
		{action: "restart", wantPower: "running"},
		{action: "generalize", wantCode: "OperationNotAllowed", wantPower: "running"},
		{action: "redeploy", wantPower: "running"},
		{action: "powerOff", wantPower: "stopped"},
		{action: "restart", wantCode: "OperationNotAllowed", wantPower: "stopped"},
		{action: "start", wantPower: "running"},
		{action: "deallocate", wantPower: "deallocated"},
		{action: "redeploy", wantCode: "OperationNotAllowed", wantPower: "deallocated"},
		{action: "powerOff", wantPower: "deallocated"},
		{action: "generalize", wantPower: "deallocated"},
		{action: "start", wantCode: "OperationNotAllowed", wantPower: "deallocated"},
	}

	s := newCmpStack(t)
	s.mustVM(t, "vm1", linuxVM(s.newNIC(t, "nic1")))

	c := s.cf.NewVirtualMachinesClient()

	for _, st := range steps {
		var err error

		switch st.action {
		case "restart":
			pl, perr := c.BeginRestart(t.Context(), netRG, "vm1", nil)
			err = waitPoller(t, pl, perr)
		case "generalize":
			_, err = c.Generalize(t.Context(), netRG, "vm1", nil)
		case "redeploy":
			pl, perr := c.BeginRedeploy(t.Context(), netRG, "vm1", nil)
			err = waitPoller(t, pl, perr)
		case "powerOff":
			pl, perr := c.BeginPowerOff(t.Context(), netRG, "vm1", nil)
			err = waitPoller(t, pl, perr)
		case "start":
			pl, perr := c.BeginStart(t.Context(), netRG, "vm1", nil)
			err = waitPoller(t, pl, perr)
		case "deallocate":
			pl, perr := c.BeginDeallocate(t.Context(), netRG, "vm1", nil)
			err = waitPoller(t, pl, perr)
		}

		if st.wantCode != "" {
			require.Error(t, err, st.action)
			assert.Equal(t, st.wantCode, sdkErrCode(err), st.action)
		} else {
			require.NoError(t, err, st.action)
		}

		assert.Equal(t, st.wantPower, s.power(t, "vm1"), st.action)
	}

	iv, err := c.InstanceView(t.Context(), netRG, "vm1", nil)
	require.NoError(t, err)

	codes := make([]string, 0, len(iv.Statuses))

	for _, st := range iv.Statuses {
		codes = append(codes, *st.Code)
	}

	assert.Contains(t, codes, "OSState/generalized")

	ghost, perr := c.BeginStart(t.Context(), netRG, "ghost", nil)
	err = waitPoller(t, ghost, perr)
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
}

func waitPoller[T any](t *testing.T, p *runtime.Poller[T], err error) error {
	t.Helper()

	if err != nil {
		return err
	}

	_, err = p.PollUntilDone(t.Context(), pollFast())

	return err
}
