package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeSDK_VMValidation(t *testing.T) {
	t.Parallel()

	windows := func(vm *armcompute.VirtualMachine) {
		vm.Properties.OSProfile.WindowsConfiguration = &armcompute.WindowsConfiguration{}
	}
	dataDisks := func(n int) func(vm *armcompute.VirtualMachine) {
		return func(vm *armcompute.VirtualMachine) {
			for i := range n {
				vm.Properties.StorageProfile.DataDisks = append(
					vm.Properties.StorageProfile.DataDisks,
					&armcompute.DataDisk{
						Lun: new(
							int32(i),
						), CreateOption: new(armcompute.DiskCreateOptionTypesEmpty), DiskSizeGB: new(int32(8)),
					},
				)
			}
		}
	}

	tests := []struct {
		mutate   func(vm *armcompute.VirtualMachine)
		name     string
		vmName   string
		wantCode string
	}{
		{name: "valid linux password", mutate: func(*armcompute.VirtualMachine) {}},
		{name: "unknown size", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.HardwareProfile.VMSize = new(armcompute.VirtualMachineSizeTypes("Standard_Nope"))
		}},
		{name: "missing size", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.HardwareProfile.VMSize = nil
		}},
		{name: "weak password", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.OSProfile.AdminPassword = new("password")
		}},
		{name: "short linux password", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.OSProfile.AdminPassword = new("Ab1!")
		}},
		{name: "banned password", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.OSProfile.AdminPassword = new("Password1")
		}},
		{name: "windows short password", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			windows(vm)
			vm.Properties.OSProfile.AdminPassword = new("Ab1!xyz")
		}},
		{name: "reserved username", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.OSProfile.AdminUsername = new("Administrator")
		}},
		{
			name:     "windows computer name too long",
			wantCode: "InvalidParameter",
			mutate: func(vm *armcompute.VirtualMachine) {
				windows(vm)
				vm.Properties.OSProfile.ComputerName = new("thisnameiswaytoolong")
			},
		},
		{
			name:     "windows numeric computer name",
			wantCode: "InvalidParameter",
			mutate: func(vm *armcompute.VirtualMachine) {
				windows(vm)
				vm.Properties.OSProfile.ComputerName = new("12345")
			},
		},
		{name: "ssh key wrong path", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.OSProfile.AdminPassword = nil
			vm.Properties.OSProfile.LinuxConfiguration = &armcompute.LinuxConfiguration{
				DisablePasswordAuthentication: new(true),
				SSH: &armcompute.SSHConfiguration{PublicKeys: []*armcompute.SSHPublicKey{
					{Path: new("/root/.ssh/authorized_keys"), KeyData: new(cmpTestKey)},
				}},
			}
		}},
		{name: "ssh key garbage", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.OSProfile.AdminPassword = nil
			vm.Properties.OSProfile.LinuxConfiguration = &armcompute.LinuxConfiguration{
				DisablePasswordAuthentication: new(true),
				SSH: &armcompute.SSHConfiguration{PublicKeys: []*armcompute.SSHPublicKey{
					{Path: new("/home/azureuser/.ssh/authorized_keys"), KeyData: new("not a key")},
				}},
			}
		}},
		{name: "ssh disabled without keys", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.OSProfile.AdminPassword = nil
			vm.Properties.OSProfile.LinuxConfiguration = &armcompute.LinuxConfiguration{
				DisablePasswordAuthentication: new(true),
			}
		}},
		{name: "ssh disabled with password", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.OSProfile.LinuxConfiguration = &armcompute.LinuxConfiguration{
				DisablePasswordAuthentication: new(true),
				SSH: &armcompute.SSHConfiguration{PublicKeys: []*armcompute.SSHPublicKey{
					{Path: new("/home/azureuser/.ssh/authorized_keys"), KeyData: new(cmpTestKey)},
				}},
			}
		}},
		{name: "valid ssh key", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.OSProfile.AdminPassword = nil
			vm.Properties.OSProfile.LinuxConfiguration = &armcompute.LinuxConfiguration{
				DisablePasswordAuthentication: new(true),
				SSH: &armcompute.SSHConfiguration{PublicKeys: []*armcompute.SSHPublicKey{
					{Path: new("/home/azureuser/.ssh/authorized_keys"), KeyData: new(cmpTestKey)},
				}},
			}
		}},
		{name: "no password and no keys", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.OSProfile.AdminPassword = nil
		}},
		{name: "missing nic", wantCode: "ResourceNotFound", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.NetworkProfile.NetworkInterfaces[0].ID = new(
				"/subscriptions/" + aciSub + "/resourceGroups/" + netRG + "/providers/Microsoft.Network/networkInterfaces/ghost",
			)
		}},
		{name: "no nics", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.NetworkProfile.NetworkInterfaces = nil
		}},
		{name: "bad zone", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Zones = []*string{new("4")}
		}},
		{name: "valid zone", mutate: func(vm *armcompute.VirtualMachine) { vm.Zones = []*string{new("2")} }},
		{name: "image offer missing", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.StorageProfile.ImageReference.Offer = nil
		}},
		{name: "image version bad", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.StorageProfile.ImageReference.Version = new("newest")
		}},
		{name: "image version exact", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.StorageProfile.ImageReference.Version = new("24.04.202501010")
		}},
		{name: "missing os disk option", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.StorageProfile.OSDisk.CreateOption = nil
		}},
		{
			name:     "premium disk on standard-only size",
			wantCode: "InvalidParameter",
			mutate: func(vm *armcompute.VirtualMachine) {
				vm.Properties.HardwareProfile.VMSize = new(armcompute.VirtualMachineSizeTypes("Standard_D2_v5"))
				vm.Properties.StorageProfile.OSDisk.ManagedDisk = &armcompute.ManagedDiskParameters{
					StorageAccountType: new(armcompute.StorageAccountTypesPremiumLRS),
				}
			},
		},
		{name: "too many data disks", wantCode: "OperationNotAllowed", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.HardwareProfile.VMSize = new(armcompute.VirtualMachineSizeTypes("Standard_B1s"))
			dataDisks(3)(vm)
		}},
		{name: "data disks within limit", mutate: dataDisks(4)},
		{name: "duplicate lun", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			dataDisks(2)(vm)
			vm.Properties.StorageProfile.DataDisks[1].Lun = new(int32(0))
		}},
		{name: "empty disk without size", wantCode: "InvalidParameter", mutate: func(vm *armcompute.VirtualMachine) {
			dataDisks(1)(vm)
			vm.Properties.StorageProfile.DataDisks[0].DiskSizeGB = nil
		}},
		{
			name:     "bad vm name",
			vmName:   "-bad",
			wantCode: "InvalidResourceName",
			mutate:   func(*armcompute.VirtualMachine) {},
		},
		{name: "missing location", wantCode: "LocationRequired", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Location = nil
		}},
		{name: "availability set missing", wantCode: "ResourceNotFound", mutate: func(vm *armcompute.VirtualMachine) {
			vm.Properties.AvailabilitySet = &armcompute.SubResource{ID: new("/subscriptions/" + aciSub +
				"/resourceGroups/" + netRG + "/providers/Microsoft.Compute/availabilitySets/ghost")}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			vm := linuxVM(s.newNIC(t, "nic1"))
			tt.mutate(&vm)

			name := tt.vmName
			if name == "" {
				name = "vm1"
			}

			got, err := s.putVM(t, netRG, name, vm)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				_, getErr := s.cf.NewVirtualMachinesClient().Get(t.Context(), netRG, name, nil)
				require.Error(t, getErr, "a rejected write leaves no virtual machine")

				nic, nerr := s.net.NewInterfacesClient().Get(t.Context(), netRG, "nic1", nil)
				require.NoError(t, nerr)
				assert.Nil(t, nic.Properties.VirtualMachine, "a rejected write leaves the NIC unattached")

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, *got.Properties.VMID)
			assert.Nil(t, got.Properties.OSProfile.AdminPassword)
		})
	}
}

func TestComputeSDK_VMUnknownGroup(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	_, err := s.putVM(t, "ghost-rg", "vm1", linuxVM(s.newNIC(t, "nic1")))
	require.Error(t, err)
	assert.Equal(t, "ResourceGroupNotFound", sdkErrCode(err))
}

func TestComputeSDK_VMNICRules(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	nic1, nic2 := s.newNIC(t, "nic1"), s.newNIC(t, "nic2")

	s.mustVM(t, "vm1", linuxVM(nic1))

	other := linuxVM(nic1)
	other.Properties.OSProfile.ComputerName = new("other")
	_, err := s.putVM(t, netRG, "vm2", other)
	require.Error(t, err)
	assert.Equal(t, "NicInUse", sdkErrCode(err), "a NIC attaches to one VM")

	west := linuxVM(nic2)
	west.Location = new("eastus")
	_, err = s.putVM(t, netRG, "vm3", west)
	require.Error(t, err)
	assert.Equal(t, "NetworkInterfaceAndVirtualMachineMustBeInSameLocation", sdkErrCode(err))

	two := linuxVM(nic2)
	two.Properties.NetworkProfile.NetworkInterfaces = append(two.Properties.NetworkProfile.NetworkInterfaces,
		&armcompute.NetworkInterfaceReference{ID: new(s.newNIC(t, "nic3"))})
	_, err = s.putVM(t, netRG, "vm4", two)
	require.Error(t, err, "several NICs need exactly one primary")

	two.Properties.NetworkProfile.NetworkInterfaces[0].Properties = &armcompute.NetworkInterfaceReferenceProperties{
		Primary: new(true), DeleteOption: new(armcompute.DeleteOptionsDelete),
	}
	two.Properties.NetworkProfile.NetworkInterfaces[1].Properties = &armcompute.NetworkInterfaceReferenceProperties{
		Primary: new(false),
	}
	vm4 := s.mustVM(t, "vm4", two)
	assert.Len(t, vm4.Properties.NetworkProfile.NetworkInterfaces, 2)

	del := func() error {
		pl, perr := s.cf.NewVirtualMachinesClient().BeginDelete(t.Context(), netRG, "vm4", nil)

		return waitPoller(t, pl, perr)
	}
	require.NoError(t, del())

	_, err = s.net.NewInterfacesClient().Get(t.Context(), netRG, "nic2", nil)
	require.Error(t, err, "deleteOption Delete removes the NIC")

	n3, err := s.net.NewInterfacesClient().Get(t.Context(), netRG, "nic3", nil)
	require.NoError(t, err, "deleteOption Detach (the default) keeps the NIC")
	assert.Nil(t, n3.Properties.VirtualMachine)

	pl, perr := s.cf.NewVirtualMachinesClient().BeginDelete(t.Context(), netRG, "vm1", nil)
	require.NoError(t, waitPoller(t, pl, perr))

	n1, err := s.net.NewInterfacesClient().Get(t.Context(), netRG, "nic1", nil)
	require.NoError(t, err)
	assert.Nil(t, n1.Properties.VirtualMachine, "deleting the VM releases its NIC")

	again, perr := s.cf.NewVirtualMachinesClient().BeginDelete(t.Context(), netRG, "vm1", nil)
	require.NoError(t, waitPoller(t, again, perr), "deleting a missing VM succeeds")
}
