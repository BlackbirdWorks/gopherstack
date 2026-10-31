package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *cmpStack) putDisk(t *testing.T, name string, d armcompute.Disk) (armcompute.Disk, error) {
	t.Helper()

	pl, err := s.cf.NewDisksClient().BeginCreateOrUpdate(t.Context(), netRG, name, d, nil)
	if err != nil {
		return armcompute.Disk{}, err
	}

	res, err := pl.PollUntilDone(t.Context(), pollFast())

	return res.Disk, err
}

func emptyDisk(gb int32) armcompute.Disk {
	return armcompute.Disk{
		Location: new("westus"),
		Properties: &armcompute.DiskProperties{
			CreationData: &armcompute.CreationData{CreateOption: new(armcompute.DiskCreateOptionEmpty)},
			DiskSizeGB:   new(gb),
		},
	}
}

func TestComputeSDK_DiskCreation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		build     func(s *cmpStack) armcompute.Disk
		name      string
		wantCode  string
		wantState string
		wantGB    int32
	}{
		{
			name:  "empty",
			build: func(*cmpStack) armcompute.Disk { return emptyDisk(64) }, wantGB: 64, wantState: "Unattached",
		},
		{
			name: "empty without size", wantCode: "InvalidParameter",
			build: func(*cmpStack) armcompute.Disk {
				d := emptyDisk(0)
				d.Properties.DiskSizeGB = nil

				return d
			},
		},
		{
			name: "empty too large", wantCode: "InvalidParameter",
			build: func(*cmpStack) armcompute.Disk { return emptyDisk(40000) },
		},
		{
			name: "from image", wantGB: 30, wantState: "Unattached",
			build: func(*cmpStack) armcompute.Disk {
				return armcompute.Disk{Location: new("westus"), Properties: &armcompute.DiskProperties{
					CreationData: &armcompute.CreationData{
						CreateOption: new(armcompute.DiskCreateOptionFromImage),
						ImageReference: &armcompute.ImageDiskReference{ID: new(
							"/Subscriptions/" + aciSub + "/Providers/Microsoft.Compute/Locations/westus/Publishers/Canonical" +
								"/ArtifactTypes/VMImage/Offers/o/Skus/s/Versions/1.0.0",
						)},
					},
				}}
			},
		},
		{
			name: "from image without reference", wantCode: "InvalidParameter",
			build: func(*cmpStack) armcompute.Disk {
				return armcompute.Disk{Location: new("westus"), Properties: &armcompute.DiskProperties{
					CreationData: &armcompute.CreationData{CreateOption: new(armcompute.DiskCreateOptionFromImage)},
				}}
			},
		},
		{
			name: "upload", wantGB: 2, wantState: "ReadyToUpload",
			build: func(*cmpStack) armcompute.Disk {
				return armcompute.Disk{Location: new("westus"), Properties: &armcompute.DiskProperties{
					CreationData: &armcompute.CreationData{
						CreateOption: new(armcompute.DiskCreateOptionUpload), UploadSizeBytes: new(int64(1<<30 + 512)),
					},
				}}
			},
		},
		{
			name: "upload without size", wantCode: "InvalidParameter",
			build: func(*cmpStack) armcompute.Disk {
				return armcompute.Disk{Location: new("westus"), Properties: &armcompute.DiskProperties{
					CreationData: &armcompute.CreationData{CreateOption: new(armcompute.DiskCreateOptionUpload)},
				}}
			},
		},
		{
			name: "import unsupported", wantCode: "InvalidParameter",
			build: func(*cmpStack) armcompute.Disk {
				return armcompute.Disk{Location: new("westus"), Properties: &armcompute.DiskProperties{
					CreationData: &armcompute.CreationData{CreateOption: new(armcompute.DiskCreateOptionImport)},
				}}
			},
		},
		{
			name: "copy of missing source", wantCode: "ResourceNotFound",
			build: func(*cmpStack) armcompute.Disk {
				return armcompute.Disk{Location: new("westus"), Properties: &armcompute.DiskProperties{
					CreationData: &armcompute.CreationData{
						CreateOption: new(armcompute.DiskCreateOptionCopy),
						SourceResourceID: new("/subscriptions/" + aciSub + "/resourceGroups/" + netRG +
							"/providers/Microsoft.Compute/disks/ghost"),
					},
				}}
			},
		},
		{
			name: "bad sku", wantCode: "InvalidParameter",
			build: func(*cmpStack) armcompute.Disk {
				d := emptyDisk(8)
				d.SKU = &armcompute.DiskSKU{Name: new(armcompute.DiskStorageAccountTypes("Gold_LRS"))}

				return d
			},
		},
		{
			name: "premium sku", wantGB: 8, wantState: "Unattached",
			build: func(*cmpStack) armcompute.Disk {
				d := emptyDisk(8)
				d.SKU = &armcompute.DiskSKU{Name: new(armcompute.DiskStorageAccountTypesPremiumLRS)}

				return d
			},
		},
		{
			name: "bad zone", wantCode: "InvalidParameter",
			build: func(*cmpStack) armcompute.Disk {
				d := emptyDisk(8)
				d.Zones = []*string{new("9")}

				return d
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			got, err := s.putDisk(t, "disk1", tt.build(s))

			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantState, string(*got.Properties.DiskState))
			assert.Equal(t, tt.wantGB, *got.Properties.DiskSizeGB)
			assert.Equal(t, int64(tt.wantGB)<<30, *got.Properties.DiskSizeBytes)
			assert.NotEmpty(t, *got.Properties.UniqueID)
			assert.Nil(t, got.ManagedBy)
		})
	}
}

func TestComputeSDK_SnapshotsAndCopy(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	src, err := s.putDisk(t, "src", emptyDisk(32))
	require.NoError(t, err)

	snapPoller, err := s.cf.NewSnapshotsClient().BeginCreateOrUpdate(t.Context(), netRG, "snap1", armcompute.Snapshot{
		Location: new("westus"),
		Properties: &armcompute.SnapshotProperties{CreationData: &armcompute.CreationData{
			CreateOption: new(armcompute.DiskCreateOptionCopy), SourceResourceID: src.ID,
		}},
	}, nil)
	require.NoError(t, err)

	snap, err := snapPoller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.EqualValues(t, 32, *snap.Properties.DiskSizeGB)
	assert.Equal(t, "Standard_LRS", string(*snap.SKU.Name))
	assert.Equal(t, *src.Properties.UniqueID, *snap.Properties.CreationData.SourceUniqueID)

	_, err = s.cf.NewSnapshotsClient().BeginCreateOrUpdate(t.Context(), netRG, "snap-empty", armcompute.Snapshot{
		Location: new("westus"),
		Properties: &armcompute.SnapshotProperties{CreationData: &armcompute.CreationData{
			CreateOption: new(armcompute.DiskCreateOptionEmpty),
		}},
	}, nil)
	require.Error(t, err, "snapshots are copies")

	restored, err := s.putDisk(
		t,
		"restored",
		armcompute.Disk{Location: new("westus"), Properties: &armcompute.DiskProperties{
			CreationData: &armcompute.CreationData{
				CreateOption:     new(armcompute.DiskCreateOptionCopy),
				SourceResourceID: snap.ID,
			},
		}},
	)
	require.NoError(t, err)
	assert.EqualValues(t, 32, *restored.Properties.DiskSizeGB)

	smaller := armcompute.Disk{Location: new("westus"), Properties: &armcompute.DiskProperties{
		DiskSizeGB: new(int32(8)),
		CreationData: &armcompute.CreationData{
			CreateOption:     new(armcompute.DiskCreateOptionCopy),
			SourceResourceID: snap.ID,
		},
	}}
	_, err = s.putDisk(t, "smaller", smaller)
	require.Error(t, err)
	assert.Equal(t, "InvalidParameter", sdkErrCode(err))

	grown, err := s.putDisk(t, "src", emptyDisk(64))
	require.NoError(t, err)
	assert.EqualValues(t, 64, *grown.Properties.DiskSizeGB)

	_, err = s.putDisk(t, "src", emptyDisk(16))
	require.Error(t, err)
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err), "disks only grow")

	copyBody := emptyDisk(64)
	copyBody.Properties.CreationData.CreateOption = new(armcompute.DiskCreateOptionUpload)
	_, err = s.putDisk(t, "src", copyBody)
	require.Error(t, err)
	assert.Equal(t, "PropertyChangeNotAllowed", sdkErrCode(err), "creationData is immutable")

	pager := s.cf.NewSnapshotsClient().NewListByResourceGroupPager(netRG, nil)
	names := []string{}

	for pager.More() {
		page, perr := pager.NextPage(t.Context())
		require.NoError(t, perr)

		for _, e := range page.Value {
			names = append(names, *e.Name)
		}
	}

	assert.Equal(t, []string{"snap1"}, names)

	delPoller, err := s.cf.NewSnapshotsClient().BeginDelete(t.Context(), netRG, "snap1", nil)
	require.NoError(t, waitPoller(t, delPoller, err))
}

func TestComputeSDK_DataDiskAttachDetach(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	vm := linuxVM(s.newNIC(t, "nic1"))
	vm.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{
		{
			Lun:          new(int32(0)),
			Name:         new("data0"),
			CreateOption: new(armcompute.DiskCreateOptionTypesEmpty),
			DiskSizeGB:   new(int32(16)),
		},
	}
	created := s.mustVM(t, "vm1", vm)

	dd := created.Properties.StorageProfile.DataDisks
	require.Len(t, dd, 1)
	assert.Equal(t, "data0", *dd[0].Name)
	assert.EqualValues(t, 16, *dd[0].DiskSizeGB)
	assert.NotEmpty(t, *dd[0].ManagedDisk.ID)

	state, managedBy := s.diskState(t, "data0")
	assert.Equal(t, "Attached", state)
	assert.Equal(t, *created.ID, managedBy)

	standalone, err := s.putDisk(t, "spare", emptyDisk(8))
	require.NoError(t, err)

	delPoller, err := s.cf.NewDisksClient().BeginDelete(t.Context(), netRG, "data0", nil)
	err = waitPoller(t, delPoller, err)
	require.Error(t, err)
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err), "an attached disk cannot be deleted")

	update := created
	update.Properties.StorageProfile.DataDisks = append(
		update.Properties.StorageProfile.DataDisks,
		&armcompute.DataDisk{
			Lun: new(int32(1)), CreateOption: new(armcompute.DiskCreateOptionTypesAttach),
			ManagedDisk: &armcompute.ManagedDiskParameters{ID: standalone.ID},
		},
	)
	updated, err := s.putVM(t, netRG, "vm1", update)
	require.NoError(t, err)
	require.Len(t, updated.Properties.StorageProfile.DataDisks, 2)
	assert.Equal(t, "Attach", string(*updated.Properties.StorageProfile.DataDisks[1].CreateOption))

	state, managedBy = s.diskState(t, "spare")
	assert.Equal(t, "Attached", state)
	assert.Equal(t, *created.ID, managedBy)

	other := linuxVM(s.newNIC(t, "nic2"))
	other.Properties.OSProfile.ComputerName = new("other")
	other.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{
		{
			Lun:          new(int32(0)),
			CreateOption: new(armcompute.DiskCreateOptionTypesAttach),
			ManagedDisk:  &armcompute.ManagedDiskParameters{ID: standalone.ID},
		},
	}
	_, err = s.putVM(t, netRG, "vm2", other)
	require.Error(t, err)
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err), "a disk attaches to one VM")

	cp := updated
	cp.Properties.StorageProfile.DataDisks[0].DiskSizeGB = new(int32(32))
	_, err = s.putVM(t, netRG, "vm1", cp)
	require.Error(t, err)
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err), "growing an attached disk needs a deallocated VM")

	dpl, derr := s.cf.NewVirtualMachinesClient().BeginDeallocate(t.Context(), netRG, "vm1", nil)
	require.NoError(t, waitPoller(t, dpl, derr))

	state, _ = s.diskState(t, "spare")
	assert.Equal(t, "Reserved", state, "disks of a deallocated VM are reserved")

	grown, err := s.putVM(t, netRG, "vm1", cp)
	require.NoError(t, err)
	assert.EqualValues(t, 32, *grown.Properties.StorageProfile.DataDisks[0].DiskSizeGB)

	detach := grown
	detach.Properties.StorageProfile.DataDisks = detach.Properties.StorageProfile.DataDisks[:1]
	_, err = s.putVM(t, netRG, "vm1", detach)
	require.NoError(t, err)

	state, managedBy = s.diskState(t, "spare")
	assert.Equal(t, "Unattached", state)
	assert.Empty(t, managedBy)

	delPoller, err = s.cf.NewDisksClient().BeginDelete(t.Context(), netRG, "spare", nil)
	require.NoError(t, waitPoller(t, delPoller, err))
}

func TestComputeSDK_VMDeleteOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		option    armcompute.DeleteOptions
		wantExist bool
	}{
		{name: "detach keeps disks", option: armcompute.DeleteOptionsDetach, wantExist: true},
		{name: "delete removes disks", option: armcompute.DeleteOptionsDelete, wantExist: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			vm := linuxVM(s.newNIC(t, "nic1"))
			vm.Properties.StorageProfile.OSDisk.DeleteOption = new(armcompute.DiskDeleteOptionTypes(tt.option))
			vm.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{{
				Lun: new(int32(0)), Name: new("data0"), CreateOption: new(armcompute.DiskCreateOptionTypesEmpty),
				DiskSizeGB: new(int32(8)), DeleteOption: new(armcompute.DiskDeleteOptionTypes(tt.option)),
			}}
			created := s.mustVM(t, "vm1", vm)
			osName := *created.Properties.StorageProfile.OSDisk.Name

			pl, err := s.cf.NewVirtualMachinesClient().BeginDelete(t.Context(), netRG, "vm1", nil)
			require.NoError(t, waitPoller(t, pl, err))

			for _, name := range []string{osName, "data0"} {
				d, gerr := s.cf.NewDisksClient().Get(t.Context(), netRG, name, nil)
				if !tt.wantExist {
					require.Error(t, gerr, name)
					assert.Equal(t, "ResourceNotFound", sdkErrCode(gerr))

					continue
				}

				require.NoError(t, gerr, name)
				assert.Equal(t, "Unattached", string(*d.Properties.DiskState))
				assert.Nil(t, d.ManagedBy)
			}
		})
	}
}

func TestComputeSDK_AttachOSDisk(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	first := s.mustVM(t, "vm1", linuxVM(s.newNIC(t, "nic1")))
	osName := *first.Properties.StorageProfile.OSDisk.Name

	pl, err := s.cf.NewVirtualMachinesClient().BeginDelete(t.Context(), netRG, "vm1", nil)
	require.NoError(t, waitPoller(t, pl, err))

	disk, err := s.cf.NewDisksClient().Get(t.Context(), netRG, osName, nil)
	require.NoError(t, err)

	spec := linuxVM(s.newNIC(t, "nic2"))
	spec.Properties.OSProfile = &armcompute.OSProfile{ComputerName: new("x"), AdminUsername: new("azureuser")}
	spec.Properties.StorageProfile = &armcompute.StorageProfile{OSDisk: &armcompute.OSDisk{
		CreateOption: new(armcompute.DiskCreateOptionTypesAttach), OSType: new(armcompute.OperatingSystemTypesLinux),
		ManagedDisk: &armcompute.ManagedDiskParameters{ID: disk.ID},
	}}
	_, err = s.putVM(t, netRG, "vm2", spec)
	require.Error(t, err, "an attached OS disk takes no osProfile")
	assert.Equal(t, "InvalidParameter", sdkErrCode(err))

	spec.Properties.OSProfile = nil
	got := s.mustVM(t, "vm2", spec)
	assert.Equal(t, osName, *got.Properties.StorageProfile.OSDisk.Name)
	assert.Nil(t, got.Properties.OSProfile)

	state, managedBy := s.diskState(t, osName)
	assert.Equal(t, "Attached", state)
	assert.Equal(t, *got.ID, managedBy)
}
