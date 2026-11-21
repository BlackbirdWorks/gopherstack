package azurearm_test

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const (
	importAcct = "vhdacct"
	importHost = "https://" + importAcct + ".blob.core.windows.net/vhds/"
	importAcID = "/subscriptions/" + aciSub + "/resourceGroups/" + netRG +
		"/providers/Microsoft.Storage/storageAccounts/" + importAcct
	gib = int64(1) << 30
)

type fakeDiskBlobs struct {
	blobs map[string]azurearm.BlobProbe
	mu    sync.Mutex
}

func (f *fakeDiskBlobs) ProbeBlob(container, blob string) (azurearm.BlobProbe, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if p, ok := f.blobs[container+"/"+blob]; ok {
		return p, nil
	}

	return azurearm.BlobProbe{}, azurearm.ErrDiskBlobNotFound
}

func vhdFooter(virtual int64, diskType uint32) []byte {
	f := make([]byte, 512)
	copy(f, "conectix")
	binary.BigEndian.PutUint64(f[48:], uint64(virtual))
	binary.BigEndian.PutUint32(f[60:], diskType)

	var sum uint32

	for i, b := range f {
		if i < 64 || i >= 68 {
			sum += uint32(b)
		}
	}

	binary.BigEndian.PutUint32(f[64:], ^sum)

	return f
}

func fixedVHD(virtual int64) azurearm.BlobProbe {
	return azurearm.BlobProbe{PageBlob: true, Size: virtual + 512, Footer: vhdFooter(virtual, 2)}
}

func importDisk(opt armcompute.DiskCreateOption, mut func(*armcompute.CreationData)) armcompute.Disk {
	cd := &armcompute.CreationData{
		CreateOption: &opt, SourceURI: new(importHost + "os.vhd"), StorageAccountID: new(importAcID),
	}
	if mut != nil {
		mut(cd)
	}

	return armcompute.Disk{Location: new("westus"), Properties: &armcompute.DiskProperties{CreationData: cd}}
}

func TestComputeSDK_DiskImport(t *testing.T) {
	t.Parallel()

	badSum := fixedVHD(gib)
	badSum.Footer[64] ^= 0xff

	blobs := map[string]azurearm.BlobProbe{
		"vhds/os.vhd":    fixedVHD(2*gib + 512),
		"vhds/small.vhd": fixedVHD(gib),
		"vhds/block.vhd": {PageBlob: false, Size: gib + 512},
		"vhds/dyn.vhd":   {PageBlob: true, Size: gib + 512, Footer: vhdFooter(gib, 3)},
		"vhds/sum.vhd":   badSum,
		"vhds/odd.vhd":   {PageBlob: true, Size: gib + 700, Footer: vhdFooter(gib, 2)},
		"vhds/huge.vhd":  fixedVHD(40 * 1024 * gib),
		"vhds/guest.vmgs": {
			PageBlob: false, Size: 1024,
		},
	}

	tests := []struct {
		build    func() armcompute.Disk
		name     string
		wantCode string
		wantGB   int32
	}{
		{name: "fixed vhd rounds up", wantGB: 3, build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImport, nil)
		}},
		{name: "without storage account id", wantGB: 1, build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImport, func(c *armcompute.CreationData) {
				c.StorageAccountID = nil
				c.SourceURI = new(importHost + "small.vhd")
			})
		}},
		{name: "path style uri", wantGB: 1, build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImport, func(c *armcompute.CreationData) {
				c.SourceURI = new("http://127.0.0.1:10000/" + importAcct + "/vhds/small.vhd")
			})
		}},
		{name: "larger requested size", wantGB: 10, build: func() armcompute.Disk {
			d := importDisk(armcompute.DiskCreateOptionImport, nil)
			d.Properties.DiskSizeGB = new(int32(10))

			return d
		}},
		{name: "smaller requested size", wantCode: "InvalidParameter", build: func() armcompute.Disk {
			d := importDisk(armcompute.DiskCreateOptionImport, nil)
			d.Properties.DiskSizeGB = new(int32(2))

			return d
		}},
		{name: "missing blob", wantCode: "InvalidParameter", build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImport, func(c *armcompute.CreationData) {
				c.SourceURI = new(importHost + "ghost.vhd")
			})
		}},
		{name: "block blob", wantCode: "InvalidParameter", build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImport, func(c *armcompute.CreationData) {
				c.SourceURI = new(importHost + "block.vhd")
			})
		}},
		{name: "dynamic vhd", wantCode: "InvalidParameter", build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImport, func(c *armcompute.CreationData) {
				c.SourceURI = new(importHost + "dyn.vhd")
			})
		}},
		{name: "bad checksum", wantCode: "InvalidParameter", build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImport, func(c *armcompute.CreationData) {
				c.SourceURI = new(importHost + "sum.vhd")
			})
		}},
		{name: "unaligned size", wantCode: "InvalidParameter", build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImport, func(c *armcompute.CreationData) {
				c.SourceURI = new(importHost + "odd.vhd")
			})
		}},
		{name: "over max size", wantCode: "InvalidParameter", build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImport, func(c *armcompute.CreationData) {
				c.SourceURI = new(importHost + "huge.vhd")
			})
		}},
		{name: "no source uri", wantCode: "InvalidParameter", build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImport, func(c *armcompute.CreationData) {
				c.SourceURI = nil
			})
		}},
		{name: "uri without blob", wantCode: "InvalidParameter", build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImport, func(c *armcompute.CreationData) {
				c.SourceURI = new("https://" + importAcct + ".blob.core.windows.net/vhds")
			})
		}},
		{name: "account mismatch", wantCode: "InvalidParameter", build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImport, func(c *armcompute.CreationData) {
				c.SourceURI = new("https://other.blob.core.windows.net/vhds/os.vhd")
			})
		}},
		{name: "bad storage account id", wantCode: "InvalidParameter", build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImport, func(c *armcompute.CreationData) {
				c.StorageAccountID = new("/subscriptions/x/resourceGroups/y/providers/Microsoft.Compute/disks/z")
			})
		}},
		{name: "secure", wantGB: 3, build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImportSecure, func(c *armcompute.CreationData) {
				c.SecurityDataURI = new(importHost + "guest.vmgs")
				c.SecurityMetadataURI = new(importHost + "guest.vmgs")
			})
		}},
		{name: "secure without data uri", wantCode: "InvalidParameter", build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImportSecure, nil)
		}},
		{name: "secure missing data blob", wantCode: "InvalidParameter", build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImportSecure, func(c *armcompute.CreationData) {
				c.SecurityDataURI = new(importHost + "nope.vmgs")
			})
		}},
		{name: "secure without storage account id", wantCode: "InvalidParameter", build: func() armcompute.Disk {
			return importDisk(armcompute.DiskCreateOptionImportSecure, func(c *armcompute.CreationData) {
				c.StorageAccountID = nil
				c.SecurityDataURI = new(importHost + "guest.vmgs")
			})
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			s.handler.Registry.SetDiskBlobs(&fakeDiskBlobs{blobs: blobs})

			got, err := s.putDisk(t, "imp", tt.build())
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantGB, *got.Properties.DiskSizeGB)
			assert.Equal(t, "Unattached", string(*got.Properties.DiskState))
			assert.NotEmpty(t, *got.Properties.CreationData.SourceURI)
		})
	}
}

func TestComputeSDK_DiskImportUnwired(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	_, err := s.putDisk(t, "imp", importDisk(armcompute.DiskCreateOptionImport, nil))
	require.Error(t, err)
	assert.Equal(t, "InvalidParameter", sdkErrCode(err))
}

func TestComputeSDK_SnapshotImport(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	s.handler.Registry.SetDiskBlobs(&fakeDiskBlobs{blobs: map[string]azurearm.BlobProbe{"vhds/os.vhd": fixedVHD(gib)}})

	pl, err := s.cf.NewSnapshotsClient().BeginCreateOrUpdate(t.Context(), netRG, "snap", armcompute.Snapshot{
		Location: new("westus"),
		Properties: &armcompute.SnapshotProperties{CreationData: importDisk(
			armcompute.DiskCreateOptionImport, nil).Properties.CreationData},
	}, nil)
	require.NoError(t, err)

	got, err := pl.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Equal(t, int32(1), *got.Properties.DiskSizeGB)
	assert.Equal(t, importHost+"os.vhd", *got.Properties.CreationData.SourceURI)
}

func TestComputeSDK_DiskImportReplayKeepsDisk(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	blobs := &fakeDiskBlobs{blobs: map[string]azurearm.BlobProbe{"vhds/os.vhd": fixedVHD(gib)}}
	s.handler.Registry.SetDiskBlobs(blobs)

	_, err := s.putDisk(t, "imp", importDisk(armcompute.DiskCreateOptionImport, nil))
	require.NoError(t, err)

	blobs.mu.Lock()
	clear(blobs.blobs)
	blobs.mu.Unlock()

	got, err := s.putDisk(t, "imp", importDisk(armcompute.DiskCreateOptionImport, nil))
	require.NoError(t, err)
	assert.Equal(t, int32(1), *got.Properties.DiskSizeGB)
}
