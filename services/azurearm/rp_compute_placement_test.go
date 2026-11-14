package azurearm_test

import (
	"crypto/rand"
	"crypto/rsa"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

func rsaPublicKey(t *testing.T, bits int) string {
	t.Helper()

	k, err := rsa.GenerateKey(rand.Reader, bits)
	require.NoError(t, err)

	pub, err := ssh.NewPublicKey(&k.PublicKey)
	require.NoError(t, err)

	return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pub)))
}

func TestComputeSDK_ProximityPlacementGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		typ      string
		wantCode string
	}{
		{name: "default"},
		{name: "ultra", typ: "Ultra"},
		{name: "bad type", typ: "Fast", wantCode: "InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			ppgs := s.cf.NewProximityPlacementGroupsClient()
			in := armcompute.ProximityPlacementGroup{
				Location:   new("westus"),
				Properties: &armcompute.ProximityPlacementGroupProperties{},
			}

			if tt.typ != "" {
				in.Properties.ProximityPlacementGroupType = new(armcompute.ProximityPlacementGroupType(tt.typ))
			}

			got, err := ppgs.CreateOrUpdate(t.Context(), netRG, "ppg1", in, nil)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			want := "Standard"
			if tt.typ != "" {
				want = tt.typ
			}

			assert.Equal(t, want, string(*got.Properties.ProximityPlacementGroupType))
		})
	}
}

func TestComputeSDK_ProximityPlacementGroupMembership(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	ppgs := s.cf.NewProximityPlacementGroupsClient()

	ppg, err := ppgs.CreateOrUpdate(
		t.Context(),
		netRG,
		"ppg1",
		armcompute.ProximityPlacementGroup{Location: new("westus")},
		nil,
	)
	require.NoError(t, err)

	vm := linuxVM(s.newNIC(t, "nic1"))
	vm.Properties.ProximityPlacementGroup = &armcompute.SubResource{ID: new(*ppg.ID + "-missing")}
	_, err = s.putVM(t, netRG, "vm1", vm)
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	vm.Properties.ProximityPlacementGroup = &armcompute.SubResource{ID: ppg.ID}
	s.mustVM(t, "vm1", vm)

	got, err := ppgs.Get(t.Context(), netRG, "ppg1", nil)
	require.NoError(t, err)
	require.Len(t, got.Properties.VirtualMachines, 1)
	assert.Equal(t, "ColocationStatus/Aligned", *got.Properties.ColocationStatus.Code)

	_, err = ppgs.Delete(t.Context(), netRG, "ppg1", nil)
	require.Error(t, err)
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err))
}

func TestComputeSDK_SSHPublicKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		key      string
		wantCode string
	}{
		{name: "rsa 2048", key: rsaPublicKey(t, 2048)},
		{name: "rsa 1024", key: rsaPublicKey(t, 1024), wantCode: "InvalidParameter"},
		{name: "garbage", key: "not-a-key", wantCode: "InvalidParameter"},
		{name: "no key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			keys := s.cf.NewSSHPublicKeysClient()
			in := armcompute.SSHPublicKeyResource{Location: new("westus")}

			if tt.key != "" {
				in.Properties = &armcompute.SSHPublicKeyResourceProperties{PublicKey: new(tt.key)}
			}

			got, err := keys.Create(t.Context(), netRG, "key1", in, nil)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			if tt.key != "" {
				assert.Equal(t, tt.key, *got.Properties.PublicKey)

				_, err = keys.GenerateKeyPair(t.Context(), netRG, "key1", nil)
				require.Error(t, err, "a key that already has a public key cannot be generated")

				return
			}

			pair, err := keys.GenerateKeyPair(t.Context(), netRG, "key1", nil)
			require.NoError(t, err)
			assert.Contains(t, *pair.PrivateKey, "RSA PRIVATE KEY")
			assert.Equal(t, *got.ID, *pair.ID)

			read, err := keys.Get(t.Context(), netRG, "key1", nil)
			require.NoError(t, err)
			assert.Equal(t, *pair.PublicKey, *read.Properties.PublicKey)
		})
	}
}

func TestComputeSDK_SSHPublicKeyEd25519(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	keys := s.cf.NewSSHPublicKeysClient()

	_, err := keys.Create(t.Context(), netRG, "key1", armcompute.SSHPublicKeyResource{Location: new("westus")}, nil)
	require.NoError(t, err)

	pair, err := keys.GenerateKeyPair(t.Context(), netRG, "key1", &armcompute.SSHPublicKeysClientGenerateKeyPairOptions{
		Parameters: &armcompute.SSHGenerateKeyPairInputParameters{
			EncryptionType: new(armcompute.SSHEncryptionTypesEd25519),
		},
	})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(*pair.PublicKey, "ssh-ed25519 "))
	assert.Contains(t, *pair.PrivateKey, "OPENSSH PRIVATE KEY")
}

func TestComputeSDK_CapacityReservations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sku      string
		wantCode string
		zones    []string
		capacity int64
	}{
		{name: "ok", sku: "Standard_D2s_v3", capacity: 2},
		{name: "zone in group", sku: "Standard_D2s_v3", capacity: 1, zones: []string{"1"}},
		{
			name:     "zone outside group",
			sku:      "Standard_D2s_v3",
			capacity: 1,
			zones:    []string{"3"},
			wantCode: "InvalidParameter",
		},
		{name: "no sku", capacity: 1, wantCode: "InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			groups := s.cf.NewCapacityReservationGroupsClient()

			grp, err := groups.CreateOrUpdate(t.Context(), netRG, "grp1", armcompute.CapacityReservationGroup{
				Location: new("westus"), Zones: []*string{new("1"), new("2")},
			}, nil)
			require.NoError(t, err)

			in := armcompute.CapacityReservation{
				Location: new("westus"), SKU: &armcompute.SKU{Capacity: new(tt.capacity)},
			}
			if tt.sku != "" {
				in.SKU.Name = new(tt.sku)
			}

			for _, z := range tt.zones {
				in.Zones = append(in.Zones, new(z))
			}

			poller, err := s.cf.NewCapacityReservationsClient().
				BeginCreateOrUpdate(t.Context(), netRG, "grp1", "res1", in, nil)
			if err == nil {
				_, err = poller.PollUntilDone(t.Context(), pollFast())
			}

			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			got, err := groups.Get(t.Context(), netRG, "grp1", &armcompute.CapacityReservationGroupsClientGetOptions{})
			require.NoError(t, err)
			require.Len(t, got.Properties.CapacityReservations, 1)
			assert.Equal(t, *grp.ID+"/capacityReservations/res1", *got.Properties.CapacityReservations[0].ID)

			err = deleteCapGroup(t, groups, "grp1")
			require.Error(t, err)
			assert.Equal(t, "OperationNotAllowed", sdkErrCode(err), "group with reservations cannot be deleted")

			dp, err := s.cf.NewCapacityReservationsClient().BeginDelete(t.Context(), netRG, "grp1", "res1", nil)
			require.NoError(t, err)
			_, err = dp.PollUntilDone(t.Context(), pollFast())
			require.NoError(t, err)

			require.NoError(t, deleteCapGroup(t, groups, "grp1"))
		})
	}
}

func TestComputeSDK_DedicatedHosts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		license  string
		wantCode string
		fd       int32
	}{
		{name: "ok", fd: 1},
		{name: "license", fd: 0, license: "Windows_Server_Hybrid"},
		{name: "fault domain out of range", fd: 2, wantCode: "InvalidParameter"},
		{name: "bad license", license: "Free", wantCode: "InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			groups := s.cf.NewDedicatedHostGroupsClient()

			grp, err := groups.CreateOrUpdate(t.Context(), netRG, "hg1", armcompute.DedicatedHostGroup{
				Location:   new("westus"),
				Properties: &armcompute.DedicatedHostGroupProperties{PlatformFaultDomainCount: new(int32(2))},
			}, nil)
			require.NoError(t, err)
			assert.False(t, *grp.Properties.SupportAutomaticPlacement)

			host := armcompute.DedicatedHost{
				Location:   new("westus"),
				SKU:        &armcompute.SKU{Name: new("DSv3-Type1")},
				Properties: &armcompute.DedicatedHostProperties{PlatformFaultDomain: new(tt.fd)},
			}
			if tt.license != "" {
				host.Properties.LicenseType = new(armcompute.DedicatedHostLicenseTypes(tt.license))
			}

			hosts := s.cf.NewDedicatedHostsClient()
			poller, err := hosts.BeginCreateOrUpdate(t.Context(), netRG, "hg1", "h1", host, nil)
			if err == nil {
				_, err = poller.PollUntilDone(t.Context(), pollFast())
			}

			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			got, err := hosts.Get(t.Context(), netRG, "hg1", "h1", &armcompute.DedicatedHostsClientGetOptions{
				Expand: new(armcompute.InstanceViewTypesInstanceView),
			})
			require.NoError(t, err)
			assert.True(t, *got.Properties.AutoReplaceOnFailure)
			assert.NotEmpty(t, *got.Properties.HostID)
			assert.NotNil(t, got.Properties.InstanceView)

			if tt.license != "" {
				assert.Equal(t, tt.license, string(*got.Properties.LicenseType))
			}

			_, err = groups.Delete(t.Context(), netRG, "hg1", nil)
			require.Error(t, err)
			assert.Equal(t, "CannotDeleteResource", sdkErrCode(err))

			dp, err := hosts.BeginDelete(t.Context(), netRG, "hg1", "h1", nil)
			require.NoError(t, err)
			_, err = dp.PollUntilDone(t.Context(), pollFast())
			require.NoError(t, err)
		})
	}
}

func TestComputeSDK_DedicatedHostGroupImmutable(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	groups := s.cf.NewDedicatedHostGroupsClient()
	in := armcompute.DedicatedHostGroup{
		Location:   new("westus"),
		Properties: &armcompute.DedicatedHostGroupProperties{PlatformFaultDomainCount: new(int32(1))},
	}

	_, err := groups.CreateOrUpdate(t.Context(), netRG, "hg1", in, nil)
	require.NoError(t, err)

	in.Properties.PlatformFaultDomainCount = new(int32(2))
	_, err = groups.CreateOrUpdate(t.Context(), netRG, "hg1", in, nil)
	require.Error(t, err)
	assert.Equal(t, "PropertyChangeNotAllowed", sdkErrCode(err))

	in.Properties.PlatformFaultDomainCount = nil
	_, err = groups.CreateOrUpdate(t.Context(), netRG, "hg2", in, nil)
	require.Error(t, err)
	assert.Equal(t, "InvalidParameter", sdkErrCode(err))

	in.Properties = &armcompute.DedicatedHostGroupProperties{
		PlatformFaultDomainCount: new(int32(1)),
		AdditionalCapabilities: &armcompute.DedicatedHostGroupPropertiesAdditionalCapabilities{
			UltraSSDEnabled: new(true),
		},
	}
	_, err = groups.CreateOrUpdate(t.Context(), netRG, "hg3", in, nil)
	require.Error(t, err, "ultra SSD needs a zonal group")
}

func TestComputeSDK_DiskEncryptionSetsAndAccesses(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	sets := s.cf.NewDiskEncryptionSetsClient()
	accesses := s.cf.NewDiskAccessesClient()

	bad := armcompute.DiskEncryptionSet{
		Location: new("westus"),
		Properties: &armcompute.EncryptionSetProperties{
			ActiveKey: &armcompute.KeyForDiskEncryptionSet{KeyURL: new("ftp://vault/keys/k/1")},
		},
	}
	poller, err := sets.BeginCreateOrUpdate(t.Context(), netRG, "des1", bad, nil)
	if err == nil {
		_, err = poller.PollUntilDone(t.Context(), pollFast())
	}

	require.Error(t, err)
	assert.Equal(t, "InvalidParameter", sdkErrCode(err))

	bad.Properties.ActiveKey.KeyURL = new("https://vault.vault.azure.net/keys/k/0123456789abcdef")
	bad.Identity = &armcompute.EncryptionSetIdentity{Type: new(armcompute.DiskEncryptionSetIdentityTypeSystemAssigned)}
	poller, err = sets.BeginCreateOrUpdate(t.Context(), netRG, "des1", bad, nil)
	require.NoError(t, err)
	des, err := poller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Equal(t, "EncryptionAtRestWithCustomerKey", string(*des.Properties.EncryptionType))
	assert.NotNil(t, des.Properties.LastKeyRotationTimestamp)
	assert.NotEmpty(t, *des.Identity.PrincipalID)

	ap, err := accesses.BeginCreateOrUpdate(
		t.Context(),
		netRG,
		"da1",
		armcompute.DiskAccess{Location: new("westus")},
		nil,
	)
	require.NoError(t, err)
	da, err := ap.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Equal(t, "Succeeded", *da.Properties.ProvisioningState)

	disks := s.cf.NewDisksClient()
	dp, err := disks.BeginCreateOrUpdate(t.Context(), netRG, "d1", armcompute.Disk{
		Location: new("westus"), SKU: &armcompute.DiskSKU{Name: new(armcompute.DiskStorageAccountTypesStandardLRS)},
		Properties: &armcompute.DiskProperties{
			CreationData: &armcompute.CreationData{CreateOption: new(armcompute.DiskCreateOptionEmpty)},
			DiskSizeGB:   new(int32(4)),
			Encryption:   &armcompute.Encryption{DiskEncryptionSetID: new(*des.ID + "-missing")},
		},
	}, nil)
	if err == nil {
		_, err = dp.PollUntilDone(t.Context(), pollFast())
	}

	require.Error(t, err)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	dp, err = disks.BeginCreateOrUpdate(t.Context(), netRG, "d1", armcompute.Disk{
		Location: new("westus"), SKU: &armcompute.DiskSKU{Name: new(armcompute.DiskStorageAccountTypesStandardLRS)},
		Properties: &armcompute.DiskProperties{
			CreationData: &armcompute.CreationData{CreateOption: new(armcompute.DiskCreateOptionEmpty)},
			DiskSizeGB:   new(int32(4)),
			Encryption: &armcompute.Encryption{
				DiskEncryptionSetID: des.ID,
				Type:                new(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey),
			},
			NetworkAccessPolicy: new(armcompute.NetworkAccessPolicyAllowPrivate),
			DiskAccessID:        da.ID,
		},
	}, nil)
	require.NoError(t, err)
	_, err = dp.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	dd, err := sets.BeginDelete(t.Context(), netRG, "des1", nil)
	if err == nil {
		_, err = dd.PollUntilDone(t.Context(), pollFast())
	}

	require.Error(t, err)
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err))

	da2, err := accesses.BeginDelete(t.Context(), netRG, "da1", nil)
	if err == nil {
		_, err = da2.PollUntilDone(t.Context(), pollFast())
	}

	require.Error(t, err)
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err))
}

func deleteCapGroup(t *testing.T, c *armcompute.CapacityReservationGroupsClient, name string) error {
	t.Helper()

	p, err := c.BeginDelete(t.Context(), netRG, name, nil)
	if err != nil {
		return err
	}

	_, err = p.PollUntilDone(t.Context(), pollFast())

	return err
}

func TestComputePlacementSurvivesRestore(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)

	_, err := s.cf.NewSSHPublicKeysClient().Create(t.Context(), netRG, "key1",
		armcompute.SSHPublicKeyResource{Location: new("westus")}, nil)
	require.NoError(t, err)

	pair, err := s.cf.NewSSHPublicKeysClient().GenerateKeyPair(t.Context(), netRG, "key1", nil)
	require.NoError(t, err)

	_, err = s.cf.NewDedicatedHostGroupsClient().
		CreateOrUpdate(t.Context(), netRG, "hg1", armcompute.DedicatedHostGroup{
			Location:   new("westus"),
			Properties: &armcompute.DedicatedHostGroupProperties{PlatformFaultDomainCount: new(int32(1))},
		}, nil)
	require.NoError(t, err)

	other := newCmpStack(t)
	require.NoError(t, other.handler.Restore(t.Context(), s.handler.Snapshot(t.Context())))

	got, err := other.cf.NewSSHPublicKeysClient().Get(t.Context(), netRG, "key1", nil)
	require.NoError(t, err)
	assert.Equal(t, *pair.PublicKey, *got.Properties.PublicKey)

	_, err = other.cf.NewDedicatedHostGroupsClient().Get(t.Context(), netRG, "hg1", nil)
	require.NoError(t, err)
}
