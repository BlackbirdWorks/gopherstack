package azurearm_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeSDK_AvailabilitySets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sku      string
		wantCode string
		fd       int32
		ud       int32
	}{
		{name: "defaults", sku: "Aligned"},
		{name: "classic", fd: 2, ud: 10, sku: "Classic"},
		{name: "fault domains too many", fd: 5, ud: 5, sku: "Aligned", wantCode: "InvalidParameter"},
		{name: "update domains too many", fd: 3, ud: 25, sku: "Aligned", wantCode: "InvalidParameter"},
		{name: "bad sku", sku: "Premium", wantCode: "InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			set := armcompute.AvailabilitySet{
				Location: new("westus"), SKU: &armcompute.SKU{Name: new(tt.sku)},
				Properties: &armcompute.AvailabilitySetProperties{},
			}

			if tt.fd != 0 {
				set.Properties.PlatformFaultDomainCount = new(tt.fd)
				set.Properties.PlatformUpdateDomainCount = new(tt.ud)
			}

			got, err := s.cf.NewAvailabilitySetsClient().CreateOrUpdate(t.Context(), netRG, "as1", set, nil)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.sku, *got.SKU.Name)

			wantFD, wantUD := int32(3), int32(5)
			if tt.fd != 0 {
				wantFD, wantUD = tt.fd, tt.ud
			}

			assert.Equal(t, wantFD, *got.Properties.PlatformFaultDomainCount)
			assert.Equal(t, wantUD, *got.Properties.PlatformUpdateDomainCount)
			assert.Empty(t, got.Properties.VirtualMachines)
		})
	}
}

func TestComputeSDK_AvailabilitySetMembership(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	sets := s.cf.NewAvailabilitySetsClient()

	set, err := sets.CreateOrUpdate(t.Context(), netRG, "as1", armcompute.AvailabilitySet{
		Location: new("westus"), SKU: &armcompute.SKU{Name: new("Aligned")},
		Properties: &armcompute.AvailabilitySetProperties{PlatformFaultDomainCount: new(int32(2))},
	}, nil)
	require.NoError(t, err)

	vm := linuxVM(s.newNIC(t, "nic1"))
	vm.Properties.AvailabilitySet = &armcompute.SubResource{ID: set.ID}
	first := s.mustVM(t, "vm1", vm)
	assert.Equal(t, *set.ID, *first.Properties.AvailabilitySet.ID)

	second := linuxVM(s.newNIC(t, "nic2"))
	second.Properties.OSProfile.ComputerName = new("two")
	second.Properties.AvailabilitySet = &armcompute.SubResource{ID: set.ID}
	s.mustVM(t, "vm2", second)

	got, err := sets.Get(t.Context(), netRG, "as1", nil)
	require.NoError(t, err)
	require.Len(t, got.Properties.VirtualMachines, 2)

	fds := make([]int32, 0, 2)

	for _, name := range []string{"vm1", "vm2"} {
		iv, ierr := s.cf.NewVirtualMachinesClient().InstanceView(t.Context(), netRG, name, nil)
		require.NoError(t, ierr)
		fds = append(fds, *iv.PlatformFaultDomain)
	}

	assert.Equal(t, []int32{0, 1}, fds, "machines spread across fault domains")

	_, err = sets.Delete(t.Context(), netRG, "as1", nil)
	require.Error(t, err)
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err), "a set with machines cannot be deleted")

	other, err := sets.CreateOrUpdate(t.Context(), netRG, "as2", armcompute.AvailabilitySet{
		Location: new("westus"), SKU: &armcompute.SKU{Name: new("Aligned")},
	}, nil)
	require.NoError(t, err)

	moved := first
	moved.Properties.AvailabilitySet = &armcompute.SubResource{ID: other.ID}
	_, err = s.putVM(t, netRG, "vm1", moved)
	require.Error(t, err)
	assert.Equal(t, "PropertyChangeNotAllowed", sdkErrCode(err), "a machine cannot leave its availability set")

	for _, name := range []string{"vm1", "vm2"} {
		pl, perr := s.cf.NewVirtualMachinesClient().BeginDelete(t.Context(), netRG, name, nil)
		require.NoError(t, waitPoller(t, pl, perr))
	}

	_, err = sets.Delete(t.Context(), netRG, "as1", nil)
	require.NoError(t, err)

	zonal := linuxVM(s.newNIC(t, "nic3"))
	zonal.Zones = []*string{new("1")}
	zonal.Properties.AvailabilitySet = &armcompute.SubResource{ID: set.ID}
	_, err = s.putVM(t, netRG, "vm3", zonal)
	require.Error(t, err)
}

func (s *cmpStack) scaleSet(t *testing.T, capacity int64) armcompute.VirtualMachineScaleSet {
	t.Helper()

	return armcompute.VirtualMachineScaleSet{
		Location: new("westus"),
		SKU:      &armcompute.SKU{Name: new("Standard_B2s"), Capacity: new(capacity)},
		Properties: &armcompute.VirtualMachineScaleSetProperties{
			UpgradePolicy: &armcompute.UpgradePolicy{Mode: new(armcompute.UpgradeModeManual)},
			VirtualMachineProfile: &armcompute.VirtualMachineScaleSetVMProfile{
				OSProfile: &armcompute.VirtualMachineScaleSetOSProfile{
					ComputerNamePrefix: new("web"), AdminUsername: new("azureuser"), AdminPassword: new(cmpTestPass),
				},
				StorageProfile: &armcompute.VirtualMachineScaleSetStorageProfile{
					ImageReference: &armcompute.ImageReference{
						Publisher: new(
							"Canonical",
						), Offer: new("ubuntu-24_04-lts"), SKU: new("server"), Version: new("latest"),
					},
				},
				NetworkProfile: &armcompute.VirtualMachineScaleSetNetworkProfile{
					NetworkInterfaceConfigurations: []*armcompute.VirtualMachineScaleSetNetworkConfiguration{{
						Name: new("nic"),
						Properties: &armcompute.VirtualMachineScaleSetNetworkConfigurationProperties{
							Primary: new(true),
							IPConfigurations: []*armcompute.VirtualMachineScaleSetIPConfiguration{{
								Name: new("ip"),
								Properties: &armcompute.VirtualMachineScaleSetIPConfigurationProperties{
									Subnet: &armcompute.APIEntityReference{ID: new(s.subnetID)},
								},
							}},
						},
					}},
				},
			},
		},
	}
}

func (s *cmpStack) putScaleSet(
	t *testing.T,
	name string,
	ss armcompute.VirtualMachineScaleSet,
) (armcompute.VirtualMachineScaleSet, error) {
	t.Helper()

	pl, err := s.cf.NewVirtualMachineScaleSetsClient().BeginCreateOrUpdate(t.Context(), netRG, name, ss, nil)
	if err != nil {
		return armcompute.VirtualMachineScaleSet{}, err
	}

	res, err := pl.PollUntilDone(t.Context(), pollFast())

	return res.VirtualMachineScaleSet, err
}

func (s *cmpStack) instances(t *testing.T) []*armcompute.VirtualMachineScaleSetVM {
	t.Helper()

	pager := s.cf.NewVirtualMachineScaleSetVMsClient().NewListPager(netRG, "vmss1", nil)

	var out []*armcompute.VirtualMachineScaleSetVM

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		out = append(out, page.Value...)
	}

	return out
}

func TestComputeSDK_ScaleSetScaling(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	ss, err := s.putScaleSet(t, "vmss1", s.scaleSet(t, 3))
	require.NoError(t, err)
	assert.EqualValues(t, 3, *ss.SKU.Capacity)
	assert.NotEmpty(t, *ss.Properties.UniqueID)
	assert.Nil(t, ss.Properties.VirtualMachineProfile.OSProfile.AdminPassword)
	assert.Equal(t, "Uniform", string(*ss.Properties.OrchestrationMode))

	insts := s.instances(t)
	require.Len(t, insts, 3)
	assert.Equal(t, "0", *insts[0].InstanceID)
	assert.Equal(t, "web000000", *insts[0].Properties.OSProfile.ComputerName)
	assert.Equal(t, "web000002", *insts[2].Properties.OSProfile.ComputerName)
	assert.True(t, *insts[0].Properties.LatestModelApplied)

	got, err := s.putScaleSet(t, "vmss1", s.scaleSet(t, 5))
	require.NoError(t, err)
	assert.EqualValues(t, 5, *got.SKU.Capacity)
	assert.Len(t, s.instances(t), 5)

	got, err = s.putScaleSet(t, "vmss1", s.scaleSet(t, 2))
	require.NoError(t, err)
	assert.EqualValues(t, 2, *got.SKU.Capacity)
	assert.Len(t, s.instances(t), 2)

	one, err := s.cf.NewVirtualMachineScaleSetVMsClient().Get(t.Context(), netRG, "vmss1", "1", nil)
	require.NoError(t, err)
	assert.Equal(t, "vmss1_1", *one.Name)

	_, err = s.cf.NewVirtualMachineScaleSetVMsClient().Get(t.Context(), netRG, "vmss1", "9", nil)
	require.Error(t, err)

	delPoller, err := s.cf.NewVirtualMachineScaleSetVMsClient().BeginDelete(t.Context(), netRG, "vmss1", "0", nil)
	require.NoError(t, waitPoller(t, delPoller, err))
	assert.Len(t, s.instances(t), 1)

	grown, err := s.putScaleSet(t, "vmss1", s.scaleSet(t, 2))
	require.NoError(t, err)
	assert.EqualValues(t, 2, *grown.SKU.Capacity)

	final := s.instances(t)
	ids := make([]string, 0, len(final))

	for _, in := range final {
		ids = append(ids, *in.InstanceID)
	}

	assert.Equal(t, []string{"1", "5"}, ids, "instance ids are never reused")
}

func TestComputeSDK_ScaleSetUpgrade(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mode        armcompute.UpgradeMode
		wantLatest  bool
		wantUpgrade bool
	}{
		{mode: armcompute.UpgradeModeManual, wantLatest: false, wantUpgrade: true},
		{mode: armcompute.UpgradeModeAutomatic, wantLatest: true},
	}

	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			base := s.scaleSet(t, 2)
			base.Properties.UpgradePolicy.Mode = new(tt.mode)
			_, err := s.putScaleSet(t, "vmss1", base)
			require.NoError(t, err)

			changed := s.scaleSet(t, 2)
			changed.Properties.UpgradePolicy.Mode = new(tt.mode)
			changed.Properties.VirtualMachineProfile.StorageProfile.ImageReference.SKU = new("server-arm")
			_, err = s.putScaleSet(t, "vmss1", changed)
			require.NoError(t, err)

			for _, in := range s.instances(t) {
				assert.Equal(t, tt.wantLatest, *in.Properties.LatestModelApplied, *in.InstanceID)
			}

			if !tt.wantUpgrade {
				return
			}

			pl, err := s.cf.NewVirtualMachineScaleSetsClient().BeginUpdateInstances(t.Context(), netRG, "vmss1",
				armcompute.VirtualMachineScaleSetVMInstanceRequiredIDs{InstanceIDs: []*string{new("0")}}, nil)
			require.NoError(t, waitPoller(t, pl, err))

			latest := map[string]bool{}
			for _, in := range s.instances(t) {
				latest[*in.InstanceID] = *in.Properties.LatestModelApplied
			}

			assert.Equal(t, map[string]bool{"0": true, "1": false}, latest)

			bad, err := s.cf.NewVirtualMachineScaleSetsClient().BeginUpdateInstances(t.Context(), netRG, "vmss1",
				armcompute.VirtualMachineScaleSetVMInstanceRequiredIDs{InstanceIDs: []*string{new("42")}}, nil)
			err = waitPoller(t, bad, err)
			require.Error(t, err)
			assert.Equal(t, "InvalidParameter", sdkErrCode(err))
		})
	}
}

func TestComputeSDK_ScaleSetPowerAndValidation(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	_, err := s.putScaleSet(t, "vmss1", s.scaleSet(t, 2))
	require.NoError(t, err)

	c := s.cf.NewVirtualMachineScaleSetsClient()
	pl, err := c.BeginDeallocate(
		t.Context(),
		netRG,
		"vmss1",
		&armcompute.VirtualMachineScaleSetsClientBeginDeallocateOptions{
			VMInstanceIDs: &armcompute.VirtualMachineScaleSetVMInstanceIDs{InstanceIDs: []*string{new("1")}},
		},
	)
	require.NoError(t, waitPoller(t, pl, err))

	states := map[string]string{}

	for _, in := range s.instances(t) {
		for _, st := range in.Properties.InstanceView.Statuses {
			if strings.HasPrefix(*st.Code, "PowerState/") {
				states[*in.InstanceID] = *st.Code
			}
		}
	}

	assert.Equal(t, map[string]string{"0": "PowerState/running", "1": "PowerState/deallocated"}, states)

	tests := []struct {
		mutate   func(*armcompute.VirtualMachineScaleSet)
		name     string
		wantCode string
	}{
		{name: "unknown size", wantCode: "InvalidParameter", mutate: func(v *armcompute.VirtualMachineScaleSet) {
			v.SKU.Name = new("Standard_Nope")
		}},
		{name: "negative capacity", wantCode: "InvalidParameter", mutate: func(v *armcompute.VirtualMachineScaleSet) {
			v.SKU.Capacity = new(int64(-1))
		}},
		{
			name:     "missing subnet",
			wantCode: "InvalidResourceReference",
			mutate: func(v *armcompute.VirtualMachineScaleSet) {
				cfg := v.Properties.VirtualMachineProfile.NetworkProfile.NetworkInterfaceConfigurations[0]
				cfg.Properties.IPConfigurations[0].Properties.Subnet.ID = new(s.subnetID + "x")
			},
		},
		{name: "weak password", wantCode: "InvalidParameter", mutate: func(v *armcompute.VirtualMachineScaleSet) {
			v.Properties.VirtualMachineProfile.OSProfile.AdminPassword = new("weak")
		}},
		{name: "no network", wantCode: "InvalidParameter", mutate: func(v *armcompute.VirtualMachineScaleSet) {
			v.Properties.VirtualMachineProfile.NetworkProfile = nil
		}},
		{name: "bad upgrade mode", wantCode: "InvalidParameter", mutate: func(v *armcompute.VirtualMachineScaleSet) {
			v.Properties.UpgradePolicy.Mode = new(armcompute.UpgradeMode("Sometimes"))
		}},
	}

	for _, tt := range tests {
		ss := s.scaleSet(t, 1)
		tt.mutate(&ss)

		_, err = s.putScaleSet(t, "bad-"+strings.ReplaceAll(tt.name, " ", "-"), ss)
		require.Error(t, err, tt.name)
		assert.Equal(t, tt.wantCode, sdkErrCode(err), tt.name)
	}

	delPoller, err := c.BeginDelete(t.Context(), netRG, "vmss1", nil)
	require.NoError(t, waitPoller(t, delPoller, err))

	_, err = c.Get(t.Context(), netRG, "vmss1", nil)
	require.Error(t, err)
}

func TestComputeSDK_SizesAndSKUs(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)

	sizes := []string{}
	pager := s.cf.NewVirtualMachineSizesClient().NewListPager("westus", nil)

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		for _, e := range page.Value {
			sizes = append(sizes, *e.Name)
			assert.Positive(t, *e.NumberOfCores)
			assert.Positive(t, *e.MemoryInMB)
		}
	}

	assert.Contains(t, sizes, "Standard_B2s")
	assert.Contains(t, sizes, "Standard_D2s_v5")

	skus := map[string]*armcompute.ResourceSKU{}
	skuPager := s.cf.NewResourceSKUsClient().NewListPager(&armcompute.ResourceSKUsClientListOptions{
		Filter: new("location eq 'westus'"),
	})

	for skuPager.More() {
		page, err := skuPager.NextPage(t.Context())
		require.NoError(t, err)

		for _, e := range page.Value {
			skus[*e.ResourceType+"/"+*e.Name] = e
		}
	}

	require.Contains(t, skus, "virtualMachines/Standard_B2s")
	assert.Equal(t, "westus", *skus["virtualMachines/Standard_B2s"].Locations[0])
	assert.Contains(t, skus, "disks/Premium_LRS")

	for _, name := range sizes {
		assert.Contains(t, skus, "virtualMachines/"+name, "every listed size is a sku")
	}

	vm := s.mustVM(t, "vm1", linuxVM(s.newNIC(t, "nic1")))
	avail := s.cf.NewVirtualMachinesClient().NewListAvailableSizesPager(netRG, "vm1", nil)
	page, err := avail.NextPage(t.Context())
	require.NoError(t, err)
	assert.Len(t, page.Value, len(sizes))
	assert.NotEmpty(t, *vm.ID)
}

func TestComputeSDK_ListPagingAndPatch(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)

	for _, n := range []string{"a", "b", "c"} {
		_, err := s.putDisk(t, "disk-"+n, emptyDisk(4))
		require.NoError(t, err)
	}

	base := computePath("disks") + "?api-version=2026-03-02"
	resp := s.rawDo(t, http.MethodGet, base+"&$top=2", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	type listPage struct {
		NextLink string           `json:"nextLink"`
		Value    []map[string]any `json:"value"`
	}

	var page listPage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&page))
	assert.Len(t, page.Value, 2)
	require.NotEmpty(t, page.NextLink)

	next := strings.TrimPrefix(page.NextLink, s.url)
	resp = s.rawDo(t, http.MethodGet, next, "")

	var last listPage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&last))
	assert.Len(t, last.Value, 1)
	assert.Empty(t, last.NextLink)

	all := 0
	pager := s.cf.NewDisksClient().NewListByResourceGroupPager(netRG, nil)

	for pager.More() {
		p, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		all += len(p.Value)
	}

	assert.Equal(t, 3, all)

	pl, err := s.cf.NewDisksClient().BeginUpdate(t.Context(), netRG, "disk-a", armcompute.DiskUpdate{
		Tags: map[string]*string{
			"env": new("test"),
		}, Properties: &armcompute.DiskUpdateProperties{DiskSizeGB: new(int32(8))},
	}, nil)
	require.NoError(t, err)

	patched, err := pl.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Equal(t, "test", *patched.Tags["env"])
	assert.EqualValues(t, 8, *patched.Properties.DiskSizeGB)
}

func TestComputeSDK_EtagPreconditions(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	_, err := s.putDisk(t, "disk1", emptyDisk(4))
	require.NoError(t, err)

	path := computePath("disks/disk1") + "?api-version=2026-03-02"
	body := `{"location":"westus","properties":{"creationData":{"createOption":"Empty"},"diskSizeGB":4}}`

	do := func(method string, hdr map[string]string) *http.Response {
		req, rerr := http.NewRequestWithContext(t.Context(), method, s.url+path, strings.NewReader(body))
		require.NoError(t, rerr)

		for k, v := range hdr {
			req.Header.Set(k, v)
		}

		resp, derr := s.hc.Do(req)
		require.NoError(t, derr)

		t.Cleanup(func() { _ = resp.Body.Close() })

		return resp
	}

	first := do(http.MethodPut, nil)
	require.Equal(t, http.StatusOK, first.StatusCode)

	etag := first.Header.Get("ETag")
	require.NotEmpty(t, etag)

	assert.Equal(
		t,
		http.StatusPreconditionFailed,
		do(http.MethodPut, map[string]string{"If-Match": `"stale"`}).StatusCode,
	)
	assert.Equal(
		t,
		http.StatusPreconditionFailed,
		do(http.MethodPut, map[string]string{"If-None-Match": "*"}).StatusCode,
	)

	ok := do(http.MethodPut, map[string]string{"If-Match": etag})
	_, _ = io.Copy(io.Discard, ok.Body)
	require.Equal(t, http.StatusOK, ok.StatusCode)
	assert.NotEqual(t, etag, ok.Header.Get("ETag"), "a write changes the etag")

	assert.Equal(
		t,
		http.StatusPreconditionFailed,
		do(http.MethodDelete, map[string]string{"If-Match": etag}).StatusCode,
	)
}

func TestComputeSDK_APIVersionRequired(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	resp := s.rawDo(
		t,
		http.MethodGet,
		computePath("disks"),
		"",
	)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp = s.rawDo(
		t,
		http.MethodPost,
		computePath("disks/x/beginGetAccess")+"?api-version=2026-03-02",
		"{}",
	)
	assert.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}
