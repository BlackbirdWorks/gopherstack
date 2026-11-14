package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func appVersion(media, remove string) armcompute.GalleryApplicationVersion {
	return armcompute.GalleryApplicationVersion{
		Location: new("westus"),
		Properties: &armcompute.GalleryApplicationVersionProperties{
			PublishingProfile: &armcompute.GalleryApplicationVersionPublishingProfile{
				Source:        &armcompute.UserArtifactSource{MediaLink: new(media)},
				ManageActions: &armcompute.UserArtifactManage{Install: new("i"), Remove: new(remove)},
			},
		},
	}
}

func TestComputeSDK_GalleryApplications(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	ctx := t.Context()
	s.gallery(t)

	apps := s.cf.NewGalleryApplicationsClient()
	versions := s.cf.NewGalleryApplicationVersionsClient()

	app := armcompute.GalleryApplication{
		Location: new("westus"),
		Properties: &armcompute.GalleryApplicationProperties{
			SupportedOSType: new(armcompute.OperatingSystemTypesLinux), Description: new("d"),
			CustomActions: []*armcompute.GalleryApplicationCustomAction{{Name: new("a"), Script: new("echo")}},
		},
	}

	missing := app
	missing.Properties = &armcompute.GalleryApplicationProperties{}

	pl, err := apps.BeginCreateOrUpdate(ctx, netRG, "gal1", "app1", missing, nil)
	require.Error(t, waitPoller(t, pl, err))

	dup := app
	dup.Properties = &armcompute.GalleryApplicationProperties{
		SupportedOSType: new(armcompute.OperatingSystemTypesLinux),
		CustomActions: []*armcompute.GalleryApplicationCustomAction{
			{Name: new("a"), Script: new("x")}, {Name: new("a"), Script: new("y")},
		},
	}
	pl, err = apps.BeginCreateOrUpdate(ctx, netRG, "gal1", "app1", dup, nil)
	require.Error(t, waitPoller(t, pl, err))

	pl, err = apps.BeginCreateOrUpdate(ctx, netRG, "gal1", "app1", app, nil)
	require.NoError(t, waitPoller(t, pl, err))

	got, err := apps.Get(ctx, netRG, "gal1", "app1", nil)
	require.NoError(t, err)
	assert.Equal(t, armcompute.OperatingSystemTypesLinux, *got.Properties.SupportedOSType)
	assert.Equal(t, "Microsoft.Compute/galleries/applications", *got.Type)

	app.Properties.SupportedOSType = new(armcompute.OperatingSystemTypesWindows)
	pl, err = apps.BeginCreateOrUpdate(ctx, netRG, "gal1", "app1", app, nil)
	require.Error(t, waitPoller(t, pl, err))

	vp, err := versions.BeginCreateOrUpdate(ctx, netRG, "gal1", "ghost", "1.0.0", appVersion("https://x/y", "r"), nil)
	require.Error(t, waitPoller(t, vp, err))

	for name, v := range map[string]armcompute.GalleryApplicationVersion{
		"1.0":   appVersion("https://x/y", "r"),
		"1.0.0": appVersion("", "r"),
	} {
		vp, err = versions.BeginCreateOrUpdate(ctx, netRG, "gal1", "app1", name, v, nil)
		require.Error(t, waitPoller(t, vp, err), name)
	}

	noRemove := appVersion("https://x/y", "")
	vp, err = versions.BeginCreateOrUpdate(ctx, netRG, "gal1", "app1", "1.0.0", noRemove, nil)
	require.Error(t, waitPoller(t, vp, err))

	vp, err = versions.BeginCreateOrUpdate(ctx, netRG, "gal1", "app1", "1.0.0", appVersion("https://x/y", "r"), nil)
	require.NoError(t, waitPoller(t, vp, err))

	ver, err := versions.Get(
		ctx,
		netRG,
		"gal1",
		"app1",
		"1.0.0",
		&armcompute.GalleryApplicationVersionsClientGetOptions{
			Expand: new(armcompute.ReplicationStatusTypesReplicationStatus),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, int32(1), *ver.Properties.PublishingProfile.ReplicaCount)
	assert.Equal(t, "westus", *ver.Properties.PublishingProfile.TargetRegions[0].Name)
	assert.NotNil(t, ver.Properties.ReplicationStatus)

	n := 0

	for pager := versions.NewListByGalleryApplicationPager(netRG, "gal1", "app1", nil); pager.More(); {
		page, perr := pager.NextPage(ctx)
		require.NoError(t, perr)

		n += len(page.Value)
	}

	assert.Equal(t, 1, n)

	_, err = apps.BeginDelete(ctx, netRG, "gal1", "app1", nil)
	require.Error(t, err)
	assert.Equal(t, "CannotDeleteResource", sdkErrCode(err))

	dp, err := versions.BeginDelete(ctx, netRG, "gal1", "app1", "1.0.0", nil)
	require.NoError(t, waitPoller(t, dp, err))

	ap, err := apps.BeginDelete(ctx, netRG, "gal1", "app1", nil)
	require.NoError(t, waitPoller(t, ap, err))
}

func TestComputeSDK_GallerySharingProfileUpdate(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	ctx := t.Context()
	s.gallery(t)

	sharing := s.cf.NewGallerySharingProfileClient()
	galleries := s.cf.NewGalleriesClient()
	subA := "34a4ab42-0d72-47d9-bd1a-aed207386dac"
	subB := "380fd389-260b-41aa-bad9-0a83108c370b"
	tenant := "c24c76aa-8897-4027-9b03-8f7928b54ff6"

	update := func(op armcompute.SharingUpdateOperationTypes, groups ...*armcompute.SharingProfileGroup) error {
		p, err := sharing.BeginUpdate(ctx, netRG, "gal1", armcompute.SharingUpdate{
			OperationType: new(op), Groups: groups,
		}, nil)

		return waitPoller(t, p, err)
	}
	group := func(typ armcompute.SharingProfileGroupTypes, ids ...string) *armcompute.SharingProfileGroup {
		g := &armcompute.SharingProfileGroup{Type: new(typ)}
		for _, id := range ids {
			g.IDs = append(g.IDs, new(id))
		}

		return g
	}
	groupsOf := func() map[string]int {
		g, err := galleries.Get(ctx, netRG, "gal1", nil)
		require.NoError(t, err)

		out := map[string]int{}
		for _, grp := range g.Properties.SharingProfile.Groups {
			out[string(*grp.Type)] = len(grp.IDs)
		}

		return out
	}

	require.Error(t, update("Sideways"))
	require.Error(t, update(armcompute.SharingUpdateOperationTypesAdd))
	require.Error(t, update(armcompute.SharingUpdateOperationTypesAdd, group("Nope", subA)))
	require.Error(
		t,
		update(armcompute.SharingUpdateOperationTypesAdd, group(armcompute.SharingProfileGroupTypesSubscriptions, "x")),
	)

	require.NoError(t, update(armcompute.SharingUpdateOperationTypesAdd,
		group(armcompute.SharingProfileGroupTypesSubscriptions, subA, subB),
		group(armcompute.SharingProfileGroupTypesAADTenants, tenant)))
	assert.Equal(t, map[string]int{"Subscriptions": 2, "AADTenants": 1}, groupsOf())

	g, err := galleries.Get(ctx, netRG, "gal1", nil)
	require.NoError(t, err)
	assert.Equal(t, armcompute.GallerySharingPermissionTypesGroups, *g.Properties.SharingProfile.Permissions)

	require.NoError(
		t,
		update(
			armcompute.SharingUpdateOperationTypesAdd,
			group(armcompute.SharingProfileGroupTypesSubscriptions, subA),
		),
	)
	assert.Equal(t, 2, groupsOf()["Subscriptions"])

	require.NoError(
		t,
		update(
			armcompute.SharingUpdateOperationTypesRemove,
			group(armcompute.SharingProfileGroupTypesSubscriptions, subA),
		),
	)
	assert.Equal(t, map[string]int{"Subscriptions": 1, "AADTenants": 1}, groupsOf())

	require.Error(t, update(armcompute.SharingUpdateOperationTypesEnableCommunity))

	require.NoError(t, update(armcompute.SharingUpdateOperationTypesReset))
	assert.Empty(t, groupsOf())

	g, err = galleries.Get(ctx, netRG, "gal1", nil)
	require.NoError(t, err)
	assert.Equal(t, armcompute.GallerySharingPermissionTypesPrivate, *g.Properties.SharingProfile.Permissions)

	cp, err := galleries.BeginCreateOrUpdate(ctx, netRG, "gal1", armcompute.Gallery{
		Location: new("westus"),
		Properties: &armcompute.GalleryProperties{SharingProfile: &armcompute.SharingProfile{
			CommunityGalleryInfo: &armcompute.CommunityGalleryInfo{
				PublisherURI: new("https://contoso.example"), PublisherContact: new("a@contoso.example"),
				Eula: new("https://contoso.example/eula"), PublicNamePrefix: new("contoso"),
			},
		}},
	}, nil)
	require.NoError(t, waitPoller(t, cp, err))
	require.NoError(t, update(armcompute.SharingUpdateOperationTypesEnableCommunity))

	g, err = galleries.Get(ctx, netRG, "gal1", nil)
	require.NoError(t, err)
	assert.Equal(t, armcompute.GallerySharingPermissionTypesCommunity, *g.Properties.SharingProfile.Permissions)

	_, err = sharing.BeginUpdate(ctx, netRG, "ghost", armcompute.SharingUpdate{
		OperationType: new(armcompute.SharingUpdateOperationTypesReset),
	}, nil)
	require.Error(t, err)
}

func TestComputeSDK_DedicatedHostRestart(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	ctx := t.Context()
	hosts := s.cf.NewDedicatedHostsClient()

	_, err := s.cf.NewDedicatedHostGroupsClient().CreateOrUpdate(ctx, netRG, "hg1", armcompute.DedicatedHostGroup{
		Location:   new("westus"),
		Properties: &armcompute.DedicatedHostGroupProperties{PlatformFaultDomainCount: new(int32(1))},
	}, nil)
	require.NoError(t, err)

	hp, err := hosts.BeginCreateOrUpdate(ctx, netRG, "hg1", "h1", armcompute.DedicatedHost{
		Location: new("westus"), SKU: &armcompute.SKU{Name: new("DSv3-Type1")},
	}, nil)
	require.NoError(t, waitPoller(t, hp, err))

	host, err := hosts.Get(ctx, netRG, "hg1", "h1", nil)
	require.NoError(t, err)

	vm := linuxVM(s.newNIC(t, "nic1"))
	vm.Properties.Host = &armcompute.SubResource{ID: host.ID}
	s.mustVM(t, "vm1", vm)

	powerTime := func(name string) string {
		iv, ierr := s.cf.NewVirtualMachinesClient().InstanceView(ctx, netRG, name, nil)
		require.NoError(t, ierr)

		for _, st := range iv.Statuses {
			if *st.Code == "PowerState/running" {
				return st.Time.String()
			}
		}

		return ""
	}

	before := powerTime("vm1")
	require.NotEmpty(t, before)

	rp, err := hosts.BeginRestart(ctx, netRG, "hg1", "h1", nil)
	require.NoError(t, waitPoller(t, rp, err))
	assert.Equal(t, "running", s.power(t, "vm1"))
	assert.NotEqual(t, before, powerTime("vm1"))

	vp, err := s.cf.NewVirtualMachinesClient().BeginPowerOff(ctx, netRG, "vm1", nil)
	require.NoError(t, waitPoller(t, vp, err))

	rp, err = hosts.BeginRestart(ctx, netRG, "hg1", "h1", nil)
	require.NoError(t, waitPoller(t, rp, err))
	assert.Equal(t, "stopped", s.power(t, "vm1"))

	_, err = hosts.BeginRestart(ctx, netRG, "hg1", "ghost", nil)
	require.Error(t, err)
}
