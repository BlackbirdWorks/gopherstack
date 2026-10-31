package azurearm_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *cmpStack) generalizedImage(t *testing.T) string {
	t.Helper()

	const (
		imageName = "img1"
		vmName    = "src"
	)

	s.mustVM(t, vmName, linuxVM(s.newNIC(t, "nic-"+vmName)))

	pl, err := s.cf.NewVirtualMachinesClient().BeginDeallocate(t.Context(), netRG, vmName, nil)
	require.NoError(t, waitPoller(t, pl, err))

	_, err = s.cf.NewVirtualMachinesClient().Generalize(t.Context(), netRG, vmName, nil)
	require.NoError(t, err)

	vm, err := s.cf.NewVirtualMachinesClient().Get(t.Context(), netRG, vmName, nil)
	require.NoError(t, err)

	ip, err := s.cf.NewImagesClient().BeginCreateOrUpdate(t.Context(), netRG, imageName, armcompute.Image{
		Location:   new("westus"),
		Properties: &armcompute.ImageProperties{SourceVirtualMachine: &armcompute.SubResource{ID: vm.ID}},
	}, nil)
	require.NoError(t, waitPoller(t, ip, err))

	img, err := s.cf.NewImagesClient().Get(t.Context(), netRG, imageName, nil)
	require.NoError(t, err)

	return *img.ID
}

func (s *cmpStack) gallery(t *testing.T) {
	t.Helper()

	pl, err := s.cf.NewGalleriesClient().BeginCreateOrUpdate(t.Context(), netRG, "gal1",
		armcompute.Gallery{Location: new("westus")}, nil)
	require.NoError(t, waitPoller(t, pl, err))
}

func galleryDef(osState armcompute.OperatingSystemStateTypes, sku string) armcompute.GalleryImage {
	return armcompute.GalleryImage{
		Location: new("westus"),
		Properties: &armcompute.GalleryImageProperties{
			OSType:  new(armcompute.OperatingSystemTypesLinux),
			OSState: new(osState),
			Identifier: &armcompute.GalleryImageIdentifier{
				Publisher: new("Contoso"),
				Offer:     new("Finance"),
				SKU:       new(sku),
			},
		},
	}
}

func (s *cmpStack) galleryDef(t *testing.T, gallery, name string, def armcompute.GalleryImage) error {
	t.Helper()

	pl, err := s.cf.NewGalleryImagesClient().BeginCreateOrUpdate(t.Context(), netRG, gallery, name, def, nil)

	return waitPoller(t, pl, err)
}

func (s *cmpStack) galleryVersion(t *testing.T, def, name string, v armcompute.GalleryImageVersion) error {
	t.Helper()

	pl, err := s.cf.NewGalleryImageVersionsClient().BeginCreateOrUpdate(t.Context(), netRG, "gal1", def, name, v, nil)

	return waitPoller(t, pl, err)
}

func versionFrom(source string, regions ...string) armcompute.GalleryImageVersion {
	v := armcompute.GalleryImageVersion{
		Location: new("westus"),
		Properties: &armcompute.GalleryImageVersionProperties{
			StorageProfile: &armcompute.GalleryImageVersionStorageProfile{
				Source: &armcompute.GalleryArtifactVersionFullSource{ID: new(source)},
			},
		},
	}

	if len(regions) > 0 {
		v.Properties.PublishingProfile = &armcompute.GalleryImageVersionPublishingProfile{}
		for _, r := range regions {
			v.Properties.PublishingProfile.TargetRegions = append(v.Properties.PublishingProfile.TargetRegions,
				&armcompute.TargetRegion{Name: new(r)})
		}
	}

	return v
}

func TestComputeSDK_Galleries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		gallery  string
		wantCode string
	}{
		{name: "plain", gallery: "gal1"},
		{name: "dots and underscores", gallery: "my.gal_1"},
		{name: "dash rejected", gallery: "my-gal", wantCode: "InvalidParameter"},
		{name: "trailing dot rejected", gallery: "gal.", wantCode: "InvalidParameter"},
		{name: "leading underscore rejected", gallery: "_gal", wantCode: "InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			c := s.cf.NewGalleriesClient()
			pl, err := c.BeginCreateOrUpdate(t.Context(), netRG, tt.gallery, armcompute.Gallery{
				Location:   new("westus"),
				Properties: &armcompute.GalleryProperties{Description: new("d")},
			}, nil)
			err = waitPoller(t, pl, err)

			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			got, err := c.Get(t.Context(), netRG, tt.gallery, nil)
			require.NoError(t, err)
			assert.Equal(t, "Succeeded", string(*got.Properties.ProvisioningState))
			assert.Equal(t, "Private", string(*got.Properties.SharingProfile.Permissions))
			assert.Contains(t, *got.Properties.Identifier.UniqueName, "-"+upper(tt.gallery))

			up, err := c.BeginUpdate(t.Context(), netRG, tt.gallery, armcompute.GalleryUpdate{
				Properties: &armcompute.GalleryProperties{Description: new("changed")},
			}, nil)
			require.NoError(t, waitPoller(t, up, err))

			got, err = c.Get(t.Context(), netRG, tt.gallery, nil)
			require.NoError(t, err)
			assert.Equal(t, "changed", *got.Properties.Description)

			lp := c.NewListByResourceGroupPager(netRG, nil)
			page, err := lp.NextPage(t.Context())
			require.NoError(t, err)
			assert.Len(t, page.Value, 1)
		})
	}
}

func upper(s string) string {
	out := []byte(s)
	for i, b := range out {
		if b >= 'a' && b <= 'z' {
			out[i] = b - ('a' - 'A')
		}
	}

	return string(out)
}

func TestComputeSDK_GalleryNameUniquePerSubscription(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	s.gallery(t)

	s.rawDo(t, http.MethodPut, "/subscriptions/"+aciSub+"/resourcegroups/rg-other", `{"location":"westus"}`)

	pl, err := s.cf.NewGalleriesClient().BeginCreateOrUpdate(t.Context(), "rg-other", "GAL1",
		armcompute.Gallery{Location: new("westus")}, nil)
	err = waitPoller(t, pl, err)
	require.Error(t, err)
	assert.Equal(t, "InvalidParameter", sdkErrCode(err))
}

func TestComputeSDK_GalleryImageDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(*armcompute.GalleryImage)
		name     string
		defName  string
		wantCode string
	}{
		{name: "valid", defName: "def1", mutate: func(*armcompute.GalleryImage) {}},
		{name: "defaults hyperV", defName: "def.2-x", mutate: func(*armcompute.GalleryImage) {}},
		{name: "missing osType", defName: "def1", wantCode: "InvalidParameter",
			mutate: func(g *armcompute.GalleryImage) { g.Properties.OSType = nil }},
		{name: "missing osState", defName: "def1", wantCode: "InvalidParameter",
			mutate: func(g *armcompute.GalleryImage) { g.Properties.OSState = nil }},
		{name: "empty publisher", defName: "def1", wantCode: "InvalidParameter",
			mutate: func(g *armcompute.GalleryImage) { g.Properties.Identifier.Publisher = new("") }},
		{name: "offer too long", defName: "def1", wantCode: "InvalidParameter",
			mutate: func(g *armcompute.GalleryImage) { g.Properties.Identifier.Offer = new(fmt.Sprintf("%065d", 1)) }},
		{name: "sku trailing dot", defName: "def1", wantCode: "InvalidParameter",
			mutate: func(g *armcompute.GalleryImage) { g.Properties.Identifier.SKU = new("sku.") }},
		{
			name:     "name trailing dash",
			defName:  "def-",
			wantCode: "InvalidParameter",
			mutate:   func(*armcompute.GalleryImage) {},
		},
		{name: "memory min above max", defName: "def1", wantCode: "InvalidParameter",
			mutate: func(g *armcompute.GalleryImage) {
				g.Properties.Recommended = &armcompute.RecommendedMachineConfiguration{
					Memory: &armcompute.ResourceRange{Min: new(int32(8)), Max: new(int32(4))},
				}
			}},
		{name: "end of life in the past", defName: "def1", wantCode: "InvalidParameter",
			mutate: func(g *armcompute.GalleryImage) { g.Properties.EndOfLifeDate = new(time.Now().Add(-time.Hour)) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			s.gallery(t)

			def := galleryDef(armcompute.OperatingSystemStateTypesGeneralized, "Backend")
			tt.mutate(&def)

			err := s.galleryDef(t, "gal1", tt.defName, def)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			got, err := s.cf.NewGalleryImagesClient().Get(t.Context(), netRG, "gal1", tt.defName, nil)
			require.NoError(t, err)
			assert.Equal(t, "V1", string(*got.Properties.HyperVGeneration))
			assert.Equal(t, "Microsoft.Compute/galleries/images", *got.Type)
		})
	}
}

func TestComputeSDK_GalleryImageIdentity(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	s.gallery(t)
	gen := armcompute.OperatingSystemStateTypesGeneralized
	require.NoError(t, s.galleryDef(t, "gal1", "def1", galleryDef(gen, "Backend")))

	err := s.galleryDef(t, "gal1", "def2", galleryDef(gen, "Backend"))
	require.Error(t, err)
	assert.Equal(t, "Conflict", sdkErrCode(err))

	require.NoError(
		t,
		s.galleryDef(t, "gal1", "def2", galleryDef(gen, "Frontend")),
		"sharing two of three values is fine",
	)

	changes := map[string]func(*armcompute.GalleryImage){
		"osState": func(g *armcompute.GalleryImage) {
			g.Properties.OSState = new(armcompute.OperatingSystemStateTypesSpecialized)
		},
		"osType":    func(g *armcompute.GalleryImage) { g.Properties.OSType = new(armcompute.OperatingSystemTypesWindows) },
		"publisher": func(g *armcompute.GalleryImage) { g.Properties.Identifier.Publisher = new("Other") },
		"hyperV":    func(g *armcompute.GalleryImage) { g.Properties.HyperVGeneration = new(armcompute.HyperVGenerationV2) },
	}

	for name, mut := range changes {
		def := galleryDef(gen, "Backend")
		mut(&def)

		err = s.galleryDef(t, "gal1", "def1", def)
		require.Error(t, err, name)
		assert.Equal(t, "PropertyChangeNotAllowed", sdkErrCode(err), name)
	}

	up := galleryDef(gen, "Backend")
	up.Properties.Description = new("now documented")
	require.NoError(t, s.galleryDef(t, "gal1", "def1", up), "mutable properties update")

	err = s.galleryDef(t, "ghost", "def1", galleryDef(gen, "Backend"))
	require.Error(t, err)
	assert.Equal(t, "ParentResourceNotFound", sdkErrCode(err))
}

func TestComputeSDK_GalleryImageVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(*armcompute.GalleryImageVersion, string)
		name     string
		version  string
		wantCode string
	}{
		{name: "valid", version: "1.0.0", mutate: func(*armcompute.GalleryImageVersion, string) {}},
		{name: "large numbers", version: "2018.12.1", mutate: func(*armcompute.GalleryImageVersion, string) {}},
		{
			name:     "two parts",
			version:  "1.0",
			wantCode: "InvalidParameter",
			mutate:   func(*armcompute.GalleryImageVersion, string) {},
		},
		{
			name:     "non numeric",
			version:  "1.0.x",
			wantCode: "InvalidParameter",
			mutate:   func(*armcompute.GalleryImageVersion, string) {},
		},
		{
			name:     "overflow",
			version:  "1.0.2147483648",
			wantCode: "InvalidParameter",
			mutate:   func(*armcompute.GalleryImageVersion, string) {},
		},
		{name: "no source", version: "1.0.0", wantCode: "InvalidParameter",
			mutate: func(v *armcompute.GalleryImageVersion, _ string) { v.Properties.StorageProfile.Source = nil }},
		{name: "source missing", version: "1.0.0", wantCode: "ResourceNotFound",
			mutate: func(v *armcompute.GalleryImageVersion, img string) {
				v.Properties.StorageProfile.Source.ID = new(img + "-ghost")
			}},
		{name: "source not fully qualified", version: "1.0.0", wantCode: "InvalidParameter",
			mutate: func(v *armcompute.GalleryImageVersion, _ string) {
				v.Properties.StorageProfile.Source.ID = new("images/x")
			}},
		{name: "regions must include location", version: "1.0.0", wantCode: "InvalidParameter",
			mutate: func(v *armcompute.GalleryImageVersion, _ string) {
				v.Properties.PublishingProfile = &armcompute.GalleryImageVersionPublishingProfile{
					TargetRegions: []*armcompute.TargetRegion{{Name: new("eastus")}},
				}
			}},
		{name: "duplicate regions", version: "1.0.0", wantCode: "InvalidParameter",
			mutate: func(v *armcompute.GalleryImageVersion, _ string) {
				v.Properties.PublishingProfile = &armcompute.GalleryImageVersionPublishingProfile{
					TargetRegions: []*armcompute.TargetRegion{{Name: new("westus")}, {Name: new("West US")}},
				}
			}},
		{name: "bad storage account type", version: "1.0.0", wantCode: "InvalidParameter",
			mutate: func(v *armcompute.GalleryImageVersion, _ string) {
				v.Properties.PublishingProfile = &armcompute.GalleryImageVersionPublishingProfile{
					StorageAccountType: new(armcompute.StorageAccountType("Ultra_LRS")),
				}
			}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			img := s.generalizedImage(t)
			s.gallery(t)
			require.NoError(
				t,
				s.galleryDef(t, "gal1", "def1", galleryDef(armcompute.OperatingSystemStateTypesGeneralized, "Backend")),
			)

			v := versionFrom(img)
			tt.mutate(&v, img)

			err := s.galleryVersion(t, "def1", tt.version, v)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			c := s.cf.NewGalleryImageVersionsClient()

			got, err := c.Get(t.Context(), netRG, "gal1", "def1", tt.version, nil)
			require.NoError(t, err)
			require.Len(t, got.Properties.PublishingProfile.TargetRegions, 1, "defaults to the version location")
			assert.Equal(t, "westus", *got.Properties.PublishingProfile.TargetRegions[0].Name)
			assert.EqualValues(t, 1, *got.Properties.PublishingProfile.ReplicaCount)
			assert.NotNil(t, got.Properties.PublishingProfile.PublishedDate)
			assert.Nil(t, got.Properties.ReplicationStatus, "replication status only with $expand")

			exp, err := c.Get(
				t.Context(),
				netRG,
				"gal1",
				"def1",
				tt.version,
				&armcompute.GalleryImageVersionsClientGetOptions{
					Expand: new(armcompute.ReplicationStatusTypesReplicationStatus),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "Completed", string(*exp.Properties.ReplicationStatus.AggregatedState))
			assert.Len(t, exp.Properties.ReplicationStatus.Summary, 1)
		})
	}
}

func TestComputeSDK_GalleryVersionSourceChecks(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	img := s.generalizedImage(t)
	s.gallery(t)

	gen, spec := armcompute.OperatingSystemStateTypesGeneralized, armcompute.OperatingSystemStateTypesSpecialized
	require.NoError(t, s.galleryDef(t, "gal1", "gen", galleryDef(gen, "Backend")))
	require.NoError(t, s.galleryDef(t, "gal1", "spec", galleryDef(spec, "Frontend")))

	win := galleryDef(gen, "Win")
	win.Properties.OSType = new(armcompute.OperatingSystemTypesWindows)
	require.NoError(t, s.galleryDef(t, "gal1", "win", win))

	v2 := galleryDef(gen, "Gen2")
	v2.Properties.HyperVGeneration = new(armcompute.HyperVGenerationV2)
	require.NoError(t, s.galleryDef(t, "gal1", "gen2", v2))

	for def, want := range map[string]string{"spec": "OS State", "win": "OS type", "gen2": "Hypervisor generation"} {
		err := s.galleryVersion(t, def, "1.0.0", versionFrom(img))
		require.Error(t, err, def)
		assert.Equal(t, "InvalidParameter", sdkErrCode(err), def)
		assert.Contains(t, err.Error(), want, def)
	}

	require.NoError(t, s.galleryVersion(t, "gen", "1.0.0", versionFrom(img)))

	other := versionFrom(
		computePath("galleries/gal1/images/gen/versions/1.0.0"),
	)
	require.NoError(t, s.galleryVersion(t, "gen", "1.1.0", other), "another gallery image version is a source")

	changed := versionFrom(img + "2")
	err := s.galleryVersion(t, "gen", "1.0.0", changed)
	require.Error(t, err)

	err = s.galleryVersion(t, "gen", "1.0.0", other)
	require.Error(t, err)
	assert.Equal(t, "PropertyChangeNotAllowed", sdkErrCode(err))

	region := versionFrom(img, "westus", "eastus")
	region.Properties.PublishingProfile.ReplicaCount = new(int32(2))
	require.NoError(t, s.galleryVersion(t, "gen", "1.0.0", region), "same source updates replication")

	got, err := s.cf.NewGalleryImageVersionsClient().Get(t.Context(), netRG, "gal1", "gen", "1.0.0", nil)
	require.NoError(t, err)
	assert.Len(t, got.Properties.PublishingProfile.TargetRegions, 2)
	assert.EqualValues(t, 2, *got.Properties.PublishingProfile.TargetRegions[1].RegionalReplicaCount)
}

func TestComputeSDK_GalleryDeleteOrder(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	img := s.generalizedImage(t)
	s.gallery(t)
	require.NoError(
		t,
		s.galleryDef(t, "gal1", "def1", galleryDef(armcompute.OperatingSystemStateTypesGeneralized, "Backend")),
	)
	require.NoError(t, s.galleryVersion(t, "def1", "1.0.0", versionFrom(img)))

	gc, ic, vc := s.cf.NewGalleriesClient(), s.cf.NewGalleryImagesClient(), s.cf.NewGalleryImageVersionsClient()

	dg, err := gc.BeginDelete(t.Context(), netRG, "gal1", nil)
	err = waitPoller(t, dg, err)
	require.Error(t, err)
	assert.Equal(t, "CannotDeleteResource", sdkErrCode(err))

	di, err := ic.BeginDelete(t.Context(), netRG, "gal1", "def1", nil)
	err = waitPoller(t, di, err)
	require.Error(t, err)
	assert.Equal(t, "CannotDeleteResource", sdkErrCode(err))

	dv, err := vc.BeginDelete(t.Context(), netRG, "gal1", "def1", "1.0.0", nil)
	require.NoError(t, waitPoller(t, dv, err))

	di, err = ic.BeginDelete(t.Context(), netRG, "gal1", "def1", nil)
	require.NoError(t, waitPoller(t, di, err))

	dg, err = gc.BeginDelete(t.Context(), netRG, "gal1", nil)
	require.NoError(t, waitPoller(t, dg, err))
}

func TestComputeSDK_GalleryVersionBlockedDeletion(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	img := s.generalizedImage(t)
	s.gallery(t)
	require.NoError(
		t,
		s.galleryDef(t, "gal1", "def1", galleryDef(armcompute.OperatingSystemStateTypesGeneralized, "Backend")),
	)

	v := versionFrom(img)
	v.Properties.PublishingProfile = &armcompute.GalleryImageVersionPublishingProfile{
		EndOfLifeDate: new(time.Now().Add(24 * time.Hour)),
	}
	v.Properties.SafetyProfile = &armcompute.GalleryImageVersionSafetyProfile{BlockDeletionBeforeEndOfLife: new(true)}
	require.NoError(t, s.galleryVersion(t, "def1", "1.0.0", v))

	dv, err := s.cf.NewGalleryImageVersionsClient().BeginDelete(t.Context(), netRG, "gal1", "def1", "1.0.0", nil)
	err = waitPoller(t, dv, err)
	require.Error(t, err)
	assert.Equal(t, "DeleteGalleryImageVersionBlockedBeforeEndOfLife", sdkErrCode(err))
}

func TestComputeSDK_ManagedImages(t *testing.T) {
	t.Parallel()

	osDisk := func(mut func(*armcompute.ImageOSDisk)) *armcompute.Image {
		d := &armcompute.ImageOSDisk{
			OSType: new(
				armcompute.OperatingSystemTypesLinux,
			),
			OSState: new(armcompute.OperatingSystemStateTypesGeneralized),
			BlobURI: new("https://acct.blob.core.windows.net/vhds/os.vhd"),
		}
		mut(d)

		return &armcompute.Image{
			Location:   new("westus"),
			Properties: &armcompute.ImageProperties{StorageProfile: &armcompute.ImageStorageProfile{OSDisk: d}},
		}
	}

	tests := []struct {
		image    func() *armcompute.Image
		name     string
		wantCode string
	}{
		{name: "blob", image: func() *armcompute.Image { return osDisk(func(*armcompute.ImageOSDisk) {}) }},
		{name: "missing osType", wantCode: "InvalidParameter",
			image: func() *armcompute.Image { return osDisk(func(d *armcompute.ImageOSDisk) { d.OSType = nil }) }},
		{name: "missing osState", wantCode: "InvalidParameter",
			image: func() *armcompute.Image { return osDisk(func(d *armcompute.ImageOSDisk) { d.OSState = nil }) }},
		{name: "no source", wantCode: "InvalidParameter",
			image: func() *armcompute.Image { return osDisk(func(d *armcompute.ImageOSDisk) { d.BlobURI = nil }) }},
		{name: "snapshot missing", wantCode: "ResourceNotFound",
			image: func() *armcompute.Image {
				return osDisk(func(d *armcompute.ImageOSDisk) {
					d.BlobURI = nil
					d.Snapshot = &armcompute.SubResource{
						ID: new("/subscriptions/" + aciSub + "/resourceGroups/" + netRG +
							"/providers/Microsoft.Compute/snapshots/ghost"),
					}
				})
			}},
		{name: "bad storage type", wantCode: "InvalidParameter",
			image: func() *armcompute.Image {
				return osDisk(
					func(d *armcompute.ImageOSDisk) { d.StorageAccountType = new(armcompute.StorageAccountTypes("Nope")) },
				)
			}},
		{name: "no storage profile", wantCode: "InvalidParameter",
			image: func() *armcompute.Image { return &armcompute.Image{Location: new("westus")} }},
		{name: "source vm missing", wantCode: "ResourceNotFound",
			image: func() *armcompute.Image {
				return &armcompute.Image{Location: new("westus"), Properties: &armcompute.ImageProperties{
					SourceVirtualMachine: &armcompute.SubResource{
						ID: new("/subscriptions/" + aciSub + "/resourceGroups/" +
							netRG + "/providers/Microsoft.Compute/virtualMachines/ghost"),
					},
				}}
			}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			pl, err := s.cf.NewImagesClient().BeginCreateOrUpdate(t.Context(), netRG, "img1", *tt.image(), nil)
			err = waitPoller(t, pl, err)

			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			got, err := s.cf.NewImagesClient().Get(t.Context(), netRG, "img1", nil)
			require.NoError(t, err)
			assert.Equal(t, "V1", string(*got.Properties.HyperVGeneration))
			assert.Equal(t, "Succeeded", *got.Properties.ProvisioningState)

			lp := s.cf.NewImagesClient().NewListPager(nil)
			page, err := lp.NextPage(t.Context())
			require.NoError(t, err)
			assert.Len(t, page.Value, 1)
		})
	}
}

func TestComputeSDK_ManagedImageFromVM(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	id := s.generalizedImage(t)

	got, err := s.cf.NewImagesClient().Get(t.Context(), netRG, "img1", nil)
	require.NoError(t, err)
	assert.Equal(t, id, *got.ID)

	osd := got.Properties.StorageProfile.OSDisk
	assert.Equal(t, "Generalized", string(*osd.OSState))
	assert.Equal(t, "Linux", string(*osd.OSType))
	assert.NotEmpty(t, *osd.ManagedDisk.ID)

	both := armcompute.Image{Location: new("westus"), Properties: &armcompute.ImageProperties{
		SourceVirtualMachine: &armcompute.SubResource{ID: new("x")},
		StorageProfile:       got.Properties.StorageProfile,
	}}
	pl, err := s.cf.NewImagesClient().BeginCreateOrUpdate(t.Context(), netRG, "img2", both, nil)
	err = waitPoller(t, pl, err)
	require.Error(t, err)
	assert.Equal(t, "InvalidParameter", sdkErrCode(err))

	dl, err := s.cf.NewImagesClient().BeginDelete(t.Context(), netRG, "img1", nil)
	require.NoError(t, waitPoller(t, dl, err))
}

func TestComputeSDK_VMFromImage(t *testing.T) {
	t.Parallel()

	gen, spec := armcompute.OperatingSystemStateTypesGeneralized, armcompute.OperatingSystemStateTypesSpecialized
	prefix := "/subscriptions/" + aciSub + "/resourceGroups/" + netRG + "/providers/Microsoft.Compute/"

	tests := []struct {
		name       string
		imageID    func(managed string) string
		mutate     func(*armcompute.VirtualMachine)
		wantCode   string
		wantMsg    string
		specialDef bool
		noVersion  bool
	}{
		{name: "managed image", imageID: func(m string) string { return m }},
		{name: "managed image missing", wantCode: "ResourceNotFound",
			imageID: func(string) string { return prefix + "images/ghost" }},
		{
			name:    "gallery version",
			imageID: func(string) string { return prefix + "galleries/gal1/images/def1/versions/1.0.0" },
		},
		{name: "gallery latest", imageID: func(string) string { return prefix + "galleries/gal1/images/def1" }},
		{name: "gallery version missing", wantCode: "ResourceNotFound",
			imageID: func(string) string { return prefix + "galleries/gal1/images/def1/versions/9.9.9" }},
		{name: "gallery definition missing", wantCode: "ResourceNotFound",
			imageID: func(string) string { return prefix + "galleries/gal1/images/ghost" }},
		{
			name:      "no versions for latest",
			noVersion: true,
			wantCode:  "InvalidParameter",
			wantMsg:   "There is no latest image version",
			imageID:   func(string) string { return prefix + "galleries/gal1/images/def1" },
		},
		{
			name:     "generalized needs osProfile",
			wantCode: "InvalidParameter",
			wantMsg:  "Required parameter 'osProfile' is missing",
			imageID:  func(string) string { return prefix + "galleries/gal1/images/def1/versions/1.0.0" },
			mutate:   func(vm *armcompute.VirtualMachine) { vm.Properties.OSProfile = nil },
		},
		{name: "windows config on linux image", wantCode: "InvalidParameter", wantMsg: "different OS type",
			imageID: func(string) string { return prefix + "galleries/gal1/images/def1/versions/1.0.0" },
			mutate: func(vm *armcompute.VirtualMachine) {
				vm.Properties.OSProfile.WindowsConfiguration = &armcompute.WindowsConfiguration{}
			}},
		{name: "specialized forbids osProfile", specialDef: true, wantCode: "InvalidParameter",
			wantMsg: "Parameter 'osProfile' is not allowed",
			imageID: func(string) string { return prefix + "galleries/gal1/images/def1/versions/1.0.0" }},
		{name: "specialized without osProfile", specialDef: true,
			imageID: func(string) string { return prefix + "galleries/gal1/images/def1/versions/1.0.0" },
			mutate:  func(vm *armcompute.VirtualMachine) { vm.Properties.OSProfile = nil }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			managed := s.generalizedImage(t)
			s.gallery(t)

			state := gen
			if tt.specialDef {
				state = spec
			}

			require.NoError(t, s.galleryDef(t, "gal1", "def1", galleryDef(state, "Backend")))

			if !tt.noVersion {
				source := managed
				if tt.specialDef {
					s.mustVM(t, "live", linuxVM(s.newNIC(t, "nic-live")))
					live, err := s.cf.NewVirtualMachinesClient().Get(t.Context(), netRG, "live", nil)
					require.NoError(t, err)

					source = *live.ID
				}

				v := versionFrom(source)
				if tt.specialDef {
					v.Properties.StorageProfile.Source = &armcompute.GalleryArtifactVersionFullSource{
						VirtualMachineID: new(source),
					}
				}

				require.NoError(t, s.galleryVersion(t, "def1", "1.0.0", v))
			}

			vm := linuxVM(s.newNIC(t, "nic-new"))
			vm.Properties.StorageProfile.ImageReference = &armcompute.ImageReference{ID: new(tt.imageID(managed))}

			if tt.mutate != nil {
				tt.mutate(&vm)
			}

			_, err := s.putVM(t, netRG, "fromimg", vm)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))
				assert.Contains(t, err.Error(), tt.wantMsg)

				return
			}

			require.NoError(t, err)

			got, err := s.cf.NewVirtualMachinesClient().Get(t.Context(), netRG, "fromimg", nil)
			require.NoError(t, err)
			assert.Equal(t, tt.imageID(managed), *got.Properties.StorageProfile.ImageReference.ID)

			if tt.specialDef {
				assert.Nil(t, got.Properties.OSProfile)
			}
		})
	}
}

func TestComputeSDK_VMFromImageRegion(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	managed := s.generalizedImage(t)
	s.gallery(t)
	require.NoError(
		t,
		s.galleryDef(t, "gal1", "def1", galleryDef(armcompute.OperatingSystemStateTypesGeneralized, "Backend")),
	)

	v := versionFrom(managed, "westus")
	require.NoError(t, s.galleryVersion(t, "def1", "1.0.0", v))

	vm := linuxVM(s.newNIC(t, "nic-new"))
	vm.Location = new("eastus")
	vm.Properties.StorageProfile.ImageReference = &armcompute.ImageReference{ID: new(
		computePath("galleries/gal1/images/def1/versions/1.0.0"),
	)}

	_, err := s.putVM(t, netRG, "fromimg", vm)
	require.Error(t, err)
}

func TestComputeSDK_GalleryLatestSkipsExcluded(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	managed := s.generalizedImage(t)
	s.gallery(t)
	require.NoError(
		t,
		s.galleryDef(t, "gal1", "def1", galleryDef(armcompute.OperatingSystemStateTypesGeneralized, "Backend")),
	)

	only := versionFrom(managed)
	only.Properties.PublishingProfile = &armcompute.GalleryImageVersionPublishingProfile{ExcludeFromLatest: new(true)}
	require.NoError(t, s.galleryVersion(t, "def1", "1.0.0", only))

	vm := linuxVM(s.newNIC(t, "nic-new"))
	vm.Properties.StorageProfile.ImageReference = &armcompute.ImageReference{ID: new(
		"/subscriptions/" + aciSub + "/resourceGroups/" + netRG + "/providers/Microsoft.Compute/galleries/gal1/images/def1",
	)}

	_, err := s.putVM(t, netRG, "fromimg", vm)
	require.Error(t, err, "the only version is excluded from latest")
	assert.Contains(t, err.Error(), "There is no latest image version")
}

func TestComputeSDK_GallerySnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newCmpStack(t)
	managed := src.generalizedImage(t)
	src.gallery(t)
	require.NoError(
		t,
		src.galleryDef(t, "gal1", "def1", galleryDef(armcompute.OperatingSystemStateTypesGeneralized, "Backend")),
	)
	require.NoError(t, src.galleryVersion(t, "def1", "1.0.0", versionFrom(managed, "westus")))

	dst := newCmpStack(t)
	require.NoError(t, dst.handler.Restore(t.Context(), src.handler.Snapshot(t.Context())))

	got, err := dst.cf.NewGalleryImageVersionsClient().Get(t.Context(), netRG, "gal1", "def1", "1.0.0", nil)
	require.NoError(t, err)
	assert.Equal(t, "westus", *got.Properties.PublishingProfile.TargetRegions[0].Name)

	img, err := dst.cf.NewImagesClient().Get(t.Context(), netRG, "img1", nil)
	require.NoError(t, err)
	assert.Equal(t, managed, *img.ID)

	pager := dst.cf.NewGalleryImagesClient().NewListByGalleryPager(netRG, "gal1", nil)
	page, err := pager.NextPage(t.Context())
	require.NoError(t, err)
	assert.Len(t, page.Value, 1)
}
