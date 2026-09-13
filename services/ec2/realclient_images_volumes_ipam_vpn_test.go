package ec2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestRealClient_ImagesVolumesIpamVpnFields covers the gopherstack-xhu2t/99nj
// ec2query filter/field sweep's 2026-09-13 continuation: Images (Owner.N/
// IncludeDeprecated on DescribeImages, ResourceType.N/IncludeAllResourceTypes
// on DescribeImageReferences, RegisterImage's ImdsSupport/VirtualizationType),
// Volumes (AttachVolume.EbsCardIndex, ModifyVolume.Throughput,
// CreateVolume.VolumeInitializationRate), and IPAM core fields
// (EnablePrivateGua/MeteredAccount, CreateIpamPool.PublicIpSource,
// ModifyIpamPool.ClearAllocationDefaultNetmaskLength, DeleteIpam.Cascade,
// GetIpamPrefixListResolverVersions' version filter). Each row gets its own
// fresh handler+backend so rows can run in parallel without shared state.
func TestRealClient_ImagesVolumesIpamVpnFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *ec2sdk.Client)
		name string
	}{
		{runImagesOwnerDeprecatedReferences, "images_owner_deprecated_references"},
		{runImagesRegisterMetadata, "images_register_metadata"},
		{runVolumesFields, "volumes_fields"},
		{runIpamCoreFields, "ipam_core_fields"},
		{runIpamDeleteCascade, "ipam_delete_cascade"},
		{runIpamPrefixListResolverVersionFilter, "ipam_prefix_list_resolver_version_filter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
			h.AccountID = "000000000000"
			client := newTestEC2Client(t, h)
			tt.run(t, client)
		})
	}
}

// runImagesOwnerDeprecatedReferences covers DescribeImages' Owner.N/
// IncludeDeprecated filters and DescribeImageReferences' ResourceType.N/
// IncludeAllResourceTypes filters.
func runImagesOwnerDeprecatedReferences(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	regOut, err := client.RegisterImage(t.Context(), &ec2sdk.RegisterImageInput{
		Name: aws.String("owner-test-image"),
	})
	require.NoError(t, err)
	imageID := aws.ToString(regOut.ImageId)

	// Owner.N: a self-owned image must not match Owner=amazon, but must
	// match Owner=self (resolved to the caller's own account ID).
	amazonOwned, err := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{
		ImageIds: []string{imageID}, Owners: []string{"amazon"},
	})
	require.NoError(t, err)
	assert.Empty(t, amazonOwned.Images, "self-owned image must not match Owner=amazon")

	selfOwned, err := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{
		ImageIds: []string{imageID}, Owners: []string{"self"},
	})
	require.NoError(t, err)
	require.Len(t, selfOwned.Images, 1)
	assert.Equal(t, "000000000000", aws.ToString(selfOwned.Images[0].OwnerId))

	// IncludeDeprecated: default excludes a deprecated image; explicit
	// ImageId or IncludeDeprecated=true still show it.
	_, err = client.EnableImageDeprecation(t.Context(), &ec2sdk.EnableImageDeprecationInput{
		ImageId: aws.String(imageID), DeprecateAt: aws.Time(time.Now().Add(24 * time.Hour)),
	})
	require.NoError(t, err)

	defaultDesc, err := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{})
	require.NoError(t, err)
	for _, img := range defaultDesc.Images {
		assert.NotEqual(t, imageID, aws.ToString(img.ImageId), "deprecated image should be hidden by default")
	}

	explicitDesc, err := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{ImageIds: []string{imageID}})
	require.NoError(t, err)
	require.Len(t, explicitDesc.Images, 1, "explicit ImageId should still show a deprecated image")
	assert.NotEmpty(t, aws.ToString(explicitDesc.Images[0].DeprecationTime))

	includeDesc, err := client.DescribeImages(
		t.Context(), &ec2sdk.DescribeImagesInput{IncludeDeprecated: aws.Bool(true)},
	)
	require.NoError(t, err)
	found := false
	for _, img := range includeDesc.Images {
		if aws.ToString(img.ImageId) == imageID {
			found = true
		}
	}
	assert.True(t, found, "IncludeDeprecated=true should show the deprecated image")

	// DescribeImageReferences: ResourceType.N restricts to the requested
	// type; IncludeAllResourceTypes=true (or omitting both) returns all.
	runOut, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId: aws.String(imageID), InstanceType: types.InstanceTypeT3Micro,
		MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Instances, 1)

	_, err = client.CreateLaunchTemplate(t.Context(), &ec2sdk.CreateLaunchTemplateInput{
		LaunchTemplateName: aws.String("lt-" + imageID),
		LaunchTemplateData: &types.RequestLaunchTemplateData{ImageId: aws.String(imageID)},
	})
	require.NoError(t, err)

	allRefs, err := client.DescribeImageReferences(t.Context(), &ec2sdk.DescribeImageReferencesInput{
		ImageIds: []string{imageID}, IncludeAllResourceTypes: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Len(t, allRefs.ImageReferences, 2, "expected both instance and launch-template references")

	instanceOnly, err := client.DescribeImageReferences(t.Context(), &ec2sdk.DescribeImageReferencesInput{
		ImageIds:      []string{imageID},
		ResourceTypes: []types.ResourceTypeRequest{{ResourceType: types.ImageReferenceResourceTypeEc2Instance}},
	})
	require.NoError(t, err)
	require.Len(t, instanceOnly.ImageReferences, 1)
	assert.Equal(t, types.ImageReferenceResourceTypeEc2Instance, instanceOnly.ImageReferences[0].ResourceType)
}

// runImagesRegisterMetadata covers RegisterImage's ImdsSupport and
// VirtualizationType (default "paravirtual" when omitted), both echoed on
// DescribeImages.
func runImagesRegisterMetadata(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	regOut, err := client.RegisterImage(t.Context(), &ec2sdk.RegisterImageInput{
		Name:               aws.String("metadata-test-image"),
		ImdsSupport:        types.ImdsSupportValuesV20,
		VirtualizationType: aws.String("hvm"),
	})
	require.NoError(t, err)
	imageID := aws.ToString(regOut.ImageId)

	desc, err := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{ImageIds: []string{imageID}})
	require.NoError(t, err)
	require.Len(t, desc.Images, 1)
	assert.Equal(t, types.ImdsSupportValuesV20, desc.Images[0].ImdsSupport)
	assert.Equal(t, types.VirtualizationTypeHvm, desc.Images[0].VirtualizationType)

	regOut2, err := client.RegisterImage(t.Context(), &ec2sdk.RegisterImageInput{
		Name: aws.String("default-virt-image"),
	})
	require.NoError(t, err)

	desc2, err := client.DescribeImages(
		t.Context(), &ec2sdk.DescribeImagesInput{ImageIds: []string{aws.ToString(regOut2.ImageId)}},
	)
	require.NoError(t, err)
	require.Len(t, desc2.Images, 1)
	assert.Equal(t, types.VirtualizationTypeParavirtual, desc2.Images[0].VirtualizationType)
}

// runVolumesFields covers AttachVolume.EbsCardIndex, ModifyVolume.Throughput,
// and CreateVolume.VolumeInitializationRate.
func runVolumesFields(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	runOut, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId: aws.String("ami-parity-test"), InstanceType: types.InstanceTypeT3Micro,
		MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)
	instanceID := aws.ToString(runOut.Instances[0].InstanceId)
	az := aws.ToString(runOut.Instances[0].Placement.AvailabilityZone)

	volOut, err := client.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String(az), Size: aws.Int32(100), VolumeInitializationRate: aws.Int32(200),
	})
	require.NoError(t, err)
	volumeID := aws.ToString(volOut.VolumeId)
	assert.Equal(t, int32(200), aws.ToInt32(volOut.VolumeInitializationRate))

	descVol, err := client.DescribeVolumes(t.Context(), &ec2sdk.DescribeVolumesInput{VolumeIds: []string{volumeID}})
	require.NoError(t, err)
	require.Len(t, descVol.Volumes, 1)
	assert.Equal(t, int32(200), aws.ToInt32(descVol.Volumes[0].VolumeInitializationRate))

	attOut, err := client.AttachVolume(t.Context(), &ec2sdk.AttachVolumeInput{
		VolumeId: aws.String(volumeID), InstanceId: aws.String(instanceID), Device: aws.String("/dev/sdf"),
		EbsCardIndex: aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), aws.ToInt32(attOut.EbsCardIndex))

	modOut, err := client.ModifyVolume(t.Context(), &ec2sdk.ModifyVolumeInput{
		VolumeId: aws.String(volumeID), Throughput: aws.Int32(500),
	})
	require.NoError(t, err)
	require.NotNil(t, modOut.VolumeModification)
	assert.Equal(t, int32(500), aws.ToInt32(modOut.VolumeModification.TargetThroughput))
}

// runIpamCoreFields covers CreateIpam/ModifyIpam's EnablePrivateGua/
// MeteredAccount, CreateIpamPool.PublicIpSource, and
// ModifyIpamPool.ClearAllocationDefaultNetmaskLength.
func runIpamCoreFields(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	createOut, err := client.CreateIpam(t.Context(), &ec2sdk.CreateIpamInput{
		EnablePrivateGua: aws.Bool(true),
		MeteredAccount:   types.IpamMeteredAccountResourceOwner,
	})
	require.NoError(t, err)
	ipamID := aws.ToString(createOut.Ipam.IpamId)
	assert.True(t, aws.ToBool(createOut.Ipam.EnablePrivateGua))
	assert.Equal(t, types.IpamMeteredAccountResourceOwner, createOut.Ipam.MeteredAccount)

	modOut, err := client.ModifyIpam(t.Context(), &ec2sdk.ModifyIpamInput{
		IpamId: aws.String(ipamID), MeteredAccount: types.IpamMeteredAccountIpamOwner,
	})
	require.NoError(t, err)
	assert.Equal(t, types.IpamMeteredAccountIpamOwner, modOut.Ipam.MeteredAccount)

	scopeID := aws.ToString(createOut.Ipam.PublicDefaultScopeId)

	poolOut, err := client.CreateIpamPool(t.Context(), &ec2sdk.CreateIpamPoolInput{
		IpamScopeId: aws.String(scopeID), AddressFamily: types.AddressFamilyIpv6,
		PublicIpSource: types.IpamPoolPublicIpSourceAmazon,
	})
	require.NoError(t, err)
	poolID := aws.ToString(poolOut.IpamPool.IpamPoolId)
	assert.Equal(t, types.IpamPoolPublicIpSourceAmazon, poolOut.IpamPool.PublicIpSource)

	modPoolOut, err := client.ModifyIpamPool(t.Context(), &ec2sdk.ModifyIpamPoolInput{
		IpamPoolId: aws.String(poolID), AllocationDefaultNetmaskLength: aws.Int32(24),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(24), aws.ToInt32(modPoolOut.IpamPool.AllocationDefaultNetmaskLength))

	clearOut, err := client.ModifyIpamPool(t.Context(), &ec2sdk.ModifyIpamPoolInput{
		IpamPoolId: aws.String(poolID), ClearAllocationDefaultNetmaskLength: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Zero(t, aws.ToInt32(clearOut.IpamPool.AllocationDefaultNetmaskLength))
}

// runIpamDeleteCascade covers DeleteIpam.Cascade: without it, deleting an
// IPAM that still has a pool must fail; with it, the delete succeeds.
func runIpamDeleteCascade(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	createOut, err := client.CreateIpam(t.Context(), &ec2sdk.CreateIpamInput{})
	require.NoError(t, err)
	ipamID := aws.ToString(createOut.Ipam.IpamId)
	scopeID := aws.ToString(createOut.Ipam.PrivateDefaultScopeId)

	_, err = client.CreateIpamPool(t.Context(), &ec2sdk.CreateIpamPoolInput{
		IpamScopeId: aws.String(scopeID), AddressFamily: types.AddressFamilyIpv4,
	})
	require.NoError(t, err)

	_, err = client.DeleteIpam(t.Context(), &ec2sdk.DeleteIpamInput{IpamId: aws.String(ipamID)})
	require.Error(t, err, "DeleteIpam without Cascade must fail while the IPAM still has a pool")

	_, err = client.DeleteIpam(t.Context(), &ec2sdk.DeleteIpamInput{
		IpamId: aws.String(ipamID), Cascade: aws.Bool(true),
	})
	require.NoError(t, err, "DeleteIpam with Cascade must succeed")
}

// runIpamPrefixListResolverVersionFilter covers
// GetIpamPrefixListResolverVersions' IpamPrefixListResolverVersion.N filter.
func runIpamPrefixListResolverVersionFilter(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	ipamOut, err := client.CreateIpam(t.Context(), &ec2sdk.CreateIpamInput{})
	require.NoError(t, err)
	ipamID := aws.ToString(ipamOut.Ipam.IpamId)

	resolverOut, err := client.CreateIpamPrefixListResolver(t.Context(), &ec2sdk.CreateIpamPrefixListResolverInput{
		IpamId: aws.String(ipamID), AddressFamily: types.AddressFamilyIpv4,
	})
	require.NoError(t, err)
	resolverID := aws.ToString(resolverOut.IpamPrefixListResolver.IpamPrefixListResolverId)

	_, err = client.ModifyIpamPrefixListResolver(t.Context(), &ec2sdk.ModifyIpamPrefixListResolverInput{
		IpamPrefixListResolverId: aws.String(resolverID),
		Rules: []types.IpamPrefixListResolverRuleRequest{
			{RuleType: types.IpamPrefixListResolverRuleTypeStaticCidr, StaticCidr: aws.String("10.0.0.0/24")},
		},
	})
	require.NoError(t, err)

	allVersions, err := client.GetIpamPrefixListResolverVersions(
		t.Context(), &ec2sdk.GetIpamPrefixListResolverVersionsInput{
			IpamPrefixListResolverId: aws.String(resolverID),
		},
	)
	require.NoError(t, err)
	require.Len(t, allVersions.IpamPrefixListResolverVersions, 2, "expected both v1 and v2")

	filtered, err := client.GetIpamPrefixListResolverVersions(
		t.Context(), &ec2sdk.GetIpamPrefixListResolverVersionsInput{
			IpamPrefixListResolverId:       aws.String(resolverID),
			IpamPrefixListResolverVersions: []int64{2},
		},
	)
	require.NoError(t, err)
	require.Len(t, filtered.IpamPrefixListResolverVersions, 1)
	assert.Equal(t, int64(2), aws.ToInt64(filtered.IpamPrefixListResolverVersions[0].Version))
}
