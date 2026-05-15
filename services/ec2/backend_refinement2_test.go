package ec2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2 "github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestRefinement2_CreateSnapshot verifies snapshot creation from a volume.
func TestRefinement2_CreateSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		volumeID    string
		setupVolume bool
		wantErr     bool
	}{
		{
			name:        "valid_snapshot",
			setupVolume: true,
			wantErr:     false,
		},
		{
			name:     "missing_volume_id",
			volumeID: "",
			wantErr:  true,
		},
		{
			name:     "non_existent_volume",
			volumeID: "vol-nonexistent",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			volumeID := tt.volumeID

			if tt.setupVolume {
				vol, err := b.CreateVolume("us-east-1a", "gp2", 20)
				require.NoError(t, err)
				volumeID = vol.ID
			}

			snap, err := b.CreateSnapshot(volumeID, "test snapshot")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, snap.SnapshotID)
			assert.Equal(t, volumeID, snap.VolumeID)
			assert.Equal(t, "completed", snap.State)
			assert.Equal(t, "100%", snap.Progress)
		})
	}
}

// TestRefinement2_DescribeSnapshots verifies snapshot listing and filtering.
func TestRefinement2_DescribeSnapshots(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vol, err := b.CreateVolume("us-east-1a", "gp2", 20)
	require.NoError(t, err)

	snap1, err := b.CreateSnapshot(vol.ID, "snap 1")
	require.NoError(t, err)

	_, err = b.CreateSnapshot(vol.ID, "snap 2")
	require.NoError(t, err)

	// all
	all := b.DescribeSnapshots(nil)
	assert.Len(t, all, 2)

	// filtered
	filtered := b.DescribeSnapshots([]string{snap1.SnapshotID})
	require.Len(t, filtered, 1)
	assert.Equal(t, snap1.SnapshotID, filtered[0].SnapshotID)
}

// TestRefinement2_DeleteSnapshot verifies snapshot deletion.
func TestRefinement2_DeleteSnapshot(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vol, err := b.CreateVolume("us-east-1a", "gp2", 20)
	require.NoError(t, err)

	snap, err := b.CreateSnapshot(vol.ID, "to be deleted")
	require.NoError(t, err)

	require.NoError(t, b.DeleteSnapshot(snap.SnapshotID))

	all := b.DescribeSnapshots(nil)
	assert.Empty(t, all)

	// delete again -> error
	assert.Error(t, b.DeleteSnapshot(snap.SnapshotID))
}

// TestRefinement2_SnapshotPersistence verifies Snapshot/Restore round-trip.
func TestRefinement2_SnapshotPersistence(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vol, err := b.CreateVolume("us-east-1a", "gp2", 20)
	require.NoError(t, err)

	_, err = b.CreateSnapshot(vol.ID, "persisted")
	require.NoError(t, err)

	data := b.Snapshot()
	require.NotNil(t, data)

	b2 := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(data))

	snaps := b2.DescribeSnapshots(nil)
	assert.Len(t, snaps, 1)
	assert.Equal(t, "persisted", snaps[0].Description)
}

// TestRefinement2_CopyImage verifies image copying.
func TestRefinement2_CopyImage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		sourceImageID string
		newName       string
		wantErr       bool
	}{
		{
			name:          "copy_stub_ami",
			sourceImageID: "ami-0c55b159cbfafe1f0",
			newName:       "my-copy",
			wantErr:       false,
		},
		{
			name:          "missing_source",
			sourceImageID: "",
			wantErr:       true,
		},
		{
			name:          "nonexistent_source",
			sourceImageID: "ami-nonexistent",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			copied, err := b.CopyImage(tt.sourceImageID, tt.newName, "")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEqual(t, tt.sourceImageID, copied.ImageID)
			assert.Equal(t, tt.newName, copied.Name)
		})
	}
}

// TestRefinement2_DeregisterImage verifies AMI deregistration.
func TestRefinement2_DeregisterImage(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.RunInstances("ami-0c55b159cbfafe1f0", "t3.micro", "", 1)
	require.NoError(t, err)

	img, err := b.CreateImage("", "test", "")
	require.Error(t, err, "should fail on empty instance ID")
	assert.Nil(t, img)

	// Create image from a real instance
	insts := b.DescribeInstances(nil, "")
	require.NotEmpty(t, insts)

	created, err := b.CreateImage(insts[0].ID, "to-deregister", "desc")
	require.NoError(t, err)

	require.NoError(t, b.DeregisterImage(created.ImageID))

	// verify gone
	assert.Error(t, b.DeregisterImage(created.ImageID))
}

// TestRefinement2_ModifyVpcAttribute verifies VPC attribute modification.
func TestRefinement2_ModifyVpcAttribute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		attribute string
		wantErr   bool
	}{
		{name: "dns_support", attribute: "enableDnsSupport", wantErr: false},
		{name: "dns_hostnames", attribute: "enableDnsHostnames", wantErr: false},
		{name: "unknown_attribute", attribute: "unknownAttr", wantErr: true},
		{name: "missing_vpc", attribute: "enableDnsSupport", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			vpcID := "vpc-default"
			if tt.name == "missing_vpc" {
				vpcID = "vpc-nonexistent"
			}

			err := b.ModifyVpcAttribute(vpcID, tt.attribute, true)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRefinement2_ModifySubnetAttribute verifies subnet attribute modification.
func TestRefinement2_ModifySubnetAttribute(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	require.NoError(t, b.ModifySubnetAttribute("subnet-default", "mapPublicIpOnLaunch", true))
	require.Error(t, b.ModifySubnetAttribute("subnet-nonexistent", "mapPublicIpOnLaunch", true))
	require.Error(t, b.ModifySubnetAttribute("subnet-default", "unknownAttr", true))
}

// TestRefinement2_CreateNetworkAcl verifies NACL creation.
func TestRefinement2_CreateNetworkAcl(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	acl, err := b.CreateNetworkACL("vpc-default")
	require.NoError(t, err)
	assert.NotEmpty(t, acl.ID)
	assert.Equal(t, "vpc-default", acl.VPCID)
	assert.False(t, acl.IsDefault)
	assert.NotEmpty(t, acl.Entries, "should have default deny-all entries")

	_, err = b.CreateNetworkACL("vpc-nonexistent")
	assert.Error(t, err)
}

// TestRefinement2_DeleteNetworkAcl verifies NACL deletion.
func TestRefinement2_DeleteNetworkAcl(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	acl, err := b.CreateNetworkACL("vpc-default")
	require.NoError(t, err)

	require.NoError(t, b.DeleteNetworkACL(acl.ID))
	assert.Error(t, b.DeleteNetworkACL(acl.ID))
}

// TestRefinement2_CreateNetworkAclEntry verifies NACL rule creation.
func TestRefinement2_CreateNetworkAclEntry(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	acl, err := b.CreateNetworkACL("vpc-default")
	require.NoError(t, err)

	require.NoError(
		t,
		b.CreateNetworkACLEntry(acl.ID, 100, "6", "allow", "0.0.0.0/0", false, 80, 80),
	)

	// duplicate should fail
	assert.Error(t, b.CreateNetworkACLEntry(acl.ID, 100, "6", "allow", "0.0.0.0/0", false, 80, 80))
}

// TestRefinement2_DeleteNetworkAclEntry verifies NACL rule deletion.
func TestRefinement2_DeleteNetworkAclEntry(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	acl, err := b.CreateNetworkACL("vpc-default")
	require.NoError(t, err)

	require.NoError(
		t,
		b.CreateNetworkACLEntry(acl.ID, 200, "6", "allow", "0.0.0.0/0", false, 443, 443),
	)
	require.NoError(t, b.DeleteNetworkACLEntry(acl.ID, 200, false))

	// non-existent should fail
	assert.Error(t, b.DeleteNetworkACLEntry(acl.ID, 200, false))
}

// TestRefinement2_NetworkAclPersistence verifies NACL Snapshot/Restore round-trip.
func TestRefinement2_NetworkAclPersistence(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	acl, err := b.CreateNetworkACL("vpc-default")
	require.NoError(t, err)
	require.NoError(
		t,
		b.CreateNetworkACLEntry(acl.ID, 100, "6", "allow", "0.0.0.0/0", false, 80, 80),
	)

	data := b.Snapshot()
	require.NotNil(t, data)

	b2 := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(data))

	stored := b2.DescribeStoredNetworkAcls(nil)
	require.Len(t, stored, 1)
	// default deny-all (2) + our rule (1) = 3 entries
	assert.Len(t, stored[0].Entries, 3)
}

// TestRefinement2_DescribeSecurityGroupRules verifies SG rule listing.
func TestRefinement2_DescribeSecurityGroupRules(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	sg, err := b.CreateSecurityGroup("test-sg", "testing", "vpc-default")
	require.NoError(t, err)

	require.NoError(t, b.AuthorizeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{
		{Protocol: "tcp", IPRange: "0.0.0.0/0", FromPort: 80, ToPort: 80},
	}))

	rules, err := b.DescribeSecurityGroupRules(sg.ID)
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.False(t, rules[0].IsEgress)
	assert.Equal(t, "tcp", rules[0].Protocol)
}

// TestRefinement2_ModifySecurityGroupRules verifies SG rule replacement.
func TestRefinement2_ModifySecurityGroupRules(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	sg, err := b.CreateSecurityGroup("test-sg", "testing", "vpc-default")
	require.NoError(t, err)

	require.NoError(t, b.AuthorizeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{
		{Protocol: "tcp", IPRange: "0.0.0.0/0", FromPort: 80, ToPort: 80},
	}))

	// replace with port 443
	require.NoError(t, b.ModifySecurityGroupRules(sg.ID, []ec2.SecurityGroupRule{
		{Protocol: "tcp", IPRange: "0.0.0.0/0", FromPort: 443, ToPort: 443},
	}, false))

	rules, err := b.DescribeSecurityGroupRules(sg.ID)
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, 443, rules[0].FromPort)
}

// TestRefinement2_DeleteLaunchTemplate verifies launch template deletion.
func TestRefinement2_DeleteLaunchTemplate(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	lt, err := b.CreateLaunchTemplate("my-template", "ami-0c55b159cbfafe1f0", "t3.micro")
	require.NoError(t, err)

	require.NoError(t, b.DeleteLaunchTemplate(lt.ID))

	versions, err := b.DescribeLaunchTemplateVersions(lt.ID)
	require.Error(t, err)
	assert.Nil(t, versions)
}

// TestRefinement2_DescribeLaunchTemplateVersions verifies version listing.
func TestRefinement2_DescribeLaunchTemplateVersions(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	lt, err := b.CreateLaunchTemplate("versioned", "ami-0c55b159cbfafe1f0", "t3.small")
	require.NoError(t, err)

	versions, err := b.DescribeLaunchTemplateVersions(lt.ID)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	assert.Equal(t, lt.ID, versions[0].ID)
	assert.Equal(t, int64(1), versions[0].DefaultVersionNumber)
}

// TestRefinement2_DeleteVpcEndpoints verifies VPC endpoint deletion.
func TestRefinement2_DeleteVpcEndpoints(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	ep, err := b.CreateVpcEndpoint("vpc-default", "com.amazonaws.us-east-1.s3", "Gateway", nil)
	require.NoError(t, err)

	unsuccessful, err := b.DeleteVpcEndpoints([]string{ep.ID})
	require.NoError(t, err)
	assert.Empty(t, unsuccessful)

	remaining := b.DescribeVpcEndpoints(nil)
	assert.Empty(t, remaining)
}

// TestRefinement2_DeleteVpcEndpoints_NotFound verifies error on missing endpoint.
func TestRefinement2_DeleteVpcEndpoints_NotFound(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.DeleteVpcEndpoints([]string{"vpce-nonexistent"})
	assert.Error(t, err)
}

// TestRefinement2_DeleteVpcEndpoints_EmptyList verifies error on empty list.
func TestRefinement2_DeleteVpcEndpoints_EmptyList(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.DeleteVpcEndpoints(nil)
	assert.Error(t, err)
}

// TestRefinement2_Reset_ClearsNewMaps verifies Reset clears snapshots and networkACLs.
func TestRefinement2_Reset_ClearsNewMaps(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vol, err := b.CreateVolume("us-east-1a", "gp2", 20)
	require.NoError(t, err)

	_, err = b.CreateSnapshot(vol.ID, "test")
	require.NoError(t, err)

	_, err = b.CreateNetworkACL("vpc-default")
	require.NoError(t, err)

	b.Reset()

	assert.Empty(t, b.DescribeSnapshots(nil))
	assert.Empty(t, b.DescribeStoredNetworkAcls(nil))
}

// TestRefinement2_HTTP_CreateSnapshot verifies the HTTP handler for CreateSnapshot.
func TestRefinement2_HTTP_CreateSnapshot(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create volume first
	rec := postForm(
		t,
		h,
		"Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a&Size=20&VolumeType=gp2",
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Extract volume ID from response
	require.Contains(t, rec.Body.String(), "vol-")

	// Get the volume ID
	b, ok := h.Backend.(*ec2.InMemoryBackend)
	require.True(t, ok)
	vols := b.DescribeVolumes(nil)
	require.NotEmpty(t, vols)

	rec = postForm(
		t,
		h,
		"Action=CreateSnapshot&Version=2016-11-15&VolumeId="+vols[0].ID+"&Description=test",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "snap-")
	assert.Contains(t, rec.Body.String(), "completed")
}

// TestRefinement2_HTTP_DescribeSnapshots verifies the HTTP handler for DescribeSnapshots.
func TestRefinement2_HTTP_DescribeSnapshots(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=DescribeSnapshots&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeSnapshotsResponse")
}

// TestRefinement2_HTTP_CopyImage verifies the HTTP handler for CopyImage.
func TestRefinement2_HTTP_CopyImage(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(
		t,
		h,
		"Action=CopyImage&Version=2016-11-15&SourceImageId=ami-0c55b159cbfafe1f0&Name=my-copy",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ami-")
}

// TestRefinement2_HTTP_ModifyVpcAttribute verifies the HTTP handler for ModifyVpcAttribute.
func TestRefinement2_HTTP_ModifyVpcAttribute(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(
		t,
		h,
		"Action=ModifyVpcAttribute&Version=2016-11-15&VpcId=vpc-default&EnableDnsSupport.Value=true",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "true")
}

// TestRefinement2_HTTP_CreateNetworkAcl verifies the HTTP handler for CreateNetworkAcl.
func TestRefinement2_HTTP_CreateNetworkAcl(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=CreateNetworkAcl&Version=2016-11-15&VpcId=vpc-default")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "acl-")
}

// TestRefinement2_HTTP_DeleteLaunchTemplate verifies the HTTP handler for DeleteLaunchTemplate.
func TestRefinement2_HTTP_DeleteLaunchTemplate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create one first
	rec := postForm(t, h, "Action=CreateLaunchTemplate&Version=2016-11-15"+
		"&LaunchTemplateName=test&LaunchTemplateData.ImageId=ami-0c55b159cbfafe1f0")
	require.Equal(t, http.StatusOK, rec.Code)

	b, ok := h.Backend.(*ec2.InMemoryBackend)
	require.True(t, ok)
	lts := b.DescribeLaunchTemplates(nil)
	require.Len(t, lts, 1)

	rec = postForm(
		t,
		h,
		"Action=DeleteLaunchTemplate&Version=2016-11-15&LaunchTemplateId="+lts[0].ID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement2_HTTP_DescribeSecurityGroupRules verifies the HTTP handler.
func TestRefinement2_HTTP_DescribeSecurityGroupRules(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// use the default sg
	rec := postForm(
		t,
		h,
		"Action=DescribeSecurityGroupRules&Version=2016-11-15&Filter.1.Value=sg-default",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeSecurityGroupRulesResponse")
}

// TestRefinement2_HTTP_DeleteVpcEndpoints verifies the HTTP handler for DeleteVpcEndpoints.
func TestRefinement2_HTTP_DeleteVpcEndpoints(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create endpoint first
	rec := postForm(t, h, "Action=CreateVpcEndpoint&Version=2016-11-15"+
		"&VpcId=vpc-default&ServiceName=com.amazonaws.us-east-1.s3&VpcEndpointType=Gateway")
	require.Equal(t, http.StatusOK, rec.Code)

	b, ok := h.Backend.(*ec2.InMemoryBackend)
	require.True(t, ok)
	eps := b.DescribeVpcEndpoints(nil)
	require.Len(t, eps, 1)

	rec = postForm(t, h, "Action=DeleteVpcEndpoints&Version=2016-11-15&VpcEndpointId.1="+eps[0].ID)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement2_GetSupportedOperations_NewOps verifies new ops appear in the list.
func TestRefinement2_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	ops := h.GetSupportedOperations()
	opSet := make(map[string]bool, len(ops))
	for _, op := range ops {
		opSet[op] = true
	}

	for _, expected := range []string{
		"CreateSnapshot", "DescribeSnapshots", "DeleteSnapshot",
		"CopyImage", "DeregisterImage",
		"ModifyVpcAttribute", "ModifySubnetAttribute",
		"CreateNetworkAcl", "DeleteNetworkAcl",
		"CreateNetworkAclEntry", "DeleteNetworkAclEntry",
		"DescribeSecurityGroupRules", "ModifySecurityGroupRules",
		"DeleteLaunchTemplate", "DescribeLaunchTemplateVersions",
		"DeleteVpcEndpoints",
	} {
		assert.True(t, opSet[expected], "expected op %q to be in supported operations", expected)
	}
}

// TestRefinement2_DescribeSubnetsByVPC verifies indexed VPC-based subnet lookup.
func TestRefinement2_DescribeSubnetsByVPC(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	subs := b.DescribeSubnetsByVPC("vpc-default")
	require.NotEmpty(t, subs)

	for _, s := range subs {
		assert.Equal(t, "vpc-default", s.VPCID)
	}
}

// TestRefinement2_DescribeInstancesByVPC verifies secondary-index instance lookup.
func TestRefinement2_DescribeInstancesByVPC(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.RunInstances("ami-0c55b159cbfafe1f0", "t3.micro", "", 3)
	require.NoError(t, err)

	insts := b.DescribeInstancesByVPC("vpc-default")
	assert.Len(t, insts, 3)
}

// TestRefinement2_DescribeInstancesByVPC_EmptyOnNoInstances verifies no panics on empty VPC.
func TestRefinement2_DescribeInstancesByVPC_EmptyOnNoInstances(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	insts := b.DescribeInstancesByVPC("vpc-nonexistent")
	assert.Nil(t, insts)
}
