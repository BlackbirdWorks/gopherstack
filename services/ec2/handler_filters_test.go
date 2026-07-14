package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFilters_DescribeVpcs verifies that vpc-id and cidr-block filters work.
func TestFilters_DescribeVpcs(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create a VPC.
	rec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.0.0.0/16")
	require.Equal(t, http.StatusOK, rec.Code)
	vpcID := extractXMLValue(t, rec.Body.String(), "vpcId")
	require.NotEmpty(t, vpcID)

	// Filter by exact vpc-id — should return 1.
	rec = postForm(t, h, "Action=DescribeVpcs&Version=2016-11-15&Filter.1.Name=vpc-id&Filter.1.Value.1="+vpcID)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), vpcID, "filter by vpc-id should return matching VPC")

	// Filter by non-existent vpc-id — should return 0.
	rec = postForm(t, h, "Action=DescribeVpcs&Version=2016-11-15&Filter.1.Name=vpc-id&Filter.1.Value.1=vpc-notexist")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), vpcID, "filter by wrong vpc-id should exclude VPC")

	// Filter by cidr-block — should return 1.
	rec = postForm(t, h, "Action=DescribeVpcs&Version=2016-11-15&Filter.1.Name=cidr-block&Filter.1.Value.1=10.0.0.0/16")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), vpcID, "filter by cidr-block should return matching VPC")
}

// TestFilters_DescribeSubnets verifies vpc-id and availability-zone subnet filters.
func TestFilters_DescribeSubnets(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.1.0.0/16")
	require.Equal(t, http.StatusOK, rec.Code)
	vpcID := extractXMLValue(t, rec.Body.String(), "vpcId")

	body := "Action=CreateSubnet&Version=2016-11-15&VpcId=" + vpcID +
		"&CidrBlock=10.1.1.0/24&AvailabilityZone=us-east-1a"
	rec = postForm(t, h, body)
	require.Equal(t, http.StatusOK, rec.Code)
	subnetID := extractXMLValue(t, rec.Body.String(), "subnetId")
	require.NotEmpty(t, subnetID)

	// Filter by vpc-id.
	rec = postForm(t, h, "Action=DescribeSubnets&Version=2016-11-15&Filter.1.Name=vpc-id&Filter.1.Value.1="+vpcID)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), subnetID)

	// Filter by wrong vpc-id.
	rec = postForm(t, h, "Action=DescribeSubnets&Version=2016-11-15&Filter.1.Name=vpc-id&Filter.1.Value.1=vpc-notexist")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), subnetID)

	// Filter by availability-zone.
	rec = postForm(t, h,
		"Action=DescribeSubnets&Version=2016-11-15&Filter.1.Name=availability-zone&Filter.1.Value.1=us-east-1a")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), subnetID)
}

// TestFilters_DescribeVolumes verifies status and availability-zone volume filters.
func TestFilters_DescribeVolumes(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a&Size=10")
	require.Equal(t, http.StatusOK, rec.Code)
	volID := extractXMLValue(t, rec.Body.String(), "volumeId")
	require.NotEmpty(t, volID)

	// Filter by availability-zone.
	rec = postForm(t, h,
		"Action=DescribeVolumes&Version=2016-11-15&Filter.1.Name=availability-zone&Filter.1.Value.1=us-east-1a")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), volID)

	// Filter by wrong az.
	rec = postForm(t, h,
		"Action=DescribeVolumes&Version=2016-11-15&Filter.1.Name=availability-zone&Filter.1.Value.1=eu-west-1b")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), volID)
}

// TestFilters_DescribeKeyPairs verifies key-name filter.
func TestFilters_DescribeKeyPairs(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=CreateKeyPair&Version=2016-11-15&KeyName=filter-test-key")
	require.Equal(t, http.StatusOK, rec.Code)

	// Filter by key-name.
	rec = postForm(t, h,
		"Action=DescribeKeyPairs&Version=2016-11-15&Filter.1.Name=key-name&Filter.1.Value.1=filter-test-key")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "filter-test-key")

	// Filter by non-existent key-name.
	rec = postForm(t, h,
		"Action=DescribeKeyPairs&Version=2016-11-15&Filter.1.Name=key-name&Filter.1.Value.1=does-not-exist")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "filter-test-key")
}

// TestFilters_DescribeSnapshots verifies volume-id and status snapshot filters.
func TestFilters_DescribeSnapshots(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a&Size=10")
	require.Equal(t, http.StatusOK, rec.Code)
	volID := extractXMLValue(t, rec.Body.String(), "volumeId")

	rec = postForm(t, h, "Action=CreateSnapshot&Version=2016-11-15&VolumeId="+volID)
	require.Equal(t, http.StatusOK, rec.Code)
	snapID := extractXMLValue(t, rec.Body.String(), "snapshotId")
	require.NotEmpty(t, snapID)

	// Filter by volume-id.
	rec = postForm(t, h, "Action=DescribeSnapshots&Version=2016-11-15&Filter.1.Name=volume-id&Filter.1.Value.1="+volID)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), snapID)

	// Filter by wrong volume-id.
	rec = postForm(t, h,
		"Action=DescribeSnapshots&Version=2016-11-15&Filter.1.Name=volume-id&Filter.1.Value.1=vol-notexist")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), snapID)
}

// TestFilters_DescribeInternetGateways verifies attachment.vpc-id filter.
func TestFilters_DescribeInternetGateways(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.2.0.0/16")
	require.Equal(t, http.StatusOK, rec.Code)
	vpcID := extractXMLValue(t, rec.Body.String(), "vpcId")

	rec = postForm(t, h, "Action=CreateInternetGateway&Version=2016-11-15")
	require.Equal(t, http.StatusOK, rec.Code)
	igwID := extractXMLValue(t, rec.Body.String(), "internetGatewayId")
	require.NotEmpty(t, igwID)

	// Before attach — filter by vpc-id should not match.
	rec = postForm(t, h,
		"Action=DescribeInternetGateways&Version=2016-11-15&Filter.1.Name=attachment.vpc-id&Filter.1.Value.1="+vpcID)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), igwID)

	// Attach.
	postForm(t, h, "Action=AttachInternetGateway&Version=2016-11-15&InternetGatewayId="+igwID+"&VpcId="+vpcID)

	// After attach — filter by vpc-id should match.
	rec = postForm(t, h,
		"Action=DescribeInternetGateways&Version=2016-11-15&Filter.1.Name=attachment.vpc-id&Filter.1.Value.1="+vpcID)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), igwID)
}

// TestFilters_DescribeRouteTables verifies vpc-id route table filter.
func TestFilters_DescribeRouteTables(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.3.0.0/16")
	require.Equal(t, http.StatusOK, rec.Code)
	vpcID := extractXMLValue(t, rec.Body.String(), "vpcId")

	rec = postForm(t, h, "Action=CreateRouteTable&Version=2016-11-15&VpcId="+vpcID)
	require.Equal(t, http.StatusOK, rec.Code)
	rtID := extractXMLValue(t, rec.Body.String(), "routeTableId")
	require.NotEmpty(t, rtID)

	// Filter by vpc-id.
	rec = postForm(t, h, "Action=DescribeRouteTables&Version=2016-11-15&Filter.1.Name=vpc-id&Filter.1.Value.1="+vpcID)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), rtID)

	// Filter by wrong vpc-id.
	rec = postForm(t, h,
		"Action=DescribeRouteTables&Version=2016-11-15&Filter.1.Name=vpc-id&Filter.1.Value.1=vpc-notexist")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), rtID)
}

// TestFilters_DescribeNetworkInterfaces verifies vpc-id and subnet-id ENI filters.
func TestFilters_DescribeNetworkInterfaces(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.4.0.0/16")
	require.Equal(t, http.StatusOK, rec.Code)
	vpcID := extractXMLValue(t, rec.Body.String(), "vpcId")

	rec = postForm(t, h, "Action=CreateSubnet&Version=2016-11-15&VpcId="+vpcID+"&CidrBlock=10.4.1.0/24")
	require.Equal(t, http.StatusOK, rec.Code)
	subnetID := extractXMLValue(t, rec.Body.String(), "subnetId")

	rec = postForm(t, h, "Action=CreateNetworkInterface&Version=2016-11-15&SubnetId="+subnetID)
	require.Equal(t, http.StatusOK, rec.Code)
	eniID := extractXMLValue(t, rec.Body.String(), "networkInterfaceId")
	require.NotEmpty(t, eniID)

	// Filter by subnet-id.
	rec = postForm(t, h,
		"Action=DescribeNetworkInterfaces&Version=2016-11-15&Filter.1.Name=subnet-id&Filter.1.Value.1="+subnetID)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), eniID)

	// Filter by wrong subnet-id.
	rec = postForm(t, h,
		"Action=DescribeNetworkInterfaces&Version=2016-11-15&Filter.1.Name=subnet-id&Filter.1.Value.1=subnet-notexist")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), eniID)
}

// TestFilters_DescribeAddresses verifies public-ip EIP filter.
func TestFilters_DescribeAddresses(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=AllocateAddress&Version=2016-11-15&Domain=vpc")
	require.Equal(t, http.StatusOK, rec.Code)
	allocID := extractXMLValue(t, rec.Body.String(), "allocationId")
	publicIP := extractXMLValue(t, rec.Body.String(), "publicIp")
	require.NotEmpty(t, allocID)

	// Filter by allocation-id.
	rec = postForm(t, h,
		"Action=DescribeAddresses&Version=2016-11-15&Filter.1.Name=allocation-id&Filter.1.Value.1="+allocID)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), allocID)

	// Filter by public-ip.
	rec = postForm(t, h,
		"Action=DescribeAddresses&Version=2016-11-15&Filter.1.Name=public-ip&Filter.1.Value.1="+publicIP)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), publicIP)

	// Filter by wrong allocation-id.
	rec = postForm(t, h,
		"Action=DescribeAddresses&Version=2016-11-15&Filter.1.Name=allocation-id&Filter.1.Value.1=eipalloc-notexist")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), allocID)
}

// TestFilters_DescribeImages verifies name and architecture AMI filters.
func TestFilters_DescribeImages(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Seed two AMIs via RegisterImage.
	postForm(t, h, "Action=RegisterImage&Version=2016-11-15&Name=filter-ami&Architecture=x86_64")
	postForm(t, h, "Action=RegisterImage&Version=2016-11-15&Name=other-ami&Architecture=arm64")

	// Unfiltered — both images must be present.
	rec := postForm(t, h, "Action=DescribeImages&Version=2016-11-15")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "filter-ami")
	assert.Contains(t, rec.Body.String(), "other-ami")

	// Filter by name=filter-ami — only filter-ami should appear.
	rec = postForm(t, h, "Action=DescribeImages&Version=2016-11-15&Filter.1.Name=name&Filter.1.Value.1=filter-ami")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "filter-ami")
	assert.NotContains(t, rec.Body.String(), "other-ami")

	// Filter by architecture=arm64 — only other-ami should appear.
	rec = postForm(t, h, "Action=DescribeImages&Version=2016-11-15&Filter.1.Name=architecture&Filter.1.Value.1=arm64")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "other-ami")
	assert.NotContains(t, rec.Body.String(), "filter-ami")

	// Filter by non-existent name — empty imagesSet.
	rec = postForm(t, h, "Action=DescribeImages&Version=2016-11-15&Filter.1.Name=name&Filter.1.Value.1=does-not-exist")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "filter-ami")
	assert.NotContains(t, rec.Body.String(), "other-ami")
}

// TestFilters_DescribeTags_KeyAndValue verifies that key and value filters work.
func TestFilters_DescribeTags_KeyAndValue(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create a VPC and tag it.
	rec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.5.0.0/16")
	require.Equal(t, http.StatusOK, rec.Code)
	vpcID := extractXMLValue(t, rec.Body.String(), "vpcId")

	postForm(t, h, "Action=CreateTags&Version=2016-11-15&ResourceId.1="+vpcID+"&Tag.1.Key=Env&Tag.1.Value=prod")
	postForm(t, h, "Action=CreateTags&Version=2016-11-15&ResourceId.1="+vpcID+"&Tag.1.Key=Team&Tag.1.Value=infra")

	// Filter by key=Env.
	rec = postForm(t, h, "Action=DescribeTags&Version=2016-11-15&Filter.1.Name=key&Filter.1.Value.1=Env")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Env")
	assert.NotContains(t, rec.Body.String(), "Team") // Team tag should not appear

	// Filter by value=prod.
	rec = postForm(t, h, "Action=DescribeTags&Version=2016-11-15&Filter.1.Name=value&Filter.1.Value.1=prod")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "prod")

	// Filter by resource-type=vpc.
	rec = postForm(t, h, "Action=DescribeTags&Version=2016-11-15&Filter.1.Name=resource-type&Filter.1.Value.1=vpc")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), vpcID)
}

// TestFilters_DescribeVpcAttribute_PersistsValue verifies ModifyVpcAttribute is
// read back by DescribeVpcAttribute.
func TestFilters_DescribeVpcAttribute_PersistsValue(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.6.0.0/16")
	require.Equal(t, http.StatusOK, rec.Code)
	vpcID := extractXMLValue(t, rec.Body.String(), "vpcId")

	// Default: enableDnsSupport should be true.
	rec = postForm(t, h, "Action=DescribeVpcAttribute&Version=2016-11-15&VpcId="+vpcID+"&Attribute=enableDnsSupport")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "true", "enableDnsSupport default should be true")

	// Default: enableDnsHostnames should be false.
	rec = postForm(t, h, "Action=DescribeVpcAttribute&Version=2016-11-15&VpcId="+vpcID+"&Attribute=enableDnsHostnames")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "false", "enableDnsHostnames default should be false")

	// Enable DNS hostnames.
	postForm(t, h, "Action=ModifyVpcAttribute&Version=2016-11-15&VpcId="+vpcID+"&EnableDnsHostnames.Value=true")

	// Now DescribeVpcAttribute should return true.
	rec = postForm(t, h, "Action=DescribeVpcAttribute&Version=2016-11-15&VpcId="+vpcID+"&Attribute=enableDnsHostnames")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "true", "enableDnsHostnames should be true after ModifyVpcAttribute")

	// Disable DNS support.
	postForm(t, h, "Action=ModifyVpcAttribute&Version=2016-11-15&VpcId="+vpcID+"&EnableDnsSupport.Value=false")

	rec = postForm(t, h, "Action=DescribeVpcAttribute&Version=2016-11-15&VpcId="+vpcID+"&Attribute=enableDnsSupport")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "false", "enableDnsSupport should be false after disable")
}

func TestParseFilters_Empty(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	_, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	vals := url.Values{
		"Action":  {"DescribeInstances"},
		"Version": {"2016-11-15"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "DescribeInstancesResponse")
}
