package ec2_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// newTestEC2Client stands up the real aws-sdk-go-v2 EC2 client (ec2@v1.320.0)
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestEC2Client(t *testing.T, h *ec2.Handler) *ec2sdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return ec2sdk.NewFromConfig(cfg, func(o *ec2sdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateNetworkInsightsPath_RealWireKeys drives CreateNetworkInsightsPath
// through the real SDK client. The request's required fields serialize as
// "Source"/"Destination" (serializers.go:71549, api_op_CreateNetworkInsightsPath.go
// Source/Destination), but the handler read "SourceId"/"DestinationId" -- names
// that don't exist on the wire at all, so a real client's Source was always
// empty and the backend's own required-field check rejected every real call.
func TestCreateNetworkInsightsPath_RealWireKeys(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	out, err := client.CreateNetworkInsightsPath(t.Context(), &ec2sdk.CreateNetworkInsightsPathInput{
		Source:      aws.String("eni-0123456789abcdef0"),
		Destination: aws.String("eni-0fedcba9876543210"),
		Protocol:    types.ProtocolTcp,
	})
	require.NoError(t, err)
	assert.Equal(t, "eni-0123456789abcdef0", aws.ToString(out.NetworkInsightsPath.Source))
	assert.Equal(t, "eni-0fedcba9876543210", aws.ToString(out.NetworkInsightsPath.Destination))
}

// TestCreateRouteServer_RealWireKeys drives CreateRouteServer through the
// real SDK client. The real request field is "PersistRoutes"
// (serializers.go:72100), but the handler read "PersistRoutesState" -- the
// name of a different, response-only field (RouteServer.PersistRoutesState,
// botocore ec2 2016-11-15 service-2.json) -- so a client's requested
// PersistRoutes action was always discarded.
func TestCreateRouteServer_RealWireKeys(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	out, err := client.CreateRouteServer(t.Context(), &ec2sdk.CreateRouteServerInput{
		AmazonSideAsn: aws.Int64(4200000000),
		PersistRoutes: types.RouteServerPersistRoutesActionEnable,
	})
	require.NoError(t, err)
	assert.Equal(t,
		string(types.RouteServerPersistRoutesActionEnable),
		string(out.RouteServer.PersistRoutesState),
	)
}

// TestCreateInstanceExportTask_RealWireKeys drives CreateInstanceExportTask
// through the real SDK client. The nested settings serialize under the
// parent key "ExportToS3" (serializers.go:70438, matching response field
// ExportTask.ExportToS3Task -> wire "exportToS3"), but the handler read
// "ExportToS3Task.*" -- a prefix that doesn't exist on the wire -- so a real
// client's format/bucket/prefix were always discarded.
func TestCreateInstanceExportTask_RealWireKeys(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	out, err := client.CreateInstanceExportTask(t.Context(), &ec2sdk.CreateInstanceExportTaskInput{
		InstanceId:        aws.String(instanceID),
		TargetEnvironment: types.ExportEnvironmentVmware,
		ExportToS3Task: &types.ExportToS3TaskSpecification{
			ContainerFormat: types.ContainerFormatOva,
			DiskImageFormat: types.DiskImageFormatVmdk,
			S3Bucket:        aws.String("my-export-bucket"),
			S3Prefix:        aws.String("exports/"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.ExportTask.ExportToS3Task)
	assert.Equal(t, "my-export-bucket", aws.ToString(out.ExportTask.ExportToS3Task.S3Bucket))
	assert.Equal(t, types.ContainerFormatOva, out.ExportTask.ExportToS3Task.ContainerFormat)
}

// TestDescribeInstances_EbsOptimizedEnaSriovNetSupport_RealClient drives
// ModifyInstanceAttribute then DescribeInstances through the real SDK client.
// The real Instance deserializer (awsEc2query_deserializeDocumentInstance,
// ec2@v1.319.1 deserializers.go) reads "ebsOptimized", "enaSupport", and
// "sriovNetSupport" as top-level Instance fields. All three are tracked on
// the backend's Instance model (store.go's EBSOptimized/EnaSupport/
// SriovNetSupport, settable via ModifyInstanceAttribute and, for EnaSupport,
// true by default from RunInstances) but were never wired into
// DescribeInstances' instanceItem, so a real client always saw them as
// false/empty regardless of the instance's actual state -- the right
// instance count, blank contents for these three fields.
func TestDescribeInstances_EbsOptimizedEnaSriovNetSupport_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	// AWS applies only one attribute per ModifyInstanceAttribute call, so
	// EbsOptimized and SriovNetSupport each need their own request.
	_, err = client.ModifyInstanceAttribute(t.Context(), &ec2sdk.ModifyInstanceAttributeInput{
		InstanceId:   aws.String(instanceID),
		EbsOptimized: &types.AttributeBooleanValue{Value: aws.Bool(true)},
	})
	require.NoError(t, err)

	_, err = client.ModifyInstanceAttribute(t.Context(), &ec2sdk.ModifyInstanceAttributeInput{
		InstanceId:      aws.String(instanceID),
		SriovNetSupport: &types.AttributeValue{Value: aws.String("simple")},
	})
	require.NoError(t, err)

	out, err := client.DescribeInstances(t.Context(), &ec2sdk.DescribeInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)
	require.Len(t, out.Reservations, 1)
	require.Len(t, out.Reservations[0].Instances, 1)

	inst := out.Reservations[0].Instances[0]
	assert.True(t, aws.ToBool(inst.EbsOptimized), "EbsOptimized decoded false - never emitted by DescribeInstances")
	assert.True(t, aws.ToBool(inst.EnaSupport), "EnaSupport decoded false - never emitted by DescribeInstances")
	assert.Equal(
		t, "simple", aws.ToString(inst.SriovNetSupport),
		"SriovNetSupport decoded empty - never emitted by DescribeInstances",
	)
}

// TestDescribeSecurityGroups_IPPermissions_RealClient drives
// AuthorizeSecurityGroupIngress/Egress then DescribeSecurityGroups through the
// real SDK client. The real SecurityGroup deserializer
// (awsEc2query_deserializeDocumentSecurityGroup, ec2@v1.319.1
// deserializers.go) reads "ipPermissions" and "ipPermissionsEgress" as
// top-level SecurityGroup fields. The backend fully tracks authorized rules
// on SecurityGroup.IngressRules/EgressRules (confirmed working through the
// separate DescribeSecurityGroupRules op), but the classic DescribeSecurityGroups
// handler's sgItem never carried either field at all, so a real client's
// IpPermissions/IpPermissionsEgress were always empty regardless of what was
// authorized -- the right group count, completely blank rule contents.
func TestDescribeSecurityGroups_IPPermissions_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	created, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String("wire-field-fixes-sg"),
		Description: aws.String("test sg for ipPermissions wiring"),
	})
	require.NoError(t, err)
	groupID := aws.ToString(created.GroupId)

	_, err = client.AuthorizeSecurityGroupIngress(t.Context(), &ec2sdk.AuthorizeSecurityGroupIngressInput{
		GroupId: aws.String(groupID),
		IpPermissions: []types.IpPermission{{
			IpProtocol: aws.String("tcp"),
			FromPort:   aws.Int32(22),
			ToPort:     aws.Int32(22),
			IpRanges:   []types.IpRange{{CidrIp: aws.String("203.0.113.0/24"), Description: aws.String("ssh")}},
		}},
	})
	require.NoError(t, err)

	_, err = client.AuthorizeSecurityGroupEgress(t.Context(), &ec2sdk.AuthorizeSecurityGroupEgressInput{
		GroupId: aws.String(groupID),
		IpPermissions: []types.IpPermission{{
			IpProtocol: aws.String("tcp"),
			FromPort:   aws.Int32(443),
			ToPort:     aws.Int32(443),
			IpRanges:   []types.IpRange{{CidrIp: aws.String("198.51.100.0/24")}},
		}},
	})
	require.NoError(t, err)

	out, err := client.DescribeSecurityGroups(t.Context(), &ec2sdk.DescribeSecurityGroupsInput{
		GroupIds: []string{groupID},
	})
	require.NoError(t, err)
	require.Len(t, out.SecurityGroups, 1)

	sg := out.SecurityGroups[0]
	require.Len(t, sg.IpPermissions, 1, "IpPermissions empty - never emitted by DescribeSecurityGroups")
	assert.Equal(t, "tcp", aws.ToString(sg.IpPermissions[0].IpProtocol))
	assert.Equal(t, int32(22), aws.ToInt32(sg.IpPermissions[0].FromPort))
	require.Len(t, sg.IpPermissions[0].IpRanges, 1)
	assert.Equal(t, "203.0.113.0/24", aws.ToString(sg.IpPermissions[0].IpRanges[0].CidrIp))

	require.NotEmpty(t, sg.IpPermissionsEgress, "IpPermissionsEgress empty - never emitted by DescribeSecurityGroups")

	var foundEgress bool

	for _, p := range sg.IpPermissionsEgress {
		if aws.ToString(p.IpProtocol) == "tcp" && aws.ToInt32(p.FromPort) == 443 {
			foundEgress = true

			require.Len(t, p.IpRanges, 1)
			assert.Equal(t, "198.51.100.0/24", aws.ToString(p.IpRanges[0].CidrIp))
		}
	}

	assert.True(t, foundEgress, "authorized egress rule not found in IpPermissionsEgress")
}

// TestDescribeVpcs_TagSet_RealClient drives CreateVpc then CreateTags then
// DescribeVpcs through the real SDK client. The real Vpc deserializer
// (awsEc2query_deserializeDocumentVpc, ec2@v1.319.1 deserializers.go) reads
// "tagSet" as a top-level Vpc field, and DescribeVpcClassicLink already
// builds its response with h.Backend.TagsForResource(vpc.ID) -- proof the
// backend tracks VPC tags correctly. But vpcItem (DescribeVpcs/CreateVpc)
// never carried a TagSet field at all, so a real client's Vpc.Tags was
// always empty regardless of what had been tagged.
func TestDescribeVpcs_TagSet_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	created, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{
		CidrBlock: aws.String("10.20.0.0/16"),
	})
	require.NoError(t, err)
	vpcID := aws.ToString(created.Vpc.VpcId)

	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{vpcID},
		Tags:      []types.Tag{{Key: aws.String("Name"), Value: aws.String("wire-field-fixes-vpc")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeVpcs(t.Context(), &ec2sdk.DescribeVpcsInput{
		VpcIds: []string{vpcID},
	})
	require.NoError(t, err)
	require.Len(t, out.Vpcs, 1)

	require.NotEmpty(t, out.Vpcs[0].Tags, "Tags empty - never emitted by DescribeVpcs")
	assert.Equal(t, "Name", aws.ToString(out.Vpcs[0].Tags[0].Key))
	assert.Equal(t, "wire-field-fixes-vpc", aws.ToString(out.Vpcs[0].Tags[0].Value))
}

// TestDescribeRouteTables_TagSet_RealClient drives CreateRouteTable then
// CreateTags then DescribeRouteTables through the real SDK client. The real
// RouteTable deserializer (awsEc2query_deserializeDocumentRouteTable,
// ec2@v1.319.1 deserializers.go) reads "tagSet" as a top-level RouteTable
// field. The backend tracks and consults route table tags for tag: filters
// (routeTableMatchesFilter's default case, handler_filters.go), but
// routeTableItem never carried a TagSet field at all, so a real client's
// RouteTable.Tags was always empty.
func TestDescribeRouteTables_TagSet_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.21.0.0/16")})
	require.NoError(t, err)

	rt, err := client.CreateRouteTable(t.Context(), &ec2sdk.CreateRouteTableInput{
		VpcId: vpc.Vpc.VpcId,
	})
	require.NoError(t, err)
	rtID := aws.ToString(rt.RouteTable.RouteTableId)

	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{rtID},
		Tags:      []types.Tag{{Key: aws.String("Env"), Value: aws.String("wire-field-fixes")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeRouteTables(t.Context(), &ec2sdk.DescribeRouteTablesInput{
		RouteTableIds: []string{rtID},
	})
	require.NoError(t, err)
	require.Len(t, out.RouteTables, 1)

	require.NotEmpty(t, out.RouteTables[0].Tags, "Tags empty - never emitted by DescribeRouteTables")
	assert.Equal(t, "Env", aws.ToString(out.RouteTables[0].Tags[0].Key))
	assert.Equal(t, "wire-field-fixes", aws.ToString(out.RouteTables[0].Tags[0].Value))
}

// TestDescribeAddresses_TagSet_RealClient drives AllocateAddress then
// CreateTags then DescribeAddresses through the real SDK client. The real
// Address deserializer (awsEc2query_deserializeDocumentAddress, ec2@v1.319.1
// deserializers.go) reads "tagSet" as a top-level Address field. The backend
// tracks and consults address tags for tag: filters (addressMatchesFilter's
// default case, handler_filters.go), but addressItem never carried a TagSet
// field at all, so a real client's Address.Tags was always empty.
func TestDescribeAddresses_TagSet_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	alloc, err := client.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{})
	require.NoError(t, err)
	allocID := aws.ToString(alloc.AllocationId)

	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{allocID},
		Tags:      []types.Tag{{Key: aws.String("Owner"), Value: aws.String("wire-field-fixes")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeAddresses(t.Context(), &ec2sdk.DescribeAddressesInput{
		AllocationIds: []string{allocID},
	})
	require.NoError(t, err)
	require.Len(t, out.Addresses, 1)

	require.NotEmpty(t, out.Addresses[0].Tags, "Tags empty - never emitted by DescribeAddresses")
	assert.Equal(t, "Owner", aws.ToString(out.Addresses[0].Tags[0].Key))
	assert.Equal(t, "wire-field-fixes", aws.ToString(out.Addresses[0].Tags[0].Value))
}

// TestDescribeNetworkInterfaces_PublicIpDnsNameOptions_RealClient drives
// ModifyPublicIpDnsNameOptions then DescribeNetworkInterfaces through the
// real SDK client. The real NetworkInterface deserializer
// (awsEc2query_deserializeDocumentNetworkInterface, ec2@v1.319.1
// deserializers.go) reads "publicIpDnsNameOptions" as a top-level
// NetworkInterface field wrapping "dnsHostnameType"
// (awsEc2query_deserializeDocumentPublicIpDnsNameOptions). The backend
// tracks this on NetworkInterface.PublicDNSHostnameType, settable via the
// real ModifyPublicIpDnsNameOptions op, but networkInterfaceItem never
// carried the field, so a real client never saw the hostname type it had
// just set.
func TestDescribeNetworkInterfaces_PublicIpDnsNameOptions_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.22.0.0/16")})
	require.NoError(t, err)

	subnet, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId:     vpc.Vpc.VpcId,
		CidrBlock: aws.String("10.22.1.0/24"),
	})
	require.NoError(t, err)

	eni, err := client.CreateNetworkInterface(t.Context(), &ec2sdk.CreateNetworkInterfaceInput{
		SubnetId: subnet.Subnet.SubnetId,
	})
	require.NoError(t, err)
	eniID := aws.ToString(eni.NetworkInterface.NetworkInterfaceId)

	_, err = client.ModifyPublicIpDnsNameOptions(t.Context(), &ec2sdk.ModifyPublicIpDnsNameOptionsInput{
		NetworkInterfaceId: aws.String(eniID),
		HostnameType:       types.PublicIpDnsOptionPublicDualStackDnsName,
	})
	require.NoError(t, err)

	out, err := client.DescribeNetworkInterfaces(t.Context(), &ec2sdk.DescribeNetworkInterfacesInput{
		NetworkInterfaceIds: []string{eniID},
	})
	require.NoError(t, err)
	require.Len(t, out.NetworkInterfaces, 1)

	opts := out.NetworkInterfaces[0].PublicIpDnsNameOptions
	require.NotNil(t, opts, "PublicIpDnsNameOptions nil - never emitted by DescribeNetworkInterfaces")
	assert.Equal(
		t, "public-dual-stack-dns-name", aws.ToString(opts.DnsHostnameType),
		"DnsHostnameType decoded empty - never emitted by DescribeNetworkInterfaces",
	)
}

// TestDescribeImages_DisabledState_RealClient drives RegisterImage then
// DisableImage then DescribeImages through the real SDK client. Real AWS's
// ImageState enum includes "disabled" (types.ImageStateDisabled,
// ec2@v1.319.1 types/enums.go), and DisableImage/EnableImage are real
// settable state on this backend (b.imageDisabled), but DescribeImages built
// its response from AMIStub.State directly without ever consulting
// b.imageDisabled, so a real client's Image.State stayed "available" after
// DisableImage.
func TestDescribeImages_DisabledState_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	reg, err := client.RegisterImage(t.Context(), &ec2sdk.RegisterImageInput{
		Name: aws.String("wire-field-fixes-ami"),
	})
	require.NoError(t, err)
	imageID := aws.ToString(reg.ImageId)

	_, err = client.DisableImage(t.Context(), &ec2sdk.DisableImageInput{
		ImageId: aws.String(imageID),
	})
	require.NoError(t, err)

	out, err := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{
		ImageIds: []string{imageID},
	})
	require.NoError(t, err)
	require.Len(t, out.Images, 1)

	assert.Equal(
		t, types.ImageStateDisabled, out.Images[0].State,
		"State decoded available - DisableImage never reflected by DescribeImages",
	)
}
