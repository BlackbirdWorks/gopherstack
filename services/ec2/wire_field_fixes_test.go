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
//
// The request and response sides use two distinct real enums with different
// wire values for the same verb: RouteServerPersistRoutesAction ("enable"/
// "disable"/"reset", ec2@v1.319.1 types/enums.go:10663) on the way in, vs
// RouteServerPersistRoutesState ("enabled"/"disabled"/..., enums.go:10685)
// on the way out. This test previously asserted the response echoed the raw
// action string "enable" as correct -- gopherstack-6flj's exact raw-body/
// wrong-value blind spot -- when the real state enum has no "enable" value
// at all.
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
		string(types.RouteServerPersistRoutesStateEnabled),
		string(out.RouteServer.PersistRoutesState),
		"real RouteServerPersistRoutesState has no \"enable\" value, only \"enabled\"",
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

// TestDescribeInternetGateways_TagSet_RealClient drives CreateInternetGateway
// then CreateTags then DescribeInternetGateways through the real SDK client.
// The real InternetGateway deserializer (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeDocumentInternetGateway) reads "tagSet" as a
// top-level field. The backend already serves tag: filters for IGWs via
// tagMatch (handler_filters.go, igwMatchesFilter's default case), but
// igwItem never carried a TagSet field, so a real client's
// InternetGateway.Tags was always empty.
func TestDescribeInternetGateways_TagSet_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	created, err := client.CreateInternetGateway(t.Context(), &ec2sdk.CreateInternetGatewayInput{})
	require.NoError(t, err)
	igwID := aws.ToString(created.InternetGateway.InternetGatewayId)

	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{igwID},
		Tags:      []types.Tag{{Key: aws.String("Name"), Value: aws.String("wire-field-fixes-igw")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeInternetGateways(t.Context(), &ec2sdk.DescribeInternetGatewaysInput{
		InternetGatewayIds: []string{igwID},
	})
	require.NoError(t, err)
	require.Len(t, out.InternetGateways, 1)

	require.NotEmpty(t, out.InternetGateways[0].Tags, "Tags empty - never emitted by DescribeInternetGateways")
	assert.Equal(t, "Name", aws.ToString(out.InternetGateways[0].Tags[0].Key))
	assert.Equal(t, "wire-field-fixes-igw", aws.ToString(out.InternetGateways[0].Tags[0].Value))
}

// TestDescribeVpcPeeringConnections_RealShape_RealClient drives
// CreateVpcPeeringConnection then DescribeVpcPeeringConnections through the
// real SDK client. The real VpcPeeringConnection deserializer (ec2@v1.319.1
// deserializers.go, awsEc2query_deserializeDocumentVpcPeeringConnection)
// nests the VPC IDs under requesterVpcInfo/accepterVpcInfo and the status
// under status>code - vpcPeeringConnectionItem emitted flat
// requesterVpcId/accepterVpcId/statusCode fields the real deserializer never
// reads at all, so every field on a real client's VpcPeeringConnection was
// empty. CreateVpcPeeringConnectionResponse (handler_vpcs.go) already used
// the correct nested shape, which is what made this unambiguous.
func TestDescribeVpcPeeringConnections_RealShape_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	requester, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.30.0.0/16")})
	require.NoError(t, err)
	accepter, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.31.0.0/16")})
	require.NoError(t, err)

	created, err := client.CreateVpcPeeringConnection(t.Context(), &ec2sdk.CreateVpcPeeringConnectionInput{
		VpcId:     requester.Vpc.VpcId,
		PeerVpcId: accepter.Vpc.VpcId,
	})
	require.NoError(t, err)
	pcxID := aws.ToString(created.VpcPeeringConnection.VpcPeeringConnectionId)

	out, err := client.DescribeVpcPeeringConnections(t.Context(), &ec2sdk.DescribeVpcPeeringConnectionsInput{
		VpcPeeringConnectionIds: []string{pcxID},
	})
	require.NoError(t, err)
	require.Len(t, out.VpcPeeringConnections, 1)

	pcx := out.VpcPeeringConnections[0]
	require.NotNil(t, pcx.RequesterVpcInfo, "RequesterVpcInfo nil - never emitted by DescribeVpcPeeringConnections")
	require.NotNil(t, pcx.AccepterVpcInfo, "AccepterVpcInfo nil - never emitted by DescribeVpcPeeringConnections")
	assert.Equal(t, aws.ToString(requester.Vpc.VpcId), aws.ToString(pcx.RequesterVpcInfo.VpcId))
	assert.Equal(t, aws.ToString(accepter.Vpc.VpcId), aws.ToString(pcx.AccepterVpcInfo.VpcId))
	require.NotNil(t, pcx.Status, "Status nil - never emitted by DescribeVpcPeeringConnections")
	assert.Equal(t, "pending-acceptance", string(pcx.Status.Code))
}

// TestDescribeNetworkAcls_EntriesAndTags_RealClient drives CreateNetworkAcl
// then CreateNetworkAclEntry then CreateTags then DescribeNetworkAcls
// through the real SDK client. The real NetworkAcl deserializer
// (ec2@v1.319.1 deserializers.go, awsEc2query_deserializeDocumentNetworkAcl)
// reads "entrySet" and "tagSet" as top-level fields. The backend fully
// tracks and enforces NACL entries (CreateNetworkAclEntry/
// DeleteNetworkAclEntry/ReplaceNetworkAclEntry all mutate
// StoredNetworkACL.Entries) and tags (generic CreateTags store, same as the
// IGW/VPC/route-table cases), but networkACLItem never carried an entrySet
// or tagSet field at all - a caller could create a rule and never see it
// through DescribeNetworkAcls, the same class of bug as DescribeSecurityGroups
// never emitting ipPermissions.
func TestDescribeNetworkAcls_EntriesAndTags_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.32.0.0/16")})
	require.NoError(t, err)

	acl, err := client.CreateNetworkAcl(t.Context(), &ec2sdk.CreateNetworkAclInput{VpcId: vpc.Vpc.VpcId})
	require.NoError(t, err)
	aclID := aws.ToString(acl.NetworkAcl.NetworkAclId)

	_, err = client.CreateNetworkAclEntry(t.Context(), &ec2sdk.CreateNetworkAclEntryInput{
		NetworkAclId: aws.String(aclID),
		RuleNumber:   aws.Int32(150),
		Protocol:     aws.String("6"),
		RuleAction:   types.RuleActionAllow,
		CidrBlock:    aws.String("192.0.2.0/24"),
		Egress:       aws.Bool(false),
		PortRange:    &types.PortRange{From: aws.Int32(80), To: aws.Int32(80)},
	})
	require.NoError(t, err)

	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{aclID},
		Tags:      []types.Tag{{Key: aws.String("Name"), Value: aws.String("wire-field-fixes-acl")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeNetworkAcls(t.Context(), &ec2sdk.DescribeNetworkAclsInput{
		NetworkAclIds: []string{aclID},
	})
	require.NoError(t, err)
	require.Len(t, out.NetworkAcls, 1)

	nacl := out.NetworkAcls[0]

	require.NotEmpty(t, nacl.Entries, "Entries empty - never emitted by DescribeNetworkAcls")
	var found bool
	for _, e := range nacl.Entries {
		if aws.ToInt32(e.RuleNumber) == 150 {
			found = true
			assert.Equal(t, "192.0.2.0/24", aws.ToString(e.CidrBlock))
			assert.Equal(t, types.RuleActionAllow, e.RuleAction)
			require.NotNil(t, e.PortRange, "PortRange nil for tcp rule")
			assert.Equal(t, int32(80), aws.ToInt32(e.PortRange.From))
		}
	}
	assert.True(t, found, "rule 150 not found in entrySet")

	require.NotEmpty(t, nacl.Tags, "Tags empty - never emitted by DescribeNetworkAcls")
	assert.Equal(t, "Name", aws.ToString(nacl.Tags[0].Key))
}

// TestDescribeCarrierGateways_TagSet_RealClient drives CreateCarrierGateway
// then CreateTags then DescribeCarrierGateways through the real SDK client.
// The real CarrierGateway deserializer (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeDocumentCarrierGateway) reads "tagSet" as a
// top-level field. Carrier gateways are already taggable through the
// generic CreateTags store (resourceExistsLocked recognizes
// b.carrierGateways), but carrierGatewayItem never carried a TagSet field.
func TestDescribeCarrierGateways_TagSet_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.33.0.0/16")})
	require.NoError(t, err)

	created, err := client.CreateCarrierGateway(t.Context(), &ec2sdk.CreateCarrierGatewayInput{
		VpcId: vpc.Vpc.VpcId,
	})
	require.NoError(t, err)
	cagwID := aws.ToString(created.CarrierGateway.CarrierGatewayId)

	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{cagwID},
		Tags:      []types.Tag{{Key: aws.String("Name"), Value: aws.String("wire-field-fixes-cagw")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeCarrierGateways(t.Context(), &ec2sdk.DescribeCarrierGatewaysInput{
		CarrierGatewayIds: []string{cagwID},
	})
	require.NoError(t, err)
	require.Len(t, out.CarrierGateways, 1)

	require.NotEmpty(t, out.CarrierGateways[0].Tags, "Tags empty - never emitted by DescribeCarrierGateways")
	assert.Equal(t, "Name", aws.ToString(out.CarrierGateways[0].Tags[0].Key))
}

// TestDescribeEgressOnlyInternetGateways_TagSet_RealClient drives
// CreateEgressOnlyInternetGateway then CreateTags then
// DescribeEgressOnlyInternetGateways through the real SDK client. The real
// EgressOnlyInternetGateway deserializer (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeDocumentEgressOnlyInternetGateway) reads "tagSet"
// as a top-level field; egressOnlyIGWItem never carried one, despite egress-
// only IGWs already being taggable through the generic CreateTags store.
func TestDescribeEgressOnlyInternetGateways_TagSet_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.34.0.0/16")})
	require.NoError(t, err)

	created, err := client.CreateEgressOnlyInternetGateway(
		t.Context(), &ec2sdk.CreateEgressOnlyInternetGatewayInput{VpcId: vpc.Vpc.VpcId},
	)
	require.NoError(t, err)
	eigwID := aws.ToString(created.EgressOnlyInternetGateway.EgressOnlyInternetGatewayId)

	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{eigwID},
		Tags:      []types.Tag{{Key: aws.String("Name"), Value: aws.String("wire-field-fixes-eigw")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeEgressOnlyInternetGateways(
		t.Context(), &ec2sdk.DescribeEgressOnlyInternetGatewaysInput{EgressOnlyInternetGatewayIds: []string{eigwID}},
	)
	require.NoError(t, err)
	require.Len(t, out.EgressOnlyInternetGateways, 1)

	require.NotEmpty(
		t, out.EgressOnlyInternetGateways[0].Tags,
		"Tags empty - never emitted by DescribeEgressOnlyInternetGateways",
	)
	assert.Equal(t, "Name", aws.ToString(out.EgressOnlyInternetGateways[0].Tags[0].Key))
}

// TestDescribeManagedPrefixLists_TagSet_RealClient drives
// CreateManagedPrefixList then CreateTags then DescribeManagedPrefixLists
// through the real SDK client. The real ManagedPrefixList deserializer
// (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeDocumentManagedPrefixList) reads "tagSet" as a
// top-level field; managed prefix lists are already taggable through the
// generic CreateTags store, but managedPrefixListItem never carried one.
func TestDescribeManagedPrefixLists_TagSet_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	created, err := client.CreateManagedPrefixList(t.Context(), &ec2sdk.CreateManagedPrefixListInput{
		PrefixListName: aws.String("wire-field-fixes-pl"),
		AddressFamily:  aws.String("IPv4"),
		MaxEntries:     aws.Int32(5),
	})
	require.NoError(t, err)
	plID := aws.ToString(created.PrefixList.PrefixListId)

	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{plID},
		Tags:      []types.Tag{{Key: aws.String("Name"), Value: aws.String("wire-field-fixes-pl-tag")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeManagedPrefixLists(t.Context(), &ec2sdk.DescribeManagedPrefixListsInput{
		PrefixListIds: []string{plID},
	})
	require.NoError(t, err)
	require.Len(t, out.PrefixLists, 1)

	require.NotEmpty(t, out.PrefixLists[0].Tags, "Tags empty - never emitted by DescribeManagedPrefixLists")
	assert.Equal(t, "Name", aws.ToString(out.PrefixLists[0].Tags[0].Key))
}

// TestDescribeTransitGatewayVpcAttachments_CreationTimeAndTags_RealClient
// drives CreateTransitGateway, CreateVpc, CreateTransitGatewayVpcAttachment,
// CreateTags then DescribeTransitGatewayVpcAttachments through the real SDK
// client. The real TransitGatewayVpcAttachment deserializer (ec2@v1.319.1
// deserializers.go,
// awsEc2query_deserializeDocumentTransitGatewayVpcAttachment) reads
// "creationTime" and "tagSet" as top-level fields. CreationTime is real,
// backend-tracked state (TransitGatewayVpcAttachment.CreationTime, set at
// creation) that tgwVpcAttachmentItem never emitted at all, and the
// attachment is already taggable through the generic CreateTags store.
func TestDescribeTransitGatewayVpcAttachments_CreationTimeAndTags_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	tgw, err := client.CreateTransitGateway(t.Context(), &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.35.0.0/16")})
	require.NoError(t, err)
	subnet, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId:     vpc.Vpc.VpcId,
		CidrBlock: aws.String("10.35.1.0/24"),
	})
	require.NoError(t, err)

	created, err := client.CreateTransitGatewayVpcAttachment(
		t.Context(), &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
			TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
			VpcId:            vpc.Vpc.VpcId,
			SubnetIds:        []string{aws.ToString(subnet.Subnet.SubnetId)},
		},
	)
	require.NoError(t, err)
	attID := aws.ToString(created.TransitGatewayVpcAttachment.TransitGatewayAttachmentId)

	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{attID},
		Tags:      []types.Tag{{Key: aws.String("Name"), Value: aws.String("wire-field-fixes-tgw-att")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeTransitGatewayVpcAttachments(
		t.Context(), &ec2sdk.DescribeTransitGatewayVpcAttachmentsInput{TransitGatewayAttachmentIds: []string{attID}},
	)
	require.NoError(t, err)
	require.Len(t, out.TransitGatewayVpcAttachments, 1)

	att := out.TransitGatewayVpcAttachments[0]
	assert.NotNil(t, att.CreationTime, "CreationTime nil - never emitted by DescribeTransitGatewayVpcAttachments")
	require.NotEmpty(t, att.Tags, "Tags empty - never emitted by DescribeTransitGatewayVpcAttachments")
	assert.Equal(t, "Name", aws.ToString(att.Tags[0].Key))
}

// TestDescribeTransitGatewayPeeringAttachments_RealShape_RealClient drives
// two CreateTransitGateway calls, CreateTransitGatewayPeeringAttachment then
// DescribeTransitGatewayPeeringAttachments through the real SDK client. The
// real TransitGatewayPeeringAttachment deserializer (ec2@v1.319.1
// deserializers.go,
// awsEc2query_deserializeDocumentTransitGatewayPeeringAttachment) nests the
// requester/accepter transit gateway IDs under requesterTgwInfo/
// accepterTgwInfo - tgwPeeringAttachmentItem emitted flat
// requesterTransitGatewayId/accepterTransitGatewayId fields the real
// deserializer never reads, so a real client's RequesterTgwInfo/
// AccepterTgwInfo were always nil.
func TestDescribeTransitGatewayPeeringAttachments_RealShape_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	requesterTGW, err := client.CreateTransitGateway(t.Context(), &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)
	accepterTGW, err := client.CreateTransitGateway(t.Context(), &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)

	created, err := client.CreateTransitGatewayPeeringAttachment(
		t.Context(), &ec2sdk.CreateTransitGatewayPeeringAttachmentInput{
			TransitGatewayId:     requesterTGW.TransitGateway.TransitGatewayId,
			PeerTransitGatewayId: accepterTGW.TransitGateway.TransitGatewayId,
			PeerAccountId:        aws.String("000000000000"),
			PeerRegion:           aws.String("us-east-1"),
		},
	)
	require.NoError(t, err)
	attID := aws.ToString(created.TransitGatewayPeeringAttachment.TransitGatewayAttachmentId)

	out, err := client.DescribeTransitGatewayPeeringAttachments(
		t.Context(),
		&ec2sdk.DescribeTransitGatewayPeeringAttachmentsInput{TransitGatewayAttachmentIds: []string{attID}},
	)
	require.NoError(t, err)
	require.Len(t, out.TransitGatewayPeeringAttachments, 1)

	att := out.TransitGatewayPeeringAttachments[0]
	require.NotNil(t, att.RequesterTgwInfo, "RequesterTgwInfo nil - never emitted by Describe...PeeringAttachments")
	require.NotNil(t, att.AccepterTgwInfo, "AccepterTgwInfo nil - never emitted by Describe...PeeringAttachments")

	wantRequester := aws.ToString(requesterTGW.TransitGateway.TransitGatewayId)
	wantAccepter := aws.ToString(accepterTGW.TransitGateway.TransitGatewayId)
	assert.Equal(t, wantRequester, aws.ToString(att.RequesterTgwInfo.TransitGatewayId))
	assert.Equal(t, wantAccepter, aws.ToString(att.AccepterTgwInfo.TransitGatewayId))
}

// TestDescribeTransitGatewayAttachments_TagSet_RealClient drives
// CreateTransitGateway, CreateVpc, CreateTransitGatewayVpcAttachment,
// CreateTags then the unified DescribeTransitGatewayAttachments through the
// real SDK client. The real TransitGatewayAttachment deserializer
// (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeDocumentTransitGatewayAttachment) reads "tagSet" as
// a top-level field. This op aggregates rows from the VPC/peering/connect/
// clientVpn attachment maps, all individually taggable through the generic
// CreateTags store, but tgwAttachmentSummaryItem never carried a TagSet
// field.
func TestDescribeTransitGatewayAttachments_TagSet_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	tgw, err := client.CreateTransitGateway(t.Context(), &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.39.0.0/16")})
	require.NoError(t, err)
	subnet, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId:     vpc.Vpc.VpcId,
		CidrBlock: aws.String("10.39.1.0/24"),
	})
	require.NoError(t, err)

	vpcAtt, err := client.CreateTransitGatewayVpcAttachment(
		t.Context(), &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
			TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
			VpcId:            vpc.Vpc.VpcId,
			SubnetIds:        []string{aws.ToString(subnet.Subnet.SubnetId)},
		},
	)
	require.NoError(t, err)
	attID := aws.ToString(vpcAtt.TransitGatewayVpcAttachment.TransitGatewayAttachmentId)

	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{attID},
		Tags:      []types.Tag{{Key: aws.String("Name"), Value: aws.String("wire-field-fixes-tgw-summary")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeTransitGatewayAttachments(
		t.Context(), &ec2sdk.DescribeTransitGatewayAttachmentsInput{TransitGatewayAttachmentIds: []string{attID}},
	)
	require.NoError(t, err)
	require.Len(t, out.TransitGatewayAttachments, 1)

	require.NotEmpty(
		t, out.TransitGatewayAttachments[0].Tags, "Tags empty - never emitted by DescribeTransitGatewayAttachments",
	)
	assert.Equal(t, "Name", aws.ToString(out.TransitGatewayAttachments[0].Tags[0].Key))
}

// TestCreateTransitGatewayConnect_RealClient drives CreateTransitGateway,
// CreateVpc, CreateTransitGatewayVpcAttachment then
// CreateTransitGatewayConnect through the real SDK client.
// CreateTransitGatewayConnectInput has no TransitGatewayId field at all
// (ec2@v1.319.1 api_op_CreateTransitGatewayConnect.go) - it is derived from
// TransportTransitGatewayAttachmentId - but the handler required
// vals.Get("TransitGatewayId"), a parameter no real client ever sends, so
// every real CreateTransitGatewayConnect call failed with
// InvalidParameterValue regardless of a valid transport attachment.
func TestCreateTransitGatewayConnect_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	tgw, err := client.CreateTransitGateway(t.Context(), &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.38.0.0/16")})
	require.NoError(t, err)
	subnet, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId:     vpc.Vpc.VpcId,
		CidrBlock: aws.String("10.38.1.0/24"),
	})
	require.NoError(t, err)

	vpcAtt, err := client.CreateTransitGatewayVpcAttachment(
		t.Context(), &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
			TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
			VpcId:            vpc.Vpc.VpcId,
			SubnetIds:        []string{aws.ToString(subnet.Subnet.SubnetId)},
		},
	)
	require.NoError(t, err)

	connectOpts := &types.CreateTransitGatewayConnectRequestOptions{Protocol: types.ProtocolValueGre}
	out, err := client.CreateTransitGatewayConnect(t.Context(), &ec2sdk.CreateTransitGatewayConnectInput{
		TransportTransitGatewayAttachmentId: vpcAtt.TransitGatewayVpcAttachment.TransitGatewayAttachmentId,
		Options:                             connectOpts,
	})
	require.NoError(t, err, "real client never sends TransitGatewayId - handler must derive it")
	assert.Equal(
		t, aws.ToString(tgw.TransitGateway.TransitGatewayId),
		aws.ToString(out.TransitGatewayConnect.TransitGatewayId),
	)
}

// TestDescribeTransitGatewayConnects_WrapperKey_RealClient drives
// CreateTransitGateway, CreateVpc, CreateTransitGatewayVpcAttachment,
// CreateTransitGatewayConnect then DescribeTransitGatewayConnects through
// the real SDK client. The real DescribeTransitGatewayConnectsOutput
// deserializer (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentDescribeTransitGatewayConnectsOutput)
// reads the collection under "transitGatewayConnectSet" -
// describeTransitGatewayConnectsResponse emitted "transitGatewayConnects"
// (missing the "Set" suffix), a key the real deserializer never matches, so
// a real client's TransitGatewayConnects was always an empty slice
// regardless of how many Connect attachments existed. Found via
// TestDescribeTransitGatewayConnectPeers_RealShape_RealClient returning
// zero items even before any ID filter was applied.
func TestDescribeTransitGatewayConnects_WrapperKey_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	tgw, err := client.CreateTransitGateway(t.Context(), &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.37.0.0/16")})
	require.NoError(t, err)
	subnet, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId:     vpc.Vpc.VpcId,
		CidrBlock: aws.String("10.37.1.0/24"),
	})
	require.NoError(t, err)

	vpcAtt, err := client.CreateTransitGatewayVpcAttachment(
		t.Context(), &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
			TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
			VpcId:            vpc.Vpc.VpcId,
			SubnetIds:        []string{aws.ToString(subnet.Subnet.SubnetId)},
		},
	)
	require.NoError(t, err)

	connectOpts := &types.CreateTransitGatewayConnectRequestOptions{Protocol: types.ProtocolValueGre}
	connect, err := client.CreateTransitGatewayConnect(t.Context(), &ec2sdk.CreateTransitGatewayConnectInput{
		TransportTransitGatewayAttachmentId: vpcAtt.TransitGatewayVpcAttachment.TransitGatewayAttachmentId,
		Options:                             connectOpts,
	})
	require.NoError(t, err)

	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{aws.ToString(connect.TransitGatewayConnect.TransitGatewayAttachmentId)},
		Tags:      []types.Tag{{Key: aws.String("Name"), Value: aws.String("wire-field-fixes-tgw-connect")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeTransitGatewayConnects(t.Context(), &ec2sdk.DescribeTransitGatewayConnectsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.TransitGatewayConnects, "TransitGatewayConnects empty - wrong wrapper key")
	require.NotEmpty(t, out.TransitGatewayConnects[0].Tags, "Tags empty - never emitted by Describe...Connects")
	assert.Equal(t, "Name", aws.ToString(out.TransitGatewayConnects[0].Tags[0].Key))
}

// TestDescribeTransitGatewayConnectPeers_RealShape_RealClient drives
// CreateTransitGateway, CreateVpc, CreateTransitGatewayVpcAttachment,
// CreateTransitGatewayConnect, CreateTransitGatewayConnectPeer then
// DescribeTransitGatewayConnectPeers through the real SDK client. The real
// TransitGatewayConnectPeerConfiguration deserializer (ec2@v1.319.1
// deserializers.go,
// awsEc2query_deserializeDocumentTransitGatewayConnectPeerConfiguration)
// nests PeerAddress and InsideCidrBlocks under connectPeerConfiguration -
// tgwConnectPeerItem emitted a flat top-level peerAddress field the real
// deserializer never reads, and never emitted insideCidrBlocks at all
// despite the backend tracking it.
func TestDescribeTransitGatewayConnectPeers_RealShape_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	tgw, err := client.CreateTransitGateway(t.Context(), &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.36.0.0/16")})
	require.NoError(t, err)
	subnet, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId:     vpc.Vpc.VpcId,
		CidrBlock: aws.String("10.36.1.0/24"),
	})
	require.NoError(t, err)

	vpcAtt, err := client.CreateTransitGatewayVpcAttachment(
		t.Context(), &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
			TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
			VpcId:            vpc.Vpc.VpcId,
			SubnetIds:        []string{aws.ToString(subnet.Subnet.SubnetId)},
		},
	)
	require.NoError(t, err)

	connectOpts := &types.CreateTransitGatewayConnectRequestOptions{Protocol: types.ProtocolValueGre}
	connect, err := client.CreateTransitGatewayConnect(t.Context(), &ec2sdk.CreateTransitGatewayConnectInput{
		TransportTransitGatewayAttachmentId: vpcAtt.TransitGatewayVpcAttachment.TransitGatewayAttachmentId,
		Options:                             connectOpts,
	})
	require.NoError(t, err)

	peer, err := client.CreateTransitGatewayConnectPeer(t.Context(), &ec2sdk.CreateTransitGatewayConnectPeerInput{
		TransitGatewayAttachmentId: connect.TransitGatewayConnect.TransitGatewayAttachmentId,
		PeerAddress:                aws.String("192.0.2.10"),
		InsideCidrBlocks:           []string{"169.254.100.0/29"},
	})
	require.NoError(t, err)
	peerID := aws.ToString(peer.TransitGatewayConnectPeer.TransitGatewayConnectPeerId)

	out, err := client.DescribeTransitGatewayConnectPeers(
		t.Context(), &ec2sdk.DescribeTransitGatewayConnectPeersInput{TransitGatewayConnectPeerIds: []string{peerID}},
	)
	require.NoError(t, err)
	require.Len(t, out.TransitGatewayConnectPeers, 1)

	cfg := out.TransitGatewayConnectPeers[0].ConnectPeerConfiguration
	require.NotNil(t, cfg, "ConnectPeerConfiguration nil - never emitted by DescribeTransitGatewayConnectPeers")
	assert.Equal(t, "192.0.2.10", aws.ToString(cfg.PeerAddress),
		"PeerAddress empty - real deserializer reads connectPeerConfiguration>peerAddress, not a flat field")
	require.NotEmpty(t, cfg.InsideCidrBlocks, "InsideCidrBlocks empty - never emitted by Describe...ConnectPeers")
	assert.Equal(t, "169.254.100.0/29", cfg.InsideCidrBlocks[0])
}

// TestModifyInstancePlacement_GroupNameCanBeCleared drives
// ModifyInstancePlacement/DescribeInstances through the real SDK client.
// ModifyInstancePlacementInput.GroupName was a plain string guarded by
// != "" (not *string like the real SDK's ModifyInstancePlacementInput,
// api_op_ModifyInstancePlacement.go), whose doc comment says "To remove an
// instance from a placement group, specify an empty string ("")" -- so a
// real client's documented way to clear it was silently dropped, leaving the
// instance in its old placement group.
func TestModifyInstancePlacement_GroupNameCanBeCleared(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := backend.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID
	backend.TickLifecycleForTest() // pending -> running

	_, err = backend.StopInstances([]string{instanceID})
	require.NoError(t, err)
	backend.TickLifecycleForTest() // stopping -> stopped

	client := newTestEC2Client(t, ec2.NewHandler(backend))
	ctx := t.Context()

	_, err = client.ModifyInstancePlacement(ctx, &ec2sdk.ModifyInstancePlacementInput{
		InstanceId: aws.String(instanceID),
		GroupName:  aws.String("my-placement-group"),
	})
	require.NoError(t, err)

	before, err := client.DescribeInstances(ctx, &ec2sdk.DescribeInstancesInput{InstanceIds: []string{instanceID}})
	require.NoError(t, err)
	require.Len(t, before.Reservations, 1)
	require.Len(t, before.Reservations[0].Instances, 1)
	require.Equal(t, "my-placement-group",
		aws.ToString(before.Reservations[0].Instances[0].Placement.GroupName))

	_, err = client.ModifyInstancePlacement(ctx, &ec2sdk.ModifyInstancePlacementInput{
		InstanceId: aws.String(instanceID),
		GroupName:  aws.String(""),
	})
	require.NoError(t, err)

	after, err := client.DescribeInstances(ctx, &ec2sdk.DescribeInstancesInput{InstanceIds: []string{instanceID}})
	require.NoError(t, err)
	require.Len(t, after.Reservations, 1)
	require.Len(t, after.Reservations[0].Instances, 1)
	require.Empty(t, aws.ToString(after.Reservations[0].Instances[0].Placement.GroupName),
		"explicit empty GroupName on ModifyInstancePlacement must clear it, not be silently ignored")
}

// TestModifyVerifiedAccessGroup_TagSet_RealClient proves
// ModifyVerifiedAccessGroup returns the group's tags. Pre-fix, the handler
// built its response inline instead of via toVerifiedAccessGroupItem (the
// converter every other VerifiedAccessGroup response uses), so TagSet was
// always empty in a ModifyVerifiedAccessGroup response even though the
// group's tags were genuinely tracked in the backend.
func TestModifyVerifiedAccessGroup_TagSet_RealClient(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestEC2Client(t, ec2.NewHandler(backend))
	ctx := t.Context()

	inst, err := backend.CreateVerifiedAccessInstance("modify-tagset-inst")
	require.NoError(t, err)
	grp, err := backend.CreateVerifiedAccessGroup(inst.VerifiedAccessInstanceID, "orig")
	require.NoError(t, err)

	require.NoError(
		t,
		backend.CreateTags([]string{grp.VerifiedAccessGroupID}, map[string]string{"Name": "vagr-modify"}),
	)

	out, err := client.ModifyVerifiedAccessGroup(ctx, &ec2sdk.ModifyVerifiedAccessGroupInput{
		VerifiedAccessGroupId: aws.String(grp.VerifiedAccessGroupID),
		Description:           aws.String("updated"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.VerifiedAccessGroup)
	require.NotEmpty(t, out.VerifiedAccessGroup.Tags, "Tags empty - pre-fix inline construction dropped TagSet")
	assert.Equal(t, "Name", aws.ToString(out.VerifiedAccessGroup.Tags[0].Key))
	assert.Equal(t, "vagr-modify", aws.ToString(out.VerifiedAccessGroup.Tags[0].Value))
}
