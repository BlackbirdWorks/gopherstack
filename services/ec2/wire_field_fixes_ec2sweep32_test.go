package ec2_test

// ec2sweep32 covers the previously-unreached Describe list picked up after
// 16c7cbeba: DescribeImageUsageReports (the three ops the prior pass had just
// reached: DescribeImageReferences and DescribeImageUsageReportEntries were
// verified clean against the real SDK deserializer and needed no fix), plus
// DescribeInstanceSqlHaStates, DescribeInstanceTopology,
// DescribeNetworkInterfaceAttribute, DescribeSecurityGroupVpcAssociations,
// DescribeAddressesAttribute, EnableRouteServerPropagation/
// GetRouteServerPropagations, and GetRouteServerRoutingDatabase.

import (
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeImageUsageReports_RealReports_RealClient covers
// DescribeImageUsageReports. Pre-fix, the handler read from a completely
// disconnected backend store (b.imageUsageReports, keyed by ImageID) that was
// silently auto-populated by CreateImage/CopyImage with a fabricated
// "generationDate" field -- not a member of the real ImageUsageReport wire
// shape (ec2@v1.319.1 types/types.go:8578: AccountIds/CreationTime/
// ExpirationTime/ImageId/ReportId/ResourceTypes/State/StateReason/Tags).
// Reports actually created via CreateImageUsageReport (b.usageReports) never
// appeared in this list at all -- a real client could never see its own
// report.
func TestDescribeImageUsageReports_RealReports_RealClient(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	instances, err := b.RunInstances("ami-sweep32a", "t3.micro", "", 1)
	require.NoError(t, err)

	image, err := b.CreateImage(instances[0].ID, "sweep32-image", "")
	require.NoError(t, err)

	createOut, err := client.CreateImageUsageReport(t.Context(), &ec2sdk.CreateImageUsageReportInput{
		ImageId: aws.String(image.ImageID),
		ResourceTypes: []types.ImageUsageResourceTypeRequest{
			{ResourceType: aws.String("ec2:Instance")},
		},
	})
	require.NoError(t, err)
	reportID := aws.ToString(createOut.ReportId)
	require.NotEmpty(t, reportID)

	descOut, err := client.DescribeImageUsageReports(t.Context(), &ec2sdk.DescribeImageUsageReportsInput{})
	require.NoError(t, err)
	require.Len(t, descOut.ImageUsageReports, 1, "empty collection is the bug: the real report never appeared")

	report := descOut.ImageUsageReports[0]
	assert.Equal(t, reportID, aws.ToString(report.ReportId))
	assert.Equal(t, image.ImageID, aws.ToString(report.ImageId))
	assert.NotNil(t, report.State, "State pre-fix was always nil (never a wire member: generationDate was)")
	assert.NotNil(t, report.CreationTime)
}

// TestDescribeInstanceSqlHaStates_Credentials_RealClient covers
// EnableInstanceSqlHaStandbyDetections / DescribeInstanceSqlHaStates. Pre-fix,
// the backend stored the caller's SqlServerCredentials
// (RegisteredSQLHaInstance.SQLServerCredentials) but the wire response never
// rendered it -- "sqlServerCredentials" is a real member (ec2@v1.319.1
// deserializers.go:146520, types.RegisteredInstance.SqlServerCredentials),
// silently dropped every time.
func TestDescribeInstanceSqlHaStates_Credentials_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	instOut, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId:      aws.String("ami-sweep32b"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	instanceID := aws.ToString(instOut.Instances[0].InstanceId)

	const credArn = "arn:aws:secretsmanager:us-east-1:000000000000:secret:sweep32-creds"

	_, err = client.EnableInstanceSqlHaStandbyDetections(t.Context(), &ec2sdk.EnableInstanceSqlHaStandbyDetectionsInput{
		InstanceIds:          []string{instanceID},
		SqlServerCredentials: aws.String(credArn),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeInstanceSqlHaStates(t.Context(), &ec2sdk.DescribeInstanceSqlHaStatesInput{})
	require.NoError(t, err)
	require.Len(t, descOut.Instances, 1, "empty collection is the bug")
	assert.Equal(t, credArn, aws.ToString(descOut.Instances[0].SqlServerCredentials))
}

// TestDescribeInstanceTopology_GroupName_RealClient covers
// DescribeInstanceTopology. Pre-fix, InstanceTopologyItem.GroupName was
// tracked on the backend struct but never read from inst.Placement.GroupName
// nor rendered on the wire -- "groupName" is a real member (ec2@v1.319.1
// deserializers.go:116975, types.InstanceTopology.GroupName), silently
// dropped for every instance in a placement group.
func TestDescribeInstanceTopology_GroupName_RealClient(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	instOut, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId:      aws.String("ami-sweep32c"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	instanceID := aws.ToString(instOut.Instances[0].InstanceId)

	// ModifyInstancePlacement requires the instance to be stopped; StopInstances
	// only transitions it to "stopping" (a reconciler advances it to "stopped"),
	// so tick the lifecycle synchronously the way other tests here do.
	_, err = b.StopInstances([]string{instanceID})
	require.NoError(t, err)
	b.TickLifecycleForTest()

	const groupName = "sweep32-placement-group"

	_, err = client.ModifyInstancePlacement(t.Context(), &ec2sdk.ModifyInstancePlacementInput{
		InstanceId: aws.String(instanceID),
		GroupName:  aws.String(groupName),
	})
	require.NoError(t, err)

	topoOut, err := client.DescribeInstanceTopology(t.Context(), &ec2sdk.DescribeInstanceTopologyInput{})
	require.NoError(t, err)
	require.Len(t, topoOut.Instances, 1, "empty collection is the bug")
	assert.Equal(t, groupName, aws.ToString(topoOut.Instances[0].GroupName))
}

// TestDescribeNetworkInterfaceAttribute_Attachment_RealClient covers
// DescribeNetworkInterfaceAttribute. Pre-fix, the handler always rendered
// description+sourceDestCheck regardless of the requested Attribute and never
// supported Attribute=attachment at all, even though the backend already
// tracks the attaching instance -- "attachment" is a real member
// (ec2@v1.319.1 deserializers.go:202665,
// types.DescribeNetworkInterfaceAttributeOutput.Attachment), silently
// dropped.
func TestDescribeNetworkInterfaceAttribute_Attachment_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	instOut, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId:      aws.String("ami-sweep32d"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	instanceID := aws.ToString(instOut.Instances[0].InstanceId)
	subnetID := aws.ToString(instOut.Instances[0].SubnetId)

	niOut, err := client.CreateNetworkInterface(t.Context(), &ec2sdk.CreateNetworkInterfaceInput{
		SubnetId: aws.String(subnetID),
	})
	require.NoError(t, err)
	niID := aws.ToString(niOut.NetworkInterface.NetworkInterfaceId)

	attachOut, err := client.AttachNetworkInterface(t.Context(), &ec2sdk.AttachNetworkInterfaceInput{
		DeviceIndex:        aws.Int32(1),
		InstanceId:         aws.String(instanceID),
		NetworkInterfaceId: aws.String(niID),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(attachOut.AttachmentId))

	descOut, err := client.DescribeNetworkInterfaceAttribute(
		t.Context(),
		&ec2sdk.DescribeNetworkInterfaceAttributeInput{
			NetworkInterfaceId: aws.String(niID),
			Attribute:          types.NetworkInterfaceAttributeAttachment,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, descOut.Attachment, "Attachment was always nil pre-fix, regardless of Attribute")
	assert.Equal(t, instanceID, aws.ToString(descOut.Attachment.InstanceId))
	assert.Equal(t, aws.ToString(attachOut.AttachmentId), aws.ToString(descOut.Attachment.AttachmentId))
}

// TestDescribeSecurityGroupVpcAssociations_OwnerIDs_RealClient covers
// AssociateSecurityGroupVpc / DescribeSecurityGroupVpcAssociations. Pre-fix,
// SGVpcAssocItem had no owner-ID fields, so groupOwnerId/vpcOwnerId (real
// members, ec2@v1.319.1 deserializers.go:155767,155823,
// types.SecurityGroupVpcAssociation) were always empty even though this
// backend is single-account and both IDs are always known.
func TestDescribeSecurityGroupVpcAssociations_OwnerIDs_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.77.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	sgOut, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String("sweep32-sg"),
		Description: aws.String("sweep32"),
		VpcId:       aws.String(vpcID),
	})
	require.NoError(t, err)
	sgID := aws.ToString(sgOut.GroupId)

	_, err = client.AssociateSecurityGroupVpc(t.Context(), &ec2sdk.AssociateSecurityGroupVpcInput{
		GroupId: aws.String(sgID),
		VpcId:   aws.String(vpcID),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeSecurityGroupVpcAssociations(
		t.Context(), &ec2sdk.DescribeSecurityGroupVpcAssociationsInput{},
	)
	require.NoError(t, err)
	require.Len(t, descOut.SecurityGroupVpcAssociations, 1, "empty collection is the bug")
	assoc := descOut.SecurityGroupVpcAssociations[0]
	assert.NotEmpty(t, aws.ToString(assoc.GroupOwnerId))
	assert.NotEmpty(t, aws.ToString(assoc.VpcOwnerId))
}

// TestDescribeAddressesAttribute_PtrRecord_RealClient covers
// ModifyAddressAttribute / DescribeAddressesAttribute. Pre-fix, the response
// item rendered an invented "domainName" element -- not a member of the real
// AddressAttribute wire shape at all (ec2@v1.319.1 deserializers.go:75388:
// allocationId/ptrRecord/ptrRecordUpdate/publicIp). A typed client discarded
// the unknown "domainName" key silently and PtrRecord was always empty.
func TestDescribeAddressesAttribute_PtrRecord_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	allocOut, err := client.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{})
	require.NoError(t, err)
	allocationID := aws.ToString(allocOut.AllocationId)

	const domain = "sweep32.example.com"

	modOut, err := client.ModifyAddressAttribute(t.Context(), &ec2sdk.ModifyAddressAttributeInput{
		AllocationId: aws.String(allocationID),
		DomainName:   aws.String(domain),
	})
	require.NoError(t, err)
	require.NotNil(t, modOut.Address)
	assert.Equal(
		t,
		domain,
		aws.ToString(modOut.Address.PtrRecord),
		"ptrRecord was always empty pre-fix (domainName is not a wire member)",
	)
	assert.Equal(
		t,
		aws.ToString(allocOut.PublicIp),
		aws.ToString(modOut.Address.PublicIp),
		"PublicIp was silently dropped from ModifyAddressAttribute's response pre-fix",
	)

	descOut, err := client.DescribeAddressesAttribute(t.Context(), &ec2sdk.DescribeAddressesAttributeInput{
		AllocationIds: []string{allocationID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Addresses, 1, "empty collection is the bug")
	assert.Equal(t, domain, aws.ToString(descOut.Addresses[0].PtrRecord))
}

// TestRouteServerPropagation_StateEnum_RealClient covers
// EnableRouteServerPropagation / GetRouteServerPropagations. Pre-fix, the
// backend set State to "enabled", which is not a member of
// RouteServerPropagationState at all (ec2@v1.319.1 types/enums.go:10717 only
// defines pending/available/deleting). A client parsing the typed
// types.RouteServerPropagationState enum got a value AWS never sends.
func TestRouteServerPropagation_StateEnum_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	rsOut, err := client.CreateRouteServer(t.Context(), &ec2sdk.CreateRouteServerInput{
		AmazonSideAsn: aws.Int64(65000),
		PersistRoutes: types.RouteServerPersistRoutesActionDisable,
	})
	require.NoError(t, err)
	routeServerID := aws.ToString(rsOut.RouteServer.RouteServerId)

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.78.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	rtOut, err := client.CreateRouteTable(t.Context(), &ec2sdk.CreateRouteTableInput{VpcId: aws.String(vpcID)})
	require.NoError(t, err)
	routeTableID := aws.ToString(rtOut.RouteTable.RouteTableId)

	enableOut, err := client.EnableRouteServerPropagation(t.Context(), &ec2sdk.EnableRouteServerPropagationInput{
		RouteServerId: aws.String(routeServerID),
		RouteTableId:  aws.String(routeTableID),
	})
	require.NoError(t, err)
	assert.Equal(
		t, types.RouteServerPropagationStateAvailable, enableOut.RouteServerPropagation.State,
		"State was the invalid value \"enabled\" pre-fix, not a real RouteServerPropagationState member",
	)

	getOut, err := client.GetRouteServerPropagations(t.Context(), &ec2sdk.GetRouteServerPropagationsInput{
		RouteServerId: aws.String(routeServerID),
	})
	require.NoError(t, err)
	require.Len(t, getOut.RouteServerPropagations, 1, "empty collection is the bug")
	assert.Equal(t, types.RouteServerPropagationStateAvailable, getOut.RouteServerPropagations[0].State)
}

// TestGetRouteServerRoutingDatabase_NoInventedRouteServerID_RawBody covers
// GetRouteServerRoutingDatabase. Pre-fix, the response rendered a
// "routeServerId" element that is not a member of
// GetRouteServerRoutingDatabaseOutput at all (ec2@v1.319.1
// api_op_GetRouteServerRoutingDatabase.go:80-96 only defines
// AreRoutesPersisted/NextToken/Routes). A typed client discards unknown keys
// silently, so this needs a raw-body assertion.
func TestGetRouteServerRoutingDatabase_NoInventedRouteServerID_RawBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rs, err := h.Backend.CreateRouteServer(65000, "disabled", 0, false)
	require.NoError(t, err)

	body, err := dispatchHandler(h, url.Values{
		"Action":        []string{"GetRouteServerRoutingDatabase"},
		"RouteServerId": []string{rs.RouteServerID},
		"Version":       []string{"2016-11-15"},
	})
	require.NoError(t, err)
	assert.Contains(t, body, "<GetRouteServerRoutingDatabaseResponse")
	assert.NotContains(t, body, "<routeServerId>", "routeServerId is not a member of the real response shape")
}
