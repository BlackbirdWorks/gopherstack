package ec2_test

import (
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreate_Tags_RoundTrip_LGWInsightsInstance covers batch 4 of gopherstack-wjlrn: the
// Local Gateway family (CreateLocalGatewayRouteTable, CreateLocalGatewayRouteTableVpcAssociation,
// CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation), Network Insights family
// (CreateNetworkInsightsAccessScope, CreateNetworkInsightsPath) and Instance family
// (CreateInstanceConnectEndpoint, CreateInstanceEventWindow, CreateInstanceExportTask)
// never called parseTagSpecification, so TagSpecifications were silently dropped. Each case
// creates a resource with a tag, then confirms the tag comes back on the matching Describe.
func TestCreate_Tags_RoundTrip_LGWInsightsInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "local gateway route table", run: testCreateLocalGatewayRouteTableTags},
		{name: "local gateway route table vpc association", run: testCreateLocalGatewayRouteTableVpcAssociationTags},
		{name: "lgw vif group association", run: testCreateLGWVifGroupAssocTags},
		{name: "network insights access scope", run: testCreateNetworkInsightsAccessScopeTags},
		{name: "network insights path", run: testCreateNetworkInsightsPathTags},
		{name: "instance connect endpoint", run: testCreateInstanceConnectEndpointTags},
		{name: "instance event window", run: testCreateInstanceEventWindowTags},
		{name: "instance export task", run: testCreateInstanceExportTaskTags},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func testCreateLocalGatewayRouteTableTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	lg, err := h.Backend.SeedLocalGateway(ec2.LocalGateway{})
	require.NoError(t, err)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateLocalGatewayRouteTable"},
		"LocalGatewayId":                  []string{lg.LocalGatewayID},
		"Mode":                            []string{"direct-vpc-routing"},
		"TagSpecification.1.ResourceType": []string{"local-gateway-route-table"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<localGatewayRouteTableId>", "</localGatewayRouteTableId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                     []string{"DescribeLocalGatewayRouteTables"},
		"LocalGatewayRouteTableId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateLocalGatewayRouteTableVpcAssociationTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	lg, err := h.Backend.SeedLocalGateway(ec2.LocalGateway{})
	require.NoError(t, err)
	rt, err := h.Backend.CreateLocalGatewayRouteTable(lg.LocalGatewayID, "direct-vpc-routing")
	require.NoError(t, err)
	vpc, err := h.Backend.CreateVpc("10.1.0.0/16", "default")
	require.NoError(t, err)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateLocalGatewayRouteTableVpcAssociation"},
		"LocalGatewayRouteTableId":        []string{rt.LocalGatewayRouteTableID},
		"VpcId":                           []string{vpc.ID},
		"TagSpecification.1.ResourceType": []string{"local-gateway-route-table-vpc-association"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(
		t, createResp, "<localGatewayRouteTableVpcAssociationId>", "</localGatewayRouteTableVpcAssociationId>",
	)
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeLocalGatewayRouteTableVpcAssociations"},
		"LocalGatewayRouteTableVpcAssociationId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateLGWVifGroupAssocTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	lg, err := h.Backend.SeedLocalGateway(ec2.LocalGateway{})
	require.NoError(t, err)
	rt, err := h.Backend.CreateLocalGatewayRouteTable(lg.LocalGatewayID, "direct-vpc-routing")
	require.NoError(t, err)
	group, err := h.Backend.SeedLocalGatewayVirtualInterfaceGroup(ec2.LocalGatewayVirtualInterfaceGroup{
		LocalGatewayID: lg.LocalGatewayID,
	})
	require.NoError(t, err)

	resourceType := "local-gateway-route-table-virtual-interface-group-association"

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                              []string{"CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation"},
		"LocalGatewayRouteTableId":            []string{rt.LocalGatewayRouteTableID},
		"LocalGatewayVirtualInterfaceGroupId": []string{group.LocalGatewayVirtualInterfaceGroupID},
		"TagSpecification.1.ResourceType":     []string{resourceType},
		"TagSpecification.1.Tag.1.Key":        []string{"Name"},
		"TagSpecification.1.Tag.1.Value":      []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(
		t, createResp,
		"<localGatewayRouteTableVirtualInterfaceGroupAssociationId>",
		"</localGatewayRouteTableVirtualInterfaceGroupAssociationId>",
	)
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations"},
		"LocalGatewayRouteTableVirtualInterfaceGroupAssociationId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateNetworkInsightsAccessScopeTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateNetworkInsightsAccessScope"},
		"TagSpecification.1.ResourceType": []string{"network-insights-access-scope"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<networkInsightsAccessScopeId>", "</networkInsightsAccessScopeId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                         []string{"DescribeNetworkInsightsAccessScopes"},
		"NetworkInsightsAccessScopeId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateNetworkInsightsPathTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateNetworkInsightsPath"},
		"Source":                          []string{"eni-source"},
		"Destination":                     []string{"eni-dest"},
		"Protocol":                        []string{"tcp"},
		"TagSpecification.1.ResourceType": []string{"network-insights-path"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<networkInsightsPathId>", "</networkInsightsPathId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                  []string{"DescribeNetworkInsightsPaths"},
		"NetworkInsightsPathId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateInstanceConnectEndpointTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateInstanceConnectEndpoint"},
		"SubnetId":                        []string{"subnet-default"},
		"TagSpecification.1.ResourceType": []string{"instance-connect-endpoint"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<instanceConnectEndpointId>", "</instanceConnectEndpointId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                      []string{"DescribeInstanceConnectEndpoints"},
		"InstanceConnectEndpointId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateInstanceEventWindowTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateInstanceEventWindow"},
		"Name":                            []string{"my-window"},
		"CronExpression":                  []string{"0 4 * * 6,7"},
		"TagSpecification.1.ResourceType": []string{"instance-event-window"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<instanceEventWindowId>", "</instanceEventWindowId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                  []string{"DescribeInstanceEventWindows"},
		"InstanceEventWindowId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateInstanceExportTaskTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	insts, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateInstanceExportTask"},
		"InstanceId":                      []string{insts[0].ID},
		"TargetEnvironment":               []string{"vmware"},
		"ExportToS3.DiskImageFormat":      []string{"VMDK"},
		"ExportToS3.S3Bucket":             []string{"my-bucket"},
		"TagSpecification.1.ResourceType": []string{"export-instance-task"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<exportTaskId>", "</exportTaskId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":         []string{"DescribeExportTasks"},
		"ExportTaskId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}
