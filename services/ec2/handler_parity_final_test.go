package ec2_test

// EC2 final parity sweep (gopherstack-5o9) HTTP-level accuracy tests: verifies
// the 22 de-stubbed ops return AWS-accurate wire shapes through the full
// dispatch path, not just correct backend behavior.

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestParityFinalHTTP_ModifyVerifiedAccessGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	inst, err := h.Backend.CreateVerifiedAccessInstance("inst")
	require.NoError(t, err)
	grp, err := h.Backend.CreateVerifiedAccessGroup(inst.VerifiedAccessInstanceID, "orig")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                {"ModifyVerifiedAccessGroup"},
		"VerifiedAccessGroupId": {grp.VerifiedAccessGroupID},
		"Description":           {"updated"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<ModifyVerifiedAccessGroupResponse>")
	assert.Contains(t, resp, "<description>updated</description>")
	assert.Contains(t, resp, "<verifiedAccessGroupId>"+grp.VerifiedAccessGroupID+"</verifiedAccessGroupId>")
}

func TestParityFinalHTTP_ModifyVerifiedAccessInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	inst, err := h.Backend.CreateVerifiedAccessInstance("orig")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                   {"ModifyVerifiedAccessInstance"},
		"VerifiedAccessInstanceId": {inst.VerifiedAccessInstanceID},
		"Description":              {"updated"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<ModifyVerifiedAccessInstanceResponse>")
	assert.Contains(t, resp, "<description>updated</description>")
}

func TestParityFinalHTTP_ModifyVerifiedAccessTrustProvider(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tp, err := h.Backend.CreateVerifiedAccessTrustProvider("user", "orig")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                        {"ModifyVerifiedAccessTrustProvider"},
		"VerifiedAccessTrustProviderId": {tp.VerifiedAccessTrustProviderID},
		"Description":                   {"updated"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<ModifyVerifiedAccessTrustProviderResponse>")
	assert.Contains(t, resp, "<description>updated</description>")

	// Not-found surfaces as a 400, not a 500 InternalFailure.
	rec := postForm(t, h, "Action=ModifyVerifiedAccessTrustProvider&VerifiedAccessTrustProviderId=vatp-missing")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidVerifiedAccessTrustProviderId.NotFound")
}

func TestParityFinalHTTP_TransitGatewayRoutePropagation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tgw, err := h.Backend.CreateTransitGateway("tgw")
	require.NoError(t, err)
	rt, err := h.Backend.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)
	att, err := h.Backend.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-default", nil)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                     {"EnableTransitGatewayRouteTablePropagation"},
		"TransitGatewayRouteTableId": {rt.RouteTableID},
		"TransitGatewayAttachmentId": {att.TransitGatewayAttachmentID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<EnableTransitGatewayRouteTablePropagationResponse>")
	assert.Contains(t, resp, "<state>enabled</state>")

	resp, err = ec2.ExportDispatch(h, url.Values{
		"Action":                     {"DisableTransitGatewayRouteTablePropagation"},
		"TransitGatewayRouteTableId": {rt.RouteTableID},
		"TransitGatewayAttachmentId": {att.TransitGatewayAttachmentID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<DisableTransitGatewayRouteTablePropagationResponse>")
	assert.Contains(t, resp, "<state>disabled</state>")
}

func TestParityFinalHTTP_DescribeTransitGatewayAttachments(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tgw, err := h.Backend.CreateTransitGateway("tgw")
	require.NoError(t, err)

	_, err = h.Backend.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-default", nil)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{"Action": {"DescribeTransitGatewayAttachments"}})
	require.NoError(t, err)
	assert.Contains(t, resp, "<DescribeTransitGatewayAttachmentsResponse")
	assert.Contains(t, resp, "<resourceType>vpc</resourceType>")
}

func TestParityFinalHTTP_InterruptibleCapacityReservationAllocation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	cr, err := h.Backend.CreateCapacityReservation("m5.large", "us-east-1a", 10)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                {"CreateInterruptibleCapacityReservationAllocation"},
		"CapacityReservationId": {cr.CapacityReservationID},
		"InstanceCount":         {"3"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<CreateInterruptibleCapacityReservationAllocationResponse>")
	assert.Contains(t, resp, "<targetInstanceCount>3</targetInstanceCount>")
	assert.Contains(t, resp, "<status>active</status>")

	resp, err = ec2.ExportDispatch(h, url.Values{
		"Action":                {"UpdateInterruptibleCapacityReservationAllocation"},
		"CapacityReservationId": {cr.CapacityReservationID},
		"TargetInstanceCount":   {"5"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<UpdateInterruptibleCapacityReservationAllocationResponse>")
	assert.Contains(t, resp, "<targetInstanceCount>5</targetInstanceCount>")
}

func TestParityFinalHTTP_GetCapacityReservationUsage(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	cr, err := h.Backend.CreateCapacityReservation("m5.large", "us-east-1a", 10)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                {"GetCapacityReservationUsage"},
		"CapacityReservationId": {cr.CapacityReservationID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<GetCapacityReservationUsageResponse>")
	assert.Contains(t, resp, "<availableInstanceCount>10</availableInstanceCount>")
	assert.Contains(t, resp, "<totalInstanceCount>10</totalInstanceCount>")
}

func TestParityFinalHTTP_DescribeCapacityReservationTopology(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	cr, err := h.Backend.CreateCapacityReservation("m5.large", "us-east-1a", 10)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{"Action": {"DescribeCapacityReservationTopology"}})
	require.NoError(t, err)
	assert.Contains(t, resp, "<DescribeCapacityReservationTopologyResponse>")
	assert.Contains(t, resp, "<capacityReservationId>"+cr.CapacityReservationID+"</capacityReservationId>")
}

func TestParityFinalHTTP_MoveAddressToVpcAndDescribeMovingAddresses(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	addr, err := h.Backend.AllocateAddress()
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":   {"MoveAddressToVpc"},
		"PublicIp": {addr.PublicIP},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<MoveAddressToVpcResponse>")
	assert.Contains(t, resp, "<status>MoveInProgress</status>")

	resp, err = ec2.ExportDispatch(h, url.Values{"Action": {"DescribeMovingAddresses"}})
	require.NoError(t, err)
	assert.Contains(t, resp, "<DescribeMovingAddressesResponse>")
	assert.Contains(t, resp, "<publicIp>"+addr.PublicIP+"</publicIp>")
	assert.Contains(t, resp, "<moveStatus>movingToVpc</moveStatus>")
}

func TestParityFinalHTTP_RejectVpcEndpointConnections(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	_, err := h.Backend.AcceptVpcEndpointConnections("vpce-svc-1", []string{"vpce-1"})
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"RejectVpcEndpointConnections"},
		"ServiceId":       {"vpce-svc-1"},
		"VpcEndpointId.1": {"vpce-1"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<RejectVpcEndpointConnectionsResponse")

	conns := h.Backend.DescribeVpcEndpointConnections([]string{"vpce-svc-1"})
	require.Len(t, conns, 1)
	assert.Equal(t, "rejected", conns[0].State)
}

func TestParityFinalHTTP_UnassignPrivateNatGatewayAddress(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	addr, err := h.Backend.AllocateAddress()
	require.NoError(t, err)
	nat, err := h.Backend.CreateNatGateway("subnet-default", addr.AllocationID)
	require.NoError(t, err)
	require.NoError(t, h.Backend.AssignPrivateNatGatewayAddress(nat.ID))

	described := h.Backend.DescribeNatGateways([]string{nat.ID})
	require.Len(t, described, 1)
	require.Len(t, described[0].SecondaryPrivateIPs, 1)
	secondary := described[0].SecondaryPrivateIPs[0]

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":             {"UnassignPrivateNatGatewayAddress"},
		"NatGatewayId":       {nat.ID},
		"PrivateIpAddress.1": {secondary},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<UnassignPrivateNatGatewayAddressResponse")
	assert.NotContains(t, resp, "<privateIp>"+secondary+"</privateIp>")
}

func TestParityFinalHTTP_CancelImageLaunchPermission(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":  {"CancelImageLaunchPermission"},
		"ImageId": {"ami-testimg"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<CancelImageLaunchPermissionResponse>")
	assert.Contains(t, resp, "<return>true</return>")
}

func TestParityFinalHTTP_DescribeImageReferences(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	instances, err := h.Backend.RunInstances("ami-refimg", "t3.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, instances, 1)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":    {"DescribeImageReferences"},
		"ImageId.1": {"ami-refimg"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<DescribeImageReferencesResponse")
	assert.Contains(t, resp, "<imageId>ami-refimg</imageId>")
	assert.Contains(t, resp, "<resourceType>ec2:Instance</resourceType>")
}

func TestParityFinalHTTP_GetImageAncestry(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	instances, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	root, err := h.Backend.CreateImage(instances[0].ID, "root", "root image")
	require.NoError(t, err)
	child, err := h.Backend.CopyImage(root.ImageID, "child", "child image")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":  {"GetImageAncestry"},
		"ImageId": {child.ImageID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<GetImageAncestryResponse")
	assert.Contains(t, resp, "<sourceImageId>"+root.ImageID+"</sourceImageId>")
}

func TestParityFinalHTTP_GetFlowLogsIntegrationTemplate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	fls, err := h.Backend.CreateFlowLogs([]string{"vpc-default"}, "ALL", "s3", "arn:aws:s3:::dest")
	require.NoError(t, err)
	require.Len(t, fls, 1)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                         {"GetFlowLogsIntegrationTemplate"},
		"FlowLogId":                      {fls[0].FlowLogID},
		"ConfigDeliveryS3DestinationArn": {"arn:aws:s3:::cfn-bucket"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<GetFlowLogsIntegrationTemplateResponse>")
	assert.Contains(t, resp, fls[0].FlowLogID)
}

func TestParityFinalHTTP_GetSpotPlacementScores(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":         {"GetSpotPlacementScores"},
		"InstanceType.1": {"m5.large"},
		"InstanceType.2": {"m5.xlarge"},
		"TargetCapacity": {"10"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<GetSpotPlacementScoresResponse")
	assert.Contains(t, resp, "<region>")
	assert.Contains(t, resp, "<score>")
}

func TestParityFinalHTTP_SendDiagnosticInterrupt(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	instances, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":     {"SendDiagnosticInterrupt"},
		"InstanceId": {instances[0].ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<SendDiagnosticInterruptResponse")

	rec := postForm(t, h, "Action=SendDiagnosticInterrupt&InstanceId=i-missing")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidInstanceID.NotFound")
}

func TestParityFinalHTTP_EnableReachabilityAnalyzerOrganizationSharing(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp, err := ec2.ExportDispatch(h, url.Values{"Action": {"EnableReachabilityAnalyzerOrganizationSharing"}})
	require.NoError(t, err)
	assert.Contains(t, resp, "<EnableReachabilityAnalyzerOrganizationSharingResponse>")
	assert.Contains(t, resp, "<returnValue>true</returnValue>")
}

func TestParityFinalHTTP_DescribeElasticGpus(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp, err := ec2.ExportDispatch(h, url.Values{"Action": {"DescribeElasticGpus"}})
	require.NoError(t, err)
	assert.Contains(t, resp, "<DescribeElasticGpusResponse>")
	assert.Contains(t, resp, "<elasticGpuSet>")
}

// TestParityFinalHTTP_AllOpsReturn200 is a broad smoke test verifying every
// de-stubbed op returns 200 OK through the full HTTP handler for a minimal
// (frequently zero-value/empty) request, mirroring the coverage the retired
// registerStubOps registration used to get for free.
func TestParityFinalHTTP_AllOpsReturn200(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	cr, err := h.Backend.CreateCapacityReservation("m5.large", "us-east-1a", 10)
	require.NoError(t, err)
	addr, err := h.Backend.AllocateAddress()
	require.NoError(t, err)
	instances, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)

	tests := []struct {
		vals url.Values
		name string
	}{
		{name: "DescribeMovingAddresses", vals: url.Values{"Action": {"DescribeMovingAddresses"}}},
		{
			name: "DescribeCapacityReservationTopology",
			vals: url.Values{"Action": {"DescribeCapacityReservationTopology"}},
		},
		{
			name: "DescribeTransitGatewayAttachments",
			vals: url.Values{"Action": {"DescribeTransitGatewayAttachments"}},
		},
		{name: "DescribeElasticGpus", vals: url.Values{"Action": {"DescribeElasticGpus"}}},
		{name: "DescribeImageReferences", vals: url.Values{
			"Action": {"DescribeImageReferences"}, "ImageId.1": {"ami-test"},
		}},
		{name: "GetSpotPlacementScores", vals: url.Values{
			"Action": {"GetSpotPlacementScores"}, "InstanceType.1": {"m5.large"}, "TargetCapacity": {"1"},
		}},
		{name: "SendDiagnosticInterrupt", vals: url.Values{
			"Action": {"SendDiagnosticInterrupt"}, "InstanceId": {instances[0].ID},
		}},
		{name: "EnableReachabilityAnalyzerOrganizationSharing", vals: url.Values{
			"Action": {"EnableReachabilityAnalyzerOrganizationSharing"},
		}},
		{name: "GetCapacityReservationUsage", vals: url.Values{
			"Action": {"GetCapacityReservationUsage"}, "CapacityReservationId": {cr.CapacityReservationID},
		}},
		{name: "CancelImageLaunchPermission", vals: url.Values{
			"Action": {"CancelImageLaunchPermission"}, "ImageId": {"ami-test"},
		}},
		{name: "MoveAddressToVpc", vals: url.Values{"Action": {"MoveAddressToVpc"}, "PublicIp": {addr.PublicIP}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := postForm(t, h, tt.vals.Encode())
			assert.Equal(t, http.StatusOK, rec.Code, "action %s should return 200", tt.name)
		})
	}
}
