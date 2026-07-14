package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- VGW route propagation ---- //nolint:godot // existing issue.
func TestVgwRoutePropagation(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	rt, _ := b.CreateRouteTable("vpc-default")

	t.Run("enable propagation", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.EnableVgwRoutePropagation(rt.ID, "vgw-12345678"))
	})

	t.Run("disable propagation", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DisableVgwRoutePropagation(rt.ID, "vgw-12345678"))
	})

	t.Run("empty IDs return error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.EnableVgwRoutePropagation("", "vgw-123"))
	})
}

// ---- Default credit specification ---- //nolint:godot // existing issue.

// ---- ModifyTransitGateway ---- //nolint:godot // existing issue.
func TestModifyTransitGateway(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	tgw, _ := b.CreateTransitGateway("")

	t.Run("modifies description", func(t *testing.T) {
		require.NoError(t, b.ModifyTransitGateway(tgw.ID, "updated description"))
	})
}

// ---- UpdateSecurityGroupRuleDescriptions ---- //nolint:godot // existing issue.

// TestHandlerDeleteTransitGatewayRouteTable covers handleDeleteTransitGatewayRouteTable.
func TestHandlerDeleteTransitGatewayRouteTable(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	tgw, err := b.CreateTransitGateway("test-tgw")
	require.NoError(t, err)

	rt, err := b.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)

	rec := postForm(
		t,
		h,
		"Action=DeleteTransitGatewayRouteTable&Version=2016-11-15&TransitGatewayRouteTableId="+rt.RouteTableID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteTransitGatewayRouteTableResponse")

	// Not found.
	rec = postForm(
		t,
		h,
		"Action=DeleteTransitGatewayRouteTable&Version=2016-11-15&TransitGatewayRouteTableId=tgw-rtb-notfound",
	)
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// TestHandlerTGWRoutes covers handleCreateTransitGatewayRoute, handleDeleteTransitGatewayRoute,
// handleReplaceTransitGatewayRoute, tgwRouteToItem.

// TestHandlerTGWRoutes covers handleCreateTransitGatewayRoute, handleDeleteTransitGatewayRoute,
// handleReplaceTransitGatewayRoute, tgwRouteToItem.
func TestHandlerTGWRoutes(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	tgw, err := b.CreateTransitGateway("test-tgw")
	require.NoError(t, err)

	rt, err := b.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)
	rtID := rt.RouteTableID

	// Create a route.
	rec := postForm(t, h, "Action=CreateTransitGatewayRoute&Version=2016-11-15"+
		"&TransitGatewayRouteTableId="+rtID+
		"&DestinationCidrBlock=10.100.0.0/16")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateTransitGatewayRouteResponse")

	// Replace route.
	rec = postForm(t, h, "Action=ReplaceTransitGatewayRoute&Version=2016-11-15"+
		"&TransitGatewayRouteTableId="+rtID+
		"&DestinationCidrBlock=10.100.0.0/16")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ReplaceTransitGatewayRouteResponse")

	// Delete route.
	rec = postForm(t, h, "Action=DeleteTransitGatewayRoute&Version=2016-11-15"+
		"&TransitGatewayRouteTableId="+rtID+
		"&DestinationCidrBlock=10.100.0.0/16")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteTransitGatewayRouteResponse")
}

// TestHandlerTGWRouteTableAssociation covers handleAssociateTransitGatewayRouteTable
// and handleDisassociateTransitGatewayRouteTable.

// TestHandlerTGWRouteTableAssociation covers handleAssociateTransitGatewayRouteTable
// and handleDisassociateTransitGatewayRouteTable.
func TestHandlerTGWRouteTableAssociation(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	tgw, err := b.CreateTransitGateway("test-tgw")
	require.NoError(t, err)

	rt, err := b.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)
	rtID := rt.RouteTableID

	vpc, err := b.CreateVpc("10.7.0.0/16")
	require.NoError(t, err)

	attach, err := b.CreateTransitGatewayVpcAttachment(tgw.ID, vpc.ID, []string{})
	require.NoError(t, err)
	attachID := attach.TransitGatewayAttachmentID

	// Associate.
	rec := postForm(t, h, "Action=AssociateTransitGatewayRouteTable&Version=2016-11-15"+
		"&TransitGatewayRouteTableId="+rtID+
		"&TransitGatewayAttachmentId="+attachID)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AssociateTransitGatewayRouteTableResponse")

	// Disassociate.
	rec = postForm(t, h, "Action=DisassociateTransitGatewayRouteTable&Version=2016-11-15"+
		"&TransitGatewayRouteTableId="+rtID+
		"&TransitGatewayAttachmentId="+attachID)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DisassociateTransitGatewayRouteTableResponse")
}

// TestHandlerModifyTransitGatewayAttribute covers handleModifyTransitGatewayAttribute.

// TestHandlerModifyTransitGatewayAttribute covers handleModifyTransitGatewayAttribute.
func TestHandlerModifyTransitGatewayAttribute(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	tgw, err := b.CreateTransitGateway("original-desc")
	require.NoError(t, err)

	rec := postForm(t, h, "Action=ModifyTransitGatewayAttribute&Version=2016-11-15"+
		"&TransitGatewayId="+tgw.ID+
		"&Description=updated-desc")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ModifyTransitGatewayAttributeResponse")
}

// TestHandlerDeleteFlowLogs covers handleDeleteFlowLogs.

// TestHandlerDescribeTransitGatewaysAndDelete covers handleDescribeTransitGateways
// and handleDeleteTransitGateway.
func TestHandlerDescribeTransitGatewaysAndDelete(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	tgw, err := b.CreateTransitGateway("test-describe-tgw")
	require.NoError(t, err)

	// Describe with ID filter.
	rec := postForm(
		t,
		h,
		"Action=DescribeTransitGateways&Version=2016-11-15&TransitGatewayIds.TransitGatewayId.1="+tgw.ID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeTransitGatewaysResponse")
	assert.Contains(t, rec.Body.String(), tgw.ID)

	// Describe all (no ID filter).
	rec = postForm(t, h, "Action=DescribeTransitGateways&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete.
	rec = postForm(t, h, "Action=DeleteTransitGateway&Version=2016-11-15&TransitGatewayId="+tgw.ID)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteTransitGatewayResponse")

	// Delete not found.
	rec = postForm(
		t,
		h,
		"Action=DeleteTransitGateway&Version=2016-11-15&TransitGatewayId=tgw-notfound",
	)
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// extractXMLField extracts a simple XML element value from a string.
// It looks for <tag>value</tag> patterns.

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
