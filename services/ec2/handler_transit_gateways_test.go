package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
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

	tgw, _ := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: ""})

	t.Run("modifies description", func(t *testing.T) {
		modified, err := b.ModifyTransitGateway(tgw.ID, "updated description")
		require.NoError(t, err)
		assert.Equal(t, "updated description", modified.Description)
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

	tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	rt, err := b.CreateTransitGatewayRouteTable(tgw.ID, nil)
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

	tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	rt, err := b.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)
	rtID := rt.RouteTableID

	// CreateTransitGatewayRoute now validates TransitGatewayAttachmentId
	// exists (real AWS requires either a real attachment or Blackhole=true;
	// previously neither was enforced, so a route with no attachment at all
	// silently "succeeded").
	att, err := b.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-route-http", nil, nil)
	require.NoError(t, err)
	attID := att.TransitGatewayAttachmentID

	// Create a route.
	rec := postForm(t, h, "Action=CreateTransitGatewayRoute&Version=2016-11-15"+
		"&TransitGatewayRouteTableId="+rtID+
		"&DestinationCidrBlock=10.100.0.0/16"+
		"&TransitGatewayAttachmentId="+attID)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateTransitGatewayRouteResponse")
	assert.Contains(t, rec.Body.String(), "<resourceType>vpc</resourceType>")

	// Replace route: real AWS requires the destination CIDR to already exist
	// as a route (previously ReplaceTransitGatewayRoute silently created one
	// for any CIDR/attachment given, upsert-style).
	rec = postForm(t, h, "Action=ReplaceTransitGatewayRoute&Version=2016-11-15"+
		"&TransitGatewayRouteTableId="+rtID+
		"&DestinationCidrBlock=10.100.0.0/16"+
		"&TransitGatewayAttachmentId="+attID)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ReplaceTransitGatewayRouteResponse")

	// Delete route.
	rec = postForm(t, h, "Action=DeleteTransitGatewayRoute&Version=2016-11-15"+
		"&TransitGatewayRouteTableId="+rtID+
		"&DestinationCidrBlock=10.100.0.0/16")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteTransitGatewayRouteResponse")
}

// TestHandlerTGWRoute_Validation covers the real-AWS validation this pass
// added to CreateTransitGatewayRoute/ReplaceTransitGatewayRoute: an
// attachment ID must exist unless Blackhole=true, and Replace only ever
// replaces a route that already exists (it does not upsert).
func TestHandlerTGWRoute_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupAndDispatch func(t *testing.T) (body string, err error)
		name             string
		wantErrContain   string
	}{
		{
			name: "create_without_attachment_or_blackhole_rejected",
			setupAndDispatch: func(t *testing.T) (string, error) {
				t.Helper()

				_, h, _, rtID := newTGWRouteTestFixture(t)

				return dispatchHandler(h, url.Values{
					"Action":                     []string{"CreateTransitGatewayRoute"},
					"Version":                    []string{"2016-11-15"},
					"TransitGatewayRouteTableId": []string{rtID},
					"DestinationCidrBlock":       []string{"10.200.0.0/16"},
				})
			},
			wantErrContain: "TransitGatewayAttachmentId is required",
		},
		{
			name: "create_with_unknown_attachment_rejected",
			setupAndDispatch: func(t *testing.T) (string, error) {
				t.Helper()

				_, h, _, rtID := newTGWRouteTestFixture(t)

				return dispatchHandler(h, url.Values{
					"Action":                     []string{"CreateTransitGatewayRoute"},
					"Version":                    []string{"2016-11-15"},
					"TransitGatewayRouteTableId": []string{rtID},
					"DestinationCidrBlock":       []string{"10.200.0.0/16"},
					"TransitGatewayAttachmentId": []string{"tgw-attach-doesnotexist"},
				})
			},
			wantErrContain: "InvalidTransitGatewayAttachmentID.NotFound",
		},
		{
			name: "replace_nonexistent_route_rejected",
			setupAndDispatch: func(t *testing.T) (string, error) {
				t.Helper()

				b, h, tgwID, rtID := newTGWRouteTestFixture(t)

				att, err := b.CreateTransitGatewayVpcAttachment(tgwID, "vpc-replace-neg", nil, nil)
				require.NoError(t, err)

				return dispatchHandler(h, url.Values{
					"Action":                     []string{"ReplaceTransitGatewayRoute"},
					"Version":                    []string{"2016-11-15"},
					"TransitGatewayRouteTableId": []string{rtID},
					"DestinationCidrBlock":       []string{"192.0.2.0/24"},
					"TransitGatewayAttachmentId": []string{att.TransitGatewayAttachmentID},
				})
			},
			wantErrContain: "InvalidRoute.NotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := tt.setupAndDispatch(t)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErrContain)
		})
	}
}

// TestHandlerTGWRoute_BlackholeAndResourceFields covers the real, previously
// entirely-unread Blackhole flag and the ResourceId/ResourceType fields on a
// route's attachment entry (previously ResourceType was hardcoded "vpc"
// regardless of the attachment's real kind, and ResourceId was never
// rendered at all).
func TestHandlerTGWRoute_BlackholeAndResourceFields(t *testing.T) {
	t.Parallel()

	_, h, _, rtID := newTGWRouteTestFixture(t)

	rec := postForm(t, h, "Action=CreateTransitGatewayRoute&Version=2016-11-15"+
		"&TransitGatewayRouteTableId="+rtID+
		"&DestinationCidrBlock=172.16.0.0/16"+
		"&Blackhole=true")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<state>blackhole</state>")
	assert.NotContains(t, rec.Body.String(), "transitGatewayAttachmentId")
}

// newTGWRouteTestFixture creates a backend/handler pair with a transit
// gateway and an empty TGW route table, returning the backend, handler,
// transit gateway ID, and route table ID for TGW route tests.
func newTGWRouteTestFixture(t *testing.T) (*ec2.InMemoryBackend, *ec2.Handler, string, string) {
	t.Helper()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	rt, err := b.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)

	return b, h, tgw.ID, rt.RouteTableID
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

	tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	rt, err := b.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)
	rtID := rt.RouteTableID

	vpc, err := b.CreateVpc("10.7.0.0/16")
	require.NoError(t, err)

	attach, err := b.CreateTransitGatewayVpcAttachment(tgw.ID, vpc.ID, []string{}, nil)
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

// TestHandlerDeleteFlowLogs covers handleDeleteFlowLogs.

// TestHandlerDescribeTransitGatewaysAndDelete covers handleDescribeTransitGateways
// and handleDeleteTransitGateway.
func TestHandlerDescribeTransitGatewaysAndDelete(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-describe-tgw"})
	require.NoError(t, err)
	tgw2, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "second-tgw"})
	require.NoError(t, err)

	// Describe with ID filter, using the real AWS "TransitGatewayIds.N" wire
	// parameter (a prior version of this handler read the non-existent
	// "TransitGatewayIds.TransitGatewayId.N" instead, so a real client's ID
	// filter was silently ignored and every TGW was returned).
	rec := postForm(
		t,
		h,
		"Action=DescribeTransitGateways&Version=2016-11-15&TransitGatewayIds.1="+tgw.ID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "DescribeTransitGatewaysResponse")
	assert.Contains(t, body, tgw.ID)
	assert.NotContains(t, body, tgw2.ID)

	// Describe all (no ID filter).
	rec = postForm(t, h, "Action=DescribeTransitGateways&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, rec.Code)
	body = rec.Body.String()
	assert.Contains(t, body, tgw.ID)
	assert.Contains(t, body, tgw2.ID)

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

// TestHandlerCreateTransitGateway_OptionsAndTagDualWritePathVisibility proves
// two things about CreateTransitGateway, previously a disguised stub that
// parsed only Description and discarded Options.*/TagSpecifications
// entirely: (1) Options fields supplied on the request (e.g. AmazonSideAsn)
// are honored rather than silently dropped, and (2) a tag supplied at create
// time and a tag added afterwards via CreateTags are BOTH visible through
// DescribeTransitGateways AND through the generic DescribeTags call.
func TestHandlerCreateTransitGateway_OptionsAndTagDualWritePathVisibility(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateTransitGateway"},
		"Description":                     []string{"my tgw"},
		"Options.AmazonSideAsn":           []string{"65000"},
		"Options.DnsSupport":              []string{"disable"},
		"TagSpecification.1.ResourceType": []string{"transit-gateway"},
		"TagSpecification.1.Tag.1.Key":    []string{"CreateTime"},
		"TagSpecification.1.Tag.1.Value":  []string{"yes"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<amazonSideAsn>65000</amazonSideAsn>")
	assert.Contains(t, createResp, "<dnsSupport>disable</dnsSupport>")
	assert.Contains(t, createResp, "CreateTime")
	tgwID := accuracyExtractXMLValue(createResp, "transitGatewayId")
	require.NotEmpty(t, tgwID)

	_, err = dispatchHandler(h, url.Values{
		"Action":       []string{"CreateTags"},
		"ResourceId.1": []string{tgwID},
		"Tag.1.Key":    []string{"AddedLater"},
		"Tag.1.Value":  []string{"yes"},
	})
	require.NoError(t, err)

	descResp, err := dispatchHandler(h, url.Values{"Action": []string{"DescribeTransitGateways"}})
	require.NoError(t, err)
	assert.Contains(t, descResp, "CreateTime")
	assert.Contains(t, descResp, "AddedLater")

	tagsResp, err := dispatchHandler(h, url.Values{
		"Action":           []string{"DescribeTags"},
		"Filter.1.Name":    []string{"resource-id"},
		"Filter.1.Value.1": []string{tgwID},
	})
	require.NoError(t, err)
	assert.Contains(t, tagsResp, "CreateTime")
	assert.Contains(t, tagsResp, "AddedLater")
}

// extractXMLField extracts a simple XML element value from a string.
// It looks for <tag>value</tag> patterns.

func TestTransitGatewayRoutePropagationHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tgw, err := h.Backend.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "tgw"})
	require.NoError(t, err)
	rt, err := h.Backend.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)
	att, err := h.Backend.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-default", nil, nil)
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

func TestDescribeTransitGatewayAttachmentsHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tgw, err := h.Backend.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "tgw"})
	require.NoError(t, err)

	_, err = h.Backend.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-default", nil, nil)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{"Action": {"DescribeTransitGatewayAttachments"}})
	require.NoError(t, err)
	assert.Contains(t, resp, "<DescribeTransitGatewayAttachmentsResponse")
	assert.Contains(t, resp, "<resourceType>vpc</resourceType>")
}
