package ec2_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandlerDeleteEgressOnlyInternetGateway covers handleDeleteEgressOnlyInternetGateway.
func TestHandlerDeleteEgressOnlyInternetGateway(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// First create a VPC and IGW.
	rec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.5.0.0/16")
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	// Extract vpc ID from response.
	vpcID := extractXMLField(body, "vpcId")
	require.NotEmpty(t, vpcID)

	rec = postForm(t, h, "Action=CreateEgressOnlyInternetGateway&Version=2016-11-15&VpcId="+vpcID)
	require.Equal(t, http.StatusOK, rec.Code)
	igwID := extractXMLField(rec.Body.String(), "egressOnlyInternetGatewayId")
	require.NotEmpty(t, igwID)

	// Delete it.
	rec = postForm(
		t,
		h,
		"Action=DeleteEgressOnlyInternetGateway&Version=2016-11-15&EgressOnlyInternetGatewayId="+igwID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteEgressOnlyInternetGatewayResponse")

	// Delete not found.
	rec = postForm(
		t,
		h,
		"Action=DeleteEgressOnlyInternetGateway&Version=2016-11-15&EgressOnlyInternetGatewayId=eigw-notfound",
	)
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// TestHandlerReplaceRouteTableAssociation covers handleReplaceRouteTableAssociation
// and ReplaceRouteTableAssociation backend.

// TestHandlerReplaceRouteTableAssociation covers handleReplaceRouteTableAssociation
// and ReplaceRouteTableAssociation backend.
func TestHandlerReplaceRouteTableAssociation(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	// Create a VPC, subnet, and two route tables.
	vpc, err := b.CreateVpc("10.6.0.0/16")
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, "10.6.1.0/24", "us-east-1a")
	require.NoError(t, err)

	rt1, err := b.CreateRouteTable(vpc.ID)
	require.NoError(t, err)

	rt2, err := b.CreateRouteTable(vpc.ID)
	require.NoError(t, err)

	// Associate subnet with rt1.
	assocID, err := b.AssociateRouteTable(rt1.ID, subnet.ID)
	require.NoError(t, err)
	require.NotEmpty(t, assocID)

	// Replace association to point at rt2 via handler.
	rec := postForm(
		t,
		h,
		"Action=ReplaceRouteTableAssociation&Version=2016-11-15&AssociationId="+assocID+"&RouteTableId="+rt2.ID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ReplaceRouteTableAssociationResponse")

	// Missing params.
	rec = postForm(t, h, "Action=ReplaceRouteTableAssociation&Version=2016-11-15")
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// TestHandlerDeleteTransitGatewayRouteTable covers handleDeleteTransitGatewayRouteTable.

// extractXMLField extracts a simple XML element value from a string.
// It looks for <tag>value</tag> patterns.
func extractXMLField(body, field string) string {
	open := "<" + field + ">"
	closeTag := "</" + field + ">"

	start := indexOf(body, open)
	if start < 0 {
		return ""
	}

	valStart := start + len(open)
	end := indexOf(body[valStart:], closeTag)

	if end < 0 {
		return ""
	}

	return body[valStart : valStart+end]
}
