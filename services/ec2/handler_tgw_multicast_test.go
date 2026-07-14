package ec2_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// extractTag pulls the text content of the first <tag>...</tag> occurrence in body.
func extractTag(t *testing.T, body, tag string) string {
	t.Helper()

	open := "<" + tag + ">"
	closeTag := "</" + tag + ">"

	start := indexOf(body, open)
	require.GreaterOrEqual(t, start, 0, "expected to find %s in body:\n%s", open, body)
	start += len(open)

	end := indexOf(body[start:], closeTag)
	require.GreaterOrEqual(t, end, 0, "expected to find %s in body:\n%s", closeTag, body)

	return body[start : start+end]
}

func TestTGWMulticastHandler_DomainLifecycle(t *testing.T) {
	t.Parallel()

	h := newHandler()

	tgwRec := postForm(t, h, "Action=CreateTransitGateway&Version=2016-11-15")
	require.Equal(t, http.StatusOK, tgwRec.Code)
	tgwID := extractTag(t, tgwRec.Body.String(), "transitGatewayId")

	createRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayMulticastDomain&Version=2016-11-15&TransitGatewayId=%s"+
			"&Options.Igmpv2Support=enable",
		tgwID,
	))
	require.Equal(t, http.StatusOK, createRec.Code)

	createBody := createRec.Body.String()
	assert.Contains(t, createBody, "<CreateTransitGatewayMulticastDomainResponse")
	assert.Contains(t, createBody, "<igmpv2Support>enable</igmpv2Support>")
	domainID := extractTag(t, createBody, "transitGatewayMulticastDomainId")
	assert.Contains(t, domainID, "tgw-mcast-domain-")

	descRec := postForm(t, h, "Action=DescribeTransitGatewayMulticastDomains&Version=2016-11-15")
	require.Equal(t, http.StatusOK, descRec.Code)
	assert.Contains(t, descRec.Body.String(), domainID)

	assocRec := postForm(t, h, fmt.Sprintf(
		"Action=AssociateTransitGatewayMulticastDomain&Version=2016-11-15"+
			"&TransitGatewayMulticastDomainId=%s&TransitGatewayAttachmentId=tgw-attach-1"+
			"&SubnetIds.1=subnet-1",
		domainID,
	))
	require.Equal(t, http.StatusOK, assocRec.Code)
	assocBody := assocRec.Body.String()
	assert.Contains(t, assocBody, "<subnetId>subnet-1</subnetId>")
	assert.Contains(t, assocBody, "<state>associated</state>")

	getAssocRec := postForm(t, h, fmt.Sprintf(
		"Action=GetTransitGatewayMulticastDomainAssociations&Version=2016-11-15"+
			"&TransitGatewayMulticastDomainId=%s",
		domainID,
	))
	require.Equal(t, http.StatusOK, getAssocRec.Code)
	assert.Contains(t, getAssocRec.Body.String(), "<subnetId>subnet-1</subnetId>")

	disassocRec := postForm(t, h, fmt.Sprintf(
		"Action=DisassociateTransitGatewayMulticastDomain&Version=2016-11-15"+
			"&TransitGatewayMulticastDomainId=%s&TransitGatewayAttachmentId=tgw-attach-1"+
			"&SubnetIds.1=subnet-1",
		domainID,
	))
	require.Equal(t, http.StatusOK, disassocRec.Code)
	assert.Contains(t, disassocRec.Body.String(), "<state>disassociated</state>")

	deleteRec := postForm(t, h, fmt.Sprintf(
		"Action=DeleteTransitGatewayMulticastDomain&Version=2016-11-15"+
			"&TransitGatewayMulticastDomainId=%s",
		domainID,
	))
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Contains(t, deleteRec.Body.String(), "<state>deleted</state>")

	// Errors surface as EC2-shaped error responses, not 200s.
	errRec := postForm(t, h, "Action=CreateTransitGatewayMulticastDomain&Version=2016-11-15")
	assert.NotEqual(t, http.StatusOK, errRec.Code)
}

func TestTGWMulticastHandler_GroupMembersAndSources(t *testing.T) {
	t.Parallel()

	h := newHandler()

	tgwRec := postForm(t, h, "Action=CreateTransitGateway&Version=2016-11-15")
	tgwID := extractTag(t, tgwRec.Body.String(), "transitGatewayId")

	createRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayMulticastDomain&Version=2016-11-15&TransitGatewayId=%s",
		tgwID,
	))
	domainID := extractTag(t, createRec.Body.String(), "transitGatewayMulticastDomainId")

	regRec := postForm(t, h, fmt.Sprintf(
		"Action=RegisterTransitGatewayMulticastGroupMembers&Version=2016-11-15"+
			"&TransitGatewayMulticastDomainId=%s&GroupIpAddress=224.0.0.1"+
			"&NetworkInterfaceIds.1=eni-1",
		domainID,
	))
	require.Equal(t, http.StatusOK, regRec.Code)
	regBody := regRec.Body.String()
	assert.Contains(t, regBody, "<registeredMulticastGroupMembers>")
	assert.Contains(t, regBody, "<item>eni-1</item>")

	searchRec := postForm(t, h, fmt.Sprintf(
		"Action=SearchTransitGatewayMulticastGroups&Version=2016-11-15"+
			"&TransitGatewayMulticastDomainId=%s",
		domainID,
	))
	require.Equal(t, http.StatusOK, searchRec.Code)
	searchBody := searchRec.Body.String()
	assert.Contains(t, searchBody, "<groupIpAddress>224.0.0.1</groupIpAddress>")
	assert.Contains(t, searchBody, "<groupMember>true</groupMember>")

	deregRec := postForm(t, h, fmt.Sprintf(
		"Action=DeregisterTransitGatewayMulticastGroupMembers&Version=2016-11-15"+
			"&TransitGatewayMulticastDomainId=%s&GroupIpAddress=224.0.0.1"+
			"&NetworkInterfaceIds.1=eni-1",
		domainID,
	))
	require.Equal(t, http.StatusOK, deregRec.Code)
	assert.Contains(t, deregRec.Body.String(), "<deregisteredMulticastGroupMembers>")

	afterRec := postForm(t, h, fmt.Sprintf(
		"Action=SearchTransitGatewayMulticastGroups&Version=2016-11-15"+
			"&TransitGatewayMulticastDomainId=%s",
		domainID,
	))
	require.Equal(t, http.StatusOK, afterRec.Code)
	assert.NotContains(t, afterRec.Body.String(), "<groupIpAddress>224.0.0.1</groupIpAddress>")
}

func TestTGWMulticastHandler_MeteringPolicyLifecycle(t *testing.T) {
	t.Parallel()

	h := newHandler()

	tgwRec := postForm(t, h, "Action=CreateTransitGateway&Version=2016-11-15")
	tgwID := extractTag(t, tgwRec.Body.String(), "transitGatewayId")

	createRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayMeteringPolicy&Version=2016-11-15&TransitGatewayId=%s",
		tgwID,
	))
	require.Equal(t, http.StatusOK, createRec.Code)
	policyID := extractTag(t, createRec.Body.String(), "transitGatewayMeteringPolicyId")
	assert.Contains(t, policyID, "tgw-metering-policy-")

	entryRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayMeteringPolicyEntry&Version=2016-11-15"+
			"&TransitGatewayMeteringPolicyId=%s&PolicyRuleNumber=1"+
			"&MeteredAccount=source-attachment-owner&DestinationCidrBlock=10.0.0.0/8",
		policyID,
	))
	require.Equal(t, http.StatusOK, entryRec.Code)
	entryBody := entryRec.Body.String()
	assert.Contains(t, entryBody, "<policyRuleNumber>1</policyRuleNumber>")
	assert.Contains(t, entryBody, "<destinationCidrBlock>10.0.0.0/8</destinationCidrBlock>")

	descRec := postForm(t, h, "Action=DescribeTransitGatewayMeteringPolicies&Version=2016-11-15")
	require.Equal(t, http.StatusOK, descRec.Code)
	assert.Contains(t, descRec.Body.String(), policyID)

	deleteEntryRec := postForm(t, h, fmt.Sprintf(
		"Action=DeleteTransitGatewayMeteringPolicyEntry&Version=2016-11-15"+
			"&TransitGatewayMeteringPolicyId=%s&PolicyRuleNumber=1",
		policyID,
	))
	require.Equal(t, http.StatusOK, deleteEntryRec.Code)
	assert.Contains(t, deleteEntryRec.Body.String(), "<state>deleted</state>")

	deleteRec := postForm(t, h, fmt.Sprintf(
		"Action=DeleteTransitGatewayMeteringPolicy&Version=2016-11-15"+
			"&TransitGatewayMeteringPolicyId=%s",
		policyID,
	))
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Contains(t, deleteRec.Body.String(), "<state>deleted</state>")
}

// TestNonNilSlices_TGWMulticast verifies non-nil result when no subnets.
func TestNonNilSlices_TGWMulticast(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	assocs, err := b.AcceptTransitGatewayMulticastDomainAssociations(
		"tgw-mcast-1",
		"tgw-attach-1",
		nil,
	)
	require.NoError(t, err)
	assert.NotNil(t, assocs, "result should be non-nil even when no subnets provided")
	assert.Empty(t, assocs)
}

// TestSortedDescribeCapacityReservations verifies sorted output.
