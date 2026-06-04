package ec2_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2 "github.com/blackbirdworks/gopherstack/services/ec2"
)

// --- CreateVpc ---

// TestVPCBatch2Audit_CreateVpc_IDFormat verifies that CreateVpc returns an ID with the
// "vpc-" prefix, matching AWS EC2 behaviour.
func TestVPCBatch2Audit_CreateVpc_IDFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cidr string
	}{
		{name: "class_c", cidr: "10.0.0.0/24"},
		{name: "class_b", cidr: "172.16.0.0/16"},
		{name: "class_a", cidr: "192.168.0.0/16"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp, err := ec2.ExportDispatch(h, url.Values{
				"Action":    {"CreateVpc"},
				"CidrBlock": {tt.cidr},
			})
			require.NoError(t, err)
			assert.Contains(t, resp, "<vpcId>vpc-",
				"CreateVpc response must contain a vpc- prefixed ID")
			assert.Contains(t, resp, "<cidrBlock>"+tt.cidr+"</cidrBlock>",
				"CreateVpc response must echo the requested CIDR block")
			assert.Contains(t, resp, "<state>available</state>",
				"CreateVpc response must return state=available")
		})
	}
}

// TestVPCBatch2Audit_DeleteVpc_NotFound verifies that DeleteVpc on a non-existent VPC
// returns an error, matching AWS EC2 behaviour.
func TestVPCBatch2Audit_DeleteVpc_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action": {"DeleteVpc"},
		"VpcId":  {"vpc-doesnotexist"},
	})
	require.Error(t, err, "DeleteVpc on non-existent VPC must return an error")
}

// TestVPCBatch2Audit_DescribeVpcs_FilterByID verifies that DescribeVpcs filters correctly
// when a VpcId list is supplied, matching AWS EC2 behaviour.
func TestVPCBatch2Audit_DescribeVpcs_FilterByID(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp1, err := ec2.ExportDispatch(h, url.Values{
		"Action":    {"CreateVpc"},
		"CidrBlock": {"10.1.0.0/16"},
	})
	require.NoError(t, err)

	resp2, err := ec2.ExportDispatch(h, url.Values{
		"Action":    {"CreateVpc"},
		"CidrBlock": {"10.2.0.0/16"},
	})
	require.NoError(t, err)

	id1 := extractXMLTag(resp1, "vpcId")
	id2 := extractXMLTag(resp2, "vpcId")
	require.NotEmpty(t, id1)
	require.NotEmpty(t, id2)

	// Filter by id1 only.
	filtered, err := ec2.ExportDispatch(h, url.Values{
		"Action":  {"DescribeVpcs"},
		"VpcId.1": {id1},
	})
	require.NoError(t, err)
	assert.Contains(t, filtered, id1, "filtered response must include requested VPC")
	assert.NotContains(t, filtered, id2, "filtered response must exclude other VPCs")
}

// --- AssociateVpcCidrBlock / DisassociateVpcCidrBlock ---

// TestVPCBatch2Audit_AssociateVpcCidrBlock_ResponseShape verifies that
// AssociateVpcCidrBlock returns an association ID and echoes the CIDR and state,
// matching AWS EC2 behaviour.
func TestVPCBatch2Audit_AssociateVpcCidrBlock_ResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cidr string
	}{
		{name: "ipv4_secondary", cidr: "100.64.0.0/16"},
		{name: "ipv4_rfc1918", cidr: "172.31.0.0/16"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vpcResp, err := ec2.ExportDispatch(h, url.Values{
				"Action":    {"CreateVpc"},
				"CidrBlock": {"10.0.0.0/16"},
			})
			require.NoError(t, err)
			vpcID := extractXMLTag(vpcResp, "vpcId")
			require.NotEmpty(t, vpcID)

			assocResp, err := ec2.ExportDispatch(h, url.Values{
				"Action":    {"AssociateVpcCidrBlock"},
				"VpcId":     {vpcID},
				"CidrBlock": {tt.cidr},
			})
			require.NoError(t, err)
			assert.Contains(t, assocResp, "vpc-cidr-assoc-",
				"AssociateVpcCidrBlock must return a vpc-cidr-assoc- prefixed ID")
			assert.Contains(t, assocResp, tt.cidr,
				"AssociateVpcCidrBlock must echo the requested CIDR")
			assert.Contains(t, assocResp, "available",
				"AssociateVpcCidrBlock state must be available")
		})
	}
}

// TestVPCBatch2Audit_DisassociateVpcCidrBlock_RemovesAssociation verifies that
// DisassociateVpcCidrBlock removes the association so it cannot be disassociated again.
func TestVPCBatch2Audit_DisassociateVpcCidrBlock_RemovesAssociation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	vpcResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":    {"CreateVpc"},
		"CidrBlock": {"10.10.0.0/16"},
	})
	require.NoError(t, err)
	vpcID := extractXMLTag(vpcResp, "vpcId")

	assocResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":    {"AssociateVpcCidrBlock"},
		"VpcId":     {vpcID},
		"CidrBlock": {"100.64.0.0/16"},
	})
	require.NoError(t, err)
	assocID := extractXMLTag(assocResp, "associationId")
	require.NotEmpty(t, assocID)

	// First disassociate succeeds.
	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":        {"DisassociateVpcCidrBlock"},
		"AssociationId": {assocID},
	})
	require.NoError(t, err)

	// Second disassociate on same ID must fail.
	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":        {"DisassociateVpcCidrBlock"},
		"AssociationId": {assocID},
	})
	require.Error(t, err, "DisassociateVpcCidrBlock on already-removed association must error")
}

// --- CreateVpcEndpointConnectionNotification ---

// TestVPCBatch2Audit_CreateVpcEndpointConnectionNotification_ResponseShape verifies that
// CreateVpcEndpointConnectionNotification returns an ID with the "vpce-nfn-" prefix,
// state=Enabled, type=Topic, and echoes the requested events, matching AWS EC2 behaviour.
func TestVPCBatch2Audit_CreateVpcEndpointConnectionNotification_ResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		events []string
	}{
		{
			name:   "accept_reject_events",
			events: []string{"Accept", "Reject"},
		},
		{
			name:   "connect_event_only",
			events: []string{"Connect"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := url.Values{
				"Action":                    {"CreateVpcEndpointConnectionNotification"},
				"ServiceId":                 {"vpce-svc-0a123456"},
				"ConnectionNotificationArn": {"arn:aws:sns:us-east-1:123456789012:test-topic"},
			}
			for i, ev := range tt.events {
				vals.Set("ConnectionEvents.member."+itoa(i+1), ev)
			}

			resp, err := ec2.ExportDispatch(h, vals)
			require.NoError(t, err)
			assert.Contains(t, resp, "vpce-nfn-",
				"connection notification ID must start with vpce-nfn-")
			assert.Contains(t, resp, "<connectionNotificationState>Enabled</connectionNotificationState>",
				"notification state must be Enabled")
			assert.Contains(t, resp, "<connectionNotificationType>Topic</connectionNotificationType>",
				"notification type must be Topic")
			for _, ev := range tt.events {
				assert.Contains(t, resp, ev, "response must include requested event %q", ev)
			}
		})
	}
}

// TestVPCBatch2Audit_CreateVpcEndpointConnectionNotification_MissingARN verifies that
// omitting the required ConnectionNotificationArn returns an error, matching AWS EC2 behaviour.
func TestVPCBatch2Audit_CreateVpcEndpointConnectionNotification_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":    {"CreateVpcEndpointConnectionNotification"},
		"ServiceId": {"vpce-svc-0a123456"},
		// ConnectionNotificationArn intentionally omitted.
	})
	require.Error(t, err, "missing ARN must return an error")
}

// --- DeleteVpcEndpointConnectionNotifications ---

// TestVPCBatch2Audit_DeleteVpcEndpointConnectionNotifications_RemovesFromDescribe verifies
// that a deleted notification no longer appears in DescribeVpcEndpointConnectionNotifications,
// matching AWS EC2 behaviour.
func TestVPCBatch2Audit_DeleteVpcEndpointConnectionNotifications_RemovesFromDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                    {"CreateVpcEndpointConnectionNotification"},
		"ServiceId":                 {"vpce-svc-delete"},
		"ConnectionNotificationArn": {"arn:aws:sns:us-east-1:123456789012:del-topic"},
		"ConnectionEvents.member.1": {"Accept"},
	})
	require.NoError(t, err)
	notifID := extractXMLTag(createResp, "connectionNotificationId")
	require.NotEmpty(t, notifID)

	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":                     {"DeleteVpcEndpointConnectionNotifications"},
		"ConnectionNotificationId.1": {notifID},
	})
	require.NoError(t, err)

	descResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                     {"DescribeVpcEndpointConnectionNotifications"},
		"ConnectionNotificationId.1": {notifID},
	})
	require.NoError(t, err)
	assert.NotContains(t, descResp, notifID,
		"deleted notification must not appear in describe response")
}

// --- ModifyVpcEndpointConnectionNotification ---

// TestVPCBatch2Audit_ModifyVpcEndpointConnectionNotification_UpdatesARN verifies that
// ModifyVpcEndpointConnectionNotification updates the ARN visible in Describe,
// matching AWS EC2 behaviour.
func TestVPCBatch2Audit_ModifyVpcEndpointConnectionNotification_UpdatesARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                    {"CreateVpcEndpointConnectionNotification"},
		"ServiceId":                 {"vpce-svc-mod"},
		"ConnectionNotificationArn": {"arn:aws:sns:us-east-1:123456789012:original"},
		"ConnectionEvents.member.1": {"Accept"},
	})
	require.NoError(t, err)
	notifID := extractXMLTag(createResp, "connectionNotificationId")
	require.NotEmpty(t, notifID)

	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":                    {"ModifyVpcEndpointConnectionNotification"},
		"ConnectionNotificationId":  {notifID},
		"ConnectionNotificationArn": {"arn:aws:sns:us-east-1:123456789012:updated"},
	})
	require.NoError(t, err)

	descResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                     {"DescribeVpcEndpointConnectionNotifications"},
		"ConnectionNotificationId.1": {notifID},
	})
	require.NoError(t, err)
	assert.Contains(t, descResp, "updated",
		"Describe must reflect the updated ARN after Modify")
	assert.NotContains(t, descResp, "original",
		"Describe must not contain the original ARN after Modify")
}

// --- DescribeVpcEndpointConnections ---

// TestVPCBatch2Audit_DescribeVpcEndpointConnections_StatePresent verifies that
// DescribeVpcEndpointConnections includes a state field in the connection items,
// matching AWS EC2 behaviour.
func TestVPCBatch2Audit_DescribeVpcEndpointConnections_StatePresent(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	// Add a connection via internal path.
	b.AddVpcEndpointConnectionInternal(&ec2.VpcEndpointConnection{
		VpcEndpointID: "vpce-0abc1234",
		ServiceID:     "vpce-svc-0a123456",
		State:         "available",
	})

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":    {"DescribeVpcEndpointConnections"},
		"ServiceId": {"vpce-svc-0a123456"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "vpce-0abc1234",
		"describe response must include the endpoint ID")
	assert.Contains(t, resp, "<vpcEndpointState>",
		"each connection item must include a vpcEndpointState field")
}

// --- DescribeVpcEndpointAssociations ---

// TestVPCBatch2Audit_DescribeVpcEndpointAssociations_VpcIDPresent verifies that
// DescribeVpcEndpointAssociations includes vpcId in each association item,
// matching AWS EC2 behaviour.
func TestVPCBatch2Audit_DescribeVpcEndpointAssociations_VpcIDPresent(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	ep, err := b.CreateVpcEndpoint(vpc.ID, "com.amazonaws.us-east-1.s3", "Interface", nil)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"DescribeVpcEndpointAssociations"},
		"VpcEndpointId.1": {ep.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<vpcId>"+vpc.ID+"</vpcId>",
		"each endpoint association item must include the vpcId")
}

// --- ModifyVpcEndpointServicePermissions ---

// TestVPCBatch2Audit_ModifyVpcEndpointServicePermissions_AddRemove verifies that
// ModifyVpcEndpointServicePermissions adds principals visible via Describe and that
// RemoveAllowedPrincipals removes them, matching AWS EC2 behaviour.
func TestVPCBatch2Audit_ModifyVpcEndpointServicePermissions_AddRemove(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	svcCfg, err := b.CreateVpcEndpointServiceConfiguration(false, []string{"nlb-0a1b2c3d"})
	require.NoError(t, err)
	svcID := svcCfg.ServiceID

	const principal = "arn:aws:iam::111111111111:root"

	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":                        {"ModifyVpcEndpointServicePermissions"},
		"ServiceId":                     {svcID},
		"AddAllowedPrincipals.member.1": {principal},
	})
	require.NoError(t, err)

	descResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":    {"DescribeVpcEndpointServicePermissions"},
		"ServiceId": {svcID},
	})
	require.NoError(t, err)
	assert.Contains(t, descResp, principal,
		"added principal must appear in DescribeVpcEndpointServicePermissions")

	// Remove the principal.
	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":                           {"ModifyVpcEndpointServicePermissions"},
		"ServiceId":                        {svcID},
		"RemoveAllowedPrincipals.member.1": {principal},
	})
	require.NoError(t, err)

	descResp2, err := ec2.ExportDispatch(h, url.Values{
		"Action":    {"DescribeVpcEndpointServicePermissions"},
		"ServiceId": {svcID},
	})
	require.NoError(t, err)
	assert.NotContains(t, descResp2, principal,
		"removed principal must no longer appear in DescribeVpcEndpointServicePermissions")
}

// --- ModifyVpcEndpointServicePayerResponsibility ---

// TestVPCBatch2Audit_ModifyVpcEndpointServicePayerResponsibility_NotFound verifies that
// calling ModifyVpcEndpointServicePayerResponsibility on a non-existent service returns
// an error, matching AWS EC2 behaviour.
func TestVPCBatch2Audit_ModifyVpcEndpointServicePayerResponsibility_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":              {"ModifyVpcEndpointServicePayerResponsibility"},
		"ServiceId":           {"vpce-svc-doesnotexist"},
		"PayerResponsibility": {"ServiceOwner"},
	})
	require.Error(t, err, "non-existent service must return an error")
}

// --- CreateVpcEndpoint ---

// TestVPCBatch2Audit_CreateVpcEndpoint_ResponseShape verifies that CreateVpcEndpoint
// returns an ID with the "vpce-" prefix, echoes serviceName, state, and vpcId,
// matching AWS EC2 behaviour.
func TestVPCBatch2Audit_CreateVpcEndpoint_ResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		serviceName string
		epType      string
	}{
		{
			name:        "interface_endpoint",
			serviceName: "com.amazonaws.us-east-1.s3",
			epType:      "Interface",
		},
		{
			name:        "gateway_endpoint",
			serviceName: "com.amazonaws.us-east-1.dynamodb",
			epType:      "Gateway",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := newTestHandlerWithBackend(b)

			vpc, err := b.CreateVpc("10.0.0.0/16")
			require.NoError(t, err)

			resp, err := ec2.ExportDispatch(h, url.Values{
				"Action":          {"CreateVpcEndpoint"},
				"VpcId":           {vpc.ID},
				"ServiceName":     {tt.serviceName},
				"VpcEndpointType": {tt.epType},
			})
			require.NoError(t, err)
			assert.Contains(t, resp, "<vpcEndpointId>vpce-",
				"endpoint ID must start with vpce-")
			assert.Contains(t, resp, tt.serviceName,
				"response must echo the serviceName")
			assert.Contains(t, resp, vpc.ID,
				"response must echo the vpcId")
			assert.Contains(t, resp, "<vpcEndpointState>available</vpcEndpointState>",
				"endpoint state must be available")
		})
	}
}

// TestVPCBatch2Audit_CreateVpcEndpoint_MissingVPC verifies that CreateVpcEndpoint
// returns an error when the VPC does not exist, matching AWS EC2 behaviour.
func TestVPCBatch2Audit_CreateVpcEndpoint_MissingVPC(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":      {"CreateVpcEndpoint"},
		"VpcId":       {"vpc-doesnotexist"},
		"ServiceName": {"com.amazonaws.us-east-1.s3"},
	})
	require.Error(t, err, "non-existent VPC must return an error")
}

// TestVPCBatch2Audit_DescribeVpcEndpoints_FilterByID verifies that DescribeVpcEndpoints
// filters by VpcEndpointId when provided, matching AWS EC2 behaviour.
func TestVPCBatch2Audit_DescribeVpcEndpoints_FilterByID(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	ep1, err := b.CreateVpcEndpoint(vpc.ID, "com.amazonaws.us-east-1.s3", "Interface", nil)
	require.NoError(t, err)
	ep2, err := b.CreateVpcEndpoint(vpc.ID, "com.amazonaws.us-east-1.ec2", "Interface", nil)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"DescribeVpcEndpoints"},
		"VpcEndpointId.1": {ep1.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, ep1.ID, "filtered response must include requested endpoint")
	assert.NotContains(t, resp, ep2.ID, "filtered response must exclude other endpoints")
}

// --- ModifyVpcEndpoint ---

// TestVPCBatch2Audit_ModifyVpcEndpoint_AddSubnet verifies that ModifyVpcEndpoint
// succeeds when adding a valid subnet to an existing endpoint, matching AWS EC2 behaviour.
func TestVPCBatch2Audit_ModifyVpcEndpoint_AddSubnet(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, "10.0.1.0/24", "us-east-1a")
	require.NoError(t, err)

	ep, err := b.CreateVpcEndpoint(vpc.ID, "com.amazonaws.us-east-1.s3", "Interface", nil)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":        {"ModifyVpcEndpoint"},
		"VpcEndpointId": {ep.ID},
		"AddSubnetId.1": {subnet.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<return>true</return>",
		"ModifyVpcEndpoint success response must return true")
}

// TestVPCBatch2Audit_ModifyVpcEndpoint_NotFound verifies that ModifyVpcEndpoint
// returns an error when the endpoint does not exist, matching AWS EC2 behaviour.
func TestVPCBatch2Audit_ModifyVpcEndpoint_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":        {"ModifyVpcEndpoint"},
		"VpcEndpointId": {"vpce-doesnotexist"},
	})
	require.Error(t, err, "non-existent endpoint must return an error")
}

// extractXMLTag returns the text content of the first occurrence of the given XML element name.
func extractXMLTag(xmlStr, tagName string) string {
	open := "<" + tagName + ">"
	closeTag := "</" + tagName + ">"

	start := strings.Index(xmlStr, open)
	if start < 0 {
		return ""
	}

	start += len(open)
	end := strings.Index(xmlStr[start:], closeTag)

	if end < 0 {
		return ""
	}

	return xmlStr[start : start+end]
}

// itoa converts an int to its decimal string representation.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	neg := n < 0
	if neg {
		n = -n
	}

	buf := make([]byte, 0, 10)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}

	if neg {
		buf = append([]byte{'-'}, buf...)
	}

	return string(buf)
}
