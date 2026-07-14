package ec2_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- VPC endpoint connection notifications ---- //nolint:godot // existing issue.
func TestVpcEndpointConnectionNotification(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var notifID string

	t.Run("create notification", func(t *testing.T) { //nolint:paralleltest // existing issue.
		notif, err := b.CreateVpcEndpointConnectionNotification(
			"vpce-svc-123", "", "arn:aws:sns:us-east-1:000000000000:test",
			[]string{"Accept", "Reject"},
		)
		require.NoError(t, err)
		assert.NotEmpty(t, notif.ConnectionNotificationID)
		assert.Equal(t, "Enabled", notif.ConnectionNotificationState)
		notifID = notif.ConnectionNotificationID
	})

	t.Run("describe returns created", func(t *testing.T) { //nolint:paralleltest // existing issue.
		notifs := b.DescribeVpcEndpointConnectionNotifications([]string{notifID})
		require.Len(t, notifs, 1)
		assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:test", notifs[0].ConnectionNotificationARN)
	})

	t.Run("modify notification ARN", func(t *testing.T) { //nolint:paralleltest // existing issue.
		updated, err := b.ModifyVpcEndpointConnectionNotification(
			notifID, "arn:aws:sns:us-east-1:000000000000:updated", nil,
		)
		require.NoError(t, err)
		assert.Contains(t, updated.ConnectionNotificationARN, "updated")
	})

	t.Run("delete notification", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteVpcEndpointConnectionNotifications([]string{notifID}))
		notifs := b.DescribeVpcEndpointConnectionNotifications(nil)
		assert.Empty(t, notifs)
	})

	t.Run("empty ARN returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateVpcEndpointConnectionNotification("svc-1", "", "", nil)
		require.Error(t, err)
	})
}

// ---- VPC Endpoint operations ---- //nolint:godot // existing issue.

// ---- VPC Endpoint operations ---- //nolint:godot // existing issue.
func TestVpcEndpointOperations(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("DescribeVpcEndpointConnections returns empty", func(t *testing.T) { //nolint:paralleltest // existing issue.
		conns := b.DescribeVpcEndpointConnections(nil)
		assert.Empty(t, conns)
	})

	t.Run("DescribeVpcEndpointAssociations returns empty", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assocs := b.DescribeVpcEndpointAssociations(nil)
		assert.Empty(t, assocs)
	})
}

func TestVpcEndpointServicePermissions(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	// Create a service config to test against
	svcCfg, setupErr := b.CreateVpcEndpointServiceConfiguration(false, []string{"nlb-123"})
	require.NoError(t, setupErr)
	svcID := svcCfg.ServiceID

	t.Run("modify adds principals", func(t *testing.T) {
		require.NoError(t, b.ModifyVpcEndpointServicePermissions(svcID,
			[]string{"arn:aws:iam::111111111111:root"}, nil))
		principals := b.DescribeVpcEndpointServicePermissions(svcID)
		require.Len(t, principals, 1)
		assert.Equal(t, "arn:aws:iam::111111111111:root", principals[0])
	})
}

// ---- EBS encryption ---- //nolint:godot // existing issue.

func TestHTTP_DescribeVpcEndpointConnectionNotifications( //nolint:paralleltest // existing issue.
	t *testing.T,
) {
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeVpcEndpointConnectionNotifications"},
	})
	require.NoError(t, err)
}

// TestVpcEndpoint_DeleteReturnsDeleted verifies DeleteVpcEndpoints
// marks endpoints as deleted and removes them from Describe.
func TestVpcEndpoint_DeleteReturnsDeleted(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	ep, err := b.CreateVpcEndpoint("vpc-default", "com.amazonaws.us-east-1.s3", "Gateway", nil)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"DeleteVpcEndpoints"},
		"VpcEndpointId.1": {ep.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<DeleteVpcEndpointsResponse")

	// verify it no longer appears in describe
	resp2, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"DescribeVpcEndpoints"},
		"VpcEndpointId.1": {ep.ID},
	})
	require.NoError(t, err)
	assert.NotContains(t, resp2, ep.ID, "deleted endpoint must not appear in Describe")
}

// TestVpcEndpoint_DescribeAll verifies DescribeVpcEndpoints with no
// filter returns all endpoints.

// TestCreateVpcEndpointConnectionNotification_ResponseShape verifies that
// CreateVpcEndpointConnectionNotification returns an ID with the "vpce-nfn-" prefix,
// state=Enabled, type=Topic, and echoes the requested events, matching AWS EC2 behaviour.
func TestCreateVpcEndpointConnectionNotification_ResponseShape(t *testing.T) {
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

// TestCreateVpcEndpointConnectionNotification_MissingARN verifies that
// omitting the required ConnectionNotificationArn returns an error, matching AWS EC2 behaviour.

// TestCreateVpcEndpointConnectionNotification_MissingARN verifies that
// omitting the required ConnectionNotificationArn returns an error, matching AWS EC2 behaviour.
func TestCreateVpcEndpointConnectionNotification_MissingARN(t *testing.T) {
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

// TestDeleteVpcEndpointConnectionNotifications_RemovesFromDescribe verifies
// that a deleted notification no longer appears in DescribeVpcEndpointConnectionNotifications,
// matching AWS EC2 behaviour.

// TestDeleteVpcEndpointConnectionNotifications_RemovesFromDescribe verifies
// that a deleted notification no longer appears in DescribeVpcEndpointConnectionNotifications,
// matching AWS EC2 behaviour.
func TestDeleteVpcEndpointConnectionNotifications_RemovesFromDescribe(t *testing.T) {
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

// TestModifyVpcEndpointConnectionNotification_UpdatesARN verifies that
// ModifyVpcEndpointConnectionNotification updates the ARN visible in Describe,
// matching AWS EC2 behaviour.

// TestModifyVpcEndpointConnectionNotification_UpdatesARN verifies that
// ModifyVpcEndpointConnectionNotification updates the ARN visible in Describe,
// matching AWS EC2 behaviour.
func TestModifyVpcEndpointConnectionNotification_UpdatesARN(t *testing.T) {
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

// TestDescribeVpcEndpointConnections_StatePresent verifies that
// DescribeVpcEndpointConnections includes a state field in the connection items,
// matching AWS EC2 behaviour.

// TestDescribeVpcEndpointConnections_StatePresent verifies that
// DescribeVpcEndpointConnections includes a state field in the connection items,
// matching AWS EC2 behaviour.
func TestDescribeVpcEndpointConnections_StatePresent(t *testing.T) {
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

// TestDescribeVpcEndpointAssociations_VpcIDPresent verifies that
// DescribeVpcEndpointAssociations includes vpcId in each association item,
// matching AWS EC2 behaviour.

// TestDescribeVpcEndpointAssociations_VpcIDPresent verifies that
// DescribeVpcEndpointAssociations includes vpcId in each association item,
// matching AWS EC2 behaviour.
func TestDescribeVpcEndpointAssociations_VpcIDPresent(t *testing.T) {
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

// TestModifyVpcEndpointServicePermissions_AddRemove verifies that
// ModifyVpcEndpointServicePermissions adds principals visible via Describe and that
// RemoveAllowedPrincipals removes them, matching AWS EC2 behaviour.

// TestModifyVpcEndpointServicePermissions_AddRemove verifies that
// ModifyVpcEndpointServicePermissions adds principals visible via Describe and that
// RemoveAllowedPrincipals removes them, matching AWS EC2 behaviour.
func TestModifyVpcEndpointServicePermissions_AddRemove(t *testing.T) {
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

// TestModifyVpcEndpointServicePayerResponsibility_NotFound verifies that
// calling ModifyVpcEndpointServicePayerResponsibility on a non-existent service returns
// an error, matching AWS EC2 behaviour.

// TestModifyVpcEndpointServicePayerResponsibility_NotFound verifies that
// calling ModifyVpcEndpointServicePayerResponsibility on a non-existent service returns
// an error, matching AWS EC2 behaviour.
func TestModifyVpcEndpointServicePayerResponsibility_NotFound(t *testing.T) {
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

// TestCreateVpcEndpoint_ResponseShape verifies that CreateVpcEndpoint
// returns an ID with the "vpce-" prefix, echoes serviceName, state, and vpcId,
// matching AWS EC2 behaviour.

// TestModifyVpcEndpoint_AddSubnet verifies that ModifyVpcEndpoint
// succeeds when adding a valid subnet to an existing endpoint, matching AWS EC2 behaviour.
func TestModifyVpcEndpoint_AddSubnet(t *testing.T) {
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

// TestModifyVpcEndpoint_NotFound verifies that ModifyVpcEndpoint
// returns an error when the endpoint does not exist, matching AWS EC2 behaviour.

// TestModifyVpcEndpoint_NotFound verifies that ModifyVpcEndpoint
// returns an error when the endpoint does not exist, matching AWS EC2 behaviour.
func TestModifyVpcEndpoint_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":        {"ModifyVpcEndpoint"},
		"VpcEndpointId": {"vpce-doesnotexist"},
	})
	require.Error(t, err, "non-existent endpoint must return an error")
}

// extractXMLTag returns the text content of the first occurrence of the given XML element name.

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

// TestVpcEndpoint_CreateContainsEndpointID verifies the response
// wrapper element is <vpcEndpoint> with a vpce- prefixed ID.
func TestVpcEndpoint_CreateContainsEndpointID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		endpointType string
		wantType     string
	}{
		{name: "interface_type", endpointType: "Interface", wantType: "Interface"},
		{name: "gateway_type", endpointType: "Gateway", wantType: "Gateway"},
		{name: "empty_defaults_interface", endpointType: "", wantType: "Interface"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := url.Values{
				"Action":          {"CreateVpcEndpoint"},
				"VpcId":           {"vpc-default"},
				"ServiceName":     {"com.amazonaws.us-east-1.s3"},
				"VpcEndpointType": {tt.endpointType},
			}

			resp, err := ec2.ExportDispatch(h, vals)
			require.NoError(t, err)
			assert.Contains(t, resp, "<vpcEndpointId>vpce-", "must have vpce- ID prefix")
			assert.Contains(t, resp, "<vpcId>vpc-default</vpcId>")
			assert.Contains(t, resp, "<serviceName>com.amazonaws.us-east-1.s3</serviceName>")
			assert.Contains(t, resp, "<vpcEndpointState>available</vpcEndpointState>")
			if tt.wantType != "" {
				assert.Contains(t, resp, "<vpcEndpointType>"+tt.wantType+"</vpcEndpointType>")
			}
		})
	}
}

// TestVpcEndpoint_SubnetIDsInResponse verifies SubnetIds are returned
// in the DescribeVpcEndpoints response (was missing per parity.md §R).

// TestVpcEndpoint_SubnetIDsInResponse verifies SubnetIds are returned
// in the DescribeVpcEndpoints response (was missing per parity.md §R).
func TestVpcEndpoint_SubnetIDsInResponse(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h2 := ec2.NewHandler(b)
	h2.AccountID = "123456789012"
	h2.Region = "us-east-1"

	// create a VPC endpoint with subnet associations
	ep, err := b.CreateVpcEndpointWithRouteTableIDs(
		"vpc-default",
		"com.amazonaws.us-east-1.ecr.api",
		"Interface",
		[]string{"subnet-default"},
		nil,
	)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h2, url.Values{
		"Action":          {"DescribeVpcEndpoints"},
		"VpcEndpointId.1": {ep.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<subnetId>subnet-default</subnetId>",
		"SubnetId must appear in DescribeVpcEndpoints response")
}

// TestVpcEndpoint_RouteTableIDsInResponse verifies RouteTableIds
// are returned in the DescribeVpcEndpoints response (was missing per parity.md §R).

// TestVpcEndpoint_RouteTableIDsInResponse verifies RouteTableIds
// are returned in the DescribeVpcEndpoints response (was missing per parity.md §R).
func TestVpcEndpoint_RouteTableIDsInResponse(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	// create a route table to associate
	rt, err := b.CreateRouteTable("vpc-default")
	require.NoError(t, err)

	ep, err := b.CreateVpcEndpointWithRouteTableIDs(
		"vpc-default",
		"com.amazonaws.us-east-1.s3",
		"Gateway",
		nil,
		[]string{rt.ID},
	)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"DescribeVpcEndpoints"},
		"VpcEndpointId.1": {ep.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<routeTableId>"+rt.ID+"</routeTableId>",
		"RouteTableId must appear in DescribeVpcEndpoints response")
}

// TestVpcEndpoint_CreateWithRouteTableID verifies CreateVpcEndpoint
// accepts RouteTableId.N parameters and persists them.

// TestVpcEndpoint_CreateWithRouteTableID verifies CreateVpcEndpoint
// accepts RouteTableId.N parameters and persists them.
func TestVpcEndpoint_CreateWithRouteTableID(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	rt, err := b.CreateRouteTable("vpc-default")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"CreateVpcEndpoint"},
		"VpcId":           {"vpc-default"},
		"ServiceName":     {"com.amazonaws.us-east-1.s3"},
		"VpcEndpointType": {"Gateway"},
		"RouteTableId.1":  {rt.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<routeTableId>"+rt.ID+"</routeTableId>",
		"RouteTableId must be returned in CreateVpcEndpoint response")
}

// TestVpcEndpoint_MultipleSubnets verifies multiple SubnetId parameters
// are all reflected in the response.

// TestVpcEndpoint_MultipleSubnets verifies multiple SubnetId parameters
// are all reflected in the response.
func TestVpcEndpoint_MultipleSubnets(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	// create a second subnet in the same VPC
	subnet2, err := b.CreateSubnet("vpc-default", "172.31.48.0/24", "us-east-1a")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"CreateVpcEndpoint"},
		"VpcId":           {"vpc-default"},
		"ServiceName":     {"com.amazonaws.us-east-1.secretsmanager"},
		"VpcEndpointType": {"Interface"},
		"SubnetId.1":      {"subnet-default"},
		"SubnetId.2":      {subnet2.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<subnetId>subnet-default</subnetId>")
	assert.Contains(t, resp, "<subnetId>"+subnet2.ID+"</subnetId>")
}

// TestVpcEndpoint_DeleteReturnsDeleted verifies DeleteVpcEndpoints
// marks endpoints as deleted and removes them from Describe.

// TestVpcEndpoint_DescribeAll verifies DescribeVpcEndpoints with no
// filter returns all endpoints.
func TestVpcEndpoint_DescribeAll(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	ep1, err := b.CreateVpcEndpoint("vpc-default", "com.amazonaws.us-east-1.s3", "Gateway", nil)
	require.NoError(t, err)
	ep2, err := b.CreateVpcEndpoint("vpc-default", "com.amazonaws.us-east-1.ec2", "Interface", nil)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action": {"DescribeVpcEndpoints"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, ep1.ID)
	assert.Contains(t, resp, ep2.ID)
}

// TestVpcEndpoint_MissingVpcIDError verifies CreateVpcEndpoint returns
// an error when VpcId is missing.

// TestVpcEndpoint_MissingVpcIDError verifies CreateVpcEndpoint returns
// an error when VpcId is missing.
func TestVpcEndpoint_MissingVpcIDError(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":      {"CreateVpcEndpoint"},
		"ServiceName": {"com.amazonaws.us-east-1.s3"},
	})
	require.Error(t, err, "missing VpcId must return error")
}

// TestVpcEndpoint_MissingServiceNameError verifies CreateVpcEndpoint
// returns an error when ServiceName is missing.

// TestVpcEndpoint_MissingServiceNameError verifies CreateVpcEndpoint
// returns an error when ServiceName is missing.
func TestVpcEndpoint_MissingServiceNameError(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action": {"CreateVpcEndpoint"},
		"VpcId":  {"vpc-default"},
	})
	require.Error(t, err, "missing ServiceName must return error")
}

// ============================================================================
// Network ACL accuracy
// ============================================================================

// TestNetworkACL_DescribeReturnsAssociations verifies that
// DescribeNetworkAcls includes an <associationSet> with association items
// (was missing per parity.md §R).

// TestCreateVpcEndpoint_ResponseShape verifies that CreateVpcEndpoint
// returns an ID with the "vpce-" prefix, echoes serviceName, state, and vpcId,
// matching AWS EC2 behaviour.
func TestCreateVpcEndpoint_ResponseShape(t *testing.T) {
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

// TestCreateVpcEndpoint_MissingVPC verifies that CreateVpcEndpoint
// returns an error when the VPC does not exist, matching AWS EC2 behaviour.

// TestCreateVpcEndpoint_MissingVPC verifies that CreateVpcEndpoint
// returns an error when the VPC does not exist, matching AWS EC2 behaviour.
func TestCreateVpcEndpoint_MissingVPC(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":      {"CreateVpcEndpoint"},
		"VpcId":       {"vpc-doesnotexist"},
		"ServiceName": {"com.amazonaws.us-east-1.s3"},
	})
	require.Error(t, err, "non-existent VPC must return an error")
}

// TestDescribeVpcEndpoints_FilterByID verifies that DescribeVpcEndpoints
// filters by VpcEndpointId when provided, matching AWS EC2 behaviour.

// TestDescribeVpcEndpoints_FilterByID verifies that DescribeVpcEndpoints
// filters by VpcEndpointId when provided, matching AWS EC2 behaviour.
func TestDescribeVpcEndpoints_FilterByID(t *testing.T) {
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

// TestModifyVpcEndpoint_AddSubnet verifies that ModifyVpcEndpoint
// succeeds when adding a valid subnet to an existing endpoint, matching AWS EC2 behaviour.

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
