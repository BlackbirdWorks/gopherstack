package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestSlice20_RealClient covers ec2's largest remaining uncovered-op
// families (gopherstack-n3zi slice 20): Capacity Reservation extras,
// Verified Access lifecycle/policy extras, Client VPN extras, VPN
// Connection/Concentrator extras, Managed Prefix List extras, and Network
// Insights extras. Each row gets its own fresh handler+backend.
func TestSlice20_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client)
		name string
	}{
		{runCapacityReservationExtras, "capacity_reservation_extras"},
		{runVerifiedAccessExtras, "verified_access_extras"},
		{runClientVpnExtras, "client_vpn_extras"},
		{runVpnConnectionAndConcentrator, "vpn_connection_and_concentrator"},
		{runManagedPrefixListExtras, "managed_prefix_list_extras"},
		{runNetworkInsightsExtras, "network_insights_extras"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			h := ec2.NewHandler(backend)
			client := newTestEC2Client(t, h)
			tt.run(t, backend, client)
		})
	}
}

// runCapacityReservationExtras covers CreateCapacityReservationFleet,
// ModifyCapacityReservationFleet, CancelCapacityReservationFleets,
// CreateCapacityReservationBySplitting, MoveCapacityReservationInstances,
// DisassociateCapacityReservationBillingOwner,
// RejectCapacityReservationBillingOwnership, GetGroupsForCapacityReservation,
// CreateInterruptibleCapacityReservationAllocation,
// UpdateInterruptibleCapacityReservationAllocation,
// GetCapacityReservationUsage.
func runCapacityReservationExtras(
	t *testing.T,
	backend *ec2.InMemoryBackend,
	client *ec2sdk.Client,
) {
	t.Helper()

	fleetOut, err := client.CreateCapacityReservationFleet(
		t.Context(),
		&ec2sdk.CreateCapacityReservationFleetInput{
			TotalTargetCapacity: aws.Int32(5),
			InstanceTypeSpecifications: []types.ReservationFleetInstanceSpecification{
				{
					InstanceType:     types.InstanceTypeM5Xlarge,
					InstancePlatform: types.CapacityReservationInstancePlatformLinuxUnix,
					AvailabilityZone: aws.String("us-east-1a"),
					Priority:         aws.Int32(1),
					Weight:           aws.Float64(1),
				},
			},
		},
	)
	require.NoError(t, err)
	fleetID := aws.ToString(fleetOut.CapacityReservationFleetId)
	assert.Equal(t, int32(5), aws.ToInt32(fleetOut.TotalTargetCapacity))

	modFleetOut, err := client.ModifyCapacityReservationFleet(
		t.Context(),
		&ec2sdk.ModifyCapacityReservationFleetInput{
			CapacityReservationFleetId: aws.String(fleetID),
			TotalTargetCapacity:        aws.Int32(9),
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(modFleetOut.Return))

	cancelOut, err := client.CancelCapacityReservationFleets(
		t.Context(),
		&ec2sdk.CancelCapacityReservationFleetsInput{
			CapacityReservationFleetIds: []string{fleetID},
		},
	)
	require.NoError(t, err)
	require.Len(t, cancelOut.SuccessfulFleetCancellations, 1)
	assert.Equal(
		t,
		fleetID,
		aws.ToString(cancelOut.SuccessfulFleetCancellations[0].CapacityReservationFleetId),
	)
	assert.Equal(
		t, types.CapacityReservationFleetStateCancelled,
		cancelOut.SuccessfulFleetCancellations[0].CurrentFleetState,
	)

	src, err := backend.CreateCapacityReservation("m5.xlarge", "us-east-1a", 10, nil)
	require.NoError(t, err)

	splitOut, err := client.CreateCapacityReservationBySplitting(
		t.Context(), &ec2sdk.CreateCapacityReservationBySplittingInput{
			SourceCapacityReservationId: aws.String(src.CapacityReservationID),
			InstanceCount:               aws.Int32(4),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, splitOut.DestinationCapacityReservation)
	dstID := aws.ToString(splitOut.DestinationCapacityReservation.CapacityReservationId)
	assert.Equal(t, int32(4), aws.ToInt32(splitOut.InstanceCount))

	moveOut, err := client.MoveCapacityReservationInstances(
		t.Context(),
		&ec2sdk.MoveCapacityReservationInstancesInput{
			SourceCapacityReservationId:      aws.String(src.CapacityReservationID),
			DestinationCapacityReservationId: aws.String(dstID),
			InstanceCount:                    aws.Int32(2),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, int32(2), aws.ToInt32(moveOut.InstanceCount))

	billingCR, err := backend.CreateCapacityReservation("m5.xlarge", "us-east-1a", 1, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		backend.AssociateCapacityReservationBillingOwner(
			billingCR.CapacityReservationID,
			"111111111111",
		),
	)

	disOut, err := client.DisassociateCapacityReservationBillingOwner(
		t.Context(), &ec2sdk.DisassociateCapacityReservationBillingOwnerInput{
			CapacityReservationId:           aws.String(billingCR.CapacityReservationID),
			UnusedReservationBillingOwnerId: aws.String("111111111111"),
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(disOut.Return))

	billingCR2, err := backend.CreateCapacityReservation("m5.xlarge", "us-east-1a", 1, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		backend.AssociateCapacityReservationBillingOwner(
			billingCR2.CapacityReservationID,
			"111111111111",
		),
	)

	rejectOut, err := client.RejectCapacityReservationBillingOwnership(
		t.Context(), &ec2sdk.RejectCapacityReservationBillingOwnershipInput{
			CapacityReservationId: aws.String(billingCR2.CapacityReservationID),
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(rejectOut.Return))

	groupsOut, err := client.GetGroupsForCapacityReservation(
		t.Context(),
		&ec2sdk.GetGroupsForCapacityReservationInput{
			CapacityReservationId: aws.String(src.CapacityReservationID),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, groupsOut.CapacityReservationGroups)

	interCR, err := backend.CreateCapacityReservation("m5.large", "us-east-1a", 10, nil)
	require.NoError(t, err)

	allocOut, err := client.CreateInterruptibleCapacityReservationAllocation(
		t.Context(), &ec2sdk.CreateInterruptibleCapacityReservationAllocationInput{
			CapacityReservationId: aws.String(interCR.CapacityReservationID),
			InstanceCount:         aws.Int32(3),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, int32(3), aws.ToInt32(allocOut.TargetInstanceCount))
	assert.Equal(t, types.InterruptibleCapacityReservationAllocationStatusActive, allocOut.Status)

	updOut, err := client.UpdateInterruptibleCapacityReservationAllocation(
		t.Context(), &ec2sdk.UpdateInterruptibleCapacityReservationAllocationInput{
			CapacityReservationId: aws.String(interCR.CapacityReservationID),
			TargetInstanceCount:   aws.Int32(5),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, int32(5), aws.ToInt32(updOut.TargetInstanceCount))

	usageOut, err := client.GetCapacityReservationUsage(
		t.Context(),
		&ec2sdk.GetCapacityReservationUsageInput{
			CapacityReservationId: aws.String(interCR.CapacityReservationID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, interCR.CapacityReservationID, aws.ToString(usageOut.CapacityReservationId))
	assert.Equal(t, int32(10), aws.ToInt32(usageOut.TotalInstanceCount))
}

// runVerifiedAccessExtras covers CreateVerifiedAccessTrustProvider,
// AttachVerifiedAccessTrustProvider, DetachVerifiedAccessTrustProvider,
// DeleteVerifiedAccessTrustProvider, ModifyVerifiedAccessTrustProvider,
// DeleteVerifiedAccessGroup, DeleteVerifiedAccessInstance,
// ModifyVerifiedAccessInstance, ModifyVerifiedAccessInstanceLoggingConfiguration,
// DescribeVerifiedAccessInstanceLoggingConfigurations,
// ExportVerifiedAccessInstanceClientConfiguration, ModifyVerifiedAccessEndpoint,
// ModifyVerifiedAccessEndpointPolicy, GetVerifiedAccessEndpointPolicy,
// GetVerifiedAccessEndpointTargets, ModifyVerifiedAccessGroupPolicy,
// GetVerifiedAccessGroupPolicy.
func runVerifiedAccessExtras(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	instance, err := backend.CreateVerifiedAccessInstance("test instance")
	require.NoError(t, err)

	group, err := backend.CreateVerifiedAccessGroup(instance.VerifiedAccessInstanceID, "test group")
	require.NoError(t, err)

	endpoint, err := backend.CreateVerifiedAccessEndpoint(
		group.VerifiedAccessGroupID,
		"network-interface",
		"test endpoint",
	)
	require.NoError(t, err)

	tpOut, err := client.CreateVerifiedAccessTrustProvider(
		t.Context(),
		&ec2sdk.CreateVerifiedAccessTrustProviderInput{
			TrustProviderType:     types.TrustProviderTypeUser,
			PolicyReferenceName:   aws.String("policy-ref"),
			UserTrustProviderType: types.UserTrustProviderTypeIamIdentityCenter,
			Description:           aws.String("test provider"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, tpOut.VerifiedAccessTrustProvider)
	tpID := aws.ToString(tpOut.VerifiedAccessTrustProvider.VerifiedAccessTrustProviderId)
	assert.Equal(
		t,
		"policy-ref",
		aws.ToString(tpOut.VerifiedAccessTrustProvider.PolicyReferenceName),
	)

	attachOut, err := client.AttachVerifiedAccessTrustProvider(
		t.Context(),
		&ec2sdk.AttachVerifiedAccessTrustProviderInput{
			VerifiedAccessInstanceId:      aws.String(instance.VerifiedAccessInstanceID),
			VerifiedAccessTrustProviderId: aws.String(tpID),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		tpID,
		aws.ToString(attachOut.VerifiedAccessTrustProvider.VerifiedAccessTrustProviderId),
	)

	modTPOut, err := client.ModifyVerifiedAccessTrustProvider(
		t.Context(),
		&ec2sdk.ModifyVerifiedAccessTrustProviderInput{
			VerifiedAccessTrustProviderId: aws.String(tpID),
			Description:                   aws.String("updated provider"),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		"updated provider",
		aws.ToString(modTPOut.VerifiedAccessTrustProvider.Description),
	)

	detachOut, err := client.DetachVerifiedAccessTrustProvider(
		t.Context(),
		&ec2sdk.DetachVerifiedAccessTrustProviderInput{
			VerifiedAccessInstanceId:      aws.String(instance.VerifiedAccessInstanceID),
			VerifiedAccessTrustProviderId: aws.String(tpID),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		tpID,
		aws.ToString(detachOut.VerifiedAccessTrustProvider.VerifiedAccessTrustProviderId),
	)

	delTPOut, err := client.DeleteVerifiedAccessTrustProvider(
		t.Context(),
		&ec2sdk.DeleteVerifiedAccessTrustProviderInput{
			VerifiedAccessTrustProviderId: aws.String(tpID),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		tpID,
		aws.ToString(delTPOut.VerifiedAccessTrustProvider.VerifiedAccessTrustProviderId),
	)

	modInstOut, err := client.ModifyVerifiedAccessInstance(
		t.Context(),
		&ec2sdk.ModifyVerifiedAccessInstanceInput{
			VerifiedAccessInstanceId: aws.String(instance.VerifiedAccessInstanceID),
			Description:              aws.String("updated instance"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "updated instance", aws.ToString(modInstOut.VerifiedAccessInstance.Description))

	logOut, err := client.ModifyVerifiedAccessInstanceLoggingConfiguration(
		t.Context(), &ec2sdk.ModifyVerifiedAccessInstanceLoggingConfigurationInput{
			VerifiedAccessInstanceId: aws.String(instance.VerifiedAccessInstanceID),
			AccessLogs: &types.VerifiedAccessLogOptions{
				CloudWatchLogs: &types.VerifiedAccessLogCloudWatchLogsDestinationOptions{
					Enabled:  aws.Bool(true),
					LogGroup: aws.String("va-log-group"),
				},
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, logOut.LoggingConfiguration)
	require.NotNil(t, logOut.LoggingConfiguration.AccessLogs)
	require.NotNil(t, logOut.LoggingConfiguration.AccessLogs.CloudWatchLogs)
	assert.True(t, aws.ToBool(logOut.LoggingConfiguration.AccessLogs.CloudWatchLogs.Enabled))

	descLogOut, err := client.DescribeVerifiedAccessInstanceLoggingConfigurations(
		t.Context(), &ec2sdk.DescribeVerifiedAccessInstanceLoggingConfigurationsInput{
			VerifiedAccessInstanceIds: []string{instance.VerifiedAccessInstanceID},
		},
	)
	require.NoError(t, err)
	require.Len(t, descLogOut.LoggingConfigurations, 1)
	assert.Equal(
		t, instance.VerifiedAccessInstanceID,
		aws.ToString(descLogOut.LoggingConfigurations[0].VerifiedAccessInstanceId),
	)

	exportOut, err := client.ExportVerifiedAccessInstanceClientConfiguration(
		t.Context(), &ec2sdk.ExportVerifiedAccessInstanceClientConfigurationInput{
			VerifiedAccessInstanceId: aws.String(instance.VerifiedAccessInstanceID),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		instance.VerifiedAccessInstanceID,
		aws.ToString(exportOut.VerifiedAccessInstanceId),
	)

	modEpOut, err := client.ModifyVerifiedAccessEndpoint(
		t.Context(),
		&ec2sdk.ModifyVerifiedAccessEndpointInput{
			VerifiedAccessEndpointId: aws.String(endpoint.VerifiedAccessEndpointID),
			Description:              aws.String("updated endpoint"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "updated endpoint", aws.ToString(modEpOut.VerifiedAccessEndpoint.Description))

	modEpPolicyOut, err := client.ModifyVerifiedAccessEndpointPolicy(
		t.Context(), &ec2sdk.ModifyVerifiedAccessEndpointPolicyInput{
			VerifiedAccessEndpointId: aws.String(endpoint.VerifiedAccessEndpointID),
			PolicyDocument:           aws.String(`permit(principal, action, resource);`),
			PolicyEnabled:            aws.Bool(true),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		`permit(principal, action, resource);`,
		aws.ToString(modEpPolicyOut.PolicyDocument),
	)
	assert.True(t, aws.ToBool(modEpPolicyOut.PolicyEnabled))

	getEpPolicyOut, err := client.GetVerifiedAccessEndpointPolicy(
		t.Context(), &ec2sdk.GetVerifiedAccessEndpointPolicyInput{
			VerifiedAccessEndpointId: aws.String(endpoint.VerifiedAccessEndpointID),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		`permit(principal, action, resource);`,
		aws.ToString(getEpPolicyOut.PolicyDocument),
	)

	targetsOut, err := client.GetVerifiedAccessEndpointTargets(
		t.Context(), &ec2sdk.GetVerifiedAccessEndpointTargetsInput{
			VerifiedAccessEndpointId: aws.String(endpoint.VerifiedAccessEndpointID),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, targetsOut.VerifiedAccessEndpointTargets)

	modGroupPolicyOut, err := client.ModifyVerifiedAccessGroupPolicy(
		t.Context(), &ec2sdk.ModifyVerifiedAccessGroupPolicyInput{
			VerifiedAccessGroupId: aws.String(group.VerifiedAccessGroupID),
			PolicyDocument:        aws.String(`permit(principal, action, resource);`),
			PolicyEnabled:         aws.Bool(true),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		`permit(principal, action, resource);`,
		aws.ToString(modGroupPolicyOut.PolicyDocument),
	)

	getGroupPolicyOut, err := client.GetVerifiedAccessGroupPolicy(
		t.Context(), &ec2sdk.GetVerifiedAccessGroupPolicyInput{
			VerifiedAccessGroupId: aws.String(group.VerifiedAccessGroupID),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		`permit(principal, action, resource);`,
		aws.ToString(getGroupPolicyOut.PolicyDocument),
	)

	delGroupOut, err := client.DeleteVerifiedAccessGroup(
		t.Context(),
		&ec2sdk.DeleteVerifiedAccessGroupInput{
			VerifiedAccessGroupId: aws.String(group.VerifiedAccessGroupID),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		group.VerifiedAccessGroupID,
		aws.ToString(delGroupOut.VerifiedAccessGroup.VerifiedAccessGroupId),
	)

	delInstOut, err := client.DeleteVerifiedAccessInstance(
		t.Context(),
		&ec2sdk.DeleteVerifiedAccessInstanceInput{
			VerifiedAccessInstanceId: aws.String(instance.VerifiedAccessInstanceID),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t, instance.VerifiedAccessInstanceID,
		aws.ToString(delInstOut.VerifiedAccessInstance.VerifiedAccessInstanceId),
	)
}

// runClientVpnExtras covers DeleteClientVpnEndpoint, DeleteClientVpnRoute,
// DescribeClientVpnConnections, ExportClientVpnClientCertificateRevocationList,
// ExportClientVpnClientConfiguration, ImportClientVpnClientCertificateRevocationList,
// RevokeClientVpnIngress, TerminateClientVpnConnections.
func runClientVpnExtras(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	ep, err := backend.CreateClientVpnEndpoint("10.0.0.0/22", "test vpn", nil)
	require.NoError(t, err)

	require.NoError(
		t,
		backend.CreateClientVpnRoute(ep.ClientVpnEndpointID, "10.1.0.0/24", "test route"),
	)
	require.NoError(
		t,
		backend.AuthorizeClientVpnIngress(ep.ClientVpnEndpointID, "10.1.0.0/24", "test rule"),
	)

	connOut, err := client.DescribeClientVpnConnections(
		t.Context(),
		&ec2sdk.DescribeClientVpnConnectionsInput{
			ClientVpnEndpointId: aws.String(ep.ClientVpnEndpointID),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, connOut.Connections)

	exportConfOut, err := client.ExportClientVpnClientConfiguration(
		t.Context(), &ec2sdk.ExportClientVpnClientConfigurationInput{
			ClientVpnEndpointId: aws.String(ep.ClientVpnEndpointID),
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(exportConfOut.ClientConfiguration))

	exportCRLOut, err := client.ExportClientVpnClientCertificateRevocationList(
		t.Context(), &ec2sdk.ExportClientVpnClientCertificateRevocationListInput{
			ClientVpnEndpointId: aws.String(ep.ClientVpnEndpointID),
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(exportCRLOut.CertificateRevocationList))

	importOut, err := client.ImportClientVpnClientCertificateRevocationList(
		t.Context(), &ec2sdk.ImportClientVpnClientCertificateRevocationListInput{
			ClientVpnEndpointId: aws.String(ep.ClientVpnEndpointID),
			CertificateRevocationList: aws.String(
				"-----BEGIN X509 CRL-----\ntest\n-----END X509 CRL-----",
			),
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(importOut.Return))

	termOut, err := client.TerminateClientVpnConnections(
		t.Context(),
		&ec2sdk.TerminateClientVpnConnectionsInput{
			ClientVpnEndpointId: aws.String(ep.ClientVpnEndpointID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, ep.ClientVpnEndpointID, aws.ToString(termOut.ClientVpnEndpointId))

	revokeOut, err := client.RevokeClientVpnIngress(
		t.Context(),
		&ec2sdk.RevokeClientVpnIngressInput{
			ClientVpnEndpointId: aws.String(ep.ClientVpnEndpointID),
			TargetNetworkCidr:   aws.String("10.1.0.0/24"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.ClientVpnAuthorizationRuleStatusCodeRevoking, revokeOut.Status.Code)

	delRouteOut, err := client.DeleteClientVpnRoute(t.Context(), &ec2sdk.DeleteClientVpnRouteInput{
		ClientVpnEndpointId:  aws.String(ep.ClientVpnEndpointID),
		DestinationCidrBlock: aws.String("10.1.0.0/24"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ClientVpnRouteStatusCodeDeleting, delRouteOut.Status.Code)

	delEpOut, err := client.DeleteClientVpnEndpoint(
		t.Context(),
		&ec2sdk.DeleteClientVpnEndpointInput{
			ClientVpnEndpointId: aws.String(ep.ClientVpnEndpointID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.ClientVpnEndpointStatusCodeDeleting, delEpOut.Status.Code)
}

// runVpnConnectionAndConcentrator covers CreateVpnConnectionRoute,
// DeleteVpnConnection, DeleteVpnConnectionRoute, ModifyVpnConnection,
// ModifyVpnConnectionOptions, ModifyVpnTunnelCertificate,
// ModifyVpnTunnelOptions, ReplaceVpnTunnel, GetActiveVpnTunnelStatus,
// GetVpnConnectionDeviceSampleConfiguration, GetVpnTunnelReplacementStatus,
// CreateVpnConcentrator, DeleteVpnConcentrator, DescribeVpnConcentrators.
func runVpnConnectionAndConcentrator(t *testing.T, _ *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	cgwOut, err := client.CreateCustomerGateway(t.Context(), &ec2sdk.CreateCustomerGatewayInput{
		Type:     types.GatewayTypeIpsec1,
		BgpAsn:   aws.Int32(65000),
		PublicIp: aws.String("203.0.113.1"),
	})
	require.NoError(t, err)

	vgwOut, err := client.CreateVpnGateway(t.Context(), &ec2sdk.CreateVpnGatewayInput{
		Type: types.GatewayTypeIpsec1,
	})
	require.NoError(t, err)

	connOut, err := client.CreateVpnConnection(t.Context(), &ec2sdk.CreateVpnConnectionInput{
		Type:              aws.String("ipsec.1"),
		CustomerGatewayId: cgwOut.CustomerGateway.CustomerGatewayId,
		VpnGatewayId:      vgwOut.VpnGateway.VpnGatewayId,
	})
	require.NoError(t, err)
	require.NotNil(t, connOut.VpnConnection)
	connID := aws.ToString(connOut.VpnConnection.VpnConnectionId)
	require.NotEmpty(t, connOut.VpnConnection.VgwTelemetry)
	outsideIP := aws.ToString(connOut.VpnConnection.VgwTelemetry[0].OutsideIpAddress)
	require.NotEmpty(t, outsideIP)

	_, err = client.CreateVpnConnectionRoute(t.Context(), &ec2sdk.CreateVpnConnectionRouteInput{
		VpnConnectionId:      aws.String(connID),
		DestinationCidrBlock: aws.String("10.5.0.0/24"),
	})
	require.NoError(t, err)

	_, err = client.DeleteVpnConnectionRoute(t.Context(), &ec2sdk.DeleteVpnConnectionRouteInput{
		VpnConnectionId:      aws.String(connID),
		DestinationCidrBlock: aws.String("10.5.0.0/24"),
	})
	require.NoError(t, err)

	modConnOut, err := client.ModifyVpnConnection(t.Context(), &ec2sdk.ModifyVpnConnectionInput{
		VpnConnectionId: aws.String(connID),
		VpnGatewayId:    vgwOut.VpnGateway.VpnGatewayId,
	})
	require.NoError(t, err)
	assert.Equal(t, connID, aws.ToString(modConnOut.VpnConnection.VpnConnectionId))

	modOptsOut, err := client.ModifyVpnConnectionOptions(
		t.Context(),
		&ec2sdk.ModifyVpnConnectionOptionsInput{
			VpnConnectionId:      aws.String(connID),
			LocalIpv4NetworkCidr: aws.String("10.6.0.0/24"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, connID, aws.ToString(modOptsOut.VpnConnection.VpnConnectionId))

	modCertOut, err := client.ModifyVpnTunnelCertificate(
		t.Context(),
		&ec2sdk.ModifyVpnTunnelCertificateInput{
			VpnConnectionId:           aws.String(connID),
			VpnTunnelOutsideIpAddress: aws.String(outsideIP),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, connID, aws.ToString(modCertOut.VpnConnection.VpnConnectionId))

	modTunnelOut, err := client.ModifyVpnTunnelOptions(
		t.Context(),
		&ec2sdk.ModifyVpnTunnelOptionsInput{
			VpnConnectionId:           aws.String(connID),
			VpnTunnelOutsideIpAddress: aws.String(outsideIP),
			TunnelOptions: &types.ModifyVpnTunnelOptionsSpecification{
				DPDTimeoutSeconds: aws.Int32(45),
			},
			SkipTunnelReplacement: aws.Bool(true),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, connID, aws.ToString(modTunnelOut.VpnConnection.VpnConnectionId))

	replaceOut, err := client.ReplaceVpnTunnel(t.Context(), &ec2sdk.ReplaceVpnTunnelInput{
		VpnConnectionId:           aws.String(connID),
		VpnTunnelOutsideIpAddress: aws.String(outsideIP),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(replaceOut.Return))

	statusOut, err := client.GetActiveVpnTunnelStatus(
		t.Context(),
		&ec2sdk.GetActiveVpnTunnelStatusInput{
			VpnConnectionId:           aws.String(connID),
			VpnTunnelOutsideIpAddress: aws.String(outsideIP),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, statusOut.ActiveVpnTunnelStatus)

	sampleOut, err := client.GetVpnConnectionDeviceSampleConfiguration(
		t.Context(), &ec2sdk.GetVpnConnectionDeviceSampleConfigurationInput{
			VpnConnectionId:           aws.String(connID),
			VpnConnectionDeviceTypeId: aws.String("cisco-ios-17-x"),
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(sampleOut.VpnConnectionDeviceSampleConfiguration))

	replStatusOut, err := client.GetVpnTunnelReplacementStatus(
		t.Context(), &ec2sdk.GetVpnTunnelReplacementStatusInput{
			VpnConnectionId:           aws.String(connID),
			VpnTunnelOutsideIpAddress: aws.String(outsideIP),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, connID, aws.ToString(replStatusOut.VpnConnectionId))

	_, err = client.DeleteVpnConnection(t.Context(), &ec2sdk.DeleteVpnConnectionInput{
		VpnConnectionId: aws.String(connID),
	})
	require.NoError(t, err)

	vcOut, err := client.CreateVpnConcentrator(t.Context(), &ec2sdk.CreateVpnConcentratorInput{
		Type: types.VpnConcentratorTypeIpsec1,
	})
	require.NoError(t, err)
	require.NotNil(t, vcOut.VpnConcentrator)
	vcID := aws.ToString(vcOut.VpnConcentrator.VpnConcentratorId)

	descVCOut, err := client.DescribeVpnConcentrators(
		t.Context(),
		&ec2sdk.DescribeVpnConcentratorsInput{
			VpnConcentratorIds: []string{vcID},
		},
	)
	require.NoError(t, err)
	require.Len(t, descVCOut.VpnConcentrators, 1)

	delVCOut, err := client.DeleteVpnConcentrator(t.Context(), &ec2sdk.DeleteVpnConcentratorInput{
		VpnConcentratorId: aws.String(vcID),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(delVCOut.Return))
}

// runManagedPrefixListExtras covers DeleteManagedPrefixList,
// GetManagedPrefixListEntries, ModifyManagedPrefixList,
// RestoreManagedPrefixListVersion.
func runManagedPrefixListExtras(t *testing.T, _ *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	createOut, err := client.CreateManagedPrefixList(
		t.Context(),
		&ec2sdk.CreateManagedPrefixListInput{
			PrefixListName: aws.String("test-pl"),
			AddressFamily:  aws.String("IPv4"),
			MaxEntries:     aws.Int32(10),
			Entries: []types.AddPrefixListEntry{
				{Cidr: aws.String("10.10.0.0/16"), Description: aws.String("initial")},
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, createOut.PrefixList)
	plID := aws.ToString(createOut.PrefixList.PrefixListId)

	entriesOut, err := client.GetManagedPrefixListEntries(
		t.Context(),
		&ec2sdk.GetManagedPrefixListEntriesInput{
			PrefixListId: aws.String(plID),
		},
	)
	require.NoError(t, err)
	require.Len(t, entriesOut.Entries, 1)
	assert.Equal(t, "10.10.0.0/16", aws.ToString(entriesOut.Entries[0].Cidr))

	modOut, err := client.ModifyManagedPrefixList(t.Context(), &ec2sdk.ModifyManagedPrefixListInput{
		PrefixListId: aws.String(plID),
		AddEntries: []types.AddPrefixListEntry{
			{Cidr: aws.String("10.20.0.0/16"), Description: aws.String("second")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, modOut.PrefixList)
	assert.Equal(t, int64(2), aws.ToInt64(modOut.PrefixList.Version))

	restoreOut, err := client.RestoreManagedPrefixListVersion(
		t.Context(), &ec2sdk.RestoreManagedPrefixListVersionInput{
			PrefixListId:    aws.String(plID),
			PreviousVersion: aws.Int64(1),
			CurrentVersion:  aws.Int64(2),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, plID, aws.ToString(restoreOut.PrefixList.PrefixListId))

	delOut, err := client.DeleteManagedPrefixList(t.Context(), &ec2sdk.DeleteManagedPrefixListInput{
		PrefixListId: aws.String(plID),
	})
	require.NoError(t, err)
	assert.Equal(t, plID, aws.ToString(delOut.PrefixList.PrefixListId))
}

// runNetworkInsightsExtras covers DeleteNetworkInsightsAccessScope,
// DeleteNetworkInsightsAccessScopeAnalysis, DeleteNetworkInsightsAnalysis,
// DeleteNetworkInsightsPath, DescribeNetworkInsightsAccessScopes,
// DescribeNetworkInsightsPaths.
func runNetworkInsightsExtras(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	path, err := backend.CreateNetworkInsightsPath("eni-source-test", "eni-dest-test", "tcp", 443)
	require.NoError(t, err)

	descPathOut, err := client.DescribeNetworkInsightsPaths(
		t.Context(),
		&ec2sdk.DescribeNetworkInsightsPathsInput{
			NetworkInsightsPathIds: []string{path.NetworkInsightsPathID},
		},
	)
	require.NoError(t, err)
	require.Len(t, descPathOut.NetworkInsightsPaths, 1)
	assert.Equal(
		t,
		path.NetworkInsightsPathID,
		aws.ToString(descPathOut.NetworkInsightsPaths[0].NetworkInsightsPathId),
	)

	analysis, err := backend.StartNetworkInsightsAnalysis(path.NetworkInsightsPathID)
	require.NoError(t, err)

	delAnalysisOut, err := client.DeleteNetworkInsightsAnalysis(
		t.Context(), &ec2sdk.DeleteNetworkInsightsAnalysisInput{
			NetworkInsightsAnalysisId: aws.String(analysis.NetworkInsightsAnalysisID),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		analysis.NetworkInsightsAnalysisID,
		aws.ToString(delAnalysisOut.NetworkInsightsAnalysisId),
	)

	delPathOut, err := client.DeleteNetworkInsightsPath(
		t.Context(),
		&ec2sdk.DeleteNetworkInsightsPathInput{
			NetworkInsightsPathId: aws.String(path.NetworkInsightsPathID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, path.NetworkInsightsPathID, aws.ToString(delPathOut.NetworkInsightsPathId))

	scope, err := backend.CreateNetworkInsightsAccessScope()
	require.NoError(t, err)

	descScopeOut, err := client.DescribeNetworkInsightsAccessScopes(
		t.Context(), &ec2sdk.DescribeNetworkInsightsAccessScopesInput{
			NetworkInsightsAccessScopeIds: []string{scope.NetworkInsightsAccessScopeID},
		},
	)
	require.NoError(t, err)
	require.Len(t, descScopeOut.NetworkInsightsAccessScopes, 1)
	assert.Equal(
		t, scope.NetworkInsightsAccessScopeID,
		aws.ToString(descScopeOut.NetworkInsightsAccessScopes[0].NetworkInsightsAccessScopeId),
	)

	scopeAnalysis, err := backend.StartNetworkInsightsAccessScopeAnalysis(
		scope.NetworkInsightsAccessScopeID,
	)
	require.NoError(t, err)

	delScopeAnalysisOut, err := client.DeleteNetworkInsightsAccessScopeAnalysis(
		t.Context(), &ec2sdk.DeleteNetworkInsightsAccessScopeAnalysisInput{
			NetworkInsightsAccessScopeAnalysisId: aws.String(
				scopeAnalysis.NetworkInsightsAccessScopeAnalysisID,
			),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t, scopeAnalysis.NetworkInsightsAccessScopeAnalysisID,
		aws.ToString(delScopeAnalysisOut.NetworkInsightsAccessScopeAnalysisId),
	)

	delScopeOut, err := client.DeleteNetworkInsightsAccessScope(
		t.Context(), &ec2sdk.DeleteNetworkInsightsAccessScopeInput{
			NetworkInsightsAccessScopeId: aws.String(scope.NetworkInsightsAccessScopeID),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		scope.NetworkInsightsAccessScopeID,
		aws.ToString(delScopeOut.NetworkInsightsAccessScopeId),
	)
}
