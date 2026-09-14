package ec2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestRealClient_InstanceAndAccountDefaults covers ec2's next tier of uncovered ops
// (gopherstack-n3zi): core instance lifecycle
// ops, VPC/network attribute extras, instance attribute extras, the
// Application Status Check family, Capacity Manager, Mac Dedicated Host
// tasks, EBS/credit-specification account defaults, and the IPAM Policy
// family. Each row gets its own fresh handler+backend.
func TestRealClient_InstanceAndAccountDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client)
		name string
	}{
		{runCoreInstanceOps, "core_instance_ops"},
		{runVPCAndNetworkExtras, "vpc_and_network_extras"},
		{runInstanceAttributeExtras, "instance_attribute_extras"},
		{runApplicationStatusChecks, "application_status_checks"},
		{runCapacityManager, "capacity_manager"},
		{runMacHosts, "mac_hosts"},
		{runEBSAndCreditDefaults, "ebs_and_credit_defaults"},
		{runIpamPolicy, "ipam_policy"},
		{runInstanceAttrsMissed, "instance_attrs_missed"},
		{runVPCEndpointExtras, "vpc_endpoint_extras"},
		{runReservedInstancesAndHosts, "reserved_instances_and_hosts"},
		{runVPCEncryptionControl, "vpc_encryption_control"},
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

// runCoreInstanceOps covers StartInstances, RebootInstances,
// MonitorInstances, UnmonitorInstances, GetConsoleOutput,
// GetConsoleScreenshot, GetPasswordData, SendDiagnosticInterrupt.
func runCoreInstanceOps(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	instances, err := backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, instances, 1)
	instanceID := instances[0].ID

	consoleOut, err := client.GetConsoleOutput(t.Context(), &ec2sdk.GetConsoleOutputInput{
		InstanceId: aws.String(instanceID),
	})
	require.NoError(t, err)
	assert.Equal(t, instanceID, aws.ToString(consoleOut.InstanceId))
	assert.NotNil(t, consoleOut.Timestamp)

	screenshotOut, err := client.GetConsoleScreenshot(t.Context(), &ec2sdk.GetConsoleScreenshotInput{
		InstanceId: aws.String(instanceID),
	})
	require.NoError(t, err)
	assert.Equal(t, instanceID, aws.ToString(screenshotOut.InstanceId))

	pwOut, err := client.GetPasswordData(t.Context(), &ec2sdk.GetPasswordDataInput{
		InstanceId: aws.String(instanceID),
	})
	require.NoError(t, err)
	assert.Equal(t, instanceID, aws.ToString(pwOut.InstanceId))
	assert.NotNil(t, pwOut.Timestamp)

	monOut, err := client.MonitorInstances(t.Context(), &ec2sdk.MonitorInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)
	require.Len(t, monOut.InstanceMonitorings, 1)
	assert.Equal(t, instanceID, aws.ToString(monOut.InstanceMonitorings[0].InstanceId))
	assert.Equal(t, types.MonitoringStateEnabled, monOut.InstanceMonitorings[0].Monitoring.State)

	unmonOut, err := client.UnmonitorInstances(t.Context(), &ec2sdk.UnmonitorInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)
	require.Len(t, unmonOut.InstanceMonitorings, 1)
	assert.Equal(t, types.MonitoringStateDisabled, unmonOut.InstanceMonitorings[0].Monitoring.State)

	rebootOut, err := client.RebootInstances(t.Context(), &ec2sdk.RebootInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)
	require.NotNil(t, rebootOut)

	diagOut, err := client.SendDiagnosticInterrupt(t.Context(), &ec2sdk.SendDiagnosticInterruptInput{
		InstanceId: aws.String(instanceID),
	})
	require.NoError(t, err)
	require.NotNil(t, diagOut)

	_, err = client.StopInstances(t.Context(), &ec2sdk.StopInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)
	backend.TickLifecycleForTest() // stopping -> stopped

	startOut, err := client.StartInstances(t.Context(), &ec2sdk.StartInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)
	require.Len(t, startOut.StartingInstances, 1)
	assert.Equal(t, instanceID, aws.ToString(startOut.StartingInstances[0].InstanceId))
	assert.Equal(t, types.InstanceStateNamePending, startOut.StartingInstances[0].CurrentState.Name)
}

// runVPCAndNetworkExtras covers ModifyVpcAttribute, ModifyVpcTenancy,
// DisassociateIamInstanceProfile, ReplaceIamInstanceProfileAssociation,
// DetachClassicLinkVpc, DisableVpcClassicLink, DisableVpcClassicLinkDnsSupport,
// ReplaceNetworkAclAssociation, ReplaceNetworkAclEntry, ReplaceRoute,
// DisableVgwRoutePropagation, EnableVgwRoutePropagation, ReleaseAddress,
// AcceptAddressTransfer.
func runVPCAndNetworkExtras(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	vpc, err := backend.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)

	_, err = client.ModifyVpcAttribute(t.Context(), &ec2sdk.ModifyVpcAttributeInput{
		VpcId:              aws.String(vpc.ID),
		EnableDnsSupport:   &types.AttributeBooleanValue{Value: aws.Bool(false)},
		EnableDnsHostnames: nil,
	})
	require.NoError(t, err)

	_, err = client.ModifyVpcTenancy(t.Context(), &ec2sdk.ModifyVpcTenancyInput{
		VpcId:           aws.String(vpc.ID),
		InstanceTenancy: types.VpcTenancyDefault,
	})
	require.NoError(t, err)

	require.NoError(t, backend.EnableVpcClassicLink(vpc.ID))

	instances, err := backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID
	require.NoError(t, backend.AttachClassicLinkVpc(instanceID, vpc.ID, nil))

	detachOut, err := client.DetachClassicLinkVpc(t.Context(), &ec2sdk.DetachClassicLinkVpcInput{
		InstanceId: aws.String(instanceID),
		VpcId:      aws.String(vpc.ID),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(detachOut.Return))

	classicLinkOut, err := client.DisableVpcClassicLink(t.Context(), &ec2sdk.DisableVpcClassicLinkInput{
		VpcId: aws.String(vpc.ID),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(classicLinkOut.Return))

	dnsOut, err := client.DisableVpcClassicLinkDnsSupport(
		t.Context(),
		&ec2sdk.DisableVpcClassicLinkDnsSupportInput{VpcId: aws.String(vpc.ID)},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(dnsOut.Return))

	profileOut, err := client.AssociateIamInstanceProfile(
		t.Context(),
		&ec2sdk.AssociateIamInstanceProfileInput{
			InstanceId:         aws.String(instanceID),
			IamInstanceProfile: &types.IamInstanceProfileSpecification{Name: aws.String("test-profile")},
		},
	)
	require.NoError(t, err)
	assocID := aws.ToString(profileOut.IamInstanceProfileAssociation.AssociationId)

	replaceProfOut, err := client.ReplaceIamInstanceProfileAssociation(
		t.Context(),
		&ec2sdk.ReplaceIamInstanceProfileAssociationInput{
			AssociationId:      aws.String(assocID),
			IamInstanceProfile: &types.IamInstanceProfileSpecification{Name: aws.String("test-profile-2")},
		},
	)
	require.NoError(t, err)
	newAssocID := aws.ToString(replaceProfOut.IamInstanceProfileAssociation.AssociationId)
	assert.NotEmpty(t, newAssocID)
	assert.Equal(
		t,
		"test-profile-2",
		aws.ToString(replaceProfOut.IamInstanceProfileAssociation.IamInstanceProfile.Id),
	)

	disassocOut, err := client.DisassociateIamInstanceProfile(
		t.Context(),
		&ec2sdk.DisassociateIamInstanceProfileInput{AssociationId: aws.String(newAssocID)},
	)
	require.NoError(t, err)
	assert.Equal(t, newAssocID, aws.ToString(disassocOut.IamInstanceProfileAssociation.AssociationId))

	rt, err := backend.CreateRouteTable(vpc.ID)
	require.NoError(t, err)
	igw, err := backend.CreateInternetGateway()
	require.NoError(t, err)
	require.NoError(t, backend.CreateRoute(rt.ID, "0.0.0.0/0", igw.ID, ""))

	_, err = client.ReplaceRoute(t.Context(), &ec2sdk.ReplaceRouteInput{
		RouteTableId:         aws.String(rt.ID),
		DestinationCidrBlock: aws.String("0.0.0.0/0"),
		GatewayId:            aws.String(igw.ID),
	})
	require.NoError(t, err)

	enableVgwOut, err := client.EnableVgwRoutePropagation(
		t.Context(),
		&ec2sdk.EnableVgwRoutePropagationInput{
			RouteTableId: aws.String(rt.ID),
			GatewayId:    aws.String(igw.ID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, enableVgwOut)

	disableVgwOut, err := client.DisableVgwRoutePropagation(
		t.Context(),
		&ec2sdk.DisableVgwRoutePropagationInput{
			RouteTableId: aws.String(rt.ID),
			GatewayId:    aws.String(igw.ID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, disableVgwOut)

	subnet, err := backend.CreateSubnet(vpc.ID, "10.0.1.0/24", "us-east-1a")
	require.NoError(t, err)
	acl, err := backend.CreateNetworkACL(vpc.ID)
	require.NoError(t, err)

	require.NoError(
		t,
		backend.CreateNetworkACLEntry(acl.ID, 100, "6", "allow", "10.0.0.0/16", false, 22, 22),
	)

	replaceEntryOut, err := client.ReplaceNetworkAclEntry(
		t.Context(),
		&ec2sdk.ReplaceNetworkAclEntryInput{
			NetworkAclId: aws.String(acl.ID),
			RuleNumber:   aws.Int32(100),
			Protocol:     aws.String("6"),
			RuleAction:   types.RuleActionAllow,
			CidrBlock:    aws.String("10.0.0.0/8"),
			Egress:       aws.Bool(false),
			PortRange:    &types.PortRange{From: aws.Int32(443), To: aws.Int32(443)},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, replaceEntryOut)

	// ReplaceNetworkAclAssociation: this backend does not model a
	// NetworkAclAssociationId distinct from the associated SubnetId (a
	// disclosed simplification, handler_filters.go's applyNetworkACLFilters
	// doc comment) -- the handler reads the request's AssociationId
	// parameter as the subnet to (re)associate. Exercised as the handler
	// actually expects, per that documented simplification.
	replaceAssocOut, err := client.ReplaceNetworkAclAssociation(
		t.Context(),
		&ec2sdk.ReplaceNetworkAclAssociationInput{
			NetworkAclId:  aws.String(acl.ID),
			AssociationId: aws.String(subnet.ID),
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(replaceAssocOut.NewAssociationId))

	addr, err := backend.AllocateAddress()
	require.NoError(t, err)

	_, err = backend.EnableAddressTransfer(addr.AllocationID, "111111111111")
	require.NoError(t, err)

	transferOut, err := client.AcceptAddressTransfer(t.Context(), &ec2sdk.AcceptAddressTransferInput{
		Address: aws.String(addr.PublicIP),
	})
	require.NoError(t, err)
	require.NotNil(t, transferOut.AddressTransfer)
	assert.Equal(t, addr.AllocationID, aws.ToString(transferOut.AddressTransfer.AllocationId))
	assert.Equal(t, addr.PublicIP, aws.ToString(transferOut.AddressTransfer.PublicIp))
	assert.Equal(t, types.AddressTransferStatusAccepted, transferOut.AddressTransfer.AddressTransferStatus)

	releaseOut, err := client.ReleaseAddress(t.Context(), &ec2sdk.ReleaseAddressInput{
		AllocationId: aws.String(addr.AllocationID),
	})
	require.NoError(t, err)
	require.NotNil(t, releaseOut)
}

// runInstanceAttributeExtras covers ModifyPrivateDnsNameOptions,
// ModifyInstanceEventWindow, DisassociateInstanceEventWindow,
// ModifyInstanceCapacityReservationAttributes,
// ModifyInstanceNetworkPerformanceOptions, GetInstanceUefiData,
// GetInstanceTpmEkPub, DescribeInstanceEventNotificationAttributes,
// ModifyAvailabilityZoneGroup, ModifyIdentityIdFormat,
// GetInstanceMetadataDefaults, ModifyInstanceMetadataDefaults,
// CreateInstanceConnectEndpoint, DeleteInstanceConnectEndpoint,
// ModifyInstanceConnectEndpoint.
func runInstanceAttributeExtras(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	instances, err := backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	dnsOut, err := client.ModifyPrivateDnsNameOptions(
		t.Context(),
		&ec2sdk.ModifyPrivateDnsNameOptionsInput{
			InstanceId:             aws.String(instanceID),
			PrivateDnsHostnameType: types.HostnameTypeResourceName,
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(dnsOut.Return))

	ew, err := backend.CreateInstanceEventWindow("test-window", "0 2 * * *")
	require.NoError(t, err)

	modWindowOut, err := client.ModifyInstanceEventWindow(
		t.Context(),
		&ec2sdk.ModifyInstanceEventWindowInput{
			InstanceEventWindowId: aws.String(ew.InstanceEventWindowID),
			Name:                  aws.String("test-window-renamed"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modWindowOut.InstanceEventWindow)
	assert.Equal(t, "test-window-renamed", aws.ToString(modWindowOut.InstanceEventWindow.Name))

	assocOut, err := client.AssociateInstanceEventWindow(
		t.Context(),
		&ec2sdk.AssociateInstanceEventWindowInput{
			InstanceEventWindowId: aws.String(ew.InstanceEventWindowID),
			AssociationTarget: &types.InstanceEventWindowAssociationRequest{
				InstanceIds: []string{instanceID},
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, assocOut.InstanceEventWindow)

	disassocOut, err := client.DisassociateInstanceEventWindow(
		t.Context(),
		&ec2sdk.DisassociateInstanceEventWindowInput{
			InstanceEventWindowId: aws.String(ew.InstanceEventWindowID),
			AssociationTarget: &types.InstanceEventWindowDisassociationRequest{
				InstanceIds: []string{instanceID},
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, disassocOut.InstanceEventWindow)

	capResOut, err := client.ModifyInstanceCapacityReservationAttributes(
		t.Context(),
		&ec2sdk.ModifyInstanceCapacityReservationAttributesInput{
			InstanceId: aws.String(instanceID),
			CapacityReservationSpecification: &types.CapacityReservationSpecification{
				CapacityReservationPreference: types.CapacityReservationPreferenceOpen,
			},
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(capResOut.Return))

	netPerfOut, err := client.ModifyInstanceNetworkPerformanceOptions(
		t.Context(),
		&ec2sdk.ModifyInstanceNetworkPerformanceOptionsInput{
			InstanceId:         aws.String(instanceID),
			BandwidthWeighting: types.InstanceBandwidthWeightingVpc1,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, instanceID, aws.ToString(netPerfOut.InstanceId))
	assert.Equal(t, types.InstanceBandwidthWeightingVpc1, netPerfOut.BandwidthWeighting)

	uefiOut, err := client.GetInstanceUefiData(t.Context(), &ec2sdk.GetInstanceUefiDataInput{
		InstanceId: aws.String(instanceID),
	})
	require.NoError(t, err)
	assert.Equal(t, instanceID, aws.ToString(uefiOut.InstanceId))

	tpmOut, err := client.GetInstanceTpmEkPub(t.Context(), &ec2sdk.GetInstanceTpmEkPubInput{
		InstanceId: aws.String(instanceID),
		KeyType:    types.EkPubKeyTypeRsa2048,
		KeyFormat:  types.EkPubKeyFormatDer,
	})
	require.NoError(t, err)
	assert.Equal(t, instanceID, aws.ToString(tpmOut.InstanceId))
	assert.Equal(t, types.EkPubKeyTypeRsa2048, tpmOut.KeyType)

	notifAttrOut, err := client.DescribeInstanceEventNotificationAttributes(
		t.Context(), &ec2sdk.DescribeInstanceEventNotificationAttributesInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, notifAttrOut.InstanceTagAttribute)

	azGroupOut, err := client.ModifyAvailabilityZoneGroup(
		t.Context(), &ec2sdk.ModifyAvailabilityZoneGroupInput{
			GroupName:   aws.String("us-east-1-wl1-bos-wlz1"),
			OptInStatus: types.ModifyAvailabilityZoneOptInStatusOptedIn,
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(azGroupOut.Return))

	idFormatOut, err := client.ModifyIdentityIdFormat(t.Context(), &ec2sdk.ModifyIdentityIdFormatInput{
		PrincipalArn: aws.String("arn:aws:iam::000000000000:root"),
		Resource:     aws.String("instance"),
		UseLongIds:   aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotNil(t, idFormatOut)

	metaDefaultsOut, err := client.GetInstanceMetadataDefaults(
		t.Context(), &ec2sdk.GetInstanceMetadataDefaultsInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, metaDefaultsOut.AccountLevel)

	modMetaDefaultsOut, err := client.ModifyInstanceMetadataDefaults(
		t.Context(), &ec2sdk.ModifyInstanceMetadataDefaultsInput{
			HttpTokens:              types.MetadataDefaultHttpTokensStateRequired,
			HttpEndpoint:            types.DefaultInstanceMetadataEndpointStateEnabled,
			HttpPutResponseHopLimit: aws.Int32(2),
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(modMetaDefaultsOut.Return))

	subnet, err := backend.CreateSubnet("", "10.0.1.0/24", "us-east-1a")
	if err != nil {
		vpc, vpcErr := backend.CreateVpc("10.0.0.0/16", "default")
		require.NoError(t, vpcErr)
		subnet, err = backend.CreateSubnet(vpc.ID, "10.0.1.0/24", "us-east-1a")
		require.NoError(t, err)
	}

	iceOut, err := client.CreateInstanceConnectEndpoint(
		t.Context(), &ec2sdk.CreateInstanceConnectEndpointInput{
			SubnetId: aws.String(subnet.ID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, iceOut.InstanceConnectEndpoint)
	iceID := aws.ToString(iceOut.InstanceConnectEndpoint.InstanceConnectEndpointId)

	modIceOut, err := client.ModifyInstanceConnectEndpoint(
		t.Context(), &ec2sdk.ModifyInstanceConnectEndpointInput{
			InstanceConnectEndpointId: aws.String(iceID),
			PreserveClientIp:          aws.Bool(true),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modIceOut)

	delIceOut, err := client.DeleteInstanceConnectEndpoint(
		t.Context(), &ec2sdk.DeleteInstanceConnectEndpointInput{
			InstanceConnectEndpointId: aws.String(iceID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, delIceOut.InstanceConnectEndpoint)
	assert.Equal(t, iceID, aws.ToString(delIceOut.InstanceConnectEndpoint.InstanceConnectEndpointId))
}

// runApplicationStatusChecks covers CreateApplicationStatusCheck,
// ModifyApplicationStatusCheck, DescribeApplicationStatusChecks,
// AssociateApplicationStatusCheck, DisassociateApplicationStatusCheck,
// DescribeApplicationStatusCheckAssociations,
// EnableApplicationStatusCheckSuppression,
// DisableApplicationStatusCheckSuppression, DescribeApplicationStatus,
// DeleteApplicationStatusCheck.
func runApplicationStatusChecks(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	instances, err := backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	createOut, err := client.CreateApplicationStatusCheck(
		t.Context(), &ec2sdk.CreateApplicationStatusCheckInput{
			Protocol: types.NetworkProtocolEnumHttp,
			Port:     aws.Int32(80),
			Path:     aws.String("/health"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, createOut.ApplicationStatusCheck)
	checkID := aws.ToString(createOut.ApplicationStatusCheck.ApplicationStatusCheckId)
	assert.Equal(t, "/health", aws.ToString(createOut.ApplicationStatusCheck.Path))

	modOut, err := client.ModifyApplicationStatusCheck(
		t.Context(), &ec2sdk.ModifyApplicationStatusCheckInput{
			ApplicationStatusCheckId: aws.String(checkID),
			Path:                     aws.String("/status"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modOut.ApplicationStatusCheck)
	assert.Equal(t, "/status", aws.ToString(modOut.ApplicationStatusCheck.Path))

	descOut, err := client.DescribeApplicationStatusChecks(
		t.Context(), &ec2sdk.DescribeApplicationStatusChecksInput{
			ApplicationStatusCheckIds: []string{checkID},
		},
	)
	require.NoError(t, err)
	require.Len(t, descOut.ApplicationStatusChecks, 1)
	assert.Equal(t, checkID, aws.ToString(descOut.ApplicationStatusChecks[0].ApplicationStatusCheckId))

	assocOut, err := client.AssociateApplicationStatusCheck(
		t.Context(), &ec2sdk.AssociateApplicationStatusCheckInput{
			ApplicationStatusCheckId: aws.String(checkID),
			InstanceIds:              []string{instanceID},
		},
	)
	require.NoError(t, err)
	require.Len(t, assocOut.SuccessfulResults, 1)
	assert.Equal(t, checkID, aws.ToString(assocOut.SuccessfulResults[0].ApplicationStatusCheckId))

	descAssocOut, err := client.DescribeApplicationStatusCheckAssociations(
		t.Context(), &ec2sdk.DescribeApplicationStatusCheckAssociationsInput{
			ApplicationStatusCheckIds: []string{checkID},
		},
	)
	require.NoError(t, err)
	require.Len(t, descAssocOut.Associations, 1)

	statusOut, err := client.DescribeApplicationStatus(
		t.Context(), &ec2sdk.DescribeApplicationStatusInput{
			InstanceIds: []string{instanceID},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, statusOut.ApplicationStatuses)
	assert.NotEmpty(t, statusOut.ApplicationStatuses.Instances)

	suppressOut, err := client.EnableApplicationStatusCheckSuppression(
		t.Context(), &ec2sdk.EnableApplicationStatusCheckSuppressionInput{
			InstanceIds:     []string{instanceID},
			DurationSeconds: aws.Int32(300),
		},
	)
	require.NoError(t, err)
	require.Len(t, suppressOut.SuccessfulResults, 1)
	assert.Equal(t, instanceID, aws.ToString(suppressOut.SuccessfulResults[0].InstanceId))

	unsuppressOut, err := client.DisableApplicationStatusCheckSuppression(
		t.Context(), &ec2sdk.DisableApplicationStatusCheckSuppressionInput{
			InstanceIds: []string{instanceID},
		},
	)
	require.NoError(t, err)
	require.Len(t, unsuppressOut.SuccessfulResults, 1)

	disassocOut, err := client.DisassociateApplicationStatusCheck(
		t.Context(), &ec2sdk.DisassociateApplicationStatusCheckInput{
			ApplicationStatusCheckId: aws.String(checkID),
			InstanceIds:              []string{instanceID},
		},
	)
	require.NoError(t, err)
	require.Len(t, disassocOut.SuccessfulResults, 1)

	delOut, err := client.DeleteApplicationStatusCheck(
		t.Context(), &ec2sdk.DeleteApplicationStatusCheckInput{
			ApplicationStatusCheckId: aws.String(checkID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, delOut.ApplicationStatusCheck)
	assert.Equal(t, checkID, aws.ToString(delOut.ApplicationStatusCheck.ApplicationStatusCheckId))
}

// runCapacityManager covers EnableCapacityManager, GetCapacityManagerAttributes,
// GetCapacityManagerMetricData, GetCapacityManagerMetricDimensions,
// UpdateCapacityManagerOrganizationsAccess, CreateCapacityManagerDataExport
// (setup, already covered), DeleteCapacityManagerDataExport,
// DisableCapacityManager.
func runCapacityManager(t *testing.T, _ *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	enableOut, err := client.EnableCapacityManager(t.Context(), &ec2sdk.EnableCapacityManagerInput{
		OrganizationsAccess: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(enableOut.OrganizationsAccess))
	assert.NotEmpty(t, enableOut.CapacityManagerStatus)

	attrsOut, err := client.GetCapacityManagerAttributes(
		t.Context(), &ec2sdk.GetCapacityManagerAttributesInput{},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(attrsOut.OrganizationsAccess))

	now := time.Now().UTC()
	metricDataOut, err := client.GetCapacityManagerMetricData(
		t.Context(), &ec2sdk.GetCapacityManagerMetricDataInput{
			StartTime:   aws.Time(now.Add(-time.Hour)),
			EndTime:     aws.Time(now),
			Period:      aws.Int32(3600),
			MetricNames: []types.Metric{types.MetricReservationUnusedTotalCapacityHrsInst},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, metricDataOut)

	metricDimsOut, err := client.GetCapacityManagerMetricDimensions(
		t.Context(), &ec2sdk.GetCapacityManagerMetricDimensionsInput{
			StartTime:   aws.Time(now.Add(-time.Hour)),
			EndTime:     aws.Time(now),
			MetricNames: []types.Metric{types.MetricReservationUnusedTotalCapacityHrsInst},
			GroupBy:     []types.GroupBy{types.GroupByAccountId},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, metricDimsOut)

	updOrgOut, err := client.UpdateCapacityManagerOrganizationsAccess(
		t.Context(), &ec2sdk.UpdateCapacityManagerOrganizationsAccessInput{
			OrganizationsAccess: aws.Bool(false),
		},
	)
	require.NoError(t, err)
	assert.False(t, aws.ToBool(updOrgOut.OrganizationsAccess))

	exportOut, err := client.CreateCapacityManagerDataExport(
		t.Context(), &ec2sdk.CreateCapacityManagerDataExportInput{
			OutputFormat:   types.OutputFormatParquet,
			S3BucketName:   aws.String("test-bucket"),
			S3BucketPrefix: aws.String("exports/"),
			Schedule:       types.ScheduleHourly,
		},
	)
	require.NoError(t, err)
	exportID := aws.ToString(exportOut.CapacityManagerDataExportId)
	require.NotEmpty(t, exportID)

	delExportOut, err := client.DeleteCapacityManagerDataExport(
		t.Context(), &ec2sdk.DeleteCapacityManagerDataExportInput{
			CapacityManagerDataExportId: aws.String(exportID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, exportID, aws.ToString(delExportOut.CapacityManagerDataExportId))

	disableOut, err := client.DisableCapacityManager(t.Context(), &ec2sdk.DisableCapacityManagerInput{})
	require.NoError(t, err)
	require.NotNil(t, disableOut)
}

// runMacHosts covers DescribeMacHosts, CreateMacSystemIntegrityProtectionModificationTask,
// CreateDelegateMacVolumeOwnershipTask, DescribeMacModificationTasks.
func runMacHosts(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	hosts, err := backend.AllocateHosts("us-east-1a", "mac2.metal", 1)
	require.NoError(t, err)
	require.Len(t, hosts, 1)
	hostID := hosts[0].HostID

	instances, err := backend.RunInstances("ami-test", "mac2.metal", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	describeHostsOut, err := client.DescribeMacHosts(t.Context(), &ec2sdk.DescribeMacHostsInput{
		HostIds: []string{hostID},
	})
	require.NoError(t, err)
	require.Len(t, describeHostsOut.MacHosts, 1)
	assert.Equal(t, hostID, aws.ToString(describeHostsOut.MacHosts[0].HostId))

	sipOut, err := client.CreateMacSystemIntegrityProtectionModificationTask(
		t.Context(), &ec2sdk.CreateMacSystemIntegrityProtectionModificationTaskInput{
			InstanceId:                         aws.String(instanceID),
			MacSystemIntegrityProtectionStatus: types.MacSystemIntegrityProtectionSettingStatusDisabled,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, sipOut.MacModificationTask)
	assert.Equal(t, instanceID, aws.ToString(sipOut.MacModificationTask.InstanceId))
	sipTaskID := aws.ToString(sipOut.MacModificationTask.MacModificationTaskId)

	delegateOut, err := client.CreateDelegateMacVolumeOwnershipTask(
		t.Context(), &ec2sdk.CreateDelegateMacVolumeOwnershipTaskInput{
			InstanceId:     aws.String(instanceID),
			MacCredentials: aws.String("dummy-credentials"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, delegateOut.MacModificationTask)
	delegateTaskID := aws.ToString(delegateOut.MacModificationTask.MacModificationTaskId)

	tasksOut, err := client.DescribeMacModificationTasks(
		t.Context(), &ec2sdk.DescribeMacModificationTasksInput{
			MacModificationTaskIds: []string{sipTaskID, delegateTaskID},
		},
	)
	require.NoError(t, err)
	require.Len(t, tasksOut.MacModificationTasks, 2)
}

// runEBSAndCreditDefaults covers EnableEbsEncryptionByDefault,
// DisableEbsEncryptionByDefault, GetEbsEncryptionByDefault,
// GetEbsDefaultKmsKeyId, ModifyDefaultCreditSpecification,
// GetDefaultCreditSpecification, EnableSerialConsoleAccess (setup, already
// covered), DisableSerialConsoleAccess, GetSerialConsoleAccessStatus.
func runEBSAndCreditDefaults(t *testing.T, _ *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	enableEncOut, err := client.EnableEbsEncryptionByDefault(
		t.Context(), &ec2sdk.EnableEbsEncryptionByDefaultInput{},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(enableEncOut.EbsEncryptionByDefault))

	getEncOut, err := client.GetEbsEncryptionByDefault(
		t.Context(), &ec2sdk.GetEbsEncryptionByDefaultInput{},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(getEncOut.EbsEncryptionByDefault))

	disableEncOut, err := client.DisableEbsEncryptionByDefault(
		t.Context(), &ec2sdk.DisableEbsEncryptionByDefaultInput{},
	)
	require.NoError(t, err)
	assert.False(t, aws.ToBool(disableEncOut.EbsEncryptionByDefault))

	kmsKeyOut, err := client.GetEbsDefaultKmsKeyId(t.Context(), &ec2sdk.GetEbsDefaultKmsKeyIdInput{})
	require.NoError(t, err)
	require.NotNil(t, kmsKeyOut)

	modCreditOut, err := client.ModifyDefaultCreditSpecification(
		t.Context(), &ec2sdk.ModifyDefaultCreditSpecificationInput{
			InstanceFamily: types.UnlimitedSupportedInstanceFamilyT3,
			CpuCredits:     aws.String("unlimited"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "unlimited", aws.ToString(modCreditOut.InstanceFamilyCreditSpecification.CpuCredits))

	getCreditOut, err := client.GetDefaultCreditSpecification(
		t.Context(), &ec2sdk.GetDefaultCreditSpecificationInput{
			InstanceFamily: types.UnlimitedSupportedInstanceFamilyT3,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "unlimited", aws.ToString(getCreditOut.InstanceFamilyCreditSpecification.CpuCredits))

	_, err = client.EnableSerialConsoleAccess(t.Context(), &ec2sdk.EnableSerialConsoleAccessInput{})
	require.NoError(t, err)

	statusOut, err := client.GetSerialConsoleAccessStatus(
		t.Context(), &ec2sdk.GetSerialConsoleAccessStatusInput{},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(statusOut.SerialConsoleAccessEnabled))

	disableSerialOut, err := client.DisableSerialConsoleAccess(
		t.Context(), &ec2sdk.DisableSerialConsoleAccessInput{},
	)
	require.NoError(t, err)
	assert.False(t, aws.ToBool(disableSerialOut.SerialConsoleAccessEnabled))
}

// runIpamPolicy covers CreateIpamPolicy, DescribeIpamPolicies,
// EnableIpamPolicy, GetEnabledIpamPolicy, ModifyIpamPolicyAllocationRules,
// GetIpamPolicyAllocationRules, GetIpamPolicyOrganizationTargets,
// DisableIpamPolicy, DeleteIpamPolicy, EnableIpamOrganizationAdminAccount,
// DisableIpamOrganizationAdminAccount, MoveByoipCidrToIpam.
func runIpamPolicy(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	ipam, err := backend.CreateIpam()
	require.NoError(t, err)

	createOut, err := client.CreateIpamPolicy(t.Context(), &ec2sdk.CreateIpamPolicyInput{
		IpamId: aws.String(ipam.IpamID),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.IpamPolicy)
	policyID := aws.ToString(createOut.IpamPolicy.IpamPolicyId)
	assert.Equal(t, ipam.IpamID, aws.ToString(createOut.IpamPolicy.IpamId))

	descOut, err := client.DescribeIpamPolicies(t.Context(), &ec2sdk.DescribeIpamPoliciesInput{
		IpamPolicyIds: []string{policyID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.IpamPolicies, 1)

	enableOut, err := client.EnableIpamPolicy(t.Context(), &ec2sdk.EnableIpamPolicyInput{
		IpamPolicyId: aws.String(policyID),
	})
	require.NoError(t, err)
	assert.Equal(t, policyID, aws.ToString(enableOut.IpamPolicyId))

	getEnabledOut, err := client.GetEnabledIpamPolicy(
		t.Context(), &ec2sdk.GetEnabledIpamPolicyInput{},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(getEnabledOut.IpamPolicyEnabled))
	assert.Equal(t, policyID, aws.ToString(getEnabledOut.IpamPolicyId))

	modRulesOut, err := client.ModifyIpamPolicyAllocationRules(
		t.Context(), &ec2sdk.ModifyIpamPolicyAllocationRulesInput{
			IpamPolicyId: aws.String(policyID),
			Locale:       aws.String("us-east-1"),
			ResourceType: types.IpamPolicyResourceTypeEip,
			AllocationRules: []types.IpamPolicyAllocationRuleRequest{
				{SourceIpamPoolId: aws.String("ipam-pool-test")},
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modRulesOut.IpamPolicyDocument)
	require.Len(t, modRulesOut.IpamPolicyDocument.AllocationRules, 1)
	assert.Equal(
		t,
		"ipam-pool-test",
		aws.ToString(modRulesOut.IpamPolicyDocument.AllocationRules[0].SourceIpamPoolId),
	)

	getRulesOut, err := client.GetIpamPolicyAllocationRules(
		t.Context(), &ec2sdk.GetIpamPolicyAllocationRulesInput{
			IpamPolicyId: aws.String(policyID),
			Locale:       aws.String("us-east-1"),
			ResourceType: types.IpamPolicyResourceTypeEip,
		},
	)
	require.NoError(t, err)
	require.Len(t, getRulesOut.IpamPolicyDocuments, 1)

	orgTargetsOut, err := client.GetIpamPolicyOrganizationTargets(
		t.Context(), &ec2sdk.GetIpamPolicyOrganizationTargetsInput{
			IpamPolicyId: aws.String(policyID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, orgTargetsOut)

	disableOut, err := client.DisableIpamPolicy(t.Context(), &ec2sdk.DisableIpamPolicyInput{
		IpamPolicyId: aws.String(policyID),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(disableOut.Return))

	delOut, err := client.DeleteIpamPolicy(t.Context(), &ec2sdk.DeleteIpamPolicyInput{
		IpamPolicyId: aws.String(policyID),
	})
	require.NoError(t, err)
	assert.Equal(t, policyID, aws.ToString(delOut.IpamPolicy.IpamPolicyId))

	enableOrgOut, err := client.EnableIpamOrganizationAdminAccount(
		t.Context(), &ec2sdk.EnableIpamOrganizationAdminAccountInput{
			DelegatedAdminAccountId: aws.String("111111111111"),
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(enableOrgOut.Success))

	disableOrgOut, err := client.DisableIpamOrganizationAdminAccount(
		t.Context(), &ec2sdk.DisableIpamOrganizationAdminAccountInput{
			DelegatedAdminAccountId: aws.String("111111111111"),
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(disableOrgOut.Success))

	_, err = backend.ProvisionByoipCidr("203.0.113.0/24", "test byoip")
	require.NoError(t, err)
	pool, err := backend.CreateIpamPool(ipam.IpamID, "ipv4", "us-east-1", "")
	require.NoError(t, err)

	moveOut, err := client.MoveByoipCidrToIpam(t.Context(), &ec2sdk.MoveByoipCidrToIpamInput{
		Cidr:          aws.String("203.0.113.0/24"),
		IpamPoolId:    aws.String(pool.IpamPoolID),
		IpamPoolOwner: aws.String("000000000000"),
	})
	require.NoError(t, err)
	require.NotNil(t, moveOut.ByoipCidr)
	assert.Equal(t, "203.0.113.0/24", aws.ToString(moveOut.ByoipCidr.Cidr))
}

// runInstanceAttrsMissed covers ModifyInstanceEventStartTime and
// ModifyInstanceMetadataOptions.
func runInstanceAttrsMissed(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	instances, err := backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	notBefore := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	startTimeOut, err := client.ModifyInstanceEventStartTime(
		t.Context(), &ec2sdk.ModifyInstanceEventStartTimeInput{
			InstanceId:      aws.String(instanceID),
			InstanceEventId: aws.String("event-test-1"),
			NotBefore:       aws.Time(notBefore),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, startTimeOut.Event)
	assert.Equal(t, "event-test-1", aws.ToString(startTimeOut.Event.InstanceEventId))
	assert.True(t, notBefore.Equal(aws.ToTime(startTimeOut.Event.NotBefore)))

	metaOut, err := client.ModifyInstanceMetadataOptions(
		t.Context(), &ec2sdk.ModifyInstanceMetadataOptionsInput{
			InstanceId:              aws.String(instanceID),
			HttpTokens:              types.HttpTokensStateRequired,
			HttpEndpoint:            types.InstanceMetadataEndpointStateEnabled,
			HttpPutResponseHopLimit: aws.Int32(3),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, instanceID, aws.ToString(metaOut.InstanceId))
	require.NotNil(t, metaOut.InstanceMetadataOptions)
	assert.Equal(t, types.HttpTokensStateRequired, metaOut.InstanceMetadataOptions.HttpTokens)
	assert.Equal(t, int32(3), aws.ToInt32(metaOut.InstanceMetadataOptions.HttpPutResponseHopLimit))
}

// runVPCEndpointExtras covers ModifyVpcEndpoint, RejectVpcEndpointConnections,
// ModifyVpcEndpointServiceConfiguration, ModifyVpcEndpointPayerResponsibility,
// ModifyVpcEndpointServicePayerResponsibility,
// DeleteVpcEndpointConnectionNotifications,
// DeleteVpcEndpointServiceConfigurations, DescribeVpcEndpointAssociations,
// DescribeVpcEndpointServicePermissions.
func runVPCEndpointExtras(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	vpc, err := backend.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)
	subnet, err := backend.CreateSubnet(vpc.ID, "10.0.1.0/24", "us-east-1a")
	require.NoError(t, err)

	ep, err := backend.CreateVpcEndpoint(vpc.ID, "com.amazonaws.us-east-1.s3", "Gateway", nil)
	require.NoError(t, err)

	modEpOut, err := client.ModifyVpcEndpoint(t.Context(), &ec2sdk.ModifyVpcEndpointInput{
		VpcEndpointId: aws.String(ep.ID),
		AddSubnetIds:  []string{subnet.ID},
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(modEpOut.Return))

	descAssocOut, err := client.DescribeVpcEndpointAssociations(
		t.Context(), &ec2sdk.DescribeVpcEndpointAssociationsInput{
			VpcEndpointIds: []string{ep.ID},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, descAssocOut)

	payerOut, err := client.ModifyVpcEndpointPayerResponsibility(
		t.Context(), &ec2sdk.ModifyVpcEndpointPayerResponsibilityInput{
			VpcEndpointId:       aws.String(ep.ID),
			PayerResponsibility: types.PayerResponsibilityTypeVpcEndpointAccount,
			Scope:               types.PayerResponsibilityScopeVpcEndpointCharges,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, ep.ID, aws.ToString(payerOut.VpcEndpointId))

	cfg, err := backend.CreateVpcEndpointServiceConfiguration(false, nil)
	require.NoError(t, err)

	svcPayerOut, err := client.ModifyVpcEndpointServicePayerResponsibility(
		t.Context(), &ec2sdk.ModifyVpcEndpointServicePayerResponsibilityInput{
			ServiceId:           aws.String(cfg.ServiceID),
			PayerResponsibility: types.PayerResponsibilityServiceOwner,
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(svcPayerOut.ReturnValue))

	svcCfgOut, err := client.ModifyVpcEndpointServiceConfiguration(
		t.Context(), &ec2sdk.ModifyVpcEndpointServiceConfigurationInput{
			ServiceId:          aws.String(cfg.ServiceID),
			AcceptanceRequired: aws.Bool(true),
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(svcCfgOut.Return))

	permOut, err := client.DescribeVpcEndpointServicePermissions(
		t.Context(), &ec2sdk.DescribeVpcEndpointServicePermissionsInput{
			ServiceId: aws.String(cfg.ServiceID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, permOut)

	notif, err := backend.CreateVpcEndpointConnectionNotification(
		cfg.ServiceID, "", "arn:aws:sns:us-east-1:000000000000:test-topic", []string{"Accept"},
	)
	require.NoError(t, err)

	delNotifOut, err := client.DeleteVpcEndpointConnectionNotifications(
		t.Context(), &ec2sdk.DeleteVpcEndpointConnectionNotificationsInput{
			ConnectionNotificationIds: []string{notif.ConnectionNotificationID},
		},
	)
	require.NoError(t, err)
	assert.Empty(t, delNotifOut.Unsuccessful)

	ep2, err := backend.CreateVpcEndpoint(vpc.ID, cfg.ServiceName, "Interface", []string{subnet.ID})
	require.NoError(t, err)

	rejectOut, err := client.RejectVpcEndpointConnections(
		t.Context(), &ec2sdk.RejectVpcEndpointConnectionsInput{
			ServiceId:      aws.String(cfg.ServiceID),
			VpcEndpointIds: []string{ep2.ID},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, rejectOut)

	delCfgOut, err := client.DeleteVpcEndpointServiceConfigurations(
		t.Context(), &ec2sdk.DeleteVpcEndpointServiceConfigurationsInput{
			ServiceIds: []string{cfg.ServiceID},
		},
	)
	require.NoError(t, err)
	assert.Empty(t, delCfgOut.Unsuccessful)
}

// runReservedInstancesAndHosts covers AcceptReservedInstancesExchangeQuote,
// DeleteQueuedReservedInstances, DescribeReservedInstancesModifications,
// ModifyReservedInstances, ReleaseHosts, GetHostReservationPurchasePreview,
// PurchaseScheduledInstances.
func runReservedInstancesAndHosts(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	backend.SeedReservedInstancesOffering(
		"offering-slice24-test", "m5.large", "us-east-1a", "Linux/UNIX",
		"No Upfront", "convertible", 31536000, 0, 0.05,
	)
	offerings := backend.DescribeReservedInstancesOfferings("", "", "", "")
	require.NotEmpty(t, offerings)
	ri, err := backend.PurchaseReservedInstancesOffering(offerings[0].ReservedInstancesOfferingID, 1)
	require.NoError(t, err)

	modOut, err := client.ModifyReservedInstances(t.Context(), &ec2sdk.ModifyReservedInstancesInput{
		ReservedInstancesIds: []string{ri.ReservedInstancesID},
		TargetConfigurations: []types.ReservedInstancesConfiguration{
			{InstanceCount: aws.Int32(1), AvailabilityZone: aws.String("us-east-1a")},
		},
	})
	require.NoError(t, err)
	modID := aws.ToString(modOut.ReservedInstancesModificationId)
	assert.NotEmpty(t, modID)

	descModsOut, err := client.DescribeReservedInstancesModifications(
		t.Context(), &ec2sdk.DescribeReservedInstancesModificationsInput{
			ReservedInstancesModificationIds: []string{modID},
		},
	)
	require.NoError(t, err)
	require.Len(t, descModsOut.ReservedInstancesModifications, 1)

	exchangeOut, err := client.AcceptReservedInstancesExchangeQuote(
		t.Context(), &ec2sdk.AcceptReservedInstancesExchangeQuoteInput{
			ReservedInstanceIds: []string{ri.ReservedInstancesID},
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(exchangeOut.ExchangeId))

	deleteQueuedOut, err := client.DeleteQueuedReservedInstances(
		t.Context(), &ec2sdk.DeleteQueuedReservedInstancesInput{
			ReservedInstancesIds: []string{ri.ReservedInstancesID},
		},
	)
	require.NoError(t, err)
	// This backend has no scheduled/future-dated purchase mode, so no RI is
	// ever actually created in the "queued" state -- matching real AWS's own
	// behavior for a non-queued reservation (a real ID always reports the
	// same not-in-queued-state failure, per this file's own PARITY.md note).
	require.Len(t, deleteQueuedOut.FailedQueuedPurchaseDeletions, 1)
	assert.Equal(
		t, ri.ReservedInstancesID,
		aws.ToString(deleteQueuedOut.FailedQueuedPurchaseDeletions[0].ReservedInstancesId),
	)

	hosts, err := backend.AllocateHosts("us-east-1a", "m5.large", 1)
	require.NoError(t, err)

	previewOut, err := client.GetHostReservationPurchasePreview(
		t.Context(), &ec2sdk.GetHostReservationPurchasePreviewInput{
			HostIdSet:  []string{hosts[0].HostID},
			OfferingId: aws.String("hro-03f1c2a7d6e8b9012"),
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(previewOut.TotalUpfrontPrice))

	releaseOut, err := client.ReleaseHosts(t.Context(), &ec2sdk.ReleaseHostsInput{
		HostIds: []string{hosts[0].HostID},
	})
	require.NoError(t, err)
	assert.Contains(t, releaseOut.Successful, hosts[0].HostID)

	purchaseOut, err := client.PurchaseScheduledInstances(
		t.Context(), &ec2sdk.PurchaseScheduledInstancesInput{
			PurchaseRequests: []types.PurchaseRequest{
				{PurchaseToken: aws.String("sit-" + backend.Region + "-c4large-weekly"), InstanceCount: aws.Int32(1)},
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, purchaseOut.ScheduledInstanceSet, 1)
	assert.Equal(t, int32(1), aws.ToInt32(purchaseOut.ScheduledInstanceSet[0].InstanceCount))
}

// runVPCEncryptionControl covers DeleteVpcEncryptionControl,
// ModifyVpcEncryptionControl, DescribeAccountVpcEncryptionControl,
// ModifyAccountVpcEncryptionControl, GetVpcResourcesBlockingEncryptionEnforcement.
func runVPCEncryptionControl(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	vpc, err := backend.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)

	vec, err := backend.CreateVpcEncryptionControl(vpc.ID, nil)
	require.NoError(t, err)

	modOut, err := client.ModifyVpcEncryptionControl(
		t.Context(), &ec2sdk.ModifyVpcEncryptionControlInput{
			VpcEncryptionControlId: aws.String(vec.VpcEncryptionControlID),
			Mode:                   types.VpcEncryptionControlModeEnforce,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modOut.VpcEncryptionControl)
	assert.Equal(t, types.VpcEncryptionControlModeEnforce, modOut.VpcEncryptionControl.Mode)

	blockingOut, err := client.GetVpcResourcesBlockingEncryptionEnforcement(
		t.Context(), &ec2sdk.GetVpcResourcesBlockingEncryptionEnforcementInput{
			VpcId: aws.String(vpc.ID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, blockingOut)

	delOut, err := client.DeleteVpcEncryptionControl(
		t.Context(), &ec2sdk.DeleteVpcEncryptionControlInput{
			VpcEncryptionControlId: aws.String(vec.VpcEncryptionControlID),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t, vec.VpcEncryptionControlID,
		aws.ToString(delOut.VpcEncryptionControl.VpcEncryptionControlId),
	)

	descAcctOut, err := client.DescribeAccountVpcEncryptionControl(
		t.Context(), &ec2sdk.DescribeAccountVpcEncryptionControlInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, descAcctOut.AccountVpcEncryptionControl)

	modAcctOut, err := client.ModifyAccountVpcEncryptionControl(
		t.Context(), &ec2sdk.ModifyAccountVpcEncryptionControlInput{
			Mode: types.AccountVpcEncryptionControlModeAttemptMonitor,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modAcctOut.AccountVpcEncryptionControl)
	assert.Equal(
		t, types.AccountVpcEncryptionControlModeAttemptMonitor,
		modAcctOut.AccountVpcEncryptionControl.Mode,
	)
}
