package ec2_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestHandlerCoreResourceOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn      func(*ec2.Handler) string
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "StartInstances_missing_id",
			body:         "Action=StartInstances&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "StartInstances_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				b := h.Backend.(*ec2.InMemoryBackend)
				b.TickLifecycleForTest() // pending → running
				_, _ = h.Backend.StopInstances([]string{instances[0].ID})
				b.TickLifecycleForTest() // stopping → stopped

				return "Action=StartInstances&Version=2016-11-15&InstanceId.1=" + url.QueryEscape(
					instances[0].ID,
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"StartInstancesResponse"},
		},
		{
			name: "StopInstances_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)

				return "Action=StopInstances&Version=2016-11-15&InstanceId.1=" + url.QueryEscape(
					instances[0].ID,
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"StopInstancesResponse"},
		},
		{
			name: "RebootInstances_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)

				return "Action=RebootInstances&Version=2016-11-15&InstanceId.1=" + url.QueryEscape(
					instances[0].ID,
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"RebootInstancesResponse"},
		},
		{
			name:         "DescribeInstanceStatus_all",
			body:         "Action=DescribeInstanceStatus&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeInstanceStatusResponse"},
		},
		{
			name:         "DescribeImages",
			body:         "Action=DescribeImages&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeImagesResponse", "ami-"},
		},
		{
			name:         "DescribeRegions",
			body:         "Action=DescribeRegions&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeRegionsResponse", "us-east-1"},
		},
		{
			name:         "DescribeAvailabilityZones",
			body:         "Action=DescribeAvailabilityZones&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAvailabilityZonesResponse", "us-east-1a"},
		},
		{
			name:         "CreateKeyPair_success",
			body:         "Action=CreateKeyPair&Version=2016-11-15&KeyName=test-key",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateKeyPairResponse", "test-key", "keyMaterial"},
		},
		{
			name:         "CreateKeyPair_missing_name",
			body:         "Action=CreateKeyPair&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "DescribeKeyPairs",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.CreateKeyPair("list-key", nil)

				return "Action=DescribeKeyPairs&Version=2016-11-15"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeKeyPairsResponse", "list-key"},
		},
		{
			name: "DeleteKeyPair_success",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.CreateKeyPair("del-key", nil)

				return "Action=DeleteKeyPair&Version=2016-11-15&KeyName=del-key"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteKeyPairResponse"},
		},
		{
			name:         "DeleteKeyPair_not_found",
			body:         "Action=DeleteKeyPair&Version=2016-11-15&KeyName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidKeyPair.NotFound"},
		},
		{
			name:         "ImportKeyPair_success",
			body:         "Action=ImportKeyPair&Version=2016-11-15&KeyName=imported-key&PublicKeyMaterial=dGVzdA==",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateKeyPairResponse", "imported-key"},
		},
		{
			name:         "CreateVolume_success",
			body:         "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a&Size=20&VolumeType=gp2",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateVolumeResponse", "vol-", "available"},
		},
		{
			name: "DescribeVolumes",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.CreateVolume("us-east-1a", "gp2", 20, "")

				return "Action=DescribeVolumes&Version=2016-11-15"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeVolumesResponse"},
		},
		{
			name:         "DeleteVolume_missing_id",
			body:         "Action=DeleteVolume&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "DeleteVolume_not_found",
			body:         "Action=DeleteVolume&Version=2016-11-15&VolumeId=vol-nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidVolume.NotFound"},
		},
		{
			name:         "AllocateAddress",
			body:         "Action=AllocateAddress&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"AllocateAddressResponse", "eipalloc-"},
		},
		{
			name: "DescribeAddresses",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.AllocateAddress()

				return "Action=DescribeAddresses&Version=2016-11-15"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAddressesResponse"},
		},
		{
			name:         "ReleaseAddress_not_found",
			body:         "Action=ReleaseAddress&Version=2016-11-15&AllocationId=eipalloc-nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidAllocationID.NotFound"},
		},
		{
			name:         "CreateInternetGateway",
			body:         "Action=CreateInternetGateway&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateInternetGatewayResponse", "igw-"},
		},
		{
			name: "DescribeInternetGateways",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.CreateInternetGateway()

				return "Action=DescribeInternetGateways&Version=2016-11-15"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeInternetGatewaysResponse"},
		},
		{
			name:         "DeleteInternetGateway_not_found",
			body:         "Action=DeleteInternetGateway&Version=2016-11-15&InternetGatewayId=igw-nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidInternetGatewayID.NotFound"},
		},
		{
			name:         "CreateRouteTable_success",
			body:         "Action=CreateRouteTable&Version=2016-11-15&VpcId=vpc-default",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateRouteTableResponse", "rtb-"},
		},
		{
			name:         "CreateRouteTable_missing_vpc",
			body:         "Action=CreateRouteTable&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "DescribeRouteTables",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.CreateRouteTable("vpc-default")

				return "Action=DescribeRouteTables&Version=2016-11-15"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeRouteTablesResponse"},
		},
		{
			name:         "DeleteRouteTable_not_found",
			body:         "Action=DeleteRouteTable&Version=2016-11-15&RouteTableId=rtb-nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidRouteTableID.NotFound"},
		},
		{
			name: "CreateRoute_success",
			setupFn: func(h *ec2.Handler) string {
				rt, _ := h.Backend.CreateRouteTable("vpc-default")

				return fmt.Sprintf(
					"Action=CreateRoute&Version=2016-11-15&RouteTableId=%s&DestinationCidrBlock=0.0.0.0/0&GatewayId=igw-123",
					rt.ID,
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateRouteResponse"},
		},
		{
			name: "DeleteRoute_success",
			setupFn: func(h *ec2.Handler) string {
				rt, _ := h.Backend.CreateRouteTable("vpc-default")
				_ = h.Backend.CreateRoute(rt.ID, "0.0.0.0/0", "igw-123", "")

				return fmt.Sprintf(
					"Action=DeleteRoute&Version=2016-11-15&RouteTableId=%s&DestinationCidrBlock=0.0.0.0/0",
					rt.ID,
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteRouteResponse"},
		},
		{
			name: "AssociateRouteTable_success",
			setupFn: func(h *ec2.Handler) string {
				rt, _ := h.Backend.CreateRouteTable("vpc-default")

				return fmt.Sprintf(
					"Action=AssociateRouteTable&Version=2016-11-15&RouteTableId=%s&SubnetId=subnet-default",
					rt.ID,
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"AssociateRouteTableResponse", "rtbassoc-"},
		},
		{
			name:         "DescribeNatGateways_empty",
			body:         "Action=DescribeNatGateways&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeNatGatewaysResponse"},
		},
		{
			name: "CreateNatGateway_success",
			setupFn: func(h *ec2.Handler) string {
				addr, _ := h.Backend.AllocateAddress()

				return "Action=CreateNatGateway&Version=2016-11-15" +
					"&SubnetId=subnet-default&AllocationId=" + addr.AllocationID
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateNatGatewayResponse", "nat-"},
		},
		{
			name:         "DeleteNatGateway_not_found",
			body:         "Action=DeleteNatGateway&Version=2016-11-15&NatGatewayId=nat-nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidNatGatewayID.NotFound"},
		},
		{
			name:         "DescribeNetworkInterfaces",
			body:         "Action=DescribeNetworkInterfaces&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeNetworkInterfacesResponse"},
		},
		{
			name:         "AuthorizeSecurityGroupIngress_missing_group",
			body:         "Action=AuthorizeSecurityGroupIngress&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "AuthorizeSecurityGroupIngress_success",
			setupFn: func(h *ec2.Handler) string {
				sg, _ := h.Backend.CreateSecurityGroup("test-sg-auth", "test", "vpc-default")

				return "Action=AuthorizeSecurityGroupIngress&Version=2016-11-15" +
					"&GroupId=" + sg.ID +
					"&IpPermissions.1.IpProtocol=tcp&IpPermissions.1.FromPort=80" +
					"&IpPermissions.1.ToPort=80&IpPermissions.1.IpRanges.1.CidrIp=0.0.0.0/0"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"AuthorizeSecurityGroupIngressResponse"},
		},
		{
			name: "AuthorizeSecurityGroupEgress_success",
			setupFn: func(h *ec2.Handler) string {
				sg, _ := h.Backend.CreateSecurityGroup("test-sg-egr", "test", "vpc-default")

				return "Action=AuthorizeSecurityGroupEgress&Version=2016-11-15" +
					"&GroupId=" + sg.ID +
					"&IpPermissions.1.IpProtocol=tcp&IpPermissions.1.FromPort=443" +
					"&IpPermissions.1.ToPort=443&IpPermissions.1.IpRanges.1.CidrIp=0.0.0.0/0"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"AuthorizeSecurityGroupEgressResponse"},
		},
		{
			name: "RevokeSecurityGroupIngress_success",
			setupFn: func(h *ec2.Handler) string {
				sg, _ := h.Backend.CreateSecurityGroup("test-sg-revoke-h", "test", "vpc-default")
				_ = h.Backend.AuthorizeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{
					{Protocol: "tcp", FromPort: 80, ToPort: 80, IPRange: "0.0.0.0/0"},
				})

				return "Action=RevokeSecurityGroupIngress&Version=2016-11-15" +
					"&GroupId=" + sg.ID +
					"&IpPermissions.1.IpProtocol=tcp&IpPermissions.1.FromPort=80" +
					"&IpPermissions.1.ToPort=80&IpPermissions.1.IpRanges.1.CidrIp=0.0.0.0/0"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"RevokeSecurityGroupIngressResponse"},
		},
		{
			name: "RevokeSecurityGroupEgress_success",
			setupFn: func(h *ec2.Handler) string {
				sg, _ := h.Backend.CreateSecurityGroup("test-sg-revoke-egr-h", "test", "vpc-default")
				_ = h.Backend.AuthorizeSecurityGroupEgress(sg.ID, []ec2.SecurityGroupRule{
					{Protocol: "tcp", FromPort: 443, ToPort: 443, IPRange: "0.0.0.0/0"},
				})

				return "Action=RevokeSecurityGroupEgress&Version=2016-11-15" +
					"&GroupId=" + sg.ID +
					"&IpPermissions.1.IpProtocol=tcp&IpPermissions.1.FromPort=443" +
					"&IpPermissions.1.ToPort=443&IpPermissions.1.IpRanges.1.CidrIp=0.0.0.0/0"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"RevokeSecurityGroupEgressResponse"},
		},
		{
			name:         "RevokeSecurityGroupEgress_missing_group_id",
			body:         "Action=RevokeSecurityGroupEgress&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			// StartInstances on a running instance must fail with IncorrectInstanceState
			name: "StartInstances_invalid_state",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)

				return "Action=StartInstances&Version=2016-11-15&InstanceId.1=" + url.QueryEscape(
					instances[0].ID,
				)
			},
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"IncorrectInstanceState"},
		},
		{
			// StopInstances on a stopped instance must fail with IncorrectInstanceState
			name: "StopInstances_invalid_state",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				_, _ = h.Backend.StopInstances([]string{instances[0].ID})

				return "Action=StopInstances&Version=2016-11-15&InstanceId.1=" + url.QueryEscape(
					instances[0].ID,
				)
			},
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"IncorrectInstanceState"},
		},
		{
			name: "DescribeImageAttribute_success",
			body: "Action=DescribeImageAttribute&Version=2016-11-15" +
				"&ImageId=ami-0c55b159cbfafe1f0&Attribute=launchPermission",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeImageAttributeResponse", "launchPermission", "all"},
		},
		{
			name:         "DescribeImageAttribute_missing_image_id",
			body:         "Action=DescribeImageAttribute&Version=2016-11-15&Attribute=launchPermission",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "DescribeImageAttribute_missing_attribute",
			body:         "Action=DescribeImageAttribute&Version=2016-11-15&ImageId=ami-0c55b159cbfafe1f0",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			// ImportKeyPair without PublicKeyMaterial must fail
			name:         "ImportKeyPair_missing_material",
			body:         "Action=ImportKeyPair&Version=2016-11-15&KeyName=no-material-key",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			body := tt.body

			if tt.setupFn != nil {
				body = tt.setupFn(h)
			}

			rec := postForm(t, h, body)

			assert.Equal(t, tt.wantCode, rec.Code)

			respBody := rec.Body.String()
			for _, want := range tt.wantContains {
				assert.Contains(t, respBody, want, "response should contain %q", want)
			}
		})
	}
}

func TestHandlerRunInstancesIncludesPrivateIP(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(
		t,
		h,
		"Action=RunInstances&Version=2016-11-15&ImageId=ami-123&InstanceType=t2.micro&MinCount=1&MaxCount=1",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName      xml.Name `xml:"RunInstancesResponse"`
		InstancesSet struct {
			Items []struct {
				PrivateIPAddress string `xml:"privateIpAddress"`
			} `xml:"item"`
		} `xml:"instancesSet"`
	}

	require.NoError(
		t,
		xml.Unmarshal([]byte(strings.TrimPrefix(rec.Body.String(), xml.Header)), &resp),
	)
	require.Len(t, resp.InstancesSet.Items, 1)
	assert.NotEmpty(t, resp.InstancesSet.Items[0].PrivateIPAddress)
}

func TestHandlerNetworkSpotPlacementOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn      func(*ec2.Handler) string
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		// ---- NetworkInterface ----
		{
			name:         "CreateNetworkInterface_success",
			body:         "Action=CreateNetworkInterface&Version=2016-11-15&SubnetId=subnet-default&Description=test",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateNetworkInterfaceResponse", "eni-"},
		},
		{
			name:         "CreateNetworkInterface_missing_subnet",
			body:         "Action=CreateNetworkInterface&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "DeleteNetworkInterface_success",
			setupFn: func(h *ec2.Handler) string {
				eni, _ := h.Backend.CreateNetworkInterface("subnet-default", "")

				return "Action=DeleteNetworkInterface&Version=2016-11-15&NetworkInterfaceId=" + url.QueryEscape(
					eni.ID,
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteNetworkInterfaceResponse"},
		},
		{
			name:         "DeleteNetworkInterface_not_found",
			body:         "Action=DeleteNetworkInterface&Version=2016-11-15&NetworkInterfaceId=eni-nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidNetworkInterfaceID.NotFound"},
		},
		{
			name: "AttachNetworkInterface_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				eni, _ := h.Backend.CreateNetworkInterface("subnet-default", "")

				return fmt.Sprintf(
					"Action=AttachNetworkInterface&Version=2016-11-15&NetworkInterfaceId=%s&InstanceId=%s&DeviceIndex=1",
					url.QueryEscape(eni.ID),
					url.QueryEscape(instances[0].ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"AttachNetworkInterfaceResponse", "eni-attach-"},
		},
		{
			name: "DetachNetworkInterface_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				eni, _ := h.Backend.CreateNetworkInterface("subnet-default", "")
				attachID, _ := h.Backend.AttachNetworkInterface(eni.ID, instances[0].ID, 1)

				return "Action=DetachNetworkInterface&Version=2016-11-15&AttachmentId=" + url.QueryEscape(
					attachID,
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DetachNetworkInterfaceResponse"},
		},
		{
			name: "AssignPrivateIPAddresses_success",
			setupFn: func(h *ec2.Handler) string {
				eni, _ := h.Backend.CreateNetworkInterface("subnet-default", "")

				return fmt.Sprintf(
					"Action=AssignPrivateIpAddresses&Version=2016-11-15&NetworkInterfaceId=%s&SecondaryPrivateIpAddressCount=1",
					url.QueryEscape(eni.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"AssignPrivateIpAddressesResponse"},
		},
		{
			name: "UnassignPrivateIPAddresses_success",
			setupFn: func(h *ec2.Handler) string {
				eni, _ := h.Backend.CreateNetworkInterface("subnet-default", "")
				_ = h.Backend.AssignPrivateIPAddresses(eni.ID, 0, []string{"10.0.1.50"})

				return fmt.Sprintf(
					"Action=UnassignPrivateIpAddresses&Version=2016-11-15&NetworkInterfaceId=%s&PrivateIpAddress.1=10.0.1.50",
					url.QueryEscape(eni.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"UnassignPrivateIpAddressesResponse"},
		},
		{
			name: "ModifyNetworkInterfaceAttribute_description",
			setupFn: func(h *ec2.Handler) string {
				eni, _ := h.Backend.CreateNetworkInterface("subnet-default", "orig")

				return fmt.Sprintf(
					"Action=ModifyNetworkInterfaceAttribute&Version=2016-11-15&NetworkInterfaceId=%s&Description.Value=new-desc",
					url.QueryEscape(eni.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyNetworkInterfaceAttributeResponse"},
		},
		// ---- Instance Attribute stubs ----
		{
			name: "ModifyInstanceAttribute_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				b := h.Backend.(*ec2.InMemoryBackend)
				b.TickLifecycleForTest() // pending → running
				// instanceType requires stopped state.
				_, _ = h.Backend.StopInstances([]string{instances[0].ID})
				b.TickLifecycleForTest() // stopping → stopped

				return fmt.Sprintf(
					"Action=ModifyInstanceAttribute&Version=2016-11-15&InstanceId=%s&InstanceType.Value=t3.micro",
					url.QueryEscape(instances[0].ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyInstanceAttributeResponse"},
		},
		{
			name:         "ModifyInstanceAttribute_missing_id",
			body:         "Action=ModifyInstanceAttribute&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "ModifyInstanceAttribute_not_found",
			body: "Action=ModifyInstanceAttribute&Version=2016-11-15" +
				"&InstanceId=i-nonexistent&InstanceType.Value=t3.micro",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidInstanceID.NotFound"},
		},
		{
			name: "ResetInstanceAttribute_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)

				return fmt.Sprintf(
					"Action=ResetInstanceAttribute&Version=2016-11-15&InstanceId=%s&Attribute=sourceDestCheck",
					url.QueryEscape(instances[0].ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"ResetInstanceAttributeResponse"},
		},
		{
			name:         "ResetInstanceAttribute_not_found",
			body:         "Action=ResetInstanceAttribute&Version=2016-11-15&InstanceId=i-nonexistent&Attribute=sourceDestCheck",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidInstanceID.NotFound"},
		},
		// ---- Spot Instances ----
		{
			name: "RequestSpotInstances_success",
			body: "Action=RequestSpotInstances&Version=2016-11-15" +
				"&LaunchSpecification.ImageId=ami-123" +
				"&LaunchSpecification.InstanceType=t2.micro" +
				"&SpotPrice=0.05",
			wantCode:     http.StatusOK,
			wantContains: []string{"RequestSpotInstancesResponse", "sir-"},
		},
		{
			name:         "RequestSpotInstances_missing_image",
			body:         "Action=RequestSpotInstances&Version=2016-11-15&SpotPrice=0.05",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "RequestSpotInstances_missing_instance_type",
			body:         "Action=RequestSpotInstances&Version=2016-11-15&LaunchSpecification.ImageId=ami-123&SpotPrice=0.05",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "DescribeSpotInstanceRequests_empty",
			body:         "Action=DescribeSpotInstanceRequests&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeSpotInstanceRequestsResponse"},
		},
		{
			name: "DescribeSpotInstanceRequests_after_request",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.RequestSpotInstances("ami-123", "t2.micro", "", "0.01")

				return "Action=DescribeSpotInstanceRequests&Version=2016-11-15"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeSpotInstanceRequestsResponse", "sir-"},
		},
		{
			name: "CancelSpotInstanceRequests_success",
			setupFn: func(h *ec2.Handler) string {
				req, _ := h.Backend.RequestSpotInstances("ami-123", "t2.micro", "", "0.01")

				return "Action=CancelSpotInstanceRequests&Version=2016-11-15&SpotInstanceRequestId.1=" + url.QueryEscape(
					req.ID,
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"CancelSpotInstanceRequestsResponse", "cancelled"},
		},
		{
			name:         "CancelSpotInstanceRequests_missing_ids",
			body:         "Action=CancelSpotInstanceRequests&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "DescribeSpotPriceHistory",
			body:         "Action=DescribeSpotPriceHistory&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeSpotPriceHistoryResponse"},
		},
		// ---- Placement Groups ----
		{
			name:         "CreatePlacementGroup_success",
			body:         "Action=CreatePlacementGroup&Version=2016-11-15&GroupName=test-pg&Strategy=cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreatePlacementGroupResponse"},
		},
		{
			name:         "CreatePlacementGroup_missing_name",
			body:         "Action=CreatePlacementGroup&Version=2016-11-15&Strategy=cluster",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "DescribePlacementGroups_empty",
			body:         "Action=DescribePlacementGroups&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribePlacementGroupsResponse"},
		},
		{
			name: "DescribePlacementGroups_after_create",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.CreatePlacementGroup("list-pg", "spread")

				return "Action=DescribePlacementGroups&Version=2016-11-15"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribePlacementGroupsResponse", "list-pg"},
		},
		{
			name: "DeletePlacementGroup_success",
			setupFn: func(h *ec2.Handler) string {
				_, _ = h.Backend.CreatePlacementGroup("del-pg", "cluster")

				return "Action=DeletePlacementGroup&Version=2016-11-15&GroupName=del-pg"
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DeletePlacementGroupResponse"},
		},
		{
			name:         "DeletePlacementGroup_not_found",
			body:         "Action=DeletePlacementGroup&Version=2016-11-15&GroupName=nonexistent-pg",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidPlacementGroup.NotFound"},
		},
		// ---- Volume / Snapshot Attributes ----
		{
			name: "DescribeVolumeAttribute_success",
			setupFn: func(h *ec2.Handler) string {
				vol, _ := h.Backend.CreateVolume("us-east-1a", "gp2", 20, "")

				return fmt.Sprintf(
					"Action=DescribeVolumeAttribute&Version=2016-11-15&VolumeId=%s&Attribute=autoEnableIO",
					url.QueryEscape(vol.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeVolumeAttributeResponse"},
		},
		{
			name:         "DescribeVolumeAttribute_missing_volume",
			body:         "Action=DescribeVolumeAttribute&Version=2016-11-15&Attribute=autoEnableIO",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "ModifyVolumeAttribute_success",
			setupFn: func(h *ec2.Handler) string {
				vol, _ := h.Backend.CreateVolume("us-east-1a", "gp2", 20, "")

				return fmt.Sprintf(
					"Action=ModifyVolumeAttribute&Version=2016-11-15&VolumeId=%s&AutoEnableIO.Value=true",
					url.QueryEscape(vol.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyVolumeAttributeResponse"},
		},
		{
			name:         "ModifyVolumeAttribute_missing_volume",
			body:         "Action=ModifyVolumeAttribute&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "DescribeSnapshotAttribute_success",
			body: "Action=DescribeSnapshotAttribute&Version=2016-11-15" +
				"&SnapshotId=snap-12345678&Attribute=createVolumePermission",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeSnapshotAttributeResponse", "snap-12345678"},
		},
		{
			name:         "DescribeSnapshotAttribute_missing_snapshot",
			body:         "Action=DescribeSnapshotAttribute&Version=2016-11-15&Attribute=createVolumePermission",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "ModifySnapshotAttribute_success",
			body: "Action=ModifySnapshotAttribute&Version=2016-11-15" +
				"&SnapshotId=snap-12345678&Attribute=createVolumePermission",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifySnapshotAttributeResponse"},
		},
		{
			name:         "ModifySnapshotAttribute_missing_snapshot",
			body:         "Action=ModifySnapshotAttribute&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			body := tt.body
			if tt.setupFn != nil {
				body = tt.setupFn(h)
			}

			rec := postForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, want := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), want, "expected %q in response", want)
			}
		})
	}
}

// TestHandlerPreviouslyUncoveredOps covers handler functions that had 0% coverage
// to ensure the overall package coverage meets the 85% threshold.

func TestHandlerAttachDetachOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn      func(*ec2.Handler) string
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "AttachVolume_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				vol, _ := h.Backend.CreateVolume("us-east-1a", "gp2", 20, "")

				return fmt.Sprintf(
					"Action=AttachVolume&Version=2016-11-15&VolumeId=%s&InstanceId=%s&Device=/dev/sdf",
					url.QueryEscape(vol.ID),
					url.QueryEscape(instances[0].ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"AttachVolumeResponse"},
		},
		{
			name: "DetachVolume_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				vol, _ := h.Backend.CreateVolume("us-east-1a", "gp2", 20, "")
				_, _ = h.Backend.AttachVolume(vol.ID, instances[0].ID, "/dev/sdf")

				return "Action=DetachVolume&Version=2016-11-15&VolumeId=" + url.QueryEscape(vol.ID)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DetachVolumeResponse"},
		},
		{
			name: "AssociateAddress_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				addr, _ := h.Backend.AllocateAddress()

				return fmt.Sprintf(
					"Action=AssociateAddress&Version=2016-11-15&AllocationId=%s&InstanceId=%s",
					url.QueryEscape(addr.AllocationID), url.QueryEscape(instances[0].ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"AssociateAddressResponse"},
		},
		{
			name: "DisassociateAddress_success",
			setupFn: func(h *ec2.Handler) string {
				instances, _ := h.Backend.RunInstances("ami-123", "t2.micro", "", 1)
				addr, _ := h.Backend.AllocateAddress()
				assocID, _ := h.Backend.AssociateAddress(addr.AllocationID, instances[0].ID)

				return "Action=DisassociateAddress&Version=2016-11-15&AssociationId=" + url.QueryEscape(
					assocID,
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DisassociateAddressResponse"},
		},
		{
			name: "AttachInternetGateway_success",
			setupFn: func(h *ec2.Handler) string {
				igw, _ := h.Backend.CreateInternetGateway()

				return fmt.Sprintf(
					"Action=AttachInternetGateway&Version=2016-11-15&InternetGatewayId=%s&VpcId=vpc-default",
					url.QueryEscape(igw.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"AttachInternetGatewayResponse"},
		},
		{
			name: "DetachInternetGateway_success",
			setupFn: func(h *ec2.Handler) string {
				igw, _ := h.Backend.CreateInternetGateway()
				_ = h.Backend.AttachInternetGateway(igw.ID, "vpc-default")

				return fmt.Sprintf(
					"Action=DetachInternetGateway&Version=2016-11-15&InternetGatewayId=%s&VpcId=vpc-default",
					url.QueryEscape(igw.ID),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DetachInternetGatewayResponse"},
		},
		{
			name: "DisassociateRouteTable_success",
			setupFn: func(h *ec2.Handler) string {
				rt, _ := h.Backend.CreateRouteTable("vpc-default")
				assocID, _ := h.Backend.AssociateRouteTable(rt.ID, "subnet-default")

				return "Action=DisassociateRouteTable&Version=2016-11-15&AssociationId=" + url.QueryEscape(
					assocID,
				)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DisassociateRouteTableResponse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			body := tt.body
			if tt.setupFn != nil {
				body = tt.setupFn(h)
			}

			rec := postForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, want := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}
