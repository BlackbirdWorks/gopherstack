package cloudformation

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"

	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

// eipAllocIDSuffixLen is the number of hex characters used to generate
// a stub allocation ID when no EC2 backend is configured (17 chars ≈ 68 bits of randomness).
const eipAllocIDSuffixLen = 17

var errNoFlowLogs = errors.New("create EC2 flow log: no flow logs returned")

func (rc *ResourceCreator) createExtraNetworkResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeEC2Volume:
		id, err := rc.createEC2Volume(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::EC2::VolumeAttachment":
		id, err := rc.createEC2VolumeAttachment(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2NetworkInterface:
		id, err := rc.createEC2NetworkInterface(logicalID, props, params, physicalIDs)

		return id, true, err
	default:

		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteExtraNetworkResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeEC2Volume:

		return true, rc.deleteEC2Volume(physicalID)
	case "AWS::EC2::VolumeAttachment":

		return true, rc.deleteEC2VolumeAttachment(physicalID)
	case resTypeEC2NetworkInterface:

		return true, rc.deleteEC2NetworkInterface(physicalID)
	default:

		return false, nil
	}
}

const defaultVolumeSizeGiB = 8

func (rc *ResourceCreator) createEC2Volume(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	az := strProp(props, "AvailabilityZone", params, physicalIDs)
	volType := strProp(props, "VolumeType", params, physicalIDs)
	if volType == "" {
		volType = "gp2"
	}

	size := intProp(props, "Size")
	if size == 0 {
		size = defaultVolumeSizeGiB
	}

	snapshotID := strProp(props, "SnapshotId", params, physicalIDs)

	vol, err := rc.backends.EC2.Backend.CreateVolume(az, volType, size, snapshotID)
	if err != nil {
		return "", fmt.Errorf("create EC2 volume: %w", err)
	}

	return vol.ID, nil
}

func (rc *ResourceCreator) deleteEC2Volume(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteVolume(id)
}

func (rc *ResourceCreator) createEC2VolumeAttachment(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	volumeID := strProp(props, "VolumeId", params, physicalIDs)
	instanceID := strProp(props, "InstanceId", params, physicalIDs)
	device := strProp(props, "Device", params, physicalIDs)
	if device == "" {
		device = "/dev/sdf"
	}

	if _, err := rc.backends.EC2.Backend.AttachVolume(volumeID, instanceID, device); err != nil {
		return "", fmt.Errorf("attach EC2 volume %s to %s: %w", volumeID, instanceID, err)
	}

	return volumeID, nil
}

func (rc *ResourceCreator) deleteEC2VolumeAttachment(volumeID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	_, err := rc.backends.EC2.Backend.DetachVolume(volumeID, true)

	return err
}

func (rc *ResourceCreator) createEC2NetworkInterface(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	subnetID := strProp(props, "SubnetId", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)

	eni, err := rc.backends.EC2.Backend.CreateNetworkInterface(subnetID, description)
	if err != nil {
		return "", fmt.Errorf("create EC2 network interface in %s: %w", subnetID, err)
	}

	return eni.ID, nil
}

func (rc *ResourceCreator) deleteEC2NetworkInterface(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteNetworkInterface(id)
}

// ---- EC2 NatGateway and EIP ----

func (rc *ResourceCreator) createEC2NatGateway(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	subnetID := strProp(props, "SubnetId", params, physicalIDs)
	allocationID := strProp(props, "AllocationId", params, physicalIDs)

	ngw, err := rc.backends.EC2.Backend.CreateNatGateway(subnetID, allocationID, nil)
	if err != nil {
		return "", fmt.Errorf("create EC2 NAT gateway: %w", err)
	}

	return ngw.ID, nil
}

func (rc *ResourceCreator) deleteEC2NatGateway(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteNatGateway(id)
}

func (rc *ResourceCreator) createEC2EIP(_ string) (string, error) {
	if rc.backends.EC2 == nil {
		uuidHex := strings.ReplaceAll(uuid.New().String(), "-", "")

		return "eipalloc-" + uuidHex[:eipAllocIDSuffixLen], nil
	}

	addr, err := rc.backends.EC2.Backend.AllocateAddress()
	if err != nil {
		return "", fmt.Errorf("allocate EC2 EIP: %w", err)
	}

	return addr.AllocationID, nil
}

func (rc *ResourceCreator) deleteEC2EIP(allocationID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.ReleaseAddress(allocationID)
}

// ---- EC2 supplemental ----

// createEC2SupplementalResource handles EC2 VPCPeeringConnection, NetworkAcl,
// NetworkAclEntry, KeyPair, SecurityGroupIngress/Egress, and FlowLog resource creation.
func (rc *ResourceCreator) createEC2SupplementalResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::EC2::VPCPeeringConnection":
		id, err := rc.createEC2VPCPeeringConnection(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::EC2::NetworkAcl":
		id, err := rc.createEC2NetworkACL(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::EC2::NetworkAclEntry":
		id, err := rc.createEC2NetworkACLEntry(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::EC2::KeyPair":
		id, err := rc.createEC2KeyPair(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::EC2::SecurityGroupIngress":
		id, err := rc.createEC2SecurityGroupIngress(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::EC2::SecurityGroupEgress":
		id, err := rc.createEC2SecurityGroupEgress(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::EC2::FlowLog":
		id, err := rc.createEC2FlowLog(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteEC2SupplementalResource handles EC2 VPCPeeringConnection, NetworkAcl,
// NetworkAclEntry, KeyPair, SecurityGroupIngress/Egress, and FlowLog resource deletion.
func (rc *ResourceCreator) deleteEC2SupplementalResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::EC2::VPCPeeringConnection":
		return true, rc.deleteEC2VPCPeeringConnection(physicalID)
	case "AWS::EC2::NetworkAcl":
		return true, rc.deleteEC2NetworkACL(physicalID)
	case "AWS::EC2::NetworkAclEntry":
		return true, rc.deleteEC2NetworkACLEntry(physicalID)
	case "AWS::EC2::KeyPair":
		return true, rc.deleteEC2KeyPair(physicalID)
	case "AWS::EC2::SecurityGroupIngress", "AWS::EC2::SecurityGroupEgress":
		return true, nil // standalone SG rules don't map to a deletable resource
	case "AWS::EC2::FlowLog":
		return true, rc.deleteEC2FlowLog(physicalID)
	default:
		return false, nil
	}
}

func (rc *ResourceCreator) createEC2VPCPeeringConnection(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}
	pc, err := rc.backends.EC2.Backend.CreateVpcPeeringConnection(
		strProp(props, "VpcId", params, physicalIDs),
		strProp(props, "PeerVpcId", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create EC2 VPC peering connection: %w", err)
	}

	return pc.VpcPeeringConnectionID, nil
}

func (rc *ResourceCreator) deleteEC2VPCPeeringConnection(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteVpcPeeringConnection(physicalID)
}

func (rc *ResourceCreator) createEC2NetworkACL(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}
	acl, err := rc.backends.EC2.Backend.CreateNetworkACL(
		strProp(props, "VpcId", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create EC2 network ACL: %w", err)
	}

	return acl.ID, nil
}

func (rc *ResourceCreator) deleteEC2NetworkACL(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteNetworkACL(physicalID)
}

// naclEntrySep separates aclID, ruleNumber, and egress in the network ACL entry physical ID.
const naclEntrySep = "/"

// naclEntryParts is the number of parts in a network ACL entry physical ID.
const naclEntryParts = 3

func (rc *ResourceCreator) createEC2NetworkACLEntry(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}
	aclID := strProp(props, "NetworkAclId", params, physicalIDs)
	ruleNumber := intProp(props, "RuleNumber")
	protocol := strProp(props, "Protocol", params, physicalIDs)
	if protocol == "" {
		protocol = "-1"
	}
	ruleAction := strProp(props, "RuleAction", params, physicalIDs)
	if ruleAction == "" {
		ruleAction = "allow"
	}
	egress, _ := props["Egress"].(bool)
	var fromPort, toPort int
	if pr, ok := props["PortRange"].(map[string]any); ok {
		fromPort = intProp(pr, "From")
		toPort = intProp(pr, "To")
	}
	if err := rc.backends.EC2.Backend.CreateNetworkACLEntry(
		aclID, ruleNumber, protocol, ruleAction,
		strProp(props, "CidrBlock", params, physicalIDs),
		egress, fromPort, toPort,
	); err != nil {
		return "", fmt.Errorf("create EC2 network ACL entry %d: %w", ruleNumber, err)
	}
	egressStr := "false"
	if egress {
		egressStr = boolTrue
	}

	return aclID + naclEntrySep + strconv.Itoa(ruleNumber) + naclEntrySep + egressStr, nil
}

func (rc *ResourceCreator) deleteEC2NetworkACLEntry(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}
	parts := strings.SplitN(physicalID, naclEntrySep, naclEntryParts)
	if len(parts) < naclEntryParts {
		return nil
	}
	ruleNumber, err := strconv.Atoi(parts[1])
	if err != nil {
		return err
	}

	return rc.backends.EC2.Backend.DeleteNetworkACLEntry(parts[0], ruleNumber, parts[2] == boolTrue)
}

func (rc *ResourceCreator) createEC2KeyPair(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}
	keyName := strProp(props, "KeyName", params, physicalIDs)
	if keyName == "" {
		keyName = logicalID
	}
	publicKeyMaterial := strProp(props, "PublicKeyMaterial", params, physicalIDs)
	var err error
	if publicKeyMaterial != "" {
		_, err = rc.backends.EC2.Backend.ImportKeyPair(keyName, publicKeyMaterial, nil)
	} else {
		_, err = rc.backends.EC2.Backend.CreateKeyPair(keyName, nil)
	}
	if err != nil {
		return "", fmt.Errorf("create EC2 key pair %s: %w", keyName, err)
	}

	return keyName, nil
}

func (rc *ResourceCreator) deleteEC2KeyPair(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteKeyPair(physicalID)
}

func (rc *ResourceCreator) createEC2SecurityGroupIngress(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}
	groupID := strProp(props, "GroupId", params, physicalIDs)
	if groupID == "" {
		return logicalID + "-stub", nil
	}
	protocol := strProp(props, "IpProtocol", params, physicalIDs)
	rule := ec2backend.SecurityGroupRule{
		Protocol:      protocol,
		IPRange:       strProp(props, "CidrIp", params, physicalIDs),
		FromPort:      intProp(props, "FromPort"),
		ToPort:        intProp(props, "ToPort"),
		SourceGroupID: strProp(props, "SourceSecurityGroupId", params, physicalIDs),
	}
	if err := rc.backends.EC2.Backend.AuthorizeSecurityGroupIngress(
		groupID, []ec2backend.SecurityGroupRule{rule},
	); err != nil {
		return "", fmt.Errorf("create EC2 security group ingress: %w", err)
	}

	return groupID + ":ingress:" + protocol, nil
}

func (rc *ResourceCreator) createEC2SecurityGroupEgress(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}
	groupID := strProp(props, "GroupId", params, physicalIDs)
	if groupID == "" {
		return logicalID + "-stub", nil
	}
	protocol := strProp(props, "IpProtocol", params, physicalIDs)
	rule := ec2backend.SecurityGroupRule{
		Protocol:      protocol,
		IPRange:       strProp(props, "CidrIp", params, physicalIDs),
		FromPort:      intProp(props, "FromPort"),
		ToPort:        intProp(props, "ToPort"),
		SourceGroupID: strProp(props, "DestinationSecurityGroupId", params, physicalIDs),
	}
	if err := rc.backends.EC2.Backend.AuthorizeSecurityGroupEgress(
		groupID, []ec2backend.SecurityGroupRule{rule},
	); err != nil {
		return "", fmt.Errorf("create EC2 security group egress: %w", err)
	}

	return groupID + ":egress:" + protocol, nil
}

func (rc *ResourceCreator) createEC2FlowLog(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}
	resourceID := strProp(props, "ResourceId", params, physicalIDs)
	logs, err := rc.backends.EC2.Backend.CreateFlowLogs(
		[]string{resourceID},
		strProp(props, "TrafficType", params, physicalIDs),
		strProp(props, "LogDestinationType", params, physicalIDs),
		strProp(props, "LogDestination", params, physicalIDs),
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("create EC2 flow log: %w", err)
	}
	if len(logs) == 0 {
		return "", errNoFlowLogs
	}

	return logs[0].FlowLogID, nil
}

func (rc *ResourceCreator) deleteEC2FlowLog(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteFlowLogs([]string{physicalID})
}
