package cloudformation

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

const (
	resTypeEC2TGWVpcAttachment     = "AWS::EC2::TransitGatewayVpcAttachment"
	resTypeEC2TGWPeeringAttachment = "AWS::EC2::TransitGatewayPeeringAttachment"
	resTypeEC2TGWMulticastDomain   = "AWS::EC2::TransitGatewayMulticastDomain"
)

// createEC2TransitGatewayMoreResource handles the transit gateway resource
// types listed above. Returns handled=false otherwise.
func (rc *ResourceCreator) createEC2TransitGatewayMoreResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeEC2TGWVpcAttachment:
		id, err := rc.createEC2TGWVpcAttachment(props, params, physicalIDs)

		return id, true, err
	case resTypeEC2TGWPeeringAttachment:
		id, err := rc.createEC2TGWPeeringAttachment(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2TGWMulticastDomain:
		id, err := rc.createEC2TGWMulticastDomain(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteEC2TransitGatewayMoreResource handles deletion for the types created above.
func (rc *ResourceCreator) deleteEC2TransitGatewayMoreResource(resourceType, physicalID string) (bool, error) {
	if rc.backends.EC2 == nil {
		switch resourceType {
		case resTypeEC2TGWVpcAttachment, resTypeEC2TGWPeeringAttachment, resTypeEC2TGWMulticastDomain:
			return true, nil
		default:
			return false, nil
		}
	}

	switch resourceType {
	case resTypeEC2TGWVpcAttachment:
		return true, ignoreNotFound(
			rc.backends.EC2.Backend.DeleteTransitGatewayVpcAttachment(physicalID), ec2backend.ErrTGWAttachmentNotFound,
		)
	case resTypeEC2TGWPeeringAttachment:
		_, err := rc.backends.EC2.Backend.DeleteTransitGatewayPeeringAttachment(physicalID)

		return true, ignoreNotFound(err, ec2backend.ErrTransitGatewayAttachmentNotFound)
	case resTypeEC2TGWMulticastDomain:
		return true, ignoreNotFound(
			rc.backends.EC2.Backend.DeleteTransitGatewayMulticastDomain(physicalID),
			ec2backend.ErrTGWMulticastDomainNotFound,
		)
	default:
		return false, nil
	}
}

// ---- AWS::EC2::TransitGatewayVpcAttachment ----
// Ref returns the ID of the attachment (documented). Fn::GetAtt.Id is the
// same value, so no side-channel stash is needed.

func (rc *ResourceCreator) createEC2TGWVpcAttachment(
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return "tgw-attach-stub", nil
	}

	att, err := rc.backends.EC2.Backend.CreateTransitGatewayVpcAttachment(
		strProp(props, "TransitGatewayId", params, physicalIDs),
		strProp(props, "VpcId", params, physicalIDs),
		strSliceProp(props["SubnetIds"], params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create transit gateway VPC attachment: %w", err)
	}

	return att.TransitGatewayAttachmentID, nil
}

// ---- AWS::EC2::TransitGatewayPeeringAttachment ----
// Ref returns the ID of the attachment (documented). State is stashed since
// the backend sets a real value ("pendingAcceptance"); CreationTime is left
// unimplemented since the backend never populates it (zero time.Time would
// be a fabricated value).

func (rc *ResourceCreator) createEC2TGWPeeringAttachment(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	att, err := rc.backends.EC2.Backend.CreateTransitGatewayPeeringAttachment(
		strProp(props, "TransitGatewayId", params, physicalIDs),
		strProp(props, "PeerTransitGatewayId", params, physicalIDs),
		strProp(props, "PeerAccountId", params, physicalIDs),
		strProp(props, "PeerRegion", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create transit gateway peering attachment: %w", err)
	}

	physicalIDs[logicalID+"/State"] = att.State

	return att.TransitGatewayAttachmentID, nil
}

// ---- AWS::EC2::TransitGatewayMulticastDomain ----
// Ref returns the multicast domain ID (documented). CreationTime, State,
// and the ARN are all stashed since the backend provides real values.

func (rc *ResourceCreator) createEC2TGWMulticastDomain(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	options, _ := props["Options"].(map[string]any)

	domain, err := rc.backends.EC2.Backend.CreateTransitGatewayMulticastDomain(
		strProp(props, "TransitGatewayId", params, physicalIDs),
		strProp(options, "AutoAcceptSharedAssociations", params, physicalIDs),
		strProp(options, "Igmpv2Support", params, physicalIDs),
		strProp(options, "StaticSourcesSupport", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create transit gateway multicast domain: %w", err)
	}

	physicalIDs[logicalID+"/CreationTime"] = domain.CreationTime.UTC().Format(time.RFC3339)
	physicalIDs[logicalID+"/State"] = domain.State
	physicalIDs[logicalID+"/TransitGatewayMulticastDomainArn"] = arn.Build(
		"ec2", rc.backends.Region, rc.backends.AccountID, "transit-gateway-multicast-domain/"+domain.ID,
	)

	return domain.ID, nil
}
