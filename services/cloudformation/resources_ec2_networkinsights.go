package cloudformation

import (
	"fmt"

	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

const resTypeEC2NetworkInsightsPath = "AWS::EC2::NetworkInsightsPath"

// createEC2NetworkInsightsResource handles AWS::EC2::NetworkInsightsPath
// creation. Returns handled=false otherwise.
func (rc *ResourceCreator) createEC2NetworkInsightsResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if resourceType != resTypeEC2NetworkInsightsPath {
		return "", false, nil
	}

	id, err := rc.createEC2NetworkInsightsPath(logicalID, props, params, physicalIDs)

	return id, true, err
}

// deleteEC2NetworkInsightsResource handles deletion for the type created above.
func (rc *ResourceCreator) deleteEC2NetworkInsightsResource(resourceType, physicalID string) (bool, error) {
	if resourceType != resTypeEC2NetworkInsightsPath {
		return false, nil
	}

	if rc.backends.EC2 == nil {
		return true, nil
	}

	return true, ignoreNotFound(
		rc.backends.EC2.Backend.DeleteNetworkInsightsPath(physicalID), ec2backend.ErrNetworkInsightsPathNotFound,
	)
}

// ---- AWS::EC2::NetworkInsightsPath ----
// Ref returns the ID of the path (documented). NetworkInsightsPathArn,
// SourceArn, and DestinationArn are stashed; CreatedDate is not tracked by
// this backend so it is left unimplemented rather than fabricated.

func (rc *ResourceCreator) createEC2NetworkInsightsPath(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	source := strProp(props, "Source", params, physicalIDs)
	destination := strProp(props, "Destination", params, physicalIDs)

	p, err := rc.backends.EC2.Backend.CreateNetworkInsightsPath(
		source, destination, strProp(props, "Protocol", params, physicalIDs), intProp(props, "DestinationPort"),
	)
	if err != nil {
		return "", fmt.Errorf("create network insights path: %w", err)
	}

	physicalIDs[logicalID+"/NetworkInsightsPathArn"] = p.NetworkInsightsPathArn
	physicalIDs[logicalID+"/SourceArn"] = source
	physicalIDs[logicalID+"/DestinationArn"] = destination

	return p.NetworkInsightsPathID, nil
}
