package cloudformation

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

const (
	resTypeEC2RouteServer         = "AWS::EC2::RouteServer"
	resTypeEC2RouteServerEndpoint = "AWS::EC2::RouteServerEndpoint"
	resTypeEC2RouteServerPeer     = "AWS::EC2::RouteServerPeer"
)

// createEC2RouteServerResource handles the route server resource types
// listed above. Returns handled=false otherwise.
func (rc *ResourceCreator) createEC2RouteServerResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeEC2RouteServer:
		id, err := rc.createEC2RouteServer(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2RouteServerEndpoint:
		id, err := rc.createEC2RouteServerEndpoint(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2RouteServerPeer:
		id, err := rc.createEC2RouteServerPeer(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteEC2RouteServerResource handles deletion for the types created above.
func (rc *ResourceCreator) deleteEC2RouteServerResource(resourceType, physicalID string) (bool, error) {
	if rc.backends.EC2 == nil {
		switch resourceType {
		case resTypeEC2RouteServer, resTypeEC2RouteServerEndpoint, resTypeEC2RouteServerPeer:
			return true, nil
		default:
			return false, nil
		}
	}

	switch resourceType {
	case resTypeEC2RouteServer:
		_, err := rc.backends.EC2.Backend.DeleteRouteServer(physicalID)

		return true, ignoreNotFound(err, ec2backend.ErrRouteServerNotFound)
	case resTypeEC2RouteServerEndpoint:
		_, err := rc.backends.EC2.Backend.DeleteRouteServerEndpoint(physicalID)

		return true, ignoreNotFound(err, ec2backend.ErrRouteServerEndpointNotFound)
	case resTypeEC2RouteServerPeer:
		_, err := rc.backends.EC2.Backend.DeleteRouteServerPeer(physicalID)

		return true, ignoreNotFound(err, ec2backend.ErrRouteServerPeerNotFound)
	default:
		return false, nil
	}
}

// ---- AWS::EC2::RouteServer ----
// Ref returns the route server ID (documented). Arn is stashed since this
// backend has no dedicated ARN field on RouteServer.

func (rc *ResourceCreator) createEC2RouteServer(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	rs, err := rc.backends.EC2.Backend.CreateRouteServer(
		int64Prop(props, "AmazonSideAsn", params, physicalIDs),
		strProp(props, "PersistRoutesState", params, physicalIDs),
		int64Prop(props, "PersistRoutesDuration", params, physicalIDs),
		boolProp(props, "SnsNotificationsEnabled"),
	)
	if err != nil {
		return "", fmt.Errorf("create route server: %w", err)
	}

	physicalIDs[logicalID+"/Arn"] = arn.Build(
		"ec2", rc.backends.Region, rc.backends.AccountID, "route-server/"+rs.RouteServerID,
	)

	return rs.RouteServerID, nil
}

// ---- AWS::EC2::RouteServerEndpoint ----
// Ref returns the endpoint ID (documented). Arn, EniAddress, EniId, and
// VpcId are stashed from the backend's real values.

func (rc *ResourceCreator) createEC2RouteServerEndpoint(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	ep, err := rc.backends.EC2.Backend.CreateRouteServerEndpoint(
		strProp(props, "RouteServerId", params, physicalIDs), strProp(props, "SubnetId", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create route server endpoint: %w", err)
	}

	physicalIDs[logicalID+"/Arn"] = arn.Build(
		"ec2", rc.backends.Region, rc.backends.AccountID, "route-server-endpoint/"+ep.RouteServerEndpointID,
	)
	physicalIDs[logicalID+"/EniAddress"] = ep.EniAddress
	physicalIDs[logicalID+"/EniId"] = ep.EniID
	physicalIDs[logicalID+"/VpcId"] = ep.VpcID

	return ep.RouteServerEndpointID, nil
}

// ---- AWS::EC2::RouteServerPeer ----
// Ref returns the peer ID (documented). Arn, EndpointEniAddress,
// EndpointEniId, RouteServerId, SubnetId, and VpcId are stashed from the
// backend's real values.

func (rc *ResourceCreator) createEC2RouteServerPeer(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	bgpOptions, _ := props["BgpOptions"].(map[string]any)

	peer, err := rc.backends.EC2.Backend.CreateRouteServerPeer(
		strProp(props, "RouteServerEndpointId", params, physicalIDs),
		strProp(props, "PeerAddress", params, physicalIDs),
		int64Prop(bgpOptions, "PeerAsn", params, physicalIDs),
		strProp(bgpOptions, "PeerLivenessDetection", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create route server peer: %w", err)
	}

	physicalIDs[logicalID+"/Arn"] = arn.Build(
		"ec2", rc.backends.Region, rc.backends.AccountID, "route-server-peer/"+peer.RouteServerPeerID,
	)
	physicalIDs[logicalID+"/EndpointEniAddress"] = peer.EniAddress
	physicalIDs[logicalID+"/EndpointEniId"] = peer.EniID
	physicalIDs[logicalID+"/RouteServerId"] = peer.RouteServerID
	physicalIDs[logicalID+"/SubnetId"] = peer.SubnetID
	physicalIDs[logicalID+"/VpcId"] = peer.VpcID

	return peer.RouteServerPeerID, nil
}
