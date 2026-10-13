package cloudformation

import (
	"context"
	"errors"
	"fmt"
	"slices"

	directconnectbackend "github.com/blackbirdworks/gopherstack/services/directconnect"
)

func isDirectConnectCFNType(resourceType string) bool {
	switch resourceType {
	case directconnectbackend.CFNConnection, directconnectbackend.CFNLag, directconnectbackend.CFNDirectConnectGateway,
		directconnectbackend.CFNGatewayAssociation, directconnectbackend.CFNPrivateVirtualInterface,
		directconnectbackend.CFNPublicVirtualInterface, directconnectbackend.CFNTransitVirtualInterface:
		return true
	}

	return false
}

// createDirectConnectThenAdvanced tries the Direct Connect types, then the EC2 advanced-networking chain.
func (rc *ResourceCreator) createDirectConnectThenAdvanced(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if id, ok, err := rc.createDirectConnectResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	return rc.createEC2AdvancedNetworkingResource(ctx, logicalID, resourceType, props, params, physicalIDs)
}

// deleteDirectConnectThenAdvanced mirrors createDirectConnectThenAdvanced.
func (rc *ResourceCreator) deleteDirectConnectThenAdvanced(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if handled, err := rc.deleteDirectConnectResource(resourceType, physicalID); handled {
		return true, err
	}

	return rc.deleteEC2AdvancedNetworkingResource(ctx, resourceType, physicalID)
}

// directConnectReadyStates are the states a resource must reach before it is CREATE_COMPLETE.
func directConnectReadyStates(resourceType string) []string {
	switch resourceType {
	case directconnectbackend.CFNGatewayAssociation:
		return []string{"associated"}
	case directconnectbackend.CFNLag, directconnectbackend.CFNConnection, directconnectbackend.CFNDirectConnectGateway:
		return []string{"available", "down"}
	}

	return []string{"available", "down", "confirming", "verifying"}
}

func directConnectDeletedStates(resourceType string) []string {
	if resourceType == directconnectbackend.CFNGatewayAssociation {
		return []string{"disassociated"}
	}

	return []string{"deleted", "rejected"}
}

func (rc *ResourceCreator) directConnectBackend() *directconnectbackend.InMemoryBackend {
	if rc.backends.DirectConnect == nil {
		return nil
	}

	return rc.backends.DirectConnect.Backend
}

// createDirectConnectResource provisions the AWS::DirectConnect::* types. Fn::GetAtt values are stashed
// at create time because their ARNs and states come from the backend.
func (rc *ResourceCreator) createDirectConnectResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if !isDirectConnectCFNType(resourceType) {
		return "", false, nil
	}

	bk := rc.directConnectBackend()
	if bk == nil {
		return logicalID + "-stub", true, nil
	}

	resolved, _ := resolveDeep(props, params, physicalIDs).(map[string]any)

	id, attrs, err := bk.CreateCFNResource(resourceType, resolved)
	if err != nil {
		return "", true, err
	}

	err = awaitResource(resourceType+" "+id, func() (bool, error) {
		state, found := bk.CFNResourceState(resourceType, id)
		if !found {
			return false, fmt.Errorf("%w: %s", errResourceCreateFailed, id)
		}

		return slices.Contains(directConnectReadyStates(resourceType), state), nil
	})
	if err != nil {
		return "", true, err
	}

	for k, v := range attrs {
		physicalIDs[logicalID+"/"+k] = v
	}

	return id, true, nil
}

func (rc *ResourceCreator) deleteDirectConnectResource(resourceType, physicalID string) (bool, error) {
	if !isDirectConnectCFNType(resourceType) {
		return false, nil
	}

	bk := rc.directConnectBackend()
	if bk == nil {
		return true, nil
	}

	if _, found := bk.CFNResourceState(resourceType, physicalID); !found {
		return true, nil
	}

	return true, awaitResource(resourceType+" "+physicalID, func() (bool, error) {
		state, found := bk.CFNResourceState(resourceType, physicalID)
		if !found || slices.Contains(directConnectDeletedStates(resourceType), state) {
			return true, nil
		}

		if state == "deleting" || state == "disassociating" {
			return false, nil
		}

		if err := bk.DeleteCFNResource(resourceType, physicalID); err != nil &&
			!errors.Is(err, directconnectbackend.ErrCFNDeletePending) {
			return false, err
		}

		return false, nil
	})
}
