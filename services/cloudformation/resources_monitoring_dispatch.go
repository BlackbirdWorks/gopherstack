package cloudformation

import "context"

// createMonitoringThenNetworkManager tries the CloudWatch RUM family before the Network Manager chain.
func (rc *ResourceCreator) createMonitoringThenNetworkManager(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if id, ok, err := rc.createRUMResource(ctx, logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	return rc.createNetworkManagerThenDirectConnect(ctx, logicalID, resourceType, props, params, physicalIDs)
}

// deleteMonitoringThenNetworkManager mirrors createMonitoringThenNetworkManager.
func (rc *ResourceCreator) deleteMonitoringThenNetworkManager(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if handled, err := rc.deleteRUMResource(ctx, resourceType, physicalID); handled {
		return true, err
	}

	return rc.deleteNetworkManagerThenDirectConnect(ctx, resourceType, physicalID)
}

// monitoringRecreation lists the replacement-forcing properties of the families above.
func monitoringRecreation(resourceType, propName string) string {
	switch resourceType {
	case resTypeRUMAppMonitor:
		return alwaysIfOneOf(propName, "Name", "Platform")
	default:
		return recreateNever
	}
}
