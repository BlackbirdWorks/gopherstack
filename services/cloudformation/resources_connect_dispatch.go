package cloudformation

import "context"

// createConnectFamily tries the AppIntegrations, CodeGuru Reviewer and Connect types.
func (rc *ResourceCreator) createConnectFamily(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if id, ok, err := rc.createAppIntegrationsResource(ctx, logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	if id, ok, err := rc.createConnectResource(ctx, logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	if resourceType == resTypeCodeGuruRepositoryAssociation {
		id, err := rc.createCodeGuruRepositoryAssociation(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	}

	return "", false, nil
}

func (rc *ResourceCreator) deleteConnectFamily(ctx context.Context, resourceType, physicalID string) (bool, error) {
	if handled, err := rc.deleteConnectResource(ctx, resourceType, physicalID); handled {
		return true, err
	}

	if resourceType == resTypeCodeGuruRepositoryAssociation {
		return true, rc.deleteCodeGuruRepositoryAssociation(physicalID)
	}

	return rc.deleteAppIntegrationsResource(resourceType, physicalID)
}

func connectFamilyRecreation(resourceType, propName string) string {
	if resourceType == resTypeCodeGuruRepositoryAssociation {
		return recreateAlways
	}

	if v := connectRecreation(resourceType, propName); v != recreateNever {
		return v
	}

	return appIntegrationsRecreation(resourceType, propName)
}

func (rc *ResourceCreator) updateConnectFamily(
	ctx context.Context,
	resourceType, physicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (bool, error) {
	if handled, err := rc.updateConnectResource(ctx, resourceType, physicalID, props, params, physicalIDs); handled {
		return true, err
	}

	switch resourceType {
	case resTypeAIApplication, resTypeAIDataIntegration, resTypeAIEventIntegation:
		return true, rc.updateAppIntegrationsResource(resourceType, physicalID, props, params, physicalIDs)
	default:
		return false, nil
	}
}

func getConnectFamilyAttribute(resType, physID, attrName, accountID, region string) (string, bool) {
	if v, ok := getConnectAttribute(resType, physID, attrName); ok {
		return v, true
	}

	return getAppIntegrationsAttribute(resType, physID, attrName, accountID, region)
}
