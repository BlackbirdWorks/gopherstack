package cloudformation

import "context"

// createNewestSupplementalResource is createNewerSupplementalResource's own
// overflow table, for the Glue/DataSync/Transfer/AppConfig/Macie/GuardDuty/
// AccessAnalyzer/Amplify/Batch/EFS/Redshift resource families added in this
// sweep.
func (rc *ResourceCreator) createNewestSupplementalResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if id, ok, err := rc.createGlueNewerResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}
	if id, ok, err := rc.createDataSyncResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createTransferMoreResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}
	if id, ok, err := rc.createAppConfigResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}
	if id, ok, err := rc.createMacieResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}
	if id, ok, err := rc.createGuardDutyResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createAccessAnalyzerResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}
	if id, ok, err := rc.createAmplifyResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}
	if id, ok, err := rc.createBatchMoreResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createEFSMoreResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createRedshiftMoreResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}
	if id, ok, err := rc.createKinesisVideoResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}
	if id, ok, err := rc.createECRPublicResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}
	if id, ok, err := rc.createKafkaConnectResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	return rc.createEC2AdvancedNetworkingResource(ctx, logicalID, resourceType, props, params, physicalIDs)
}

// createEC2AdvancedNetworkingResource chains the EC2 VPN/networking-extras/
// transit-gateway/traffic-mirror/route-server/network-insights families
// ahead of IAM, ElastiCache, and API Gateway V2's own new-type families
// added in this sweep.
func (rc *ResourceCreator) createEC2AdvancedNetworkingResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if id, ok, err := rc.createEC2VPNResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}
	if id, ok, err := rc.createEC2NetworkingExtrasResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}
	if id, ok, err := rc.createEC2TransitGatewayMoreResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createEC2TrafficMirrorResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}
	if id, ok, err := rc.createEC2RouteServerResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}
	if id, ok, err := rc.createEC2NetworkInsightsResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}
	if id, ok, err := rc.createIAMExtrasResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}
	if id, ok, err := rc.createElastiCacheUserResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}

	return rc.createAPIGatewayV2VpcLinkResource(logicalID, resourceType, props, params, physicalIDs)
}

// deleteNewestSupplementalResource mirrors createNewestSupplementalResource
// for deletion of the same resource families.
func (rc *ResourceCreator) deleteNewestSupplementalResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if handled, err := rc.deleteGlueNewerResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteDataSyncResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteTransferMoreResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteAppConfigResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteMacieResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteGuardDutyResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteAccessAnalyzerResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteAmplifyResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteBatchMoreResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteEFSMoreResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteRedshiftMoreResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteKinesisVideoResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteECRPublicResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteKafkaConnectResource(resourceType, physicalID); handled {
		return true, err
	}

	return rc.deleteEC2AdvancedNetworkingResource(ctx, resourceType, physicalID)
}

// deleteEC2AdvancedNetworkingResource mirrors createEC2AdvancedNetworkingResource.
func (rc *ResourceCreator) deleteEC2AdvancedNetworkingResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if handled, err := rc.deleteEC2VPNResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteEC2NetworkingExtrasResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteEC2TransitGatewayMoreResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteEC2TrafficMirrorResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteEC2RouteServerResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteEC2NetworkInsightsResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteIAMExtrasResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteElastiCacheUserResource(ctx, resourceType, physicalID); handled {
		return true, err
	}

	return rc.deleteAPIGatewayV2VpcLinkResource(resourceType, physicalID)
}

// deleteNewestPropsBasedResource handles deletes needing sibling CFN
// properties for the resource families added in this sweep: AppConfig
// Environment/ConfigurationProfile and GuardDuty IPSet need physicalID plus
// a sibling property (their identifier is system-generated, not itself a
// template property); Amplify Branch and AccessAnalyzer ArchiveRule need
// only sibling properties (their identifier -- BranchName/RuleName -- is a
// declared template property).
func (rc *ResourceCreator) deleteNewestPropsBasedResource(
	physicalID, resourceType string, props map[string]any, stackPhysicalIDs map[string]string,
) (bool, error) {
	switch resourceType {
	case resTypeAppConfigEnvironment:
		return true, rc.deleteAppConfigEnvironment(physicalID, props, stackPhysicalIDs)
	case resTypeAppConfigConfigurationProfile:
		return true, rc.deleteAppConfigConfigurationProfile(physicalID, props, stackPhysicalIDs)
	case resTypeGuardDutyIPSet:
		return true, rc.deleteGuardDutyIPSet(physicalID, props, stackPhysicalIDs)
	case resTypeAmplifyBranch:
		return true, rc.deleteAmplifyBranch(props, stackPhysicalIDs)
	case resTypeAccessAnalyzerArchiveRule:
		return true, rc.deleteAccessAnalyzerArchiveRule(props, stackPhysicalIDs)
	default:
		return false, nil
	}
}
