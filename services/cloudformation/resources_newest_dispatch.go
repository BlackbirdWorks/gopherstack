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

	return "", false, nil
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

	return false, nil
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
