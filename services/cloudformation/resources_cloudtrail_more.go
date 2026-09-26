package cloudformation

import (
	"fmt"
	"time"

	cloudtrailbackend "github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

const (
	resTypeCloudTrailEventDataStore = "AWS::CloudTrail::EventDataStore"
	resTypeCloudTrailChannel        = "AWS::CloudTrail::Channel"
)

// createCloudTrailMoreResource handles the CloudTrail resource types listed
// above, added after resources_cloudtrail.go's own Trail-only scope.
// Returns handled=false when resourceType isn't one of them.
func (rc *ResourceCreator) createCloudTrailMoreResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeCloudTrailEventDataStore:
		id, err := rc.createCloudTrailEventDataStore(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeCloudTrailChannel:
		id, err := rc.createCloudTrailChannel(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteCloudTrailMoreResource handles deletion for the types described in
// createCloudTrailMoreResource.
func (rc *ResourceCreator) deleteCloudTrailMoreResource(resourceType, physicalID string) (bool, error) {
	if rc.backends.CloudTrail == nil {
		switch resourceType {
		case resTypeCloudTrailEventDataStore, resTypeCloudTrailChannel:
			return true, nil
		default:
			return false, nil
		}
	}

	b := rc.backends.CloudTrail.Backend

	switch resourceType {
	case resTypeCloudTrailEventDataStore:
		return true, ignoreNotFound(b.DeleteEventDataStore(physicalID), cloudtrailbackend.ErrEventDataStoreNotFound)
	case resTypeCloudTrailChannel:
		return true, ignoreNotFound(b.DeleteChannel(physicalID), cloudtrailbackend.ErrChannelNotFound)
	default:
		return false, nil
	}
}

// ---- AWS::CloudTrail::EventDataStore ----
// Docs literally say "Ref returns the resource name", but the backend is
// ARN-keyed (Create returns EventDataStoreArn; Delete/Get take
// edsIDOrARN) and Name isn't among the documented GetAtt attributes --
// same doc/identifier mismatch class as Athena::NamedQuery
// (resources_athena.go). EventDataStoreArn is used as Ref/physical ID.

func (rc *ResourceCreator) createCloudTrailEventDataStore(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudTrail == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	retention := int32PtrProp(props, "RetentionPeriod", params, physicalIDs)

	var retentionVal int32
	if retention != nil {
		retentionVal = *retention
	}

	eds, err := rc.backends.CloudTrail.Backend.CreateEventDataStore(
		name,
		boolProp(props, "MultiRegionEnabled"),
		boolProp(props, "OrganizationEnabled"),
		boolProp(props, "TerminationProtectionEnabled"),
		retentionVal,
		nil,
		strProp(props, "BillingMode", params, physicalIDs),
		strProp(props, "KmsKeyId", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
		true,
	)
	if err != nil {
		return "", fmt.Errorf("create CloudTrail event data store %s: %w", name, err)
	}

	physicalIDs[logicalID+"/CreatedTimestamp"] = eds.CreatedTimestamp.UTC().Format(time.RFC3339)
	physicalIDs[logicalID+"/Status"] = eds.Status
	physicalIDs[logicalID+"/UpdatedTimestamp"] = eds.UpdatedTimestamp.UTC().Format(time.RFC3339)

	return eds.EventDataStoreARN, nil
}

// ---- AWS::CloudTrail::Channel ----
// Docs literally say "Ref returns the resource name", but the backend is
// ARN-keyed the same way EventDataStore is above; ChannelArn is used as
// Ref/physical ID.

func (rc *ResourceCreator) createCloudTrailChannel(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudTrail == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	destinations := cloudTrailChannelDestinations(props, params, physicalIDs)

	ch, err := rc.backends.CloudTrail.Backend.CreateChannel(
		name, strProp(props, "Source", params, physicalIDs), destinations, tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create CloudTrail channel %s: %w", name, err)
	}

	return ch.ChannelARN, nil
}

func cloudTrailChannelDestinations(
	props map[string]any, params, physicalIDs map[string]string,
) []cloudtrailbackend.Destination {
	list, ok := props["Destinations"].([]any)
	if !ok {
		return nil
	}

	out := make([]cloudtrailbackend.Destination, 0, len(list))

	for _, v := range list {
		m, isMap := v.(map[string]any)
		if !isMap {
			continue
		}

		out = append(out, cloudtrailbackend.Destination{
			Type:     strProp(m, "Type", params, physicalIDs),
			Location: strProp(m, "Location", params, physicalIDs),
		})
	}

	return out
}
