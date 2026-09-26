package cloudformation

import (
	"context"
	"fmt"

	guarddutybackend "github.com/blackbirdworks/gopherstack/services/guardduty"
)

const (
	resTypeGuardDutyDetector = "AWS::GuardDuty::Detector"
	resTypeGuardDutyIPSet    = "AWS::GuardDuty::IPSet"
)

// createGuardDutyResource handles the GuardDuty resource types listed above.
// IPSet deletes through deleteNewestPropsBasedResource since DeleteIPSet
// needs the owning DetectorId, a sibling property not embedded in its Ref
// value.
func (rc *ResourceCreator) createGuardDutyResource(
	_ context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeGuardDutyDetector:
		id, err := rc.createGuardDutyDetector(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeGuardDutyIPSet:
		id, err := rc.createGuardDutyIPSet(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteGuardDutyResource handles AWS::GuardDuty::Detector deletion only;
// IPSet is handled by deleteNewestPropsBasedResource (see the doc comment
// above).
func (rc *ResourceCreator) deleteGuardDutyResource(
	_ context.Context, resourceType, physicalID string,
) (bool, error) {
	if resourceType != resTypeGuardDutyDetector {
		return false, nil
	}

	if rc.backends.GuardDuty == nil {
		return true, nil
	}

	return true, rc.backends.GuardDuty.Backend.DeleteDetector(physicalID)
}

// ---- AWS::GuardDuty::Detector ----
// Ref returns the detector ID (documented).

func (rc *ResourceCreator) createGuardDutyDetector(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.GuardDuty == nil {
		return logicalID + "-stub", nil
	}

	features := guarddutyFeatures(props["Features"], params, physicalIDs)

	d, err := rc.backends.GuardDuty.Backend.CreateDetector(
		guarddutyEnable(props),
		strProp(props, "FindingPublishingFrequency", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
		features,
	)
	if err != nil {
		return "", fmt.Errorf("create GuardDuty detector: %w", err)
	}

	physicalIDs[logicalID+"/Id"] = d.DetectorID

	return d.DetectorID, nil
}

// guarddutyEnable reads the Enable property, defaulting to true (real
// CreateDetectorInput requires it, but every sibling gopherstack resource
// creator treats an absent bool property as its natural zero value only
// when the AWS default itself is false; GuardDuty's is not, so Detector
// gets its own reader instead of the shared boolProp default-false one).
func guarddutyEnable(props map[string]any) bool {
	v, ok := props["Enable"].(bool)
	if !ok {
		return true
	}

	return v
}

// guarddutyFeatures converts a CFN Features property (a list of
// {Name, Status} maps) into []guarddutybackend.DetectorFeature.
func guarddutyFeatures(v any, params, physicalIDs map[string]string) []guarddutybackend.DetectorFeature {
	list, ok := v.([]any)
	if !ok {
		return nil
	}

	out := make([]guarddutybackend.DetectorFeature, 0, len(list))

	for _, item := range list {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		out = append(out, guarddutybackend.DetectorFeature{
			Name:   strProp(m, "Name", params, physicalIDs),
			Status: strProp(m, "Status", params, physicalIDs),
		})
	}

	return out
}

// ---- AWS::GuardDuty::IPSet ----
// Ref returns the IPSet ID (documented). No Fn::GetAtt attributes are
// documented for this type.

func (rc *ResourceCreator) createGuardDutyIPSet(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.GuardDuty == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	s, err := rc.backends.GuardDuty.Backend.CreateIPSet(
		strProp(props, "DetectorId", params, physicalIDs),
		name,
		strProp(props, "Format", params, physicalIDs),
		strProp(props, "Location", params, physicalIDs),
		boolProp(props, "Activate"),
		tagListProp(props, params, physicalIDs),
		strProp(props, "ExpectedBucketOwner", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create GuardDuty IP set %s: %w", name, err)
	}

	return s.IPSetID, nil
}

// deleteGuardDutyIPSet deletes an IP set. IPSetId is system-generated (not a
// template property), so physicalID -- not props -- carries it; DetectorId
// is a sibling property.
func (rc *ResourceCreator) deleteGuardDutyIPSet(
	physicalID string, props map[string]any, stackPhysicalIDs map[string]string,
) error {
	if rc.backends.GuardDuty == nil {
		return nil
	}

	detectorID := strProp(props, "DetectorId", nil, stackPhysicalIDs)

	return rc.backends.GuardDuty.Backend.DeleteIPSet(detectorID, physicalID)
}
