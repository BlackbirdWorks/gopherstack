package cloudformation

import (
	"fmt"

	macie2backend "github.com/blackbirdworks/gopherstack/services/macie2"
)

const (
	resTypeMacieAllowList      = "AWS::Macie::AllowList"
	resTypeMacieFindingsFilter = "AWS::Macie::FindingsFilter"
)

// createMacieResource handles the Macie resource types listed above.
// Returns handled=false when resourceType isn't one of them.
func (rc *ResourceCreator) createMacieResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeMacieAllowList:
		id, err := rc.createMacieAllowList(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeMacieFindingsFilter:
		id, err := rc.createMacieFindingsFilter(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteMacieResource handles deletion for the types created above; both are
// ID-keyed with the ID embedded in physicalID.
func (rc *ResourceCreator) deleteMacieResource(resourceType, physicalID string) (bool, error) {
	if rc.backends.Macie2 == nil {
		switch resourceType {
		case resTypeMacieAllowList, resTypeMacieFindingsFilter:
			return true, nil
		default:
			return false, nil
		}
	}

	switch resourceType {
	case resTypeMacieAllowList:
		return true, rc.backends.Macie2.Backend.DeleteAllowList(physicalID, false)
	case resTypeMacieFindingsFilter:
		return true, rc.backends.Macie2.Backend.DeleteFindingsFilter(physicalID)
	default:
		return false, nil
	}
}

// ---- AWS::Macie::AllowList ----
// Ref returns the allow list ID (documented).

func (rc *ResourceCreator) createMacieAllowList(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Macie2 == nil {
		return logicalID + "-stub", nil
	}

	criteriaMap, _ := props["Criteria"].(map[string]any)

	var criteria macie2backend.AllowListCriteria

	if regex := strProp(criteriaMap, "Regex", params, physicalIDs); regex != "" {
		criteria.Regex = &regex
	}

	if s3, ok := criteriaMap["S3WordsList"].(map[string]any); ok {
		criteria.S3WordsList = &macie2backend.S3WordsList{
			BucketName: strProp(s3, "BucketName", params, physicalIDs),
			ObjectKey:  strProp(s3, "ObjectKey", params, physicalIDs),
		}
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	al, err := rc.backends.Macie2.Backend.CreateAllowList(
		name, strProp(props, "Description", params, physicalIDs), criteria, tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Macie allow list %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = al.Arn
	physicalIDs[logicalID+"/Id"] = al.ID
	physicalIDs[logicalID+"/Status"] = "OK"

	return al.ID, nil
}

// ---- AWS::Macie::FindingsFilter ----
// Ref returns the findings filter ID (documented).

func (rc *ResourceCreator) createMacieFindingsFilter(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Macie2 == nil {
		return logicalID + "-stub", nil
	}

	criteria, _ := props["FindingCriteria"].(map[string]any)

	var position *int32
	if p := int32Prop(props, "Position", params, physicalIDs); p != 0 {
		position = &p
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	ff, err := rc.backends.Macie2.Backend.CreateFindingsFilter(
		name,
		strProp(props, "Description", params, physicalIDs),
		strProp(props, "Action", params, physicalIDs),
		position,
		criteria,
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Macie findings filter %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = ff.Arn
	physicalIDs[logicalID+"/Id"] = ff.ID

	return ff.ID, nil
}
