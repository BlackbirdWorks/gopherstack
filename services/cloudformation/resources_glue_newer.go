package cloudformation

import (
	"fmt"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
)

const (
	resTypeGlueBlueprint        = "AWS::Glue::Blueprint"
	resTypeGlueCustomEntityType = "AWS::Glue::CustomEntityType"
	resTypeGlueWorkflow         = "AWS::Glue::Workflow"
)

// createGlueNewerResource handles the Glue resource types added in this
// sweep. Returns handled=false when resourceType isn't one of them.
func (rc *ResourceCreator) createGlueNewerResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeGlueBlueprint:
		id, err := rc.createGlueBlueprint(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeGlueCustomEntityType:
		id, err := rc.createGlueCustomEntityType(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeGlueWorkflow:
		id, err := rc.createGlueNewerWorkflow(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteGlueNewerResource handles deletion for the types created above; all
// three are name-keyed with the name embedded in physicalID.
func (rc *ResourceCreator) deleteGlueNewerResource(resourceType, physicalID string) (bool, error) {
	if rc.backends.Glue == nil {
		switch resourceType {
		case resTypeGlueBlueprint, resTypeGlueCustomEntityType, resTypeGlueWorkflow:
			return true, nil
		default:
			return false, nil
		}
	}

	switch resourceType {
	case resTypeGlueBlueprint:
		return true, rc.backends.Glue.Backend.DeleteBlueprint(physicalID)
	case resTypeGlueCustomEntityType:
		return true, rc.backends.Glue.Backend.DeleteCustomEntityType(physicalID)
	case resTypeGlueWorkflow:
		return true, rc.backends.Glue.Backend.DeleteWorkflow(physicalID)
	default:
		return false, nil
	}
}

// ---- AWS::Glue::Blueprint ----
// Ref is undocumented; the blueprint name (its primary identifier and the
// value every other name-keyed Glue resource in this package Refs to) is
// used. Arn is not a stored Blueprint field, so it is rebuilt the same way
// the backend's own unexported blueprintARN does.

func (rc *ResourceCreator) createGlueBlueprint(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	bp, err := rc.backends.Glue.Backend.CreateBlueprint(
		name,
		strProp(props, "BlueprintLocation", params, physicalIDs),
		strProp(props, "Description", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Glue blueprint %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = arn.Build("glue", rc.backends.Region, rc.backends.AccountID, "blueprint/"+bp.Name)
	physicalIDs[logicalID+"/CreatedOn"] = strconv.FormatFloat(bp.CreatedOn, 'f', -1, 64)
	physicalIDs[logicalID+"/LastModifiedOn"] = strconv.FormatFloat(bp.LastModifiedOn, 'f', -1, 64)
	physicalIDs[logicalID+"/ParameterSpec"] = bp.ParameterSpec
	physicalIDs[logicalID+"/Status"] = bp.Status

	return bp.Name, nil
}

// ---- AWS::Glue::CustomEntityType ----
// Ref is undocumented; the type name is used, matching its name-keyed CRUD.
// No Fn::GetAtt attributes are documented for this type.

func (rc *ResourceCreator) createGlueCustomEntityType(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	cet, err := rc.backends.Glue.Backend.CreateCustomEntityType(
		name,
		strProp(props, "RegexString", params, physicalIDs),
		strSliceProp(props["ContextWords"], params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Glue custom entity type %s: %w", name, err)
	}

	return cet.Name, nil
}

// ---- AWS::Glue::Workflow ----
// Ref returns the workflow name (documented); no Fn::GetAtt attributes are
// documented for this type.

func (rc *ResourceCreator) createGlueNewerWorkflow(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	w, err := rc.backends.Glue.Backend.CreateWorkflow(
		gluebackend.Workflow{
			Name:              name,
			Description:       strProp(props, "Description", params, physicalIDs),
			MaxConcurrentRuns: intProp(props, "MaxConcurrentRuns"),
		},
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Glue workflow %s: %w", name, err)
	}

	return w.Name, nil
}
