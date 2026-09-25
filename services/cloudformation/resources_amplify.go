package cloudformation

import (
	"fmt"
)

const (
	resTypeAmplifyApp    = "AWS::Amplify::App"
	resTypeAmplifyBranch = "AWS::Amplify::Branch"
)

// createAmplifyResource handles the Amplify resource types listed above.
// Branch deletes through deleteNewestPropsBasedResource since DeleteBranch
// needs the owning AppId, a sibling property not embedded in its Ref value.
func (rc *ResourceCreator) createAmplifyResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeAmplifyApp:
		id, err := rc.createAmplifyApp(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeAmplifyBranch:
		id, err := rc.createAmplifyBranch(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteAmplifyResource handles AWS::Amplify::App deletion only; Branch is
// handled by deleteNewestPropsBasedResource (see the doc comment above).
func (rc *ResourceCreator) deleteAmplifyResource(resourceType, physicalID string) (bool, error) {
	if resourceType != resTypeAmplifyApp {
		return false, nil
	}

	if rc.backends.Amplify == nil {
		return true, nil
	}

	_, err := rc.backends.Amplify.Backend.DeleteApp(physicalID)

	return true, err
}

// ---- AWS::Amplify::App ----
// Ref is undocumented; the App ID (its primary identifier and its first
// documented Fn::GetAtt attribute) is used.

func (rc *ResourceCreator) createAmplifyApp(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Amplify == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	app, err := rc.backends.Amplify.Backend.CreateApp(
		name,
		strProp(props, "Description", params, physicalIDs),
		strProp(props, "Repository", params, physicalIDs),
		strProp(props, "Platform", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Amplify app %s: %w", name, err)
	}

	physicalIDs[logicalID+"/AppId"] = app.AppID
	physicalIDs[logicalID+"/AppName"] = app.Name
	physicalIDs[logicalID+"/Arn"] = app.ARN
	physicalIDs[logicalID+"/DefaultDomain"] = app.DefaultDomain

	return app.AppID, nil
}

// ---- AWS::Amplify::Branch ----
// Ref is undocumented; BranchName (a declared template property and a
// documented Fn::GetAtt attribute) is used.

func (rc *ResourceCreator) createAmplifyBranch(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Amplify == nil {
		return logicalID + "-stub", nil
	}

	appID := strProp(props, "AppId", params, physicalIDs)
	branchName := strProp(props, "BranchName", params, physicalIDs)
	if branchName == "" {
		branchName = logicalID
	}

	branch, err := rc.backends.Amplify.Backend.CreateBranch(
		appID,
		branchName,
		strProp(props, "Description", params, physicalIDs),
		strProp(props, "Stage", params, physicalIDs),
		boolProp(props, "EnableAutoBuild"),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Amplify branch %s: %w", branchName, err)
	}

	physicalIDs[logicalID+"/Arn"] = branch.BranchARN
	physicalIDs[logicalID+"/BranchName"] = branch.BranchName

	return branch.BranchName, nil
}

// deleteAmplifyBranch deletes a branch using the sibling AppId property (a
// declared template property, so unlike GuardDuty IPSet/AppConfig
// Environment this needs no physicalID -- BranchName is itself a property).
func (rc *ResourceCreator) deleteAmplifyBranch(props map[string]any, stackPhysicalIDs map[string]string) error {
	if rc.backends.Amplify == nil {
		return nil
	}

	appID := strProp(props, "AppId", nil, stackPhysicalIDs)
	branchName := strProp(props, "BranchName", nil, stackPhysicalIDs)

	_, err := rc.backends.Amplify.Backend.DeleteBranch(appID, branchName)

	return err
}
