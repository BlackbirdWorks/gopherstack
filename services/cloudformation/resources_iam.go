package cloudformation

import "fmt"

// ---- IAM User ----

func (rc *ResourceCreator) createIAMUser(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	userName := strProp(props, "UserName", params, physicalIDs)
	if userName == "" {
		userName = logicalID
	}

	path := strProp(props, "Path", params, physicalIDs)
	if path == "" {
		path = "/"
	}

	user, err := rc.backends.IAM.Backend.CreateUser(userName, path, "")
	if err != nil {
		return "", fmt.Errorf("create IAM user %s: %w", userName, err)
	}

	return user.Arn, nil
}

func (rc *ResourceCreator) deleteIAMUser(arn string) error {
	if rc.backends.IAM == nil {
		return nil
	}

	userName := resourceNameFromARN(arn)

	// Detach all policies before deleting; ignore errors since user may have no policies.
	attached, _ := rc.backends.IAM.Backend.ListAttachedUserPolicies(userName)
	for _, p := range attached {
		_ = rc.backends.IAM.Backend.DetachUserPolicy(userName, p.PolicyArn)
	}

	return rc.backends.IAM.Backend.DeleteUser(userName)
}

// ---- IAM Group ----

func (rc *ResourceCreator) createIAMGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	groupName := strProp(props, "GroupName", params, physicalIDs)
	if groupName == "" {
		groupName = logicalID
	}

	path := strProp(props, "Path", params, physicalIDs)
	if path == "" {
		path = "/"
	}

	group, err := rc.backends.IAM.Backend.CreateGroup(groupName, path)
	if err != nil {
		return "", fmt.Errorf("create IAM group %s: %w", groupName, err)
	}

	return group.Arn, nil
}

func (rc *ResourceCreator) deleteIAMGroup(arn string) error {
	if rc.backends.IAM == nil {
		return nil
	}

	groupName := resourceNameFromARN(arn)

	return rc.backends.IAM.Backend.DeleteGroup(groupName)
}
