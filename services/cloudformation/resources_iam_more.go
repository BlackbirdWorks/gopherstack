package cloudformation

import (
	"errors"
	"fmt"

	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
)

const (
	resTypeIAMGroupPolicy       = "AWS::IAM::GroupPolicy"
	resTypeIAMRolePolicy        = "AWS::IAM::RolePolicy"
	resTypeIAMUserPolicy        = "AWS::IAM::UserPolicy"
	resTypeIAMServerCertificate = "AWS::IAM::ServerCertificate"
)

// createIAMMoreResource handles IAM inline-policy and server-certificate
// resource creation. GroupPolicy/RolePolicy/UserPolicy delete through
// deletePropsBasedResource (props.go) since DeleteXPolicy needs the owning
// group/role/user name, a sibling property not embedded in the Ref value.
func (rc *ResourceCreator) createIAMMoreResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeIAMGroupPolicy:
		id, err := rc.createIAMGroupPolicy(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeIAMRolePolicy:
		id, err := rc.createIAMRolePolicy(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeIAMUserPolicy:
		id, err := rc.createIAMUserPolicy(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeIAMServerCertificate:
		id, err := rc.createIAMServerCertificate(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteIAMMoreResource handles AWS::IAM::ServerCertificate deletion only;
// GroupPolicy/RolePolicy/UserPolicy are handled by deletePropsBasedResource.
func (rc *ResourceCreator) deleteIAMMoreResource(resourceType, physicalID string) (bool, error) {
	if resourceType != resTypeIAMServerCertificate {
		return false, nil
	}

	return true, rc.deleteIAMServerCertificate(physicalID)
}

// deleteMorePropsBasedResource is deletePropsBasedResource's overflow table
// for resource types added after its switch reached the cyclop limit:
// IAM inline policies (need the owning group/role/user name) and
// AWS::Glue::Schema (needs the owning registry name).
func (rc *ResourceCreator) deleteMorePropsBasedResource(
	resourceType string, props map[string]any, stackPhysicalIDs map[string]string,
) (bool, error) {
	switch resourceType {
	case resTypeIAMGroupPolicy:
		return true, rc.deleteIAMGroupPolicy(props, stackPhysicalIDs)
	case resTypeIAMRolePolicy:
		return true, rc.deleteIAMRolePolicy(props, stackPhysicalIDs)
	case resTypeIAMUserPolicy:
		return true, rc.deleteIAMUserPolicy(props, stackPhysicalIDs)
	case resTypeGlueSchema:
		return true, rc.deleteGlueSchema(props, stackPhysicalIDs)
	case resTypeAthenaPreparedStatement:
		return true, rc.deleteAthenaPreparedStatement(props, stackPhysicalIDs)
	default:
		return false, nil
	}
}

// ---- AWS::IAM::GroupPolicy ----
// Ref returns the resource name (the policy name).

func (rc *ResourceCreator) createIAMGroupPolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	groupName := strProp(props, "GroupName", params, physicalIDs)
	policyName := strProp(props, "PolicyName", params, physicalIDs)
	if policyName == "" {
		policyName = logicalID
	}

	err := rc.backends.IAM.Backend.PutGroupPolicy(groupName, policyName, jsonProp(props, "PolicyDocument"))
	if err != nil {
		return "", fmt.Errorf("put IAM group policy %s on group %s: %w", policyName, groupName, err)
	}

	return policyName, nil
}

func (rc *ResourceCreator) deleteIAMGroupPolicy(props map[string]any, stackPhysicalIDs map[string]string) error {
	if rc.backends.IAM == nil {
		return nil
	}

	groupName := strProp(props, "GroupName", nil, stackPhysicalIDs)
	policyName := strProp(props, "PolicyName", nil, stackPhysicalIDs)

	err := rc.backends.IAM.Backend.DeleteGroupPolicy(groupName, policyName)
	if errors.Is(err, iambackend.ErrGroupNotFound) || errors.Is(err, iambackend.ErrInlinePolicyNotFound) {
		return nil
	}

	return err
}

// ---- AWS::IAM::RolePolicy ----
// Ref returns the resource name (the policy name).

func (rc *ResourceCreator) createIAMRolePolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	roleName := strProp(props, "RoleName", params, physicalIDs)
	policyName := strProp(props, "PolicyName", params, physicalIDs)
	if policyName == "" {
		policyName = logicalID
	}

	err := rc.backends.IAM.Backend.PutRolePolicy(roleName, policyName, jsonProp(props, "PolicyDocument"))
	if err != nil {
		return "", fmt.Errorf("put IAM role policy %s on role %s: %w", policyName, roleName, err)
	}

	return policyName, nil
}

func (rc *ResourceCreator) deleteIAMRolePolicy(props map[string]any, stackPhysicalIDs map[string]string) error {
	if rc.backends.IAM == nil {
		return nil
	}

	roleName := strProp(props, "RoleName", nil, stackPhysicalIDs)
	policyName := strProp(props, "PolicyName", nil, stackPhysicalIDs)

	err := rc.backends.IAM.Backend.DeleteRolePolicy(roleName, policyName)
	if errors.Is(err, iambackend.ErrRoleNotFound) || errors.Is(err, iambackend.ErrInlinePolicyNotFound) {
		return nil
	}

	return err
}

// ---- AWS::IAM::UserPolicy ----
// Ref returns the resource name (the policy name).

func (rc *ResourceCreator) createIAMUserPolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	userName := strProp(props, "UserName", params, physicalIDs)
	policyName := strProp(props, "PolicyName", params, physicalIDs)
	if policyName == "" {
		policyName = logicalID
	}

	err := rc.backends.IAM.Backend.PutUserPolicy(userName, policyName, jsonProp(props, "PolicyDocument"))
	if err != nil {
		return "", fmt.Errorf("put IAM user policy %s on user %s: %w", policyName, userName, err)
	}

	return policyName, nil
}

func (rc *ResourceCreator) deleteIAMUserPolicy(props map[string]any, stackPhysicalIDs map[string]string) error {
	if rc.backends.IAM == nil {
		return nil
	}

	userName := strProp(props, "UserName", nil, stackPhysicalIDs)
	policyName := strProp(props, "PolicyName", nil, stackPhysicalIDs)

	err := rc.backends.IAM.Backend.DeleteUserPolicy(userName, policyName)
	if errors.Is(err, iambackend.ErrUserNotFound) || errors.Is(err, iambackend.ErrInlinePolicyNotFound) {
		return nil
	}

	return err
}

// ---- AWS::IAM::ServerCertificate ----
// Ref returns the ServerCertificateName; Fn::GetAtt Arn is stashed.

func (rc *ResourceCreator) createIAMServerCertificate(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ServerCertificateName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	path := strProp(props, "Path", params, physicalIDs)
	if path == "" {
		path = "/"
	}

	cert, err := rc.backends.IAM.Backend.UploadServerCertificate(
		name, path,
		strProp(props, "CertificateBody", params, physicalIDs),
		strProp(props, "CertificateChain", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("upload IAM server certificate %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = cert.Arn

	return name, nil
}

func (rc *ResourceCreator) deleteIAMServerCertificate(name string) error {
	if rc.backends.IAM == nil {
		return nil
	}

	err := rc.backends.IAM.Backend.DeleteServerCertificate(name)
	if errors.Is(err, iambackend.ErrUserNotFound) {
		return nil
	}

	return err
}
