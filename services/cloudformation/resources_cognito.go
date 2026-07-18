package cloudformation

import (
	"context"
	"fmt"
	"strings"
)

// ---- Cognito ----

func (rc *ResourceCreator) createCognitoUserPool(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CognitoIDP == nil {
		return logicalID + "-stub", nil
	}

	poolName := strProp(props, "PoolName", params, physicalIDs)
	if poolName == "" {
		poolName = logicalID
	}

	pool, err := rc.backends.CognitoIDP.Backend.CreateUserPool(poolName)
	if err != nil {
		return "", fmt.Errorf("create Cognito user pool %s: %w", poolName, err)
	}

	return pool.ID, nil
}

func (rc *ResourceCreator) deleteCognitoUserPool(poolID string) error {
	if rc.backends.CognitoIDP == nil {
		return nil
	}

	return rc.backends.CognitoIDP.Backend.DeleteUserPool(poolID)
}

func (rc *ResourceCreator) createCognitoUserPoolClient(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CognitoIDP == nil {
		return logicalID + "-stub", nil
	}

	userPoolID := strProp(props, "UserPoolId", params, physicalIDs)
	clientName := strProp(props, "ClientName", params, physicalIDs)
	if clientName == "" {
		clientName = logicalID
	}

	client, err := rc.backends.CognitoIDP.Backend.CreateUserPoolClient(userPoolID, clientName)
	if err != nil {
		return "", fmt.Errorf("create Cognito user pool client %s: %w", clientName, err)
	}

	return client.ClientID, nil
}

func (rc *ResourceCreator) deleteCognitoUserPoolClient(clientID string) error {
	if rc.backends.CognitoIDP == nil {
		return nil
	}

	// physicalID for CognitoUserPoolClient is the clientID, which is enough to delete.
	// We use empty string for userPoolID since our implementation doesn't strictly need it.
	return rc.backends.CognitoIDP.Backend.DeleteUserPoolClient("", clientID)
}

// ---- Cognito supplemental ----

// createCognitoSupplementalResource handles Cognito Identity Pool and User Pool
// domain/group resource creation.
func (rc *ResourceCreator) createCognitoSupplementalResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::Cognito::IdentityPool":
		id, err := rc.createCognitoIdentityPool(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::Cognito::IdentityPoolRoleAttachment":
		id, err := rc.createCognitoIdentityPoolRoleAttachment(
			ctx,
			logicalID,
			props,
			params,
			physicalIDs,
		)

		return id, true, err
	case "AWS::Cognito::UserPoolDomain":
		id, err := rc.createCognitoUserPoolDomain(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::Cognito::UserPoolGroup":
		id, err := rc.createCognitoUserPoolGroup(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteCognitoSupplementalResource handles Cognito Identity Pool and User Pool
// domain/group resource deletion.
func (rc *ResourceCreator) deleteCognitoSupplementalResource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case "AWS::Cognito::IdentityPool":
		return true, rc.deleteCognitoIdentityPool(ctx, physicalID)
	case "AWS::Cognito::IdentityPoolRoleAttachment":
		return true, nil // role attachment is soft; deletion is a no-op
	case "AWS::Cognito::UserPoolDomain":
		return true, rc.deleteCognitoUserPoolDomain(physicalID)
	case "AWS::Cognito::UserPoolGroup":
		return true, rc.deleteCognitoUserPoolGroup(physicalID)
	default:
		return false, nil
	}
}

func (rc *ResourceCreator) createCognitoIdentityPool(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CognitoIdentity == nil {
		return logicalID + "-stub", nil
	}
	name := strProp(props, "IdentityPoolName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}
	allowUnauthenticated, _ := props["AllowUnauthenticatedIdentities"].(bool)
	pool, err := rc.backends.CognitoIdentity.Backend.CreateIdentityPool(
		ctx, name, allowUnauthenticated, false, "", nil, nil, nil,
	)
	if err != nil {
		return "", fmt.Errorf("create Cognito identity pool %s: %w", name, err)
	}

	return pool.IdentityPoolID, nil
}

func (rc *ResourceCreator) deleteCognitoIdentityPool(ctx context.Context, physicalID string) error {
	if rc.backends.CognitoIdentity == nil {
		return nil
	}

	return rc.backends.CognitoIdentity.Backend.DeleteIdentityPool(ctx, physicalID)
}

func (rc *ResourceCreator) createCognitoIdentityPoolRoleAttachment(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CognitoIdentity == nil {
		return logicalID + "-stub", nil
	}
	poolID := strProp(props, "IdentityPoolId", params, physicalIDs)
	var authRoleARN, unauthRoleARN string
	if roles, ok := props["Roles"].(map[string]any); ok {
		authRoleARN = resolve(roles["authenticated"], params, physicalIDs)
		unauthRoleARN = resolve(roles["unauthenticated"], params, physicalIDs)
	}
	if err := rc.backends.CognitoIdentity.Backend.SetIdentityPoolRoles(
		ctx, poolID, authRoleARN, unauthRoleARN, nil,
	); err != nil {
		return "", fmt.Errorf("create Cognito identity pool role attachment %s: %w", poolID, err)
	}

	return poolID + ":roles", nil
}

// cognitoUserPoolDomainSep separates domain and user pool ID in the physical ID.
const cognitoUserPoolDomainSep = "~"

func (rc *ResourceCreator) createCognitoUserPoolDomain(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CognitoIDP == nil {
		return logicalID + "-stub", nil
	}
	domain := strProp(props, "Domain", params, physicalIDs)
	if domain == "" {
		domain = logicalID
	}
	userPoolID := strProp(props, "UserPoolId", params, physicalIDs)
	_, err := rc.backends.CognitoIDP.Backend.CreateUserPoolDomain(userPoolID, domain)
	if err != nil {
		return "", fmt.Errorf("create Cognito user pool domain %s: %w", domain, err)
	}

	return domain + cognitoUserPoolDomainSep + userPoolID, nil
}

func (rc *ResourceCreator) deleteCognitoUserPoolDomain(physicalID string) error {
	if rc.backends.CognitoIDP == nil {
		return nil
	}
	domain, userPoolID, ok := strings.Cut(physicalID, cognitoUserPoolDomainSep)
	if !ok {
		return nil
	}

	return rc.backends.CognitoIDP.Backend.DeleteUserPoolDomain(userPoolID, domain)
}

func (rc *ResourceCreator) createCognitoUserPoolGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CognitoIDP == nil {
		return logicalID + "-stub", nil
	}
	userPoolID := strProp(props, "UserPoolId", params, physicalIDs)
	groupName := strProp(props, "GroupName", params, physicalIDs)
	if groupName == "" {
		groupName = logicalID
	}
	_, err := rc.backends.CognitoIDP.Backend.CreateGroup(
		userPoolID, groupName, strProp(props, "Description", params, physicalIDs), 0,
	)
	if err != nil {
		return "", fmt.Errorf("create Cognito user pool group %s: %w", groupName, err)
	}

	return userPoolID + ":" + groupName, nil
}

func (rc *ResourceCreator) deleteCognitoUserPoolGroup(physicalID string) error {
	if rc.backends.CognitoIDP == nil {
		return nil
	}
	userPoolID, groupName := splitCompositeID(physicalID)
	if userPoolID == "" {
		return nil
	}

	return rc.backends.CognitoIDP.Backend.DeleteGroup(userPoolID, groupName)
}
