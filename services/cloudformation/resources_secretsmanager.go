package cloudformation

import (
	"context"
	"fmt"

	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

func (rc *ResourceCreator) createSecretsManagerSupplementalResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool) {
	switch resourceType {
	case "AWS::SecretsManager::RotationSchedule":
		id := rc.createSecretsManagerRotationSchedule(logicalID, props, params, physicalIDs)

		return id, true
	case "AWS::SecretsManager::SecretTargetAttachment":
		id := rc.createSecretsManagerSecretTargetAttachment(logicalID, props, params, physicalIDs)

		return id, true
	default:
		return "", false
	}
}

func (rc *ResourceCreator) createSecretsManagerRotationSchedule(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) string {
	secretID := strProp(props, "SecretId", params, physicalIDs)
	if secretID == "" {
		secretID = logicalID
	}

	// Physical ID is the secret ID — the rotation is configured on the secret itself.
	return secretID + "-rotation"
}

func (rc *ResourceCreator) createSecretsManagerSecretTargetAttachment(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) string {
	secretID := strProp(props, "SecretId", params, physicalIDs)
	targetID := strProp(props, "TargetId", params, physicalIDs)
	targetType := strProp(props, "TargetType", params, physicalIDs)

	// Physical ID encodes the attachment; no real backend operation needed.
	id := secretID + ":attachment:" + targetType + ":" + targetID
	if id == ":attachment::" {
		id = logicalID + "-attachment"
	}

	return id
}

// deleteSecretsManagerSupplementalResource handles deletion for SecretsManager supplemental
// resource types that have no real backend operation to reverse.
func (rc *ResourceCreator) deleteSecretsManagerSupplementalResource(resourceType, _ string) bool {
	switch resourceType {
	case "AWS::SecretsManager::RotationSchedule", "AWS::SecretsManager::SecretTargetAttachment":
		return true
	}

	return false
}

func (rc *ResourceCreator) createSecretsManagerResourcePolicy(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SecretsManager == nil {
		return logicalID + "-stub", nil
	}

	secretID := strProp(props, "SecretId", params, physicalIDs)
	policy := strProp(props, "ResourcePolicy", params, physicalIDs)

	if _, err := rc.backends.SecretsManager.Backend.PutResourcePolicy(
		ctx,
		&secretsmanagerbackend.PutResourcePolicyInput{
			SecretID:       secretID,
			ResourcePolicy: policy,
		},
	); err != nil {
		return "", fmt.Errorf("create Secrets Manager resource policy for %s: %w", secretID, err)
	}

	return secretID, nil
}

func (rc *ResourceCreator) deleteSecretsManagerResourcePolicy(ctx context.Context, secretID string) error {
	if rc.backends.SecretsManager == nil {
		return nil
	}

	_, err := rc.backends.SecretsManager.Backend.DeleteResourcePolicy(
		ctx,
		&secretsmanagerbackend.DeleteResourcePolicyInput{
			SecretID: secretID,
		},
	)

	return err
}
