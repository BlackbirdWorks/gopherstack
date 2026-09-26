package cloudformation

import (
	"context"
	"errors"
	"fmt"
	"time"

	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
)

const (
	resTypeECRPullThroughCacheRule       = "AWS::ECR::PullThroughCacheRule"
	resTypeECRRegistryPolicy             = "AWS::ECR::RegistryPolicy"
	resTypeECRRepositoryCreationTemplate = "AWS::ECR::RepositoryCreationTemplate"
)

func (rc *ResourceCreator) createECRMoreResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeECRPullThroughCacheRule:
		id, err := rc.createECRPullThroughCacheRule(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeECRRegistryPolicy:
		id, err := rc.createECRRegistryPolicy(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeECRRepositoryCreationTemplate:
		id, err := rc.createECRRepositoryCreationTemplate(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteECRMoreResource(ctx context.Context, resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeECRPullThroughCacheRule:
		return true, rc.deleteECRPullThroughCacheRule(ctx, physicalID)
	case resTypeECRRegistryPolicy:
		return true, rc.deleteECRRegistryPolicy(ctx)
	case resTypeECRRepositoryCreationTemplate:
		return true, rc.deleteECRRepositoryCreationTemplate(ctx, physicalID)
	default:
		return false, nil
	}
}

// ---- AWS::ECR::PullThroughCacheRule ----
// Ref is undocumented in the CFN reference (no "Return values" section at
// all); the resource type's primary identifier is EcrRepositoryPrefix.

func (rc *ResourceCreator) createECRPullThroughCacheRule(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ECR == nil {
		return logicalID + "-stub", nil
	}

	prefix := strProp(props, "EcrRepositoryPrefix", params, physicalIDs)

	rule, err := rc.backends.ECR.Backend.CreatePullThroughCacheRule(
		ctx,
		prefix,
		strProp(props, "UpstreamRegistryUrl", params, physicalIDs),
		strProp(props, "CredentialArn", params, physicalIDs),
		strProp(props, "UpstreamRegistry", params, physicalIDs),
		strProp(props, "CustomRoleArn", params, physicalIDs),
		strProp(props, "UpstreamRepositoryPrefix", params, physicalIDs),
		"",
	)
	if err != nil {
		return "", fmt.Errorf("create ECR pull-through cache rule %s: %w", prefix, err)
	}

	return rule.EcrRepositoryPrefix, nil
}

func (rc *ResourceCreator) deleteECRPullThroughCacheRule(ctx context.Context, prefix string) error {
	if rc.backends.ECR == nil {
		return nil
	}

	_, err := rc.backends.ECR.Backend.DeletePullThroughCacheRule(ctx, prefix)
	if errors.Is(err, ecrbackend.ErrPullThroughCacheRuleNotFound) {
		return nil
	}

	return err
}

// ---- AWS::ECR::RegistryPolicy ----
// Ref is undocumented (the "Return values" section lists only Fn::GetAtt
// RegistryId); this backend models a single policy per registry, so the
// primary identifier used here is the registry ID.

func (rc *ResourceCreator) createECRRegistryPolicy(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	_, _ map[string]string,
) (string, error) {
	if rc.backends.ECR == nil {
		return logicalID + "-stub", nil
	}

	result, err := rc.backends.ECR.Backend.PutRegistryPolicy(ctx, jsonProp(props, "PolicyText"))
	if err != nil {
		return "", fmt.Errorf("put ECR registry policy: %w", err)
	}

	return result.RegistryID, nil
}

func (rc *ResourceCreator) deleteECRRegistryPolicy(ctx context.Context) error {
	if rc.backends.ECR == nil {
		return nil
	}

	_, err := rc.backends.ECR.Backend.DeleteRegistryPolicy(ctx)
	if errors.Is(err, ecrbackend.ErrRegistryPolicyNotFound) {
		return nil
	}

	return err
}

// ---- AWS::ECR::RepositoryCreationTemplate ----
// Ref returns the resource name (the Prefix); Fn::GetAtt CreatedAt/UpdatedAt
// are stashed.

func (rc *ResourceCreator) createECRRepositoryCreationTemplate(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ECR == nil {
		return logicalID + "-stub", nil
	}

	prefix := strProp(props, "Prefix", params, physicalIDs)

	var encType, kmsKey string
	if enc, ok := props["EncryptionConfiguration"].(map[string]any); ok {
		encType = strProp(enc, "EncryptionType", params, physicalIDs)
		kmsKey = strProp(enc, "KmsKey", params, physicalIDs)
	}

	tmpl, err := rc.backends.ECR.Backend.CreateRepositoryCreationTemplate(ctx, &ecrbackend.RepositoryCreationTemplate{
		Prefix:             prefix,
		Description:        strProp(props, "Description", params, physicalIDs),
		EncryptionType:     encType,
		KMSKey:             kmsKey,
		ImageTagMutability: strProp(props, "ImageTagMutability", params, physicalIDs),
		RepositoryPolicy:   jsonProp(props, "RepositoryPolicy"),
		LifecyclePolicy:    jsonProp(props, "LifecyclePolicy"),
		CustomRoleArn:      strProp(props, "CustomRoleArn", params, physicalIDs),
		AppliedFor:         strSliceProp(props["AppliedFor"], params, physicalIDs),
		ResourceTags:       tagListProp(props, params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create ECR repository creation template %s: %w", prefix, err)
	}

	physicalIDs[logicalID+"/CreatedAt"] = tmpl.CreatedAt.UTC().Format(time.RFC3339)
	physicalIDs[logicalID+"/UpdatedAt"] = tmpl.UpdatedAt.UTC().Format(time.RFC3339)

	return tmpl.Prefix, nil
}

func (rc *ResourceCreator) deleteECRRepositoryCreationTemplate(ctx context.Context, prefix string) error {
	if rc.backends.ECR == nil {
		return nil
	}

	_, err := rc.backends.ECR.Backend.DeleteRepositoryCreationTemplate(ctx, prefix)
	if errors.Is(err, ecrbackend.ErrRepositoryCreationTemplateNotFound) {
		return nil
	}

	return err
}
