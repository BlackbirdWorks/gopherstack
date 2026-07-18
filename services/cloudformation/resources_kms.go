package cloudformation

import (
	"context"
	"fmt"
	"strings"

	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
)

func (rc *ResourceCreator) createKMSAlias(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.KMS == nil {
		return logicalID + "-stub", nil
	}

	aliasName := strProp(props, "AliasName", params, physicalIDs)
	if aliasName == "" {
		aliasName = "alias/" + logicalID
	}
	if !strings.HasPrefix(aliasName, "alias/") {
		aliasName = "alias/" + aliasName
	}

	targetKeyID := strProp(props, "TargetKeyId", params, physicalIDs)

	if err := rc.backends.KMS.Backend.CreateAlias(ctx, &kmsbackend.CreateAliasInput{
		AliasName:   aliasName,
		TargetKeyID: targetKeyID,
	}); err != nil {
		return "", fmt.Errorf("create KMS alias %s: %w", aliasName, err)
	}

	return aliasName, nil
}

func (rc *ResourceCreator) deleteKMSAlias(ctx context.Context, aliasName string) error {
	if rc.backends.KMS == nil {
		return nil
	}

	return rc.backends.KMS.Backend.DeleteAlias(ctx, &kmsbackend.DeleteAliasInput{AliasName: aliasName})
}
