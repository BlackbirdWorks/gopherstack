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

// ---- KMS ReplicaKey ----

// createKMSSupplementalResource handles KMS ReplicaKey resource creation.
func (rc *ResourceCreator) createKMSSupplementalResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if resourceType != "AWS::KMS::ReplicaKey" {
		return "", false, nil
	}
	id, err := rc.createKMSReplicaKey(ctx, logicalID, props, params, physicalIDs)

	return id, true, err
}

// deleteKMSSupplementalResource handles KMS ReplicaKey resource deletion.
func (rc *ResourceCreator) deleteKMSSupplementalResource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	if resourceType != "AWS::KMS::ReplicaKey" {
		return false, nil
	}

	return true, rc.deleteKMSReplicaKey(ctx, physicalID)
}

func (rc *ResourceCreator) createKMSReplicaKey(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.KMS == nil {
		return logicalID + "-stub", nil
	}
	replicaRegion := strProp(props, "ReplicaRegion", params, physicalIDs)
	if replicaRegion == "" {
		replicaRegion = rc.backends.Region
	}
	out, err := rc.backends.KMS.Backend.ReplicateKey(ctx, &kmsbackend.ReplicateKeyInput{
		KeyID:         strProp(props, "PrimaryKeyArn", params, physicalIDs),
		ReplicaRegion: replicaRegion,
		Description:   strProp(props, "Description", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create KMS replica key: %w", err)
	}

	return out.ReplicaKeyMetadata.KeyID, nil
}

func (rc *ResourceCreator) deleteKMSReplicaKey(ctx context.Context, physicalID string) error {
	if rc.backends.KMS == nil {
		return nil
	}
	_, err := rc.backends.KMS.Backend.ScheduleKeyDeletion(ctx, &kmsbackend.ScheduleKeyDeletionInput{
		KeyID:               physicalID,
		PendingWindowInDays: kmsMinDeletionWindowDays,
	})

	return err
}
