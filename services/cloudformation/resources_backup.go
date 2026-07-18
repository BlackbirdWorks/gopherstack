package cloudformation

import "fmt"

// ---- Backup Vault ----

func (rc *ResourceCreator) createBackupVault(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Backup == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "BackupVaultName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	vault, err := rc.backends.Backup.Backend.CreateBackupVault(name, "", "", nil)
	if err != nil {
		return "", fmt.Errorf("create Backup Vault %s: %w", name, err)
	}

	return vault.BackupVaultArn, nil
}

func (rc *ResourceCreator) deleteBackupVault(arn string) error {
	if rc.backends.Backup == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	return rc.backends.Backup.Backend.DeleteBackupVault(name)
}

// ---- Backup Plan ----

func (rc *ResourceCreator) createBackupPlan(
	logicalID string,
	props map[string]any,
	_, _ map[string]string,
) (string, error) {
	if rc.backends.Backup == nil {
		return logicalID + "-stub", nil
	}

	name := logicalID
	if planMap, ok := props["BackupPlan"].(map[string]any); ok {
		if n, nOK := planMap["BackupPlanName"].(string); nOK && n != "" {
			name = n
		}
	}

	plan, err := rc.backends.Backup.Backend.CreateBackupPlan(name, nil, nil, nil)
	if err != nil {
		return "", fmt.Errorf("create Backup Plan %s: %w", name, err)
	}

	return plan.BackupPlanID, nil
}

func (rc *ResourceCreator) deleteBackupPlan(id string) error {
	if rc.backends.Backup == nil {
		return nil
	}

	return rc.backends.Backup.Backend.DeleteBackupPlan(id)
}

// ---- Backup Selection ----

func (rc *ResourceCreator) createBackupSelection(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Backup == nil {
		return logicalID + "-stub", nil
	}

	planID := strProp(props, "BackupPlanId", params, physicalIDs)
	selectionName := strProp(props, "SelectionName", params, physicalIDs)
	if selectionName == "" {
		selectionName = logicalID
	}

	iamRoleArn := strProp(props, "IamRoleArn", params, physicalIDs)
	sel, err := rc.backends.Backup.Backend.CreateBackupSelection(planID, selectionName, iamRoleArn, nil, nil, nil, nil)
	if err != nil {
		return "", fmt.Errorf("create Backup Selection %s: %w", selectionName, err)
	}

	return sel.SelectionID, nil
}
