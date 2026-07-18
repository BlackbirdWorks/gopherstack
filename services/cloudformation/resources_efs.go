package cloudformation

import (
	"context"
	"fmt"

	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
)

// ---- EFS ----

func (rc *ResourceCreator) createEFSFileSystem(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EFS == nil {
		return logicalID + "-stub", nil
	}

	performanceMode := strProp(props, "PerformanceMode", params, physicalIDs)
	throughputMode := strProp(props, "ThroughputMode", params, physicalIDs)

	var encrypted bool
	if v, ok := props["Encrypted"].(bool); ok {
		encrypted = v
	}

	token := logicalID + "-token"

	fs, err := rc.backends.EFS.Backend.CreateFileSystem(
		ctx,
		efsbackend.CreateFileSystemRequest{
			CreationToken:   token,
			PerformanceMode: performanceMode,
			ThroughputMode:  throughputMode,
			Encrypted:       encrypted,
		},
	)
	if err != nil {
		return "", fmt.Errorf("create EFS file system: %w", err)
	}

	return fs.FileSystemID, nil
}

func (rc *ResourceCreator) deleteEFSFileSystem(ctx context.Context, id string) error {
	if rc.backends.EFS == nil {
		return nil
	}

	return rc.backends.EFS.Backend.DeleteFileSystem(ctx, id)
}

func (rc *ResourceCreator) createEFSMountTarget(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EFS == nil {
		return logicalID + "-stub", nil
	}

	fileSystemID := strProp(props, "FileSystemId", params, physicalIDs)
	subnetID := strProp(props, "SubnetId", params, physicalIDs)

	mt, err := rc.backends.EFS.Backend.CreateMountTarget(
		ctx,
		efsbackend.CreateMountTargetRequest{
			FileSystemID: fileSystemID,
			SubnetID:     subnetID,
		},
	)
	if err != nil {
		return "", fmt.Errorf("create EFS mount target: %w", err)
	}

	return mt.MountTargetID, nil
}

func (rc *ResourceCreator) deleteEFSMountTarget(ctx context.Context, id string) error {
	if rc.backends.EFS == nil {
		return nil
	}

	return rc.backends.EFS.Backend.DeleteMountTarget(ctx, id)
}
