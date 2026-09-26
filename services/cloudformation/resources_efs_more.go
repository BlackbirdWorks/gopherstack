package cloudformation

import (
	"context"
	"fmt"
	"strconv"

	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
)

const resTypeEFSAccessPoint = "AWS::EFS::AccessPoint"

// createEFSMoreResource handles AWS::EFS::AccessPoint creation. Returns
// handled=false when resourceType isn't that type.
func (rc *ResourceCreator) createEFSMoreResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if resourceType != resTypeEFSAccessPoint {
		return "", false, nil
	}

	id, err := rc.createEFSAccessPoint(ctx, logicalID, props, params, physicalIDs)

	return id, true, err
}

// deleteEFSMoreResource handles AWS::EFS::AccessPoint deletion.
func (rc *ResourceCreator) deleteEFSMoreResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if resourceType != resTypeEFSAccessPoint {
		return false, nil
	}

	if rc.backends.EFS == nil {
		return true, nil
	}

	return true, rc.backends.EFS.Backend.DeleteAccessPoint(ctx, physicalID)
}

// ---- AWS::EFS::AccessPoint ----
// Ref returns the AccessPoint ID (documented).

func (rc *ResourceCreator) createEFSAccessPoint(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EFS == nil {
		return logicalID + "-stub", nil
	}

	req := efsbackend.CreateAccessPointRequest{
		FileSystemID:  strProp(props, "FileSystemId", params, physicalIDs),
		Tags:          tagListProp(props, params, physicalIDs),
		PosixUser:     efsPosixUser(props["PosixUser"], params, physicalIDs),
		RootDirectory: efsRootDirectory(props["RootDirectory"], params, physicalIDs),
	}

	ap, err := rc.backends.EFS.Backend.CreateAccessPoint(ctx, req)
	if err != nil {
		return "", fmt.Errorf("create EFS access point: %w", err)
	}

	physicalIDs[logicalID+"/Arn"] = ap.AccessPointArn

	return ap.AccessPointID, nil
}

func efsPosixUser(v any, params, physicalIDs map[string]string) *efsbackend.PosixUser {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}

	return &efsbackend.PosixUser{
		UID:           int64Prop(m, "Uid", params, physicalIDs),
		GID:           int64Prop(m, "Gid", params, physicalIDs),
		SecondaryGids: int64SliceProp(m["SecondaryGids"], params, physicalIDs),
	}
}

func efsRootDirectory(v any, params, physicalIDs map[string]string) *efsbackend.RootDirectory {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}

	rd := &efsbackend.RootDirectory{Path: strProp(m, "Path", params, physicalIDs)}

	if ci, isMap := m["CreationInfo"].(map[string]any); isMap {
		rd.CreationInfo = &efsbackend.CreationInfo{
			OwnerUID:    int64Prop(ci, "OwnerUid", params, physicalIDs),
			OwnerGID:    int64Prop(ci, "OwnerGid", params, physicalIDs),
			Permissions: strProp(ci, "Permissions", params, physicalIDs),
		}
	}

	return rd
}

// int64SliceProp resolves a property expected to be a list of int64-valued
// (or stringified-int) items.
func int64SliceProp(v any, params, physicalIDs map[string]string) []int64 {
	strs := strSliceProp(v, params, physicalIDs)
	out := make([]int64, 0, len(strs))

	for _, s := range strs {
		n, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			out = append(out, n)
		}
	}

	return out
}
