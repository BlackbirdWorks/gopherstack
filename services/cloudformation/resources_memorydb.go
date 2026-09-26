package cloudformation

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

const (
	resTypeMemoryDBParameterGroup = "AWS::MemoryDB::ParameterGroup"
	resTypeMemoryDBSubnetGroup    = "AWS::MemoryDB::SubnetGroup"
	resTypeMemoryDBUser           = "AWS::MemoryDB::User"
	resTypeMemoryDBACL            = "AWS::MemoryDB::ACL"
	resTypeMemoryDBCluster        = "AWS::MemoryDB::Cluster"
)

// createMemoryDBResource handles the MemoryDB resource types listed above.
// Returns handled=false when resourceType isn't one of them.
func (rc *ResourceCreator) createMemoryDBResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeMemoryDBParameterGroup:
		id, err := rc.createMemoryDBParameterGroup(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeMemoryDBSubnetGroup:
		id, err := rc.createMemoryDBSubnetGroup(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeMemoryDBUser:
		id, err := rc.createMemoryDBUser(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeMemoryDBACL:
		id, err := rc.createMemoryDBACL(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeMemoryDBCluster:
		id, err := rc.createMemoryDBCluster(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteMemoryDBResource handles deletion for the types described in createMemoryDBResource.
// Ref for all five types is the resource ARN (docs); Delete* backend methods are name-keyed,
// so physicalID is converted back to a name the same way resources_sagemaker.go does.
func (rc *ResourceCreator) deleteMemoryDBResource(ctx context.Context, resourceType, physicalID string) (bool, error) {
	if rc.backends.MemoryDB == nil {
		switch resourceType {
		case resTypeMemoryDBParameterGroup, resTypeMemoryDBSubnetGroup, resTypeMemoryDBUser,
			resTypeMemoryDBACL, resTypeMemoryDBCluster:
			return true, nil
		default:
			return false, nil
		}
	}

	b := rc.backends.MemoryDB.Backend
	name := sagemakerNameFromARN(physicalID)

	switch resourceType {
	case resTypeMemoryDBParameterGroup:
		_, err := b.DeleteParameterGroup(ctx, name)

		return true, ignoreNotFound(err, memorydb.ErrParameterGroupNotFound)
	case resTypeMemoryDBSubnetGroup:
		_, err := b.DeleteSubnetGroup(ctx, name)

		return true, ignoreNotFound(err, memorydb.ErrSubnetGroupNotFound)
	case resTypeMemoryDBUser:
		_, err := b.DeleteUser(ctx, name)

		return true, ignoreNotFound(err, memorydb.ErrUserNotFound)
	case resTypeMemoryDBACL:
		_, err := b.DeleteACL(ctx, name)

		return true, ignoreNotFound(err, memorydb.ErrACLNotFound)
	case resTypeMemoryDBCluster:
		_, err := b.DeleteCluster(ctx, name)

		return true, ignoreNotFound(err, memorydb.ErrClusterNotFound)
	default:
		return false, nil
	}
}

// ---- AWS::MemoryDB::ParameterGroup ----
// Ref returns the parameter group ARN (docs).

func (rc *ResourceCreator) createMemoryDBParameterGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.MemoryDB == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ParameterGroupName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	req := &memorydb.ExportedCreateParameterGroupRequest{
		ParameterGroupName: name,
		Family:             strProp(props, "Family", params, physicalIDs),
		Description:        strProp(props, "Description", params, physicalIDs),
		Tags:               memoryDBTagsProp(props, params, physicalIDs),
	}

	pg, err := rc.backends.MemoryDB.Backend.CreateParameterGroup(ctx, req)
	if err != nil {
		return "", fmt.Errorf("create MemoryDB parameter group %s: %w", name, err)
	}

	if entries := memoryDBParameterEntries(props, "Parameters"); len(entries) > 0 {
		_, updateErr := rc.backends.MemoryDB.Backend.UpdateParameterGroup(
			ctx,
			&memorydb.ExportedUpdateParameterGroupRequest{
				ParameterGroupName:  name,
				ParameterNameValues: entries,
			},
		)
		if updateErr != nil {
			return "", fmt.Errorf("apply MemoryDB parameter group %s parameters: %w", name, updateErr)
		}
	}

	return pg.ARN, nil
}

// ---- AWS::MemoryDB::SubnetGroup ----
// Ref returns the subnet group ARN (docs).

func (rc *ResourceCreator) createMemoryDBSubnetGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.MemoryDB == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "SubnetGroupName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	req := &memorydb.ExportedCreateSubnetGroupRequest{
		SubnetGroupName: name,
		Description:     strProp(props, "Description", params, physicalIDs),
		SubnetIDs:       strSliceProp(props["SubnetIds"], params, physicalIDs),
		Tags:            memoryDBTagsProp(props, params, physicalIDs),
	}

	sg, err := rc.backends.MemoryDB.Backend.CreateSubnetGroup(ctx, req)
	if err != nil {
		return "", fmt.Errorf("create MemoryDB subnet group %s: %w", name, err)
	}

	return sg.ARN, nil
}

// ---- AWS::MemoryDB::User ----
// Ref returns the user ARN (docs).

func (rc *ResourceCreator) createMemoryDBUser(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.MemoryDB == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "UserName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var authMode memorydb.ExportedAuthModeReq
	if am, ok := props["AuthenticationMode"].(map[string]any); ok {
		authMode.Type = strProp(am, "Type", params, physicalIDs)
		authMode.Passwords = strSliceProp(am["Passwords"], params, physicalIDs)
	}

	req := &memorydb.ExportedCreateUserRequest{
		UserName:           name,
		AccessString:       strProp(props, "AccessString", params, physicalIDs),
		AuthenticationMode: authMode,
		Tags:               memoryDBTagsProp(props, params, physicalIDs),
	}

	u, err := rc.backends.MemoryDB.Backend.CreateUser(ctx, req)
	if err != nil {
		return "", fmt.Errorf("create MemoryDB user %s: %w", name, err)
	}

	return u.ARN, nil
}

// ---- AWS::MemoryDB::ACL ----
// Ref returns the ACL ARN (docs).

func (rc *ResourceCreator) createMemoryDBACL(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.MemoryDB == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ACLName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	req := &memorydb.ExportedCreateACLRequest{
		ACLName:   name,
		UserNames: strSliceProp(props["UserNames"], params, physicalIDs),
		Tags:      memoryDBTagsProp(props, params, physicalIDs),
	}

	acl, err := rc.backends.MemoryDB.Backend.CreateACL(ctx, req)
	if err != nil {
		return "", fmt.Errorf("create MemoryDB ACL %s: %w", name, err)
	}

	return acl.ARN, nil
}

// ---- AWS::MemoryDB::Cluster ----
// Ref returns the cluster ARN (docs); Status, ParameterGroupStatus and
// ClusterEndpoint.Address/Port are backend-computed at create time and
// stashed for Fn::GetAtt the same way EKS/SageMaker side-channel attrs are
// (see resolveGetAtt in template.go).

func (rc *ResourceCreator) createMemoryDBCluster(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.MemoryDB == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ClusterName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	req := &memorydb.ExportedCreateClusterRequest{
		ClusterName:             name,
		NodeType:                strProp(props, "NodeType", params, physicalIDs),
		ACLName:                 strProp(props, "ACLName", params, physicalIDs),
		Description:             strProp(props, "Description", params, physicalIDs),
		Engine:                  strProp(props, "Engine", params, physicalIDs),
		EngineVersion:           strProp(props, "EngineVersion", params, physicalIDs),
		SubnetGroupName:         strProp(props, "SubnetGroupName", params, physicalIDs),
		ParameterGroupName:      strProp(props, "ParameterGroupName", params, physicalIDs),
		KmsKeyID:                strProp(props, "KmsKeyId", params, physicalIDs),
		SnsTopicArn:             strProp(props, "SnsTopicArn", params, physicalIDs),
		MaintenanceWindow:       strProp(props, "MaintenanceWindow", params, physicalIDs),
		SnapshotWindow:          strProp(props, "SnapshotWindow", params, physicalIDs),
		NetworkType:             strProp(props, "NetworkType", params, physicalIDs),
		IPDiscovery:             strProp(props, "IpDiscovery", params, physicalIDs),
		SnapshotName:            strProp(props, "SnapshotName", params, physicalIDs),
		MultiRegionClusterName:  strProp(props, "MultiRegionClusterName", params, physicalIDs),
		SecurityGroupIDs:        strSliceProp(props["SecurityGroupIds"], params, physicalIDs),
		SnapshotArns:            strSliceProp(props["SnapshotArns"], params, physicalIDs),
		NumShards:               int32PtrProp(props, "NumShards", params, physicalIDs),
		Port:                    int32PtrProp(props, "Port", params, physicalIDs),
		SnapshotRetentionLimit:  int32PtrProp(props, "SnapshotRetentionLimit", params, physicalIDs),
		NumReplicasPerShard:     int32PtrProp(props, "NumReplicasPerShard", params, physicalIDs),
		TLSEnabled:              memoryDBBoolPtrProp(props, "TLSEnabled"),
		AutoMinorVersionUpgrade: memoryDBBoolPtrProp(props, "AutoMinorVersionUpgrade"),
		DataTiering:             memoryDBDataTieringPtrProp(props, "DataTiering", params, physicalIDs),
		Tags:                    memoryDBTagsProp(props, params, physicalIDs),
	}

	c, err := rc.backends.MemoryDB.Backend.CreateCluster(ctx, req)
	if err != nil {
		return "", fmt.Errorf("create MemoryDB cluster %s: %w", name, err)
	}

	pgStatus := c.ParameterGroupStatus
	if pgStatus == "" {
		pgStatus = "in-sync"
	}

	physicalIDs[logicalID+"/Status"] = c.Status
	physicalIDs[logicalID+"/ParameterGroupStatus"] = pgStatus
	physicalIDs[logicalID+"/ClusterEndpoint.Address"] = c.Endpoint
	physicalIDs[logicalID+"/ClusterEndpoint.Port"] = strconv.Itoa(int(c.Port))

	return c.ARN, nil
}

// memoryDBTagsProp converts the standard Tags property to MemoryDB's tag entry shape.
func memoryDBTagsProp(props map[string]any, params, physicalIDs map[string]string) []memorydb.ExportedTagEntry {
	m := tagListProp(props, params, physicalIDs)
	if len(m) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	tags := make([]memorydb.ExportedTagEntry, 0, len(m))
	for _, k := range keys {
		tags = append(tags, memorydb.ExportedTagEntry{Key: k, Value: m[k]})
	}

	return tags
}

// memoryDBParameterEntries converts the ParameterGroup's Json-typed Parameters
// property (a flat map of parameter name to value) into ParameterNameValues.
func memoryDBParameterEntries(props map[string]any, key string) []memorydb.ExportedParameterNameValueEntry {
	m, ok := props[key].(map[string]any)
	if !ok {
		return nil
	}

	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}

	sort.Strings(names)

	entries := make([]memorydb.ExportedParameterNameValueEntry, 0, len(names))

	for _, k := range names {
		if s, isStr := m[k].(string); isStr {
			entries = append(entries, memorydb.ExportedParameterNameValueEntry{ParameterName: k, ParameterValue: s})
		}
	}

	return entries
}

// memoryDBBoolPtrProp reads a JSON-boolean property, returning nil when absent
// (distinguishing "not set" from "set to false" for optional *bool fields).
func memoryDBBoolPtrProp(props map[string]any, key string) *bool {
	if v, ok := props[key].(bool); ok {
		return &v
	}

	return nil
}

// memoryDBDataTieringPtrProp reads Cluster's DataTiering property, which the
// docs type as a String with allowed values "true"/"false" even though the
// backend request field is *bool.
func memoryDBDataTieringPtrProp(props map[string]any, key string, params, physicalIDs map[string]string) *bool {
	s := strProp(props, key, params, physicalIDs)
	if s == "" {
		return nil
	}

	v := s == "true"

	return &v
}
