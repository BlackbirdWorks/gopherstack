package cloudformation

import (
	"fmt"
	"strconv"
	"strings"

	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
)

// ---- RDS ----

func (rc *ResourceCreator) createRDSDBInstance(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.RDS == nil {
		return logicalID + "-stub", nil
	}

	id := strProp(props, "DBInstanceIdentifier", params, physicalIDs)
	if id == "" {
		id = strings.ToLower(logicalID)
	}

	engine := strProp(props, "Engine", params, physicalIDs)
	instanceClass := strProp(props, "DBInstanceClass", params, physicalIDs)
	dbName := strProp(props, "DBName", params, physicalIDs)
	masterUser := strProp(props, "MasterUsername", params, physicalIDs)
	paramGroupName := strProp(props, "DBParameterGroupName", params, physicalIDs)

	var allocatedStorage int
	if v, ok := props["AllocatedStorage"].(float64); ok {
		allocatedStorage = int(v)
	} else if s := strProp(props, "AllocatedStorage", params, physicalIDs); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			allocatedStorage = n
		}
	}

	inst, err := rc.backends.RDS.Backend.CreateDBInstance(
		id, engine, instanceClass, dbName, masterUser, paramGroupName, allocatedStorage, rdsbackend.DBInstanceOptions{},
	)
	if err != nil {
		return "", fmt.Errorf("create RDS DB instance %s: %w", id, err)
	}

	return inst.DBInstanceIdentifier, nil
}

func (rc *ResourceCreator) deleteRDSDBInstance(id string) error {
	if rc.backends.RDS == nil {
		return nil
	}

	_, err := rc.backends.RDS.Backend.DeleteDBInstance(id)

	return err
}

func (rc *ResourceCreator) createRDSDBSubnetGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.RDS == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DBSubnetGroupName", params, physicalIDs)
	if name == "" {
		name = strings.ToLower(logicalID)
	}

	description := strProp(props, "DBSubnetGroupDescription", params, physicalIDs)

	var subnetIDs []string
	if list, ok := props["SubnetIds"].([]any); ok {
		for _, v := range list {
			if s := resolve(v, params, physicalIDs); s != "" {
				subnetIDs = append(subnetIDs, s)
			}
		}
	}

	grp, err := rc.backends.RDS.Backend.CreateDBSubnetGroup(name, description, "", subnetIDs)
	if err != nil {
		return "", fmt.Errorf("create RDS DB subnet group %s: %w", name, err)
	}

	return grp.DBSubnetGroupName, nil
}

func (rc *ResourceCreator) deleteRDSDBSubnetGroup(name string) error {
	if rc.backends.RDS == nil {
		return nil
	}

	return rc.backends.RDS.Backend.DeleteDBSubnetGroup(name)
}

func (rc *ResourceCreator) createRDSDBParameterGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.RDS == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DBParameterGroupName", params, physicalIDs)
	if name == "" {
		name = strings.ToLower(logicalID)
	}

	family := strProp(props, "Family", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)

	pg, err := rc.backends.RDS.Backend.CreateDBParameterGroup(name, family, description)
	if err != nil {
		return "", fmt.Errorf("create RDS DB parameter group %s: %w", name, err)
	}

	return pg.DBParameterGroupName, nil
}

func (rc *ResourceCreator) deleteRDSDBParameterGroup(name string) error {
	if rc.backends.RDS == nil {
		return nil
	}

	return rc.backends.RDS.Backend.DeleteDBParameterGroup(name)
}

// ---- RDS DBCluster ----

func (rc *ResourceCreator) createRDSDBCluster(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.RDS == nil {
		return logicalID + "-stub", nil
	}

	id := strProp(props, "DBClusterIdentifier", params, physicalIDs)
	if id == "" {
		id = strings.ToLower(logicalID)
	}

	engine := strProp(props, "Engine", params, physicalIDs)
	masterUser := strProp(props, "MasterUsername", params, physicalIDs)
	paramGroupName := strProp(props, "DBClusterParameterGroupName", params, physicalIDs)

	cluster, err := rc.backends.RDS.Backend.CreateDBCluster(
		id, engine, masterUser, "", paramGroupName, 0, nil, rdsbackend.DBClusterOptions{},
	)
	if err != nil {
		return "", fmt.Errorf("create RDS DB cluster %s: %w", id, err)
	}

	return cluster.DBClusterIdentifier, nil
}

func (rc *ResourceCreator) deleteRDSDBCluster(id string) error {
	if rc.backends.RDS == nil {
		return nil
	}

	_, err := rc.backends.RDS.Backend.DeleteDBCluster(id)

	return err
}

// ---- RDS DBClusterParameterGroup ----

func (rc *ResourceCreator) createRDSDBClusterParameterGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.RDS == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DBClusterParameterGroupName", params, physicalIDs)
	if name == "" {
		name = strings.ToLower(logicalID)
	}

	family := strProp(props, "Family", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)

	pg, err := rc.backends.RDS.Backend.CreateDBClusterParameterGroup(name, family, description)
	if err != nil {
		return "", fmt.Errorf("create RDS DB cluster parameter group %s: %w", name, err)
	}

	return pg.DBParameterGroupName, nil
}

func (rc *ResourceCreator) deleteRDSDBClusterParameterGroup(name string) error {
	if rc.backends.RDS == nil {
		return nil
	}

	return rc.backends.RDS.Backend.DeleteDBClusterParameterGroup(name)
}
