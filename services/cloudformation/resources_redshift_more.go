package cloudformation

import "fmt"

const (
	resTypeRedshiftClusterParameterGroup = "AWS::Redshift::ClusterParameterGroup"
	resTypeRedshiftClusterSubnetGroup    = "AWS::Redshift::ClusterSubnetGroup"
)

// createRedshiftMoreResource handles the Redshift resource types listed
// above. Returns handled=false when resourceType isn't one of them.
func (rc *ResourceCreator) createRedshiftMoreResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeRedshiftClusterParameterGroup:
		id, err := rc.createRedshiftClusterParameterGroup(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeRedshiftClusterSubnetGroup:
		id, err := rc.createRedshiftClusterSubnetGroup(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteRedshiftMoreResource handles deletion for the types created above;
// both are name-keyed with the name embedded in physicalID.
func (rc *ResourceCreator) deleteRedshiftMoreResource(resourceType, physicalID string) (bool, error) {
	if rc.backends.Redshift == nil {
		switch resourceType {
		case resTypeRedshiftClusterParameterGroup, resTypeRedshiftClusterSubnetGroup:
			return true, nil
		default:
			return false, nil
		}
	}

	switch resourceType {
	case resTypeRedshiftClusterParameterGroup:
		return true, rc.backends.Redshift.Backend.DeleteClusterParameterGroup(physicalID)
	case resTypeRedshiftClusterSubnetGroup:
		return true, rc.backends.Redshift.Backend.DeleteClusterSubnetGroup(physicalID)
	default:
		return false, nil
	}
}

// ---- AWS::Redshift::ClusterParameterGroup ----
// Ref returns the parameter group name (documented). No Fn::GetAtt
// attributes are documented for this type.

func (rc *ResourceCreator) createRedshiftClusterParameterGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Redshift == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ParameterGroupName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	pg, err := rc.backends.Redshift.Backend.CreateClusterParameterGroup(
		name,
		strProp(props, "ParameterGroupFamily", params, physicalIDs),
		strProp(props, "Description", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Redshift cluster parameter group %s: %w", name, err)
	}

	return pg.ParameterGroupName, nil
}

// ---- AWS::Redshift::ClusterSubnetGroup ----
// Ref returns the subnet group name (documented).

func (rc *ResourceCreator) createRedshiftClusterSubnetGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Redshift == nil {
		return logicalID + "-stub", nil
	}

	groupName := strProp(props, "ClusterSubnetGroupName", params, physicalIDs)
	if groupName == "" {
		groupName = logicalID
	}

	sg, err := rc.backends.Redshift.Backend.CreateClusterSubnetGroup(
		groupName,
		strProp(props, "Description", params, physicalIDs),
		"",
		strSliceProp(props["SubnetIds"], params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Redshift cluster subnet group %s: %w", groupName, err)
	}

	physicalIDs[logicalID+"/ClusterSubnetGroupName"] = sg.ClusterSubnetGroupName

	return sg.ClusterSubnetGroupName, nil
}
