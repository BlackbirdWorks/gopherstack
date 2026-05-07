package cloudformation

import (
	"fmt"
	"strings"
)

// ---- EC2 Instance ----

func (rc *ResourceCreator) createEC2Instance(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	imageID := strProp(props, "ImageId", params, physicalIDs)
	if imageID == "" {
		imageID = "ami-00000000"
	}

	instanceType := strProp(props, "InstanceType", params, physicalIDs)
	if instanceType == "" {
		instanceType = "t3.micro"
	}

	subnetID := strProp(props, "SubnetId", params, physicalIDs)

	instances, err := rc.backends.EC2.Backend.RunInstances(imageID, instanceType, subnetID, 1)
	if err != nil {
		return "", fmt.Errorf("create EC2 instance: %w", err)
	}

	if len(instances) == 0 {
		return logicalID + "-stub", nil
	}

	return instances[0].ID, nil
}

func (rc *ResourceCreator) deleteEC2Instance(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	_, err := rc.backends.EC2.Backend.TerminateInstances([]string{id})

	return err
}

// ---- EC2 VPCGatewayAttachment ----

func (rc *ResourceCreator) createEC2VPCGatewayAttachment(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	vpcID := strProp(props, "VpcId", params, physicalIDs)
	igwID := strProp(props, "InternetGatewayId", params, physicalIDs)

	if igwID != "" {
		if err := rc.backends.EC2.Backend.AttachInternetGateway(igwID, vpcID); err != nil {
			return "", fmt.Errorf("attach internet gateway %s to VPC %s: %w", igwID, vpcID, err)
		}
	}

	return igwID + ":" + vpcID, nil
}

func (rc *ResourceCreator) deleteEC2VPCGatewayAttachment(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	const splitParts = 2
	parts := strings.SplitN(physicalID, ":", splitParts)
	if len(parts) < splitParts {
		return nil
	}

	igwID := parts[0]
	vpcID := parts[1]

	if igwID == "" || vpcID == "" {
		return nil
	}

	return rc.backends.EC2.Backend.DetachInternetGateway(igwID, vpcID)
}

// ---- EC2 SubnetRouteTableAssociation ----

func (rc *ResourceCreator) createEC2SubnetRouteTableAssociation(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	rtID := strProp(props, "RouteTableId", params, physicalIDs)
	subnetID := strProp(props, "SubnetId", params, physicalIDs)

	assocID, err := rc.backends.EC2.Backend.AssociateRouteTable(rtID, subnetID)
	if err != nil {
		return "", fmt.Errorf("associate route table %s with subnet %s: %w", rtID, subnetID, err)
	}

	return assocID, nil
}

func (rc *ResourceCreator) deleteEC2SubnetRouteTableAssociation(assocID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DisassociateRouteTable(assocID)
}

// ---- IAM User ----

func (rc *ResourceCreator) createIAMUser(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	userName := strProp(props, "UserName", params, physicalIDs)
	if userName == "" {
		userName = logicalID
	}

	path := strProp(props, "Path", params, physicalIDs)
	if path == "" {
		path = "/"
	}

	user, err := rc.backends.IAM.Backend.CreateUser(userName, path, "")
	if err != nil {
		return "", fmt.Errorf("create IAM user %s: %w", userName, err)
	}

	return user.Arn, nil
}

func (rc *ResourceCreator) deleteIAMUser(arn string) error {
	if rc.backends.IAM == nil {
		return nil
	}

	userName := resourceNameFromARN(arn)

	// Detach all policies before deleting; ignore errors since user may have no policies.
	attached, _ := rc.backends.IAM.Backend.ListAttachedUserPolicies(userName)
	for _, p := range attached {
		_ = rc.backends.IAM.Backend.DetachUserPolicy(userName, p.PolicyArn)
	}

	return rc.backends.IAM.Backend.DeleteUser(userName)
}

// ---- IAM Group ----

func (rc *ResourceCreator) createIAMGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	groupName := strProp(props, "GroupName", params, physicalIDs)
	if groupName == "" {
		groupName = logicalID
	}

	path := strProp(props, "Path", params, physicalIDs)
	if path == "" {
		path = "/"
	}

	group, err := rc.backends.IAM.Backend.CreateGroup(groupName, path)
	if err != nil {
		return "", fmt.Errorf("create IAM group %s: %w", groupName, err)
	}

	return group.Arn, nil
}

func (rc *ResourceCreator) deleteIAMGroup(arn string) error {
	if rc.backends.IAM == nil {
		return nil
	}

	groupName := resourceNameFromARN(arn)

	return rc.backends.IAM.Backend.DeleteGroup(groupName)
}

// ---- ELBv2 LoadBalancer (stub — ELBv2 backend not yet in ServiceBackends) ----

func (rc *ResourceCreator) createELBv2LoadBalancer(
	logicalID string,
	_ map[string]any,
	_, _ map[string]string,
) (string, error) {
	return logicalID + "-stub", nil
}

func (rc *ResourceCreator) deleteELBv2LoadBalancer(_ string) error {
	return nil
}

// ---- ELBv2 TargetGroup (stub) ----

func (rc *ResourceCreator) createELBv2TargetGroup(
	logicalID string,
	_ map[string]any,
	_, _ map[string]string,
) (string, error) {
	return logicalID + "-stub", nil
}

func (rc *ResourceCreator) deleteELBv2TargetGroup(_ string) error {
	return nil
}

// ---- ELBv2 Listener (stub) ----

func (rc *ResourceCreator) createELBv2Listener(
	logicalID string,
	_ map[string]any,
	_, _ map[string]string,
) (string, error) {
	return logicalID + "-stub", nil
}

func (rc *ResourceCreator) deleteELBv2Listener(_ string) error {
	return nil
}

// ---- WAFv2 WebACL (stub — WAFv2 backend not yet in ServiceBackends) ----

func (rc *ResourceCreator) createWAFv2WebACL(
	logicalID string,
	_ map[string]any,
	_, _ map[string]string,
) (string, error) {
	return logicalID + "-stub", nil
}

func (rc *ResourceCreator) deleteWAFv2WebACL(_ string) error {
	return nil
}

// ---- WAFv2 IPSet (stub) ----

func (rc *ResourceCreator) createWAFv2IPSet(
	logicalID string,
	_ map[string]any,
	_, _ map[string]string,
) (string, error) {
	return logicalID + "-stub", nil
}

func (rc *ResourceCreator) deleteWAFv2IPSet(_ string) error {
	return nil
}

// ---- WAFv2 RuleGroup (stub) ----

func (rc *ResourceCreator) createWAFv2RuleGroup(
	logicalID string,
	_ map[string]any,
	_, _ map[string]string,
) (string, error) {
	return logicalID + "-stub", nil
}

// ---- Backup Vault (stub — Backup backend not yet in ServiceBackends) ----

func (rc *ResourceCreator) createBackupVault(
	logicalID string,
	_ map[string]any,
	_, _ map[string]string,
) (string, error) {
	return logicalID + "-stub", nil
}

func (rc *ResourceCreator) deleteBackupVault(_ string) error {
	return nil
}

// ---- Backup Plan (stub) ----

func (rc *ResourceCreator) createBackupPlan(
	logicalID string,
	_ map[string]any,
	_, _ map[string]string,
) (string, error) {
	return logicalID + "-stub", nil
}

func (rc *ResourceCreator) deleteBackupPlan(_ string) error {
	return nil
}

// ---- Backup Selection (stub) ----

func (rc *ResourceCreator) createBackupSelection(
	logicalID string,
	_ map[string]any,
	_, _ map[string]string,
) (string, error) {
	return logicalID + "-stub", nil
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
		id, engine, masterUser, "", paramGroupName, 0, nil,
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
