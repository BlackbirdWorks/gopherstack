package cloudformation

import (
	"encoding/json"
	"fmt"
	"strings"

	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	rds "github.com/blackbirdworks/gopherstack/services/rds"
)

const wafScopeRegional = "REGIONAL"

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

// ---- ELBv2 LoadBalancer ----

func (rc *ResourceCreator) createELBv2LoadBalancer(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ELBv2 == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = strings.ToLower(logicalID)
	}

	lbType := strProp(props, "Type", params, physicalIDs)
	if lbType == "" {
		lbType = "application"
	}

	scheme := strProp(props, "Scheme", params, physicalIDs)

	lb, err := rc.backends.ELBv2.Backend.CreateLoadBalancer(elbv2backend.CreateLoadBalancerInput{
		Name:   name,
		Type:   lbType,
		Scheme: scheme,
	})
	if err != nil {
		return "", fmt.Errorf("create ELBv2 load balancer %s: %w", name, err)
	}

	return lb.LoadBalancerArn, nil
}

func (rc *ResourceCreator) deleteELBv2LoadBalancer(arn string) error {
	if rc.backends.ELBv2 == nil {
		return nil
	}

	return rc.backends.ELBv2.Backend.DeleteLoadBalancer(arn)
}

// ---- ELBv2 TargetGroup ----

func (rc *ResourceCreator) createELBv2TargetGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ELBv2 == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = strings.ToLower(logicalID)
	}

	tg, err := rc.backends.ELBv2.Backend.CreateTargetGroup(elbv2backend.CreateTargetGroupInput{
		Name:       name,
		Protocol:   strProp(props, "Protocol", params, physicalIDs),
		VpcID:      strProp(props, "VpcId", params, physicalIDs),
		TargetType: strProp(props, "TargetType", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create ELBv2 target group %s: %w", name, err)
	}

	return tg.TargetGroupArn, nil
}

func (rc *ResourceCreator) deleteELBv2TargetGroup(arn string) error {
	if rc.backends.ELBv2 == nil {
		return nil
	}

	return rc.backends.ELBv2.Backend.DeleteTargetGroup(arn)
}

// ---- ELBv2 Listener ----

func (rc *ResourceCreator) createELBv2Listener(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ELBv2 == nil {
		return logicalID + "-stub", nil
	}

	lbArn := strProp(props, "LoadBalancerArn", params, physicalIDs)
	protocol := strProp(props, "Protocol", params, physicalIDs)

	listener, err := rc.backends.ELBv2.Backend.CreateListener(elbv2backend.CreateListenerInput{
		LoadBalancerArn: lbArn,
		Protocol:        protocol,
	})
	if err != nil {
		return "", fmt.Errorf("create ELBv2 listener on %s: %w", lbArn, err)
	}

	return listener.ListenerArn, nil
}

func (rc *ResourceCreator) deleteELBv2Listener(arn string) error {
	if rc.backends.ELBv2 == nil {
		return nil
	}

	return rc.backends.ELBv2.Backend.DeleteListener(arn)
}

// ---- WAFv2 WebACL ----

func (rc *ResourceCreator) createWAFv2WebACL(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.WAFv2 == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	scope := strProp(props, "Scope", params, physicalIDs)
	if scope == "" {
		scope = wafScopeRegional
	}

	acl, err := rc.backends.WAFv2.Backend.CreateWebACL(
		name, scope, "",
		json.RawMessage(`{"Allow":{}}`), nil,
		nil, nil, nil, nil, nil, nil,
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("create WAFv2 WebACL %s: %w", name, err)
	}

	return acl.ID, nil
}

func (rc *ResourceCreator) deleteWAFv2WebACL(id string) error {
	if rc.backends.WAFv2 == nil {
		return nil
	}

	return rc.backends.WAFv2.Backend.DeleteWebACL(id, "")
}

// ---- WAFv2 IPSet ----

func (rc *ResourceCreator) createWAFv2IPSet(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.WAFv2 == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	scope := strProp(props, "Scope", params, physicalIDs)
	if scope == "" {
		scope = wafScopeRegional
	}

	ipVersion := strProp(props, "IPAddressVersion", params, physicalIDs)
	if ipVersion == "" {
		ipVersion = "IPV4"
	}

	ipset, err := rc.backends.WAFv2.Backend.CreateIPSet(name, scope, "", ipVersion, nil, nil)
	if err != nil {
		return "", fmt.Errorf("create WAFv2 IPSet %s: %w", name, err)
	}

	return ipset.ID, nil
}

func (rc *ResourceCreator) deleteWAFv2IPSet(id string) error {
	if rc.backends.WAFv2 == nil {
		return nil
	}

	return rc.backends.WAFv2.Backend.DeleteIPSet(id, "")
}

// ---- WAFv2 RuleGroup ----

func (rc *ResourceCreator) createWAFv2RuleGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.WAFv2 == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	scope := strProp(props, "Scope", params, physicalIDs)
	if scope == "" {
		scope = wafScopeRegional
	}

	rg, err := rc.backends.WAFv2.Backend.CreateRuleGroup(name, scope, "", "", 0, nil, nil)
	if err != nil {
		return "", fmt.Errorf("create WAFv2 RuleGroup %s: %w", name, err)
	}

	return rg.ID, nil
}

func (rc *ResourceCreator) deleteWAFv2RuleGroup(_ string) error {
	return nil
}

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

	plan, err := rc.backends.Backup.Backend.CreateBackupPlan(name, nil, nil)
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
	sel, err := rc.backends.Backup.Backend.CreateBackupSelection(planID, selectionName, iamRoleArn)
	if err != nil {
		return "", fmt.Errorf("create Backup Selection %s: %w", selectionName, err)
	}

	return sel.SelectionID, nil
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
		id, engine, masterUser, "", paramGroupName, 0, nil, rds.DBClusterOptions{},
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
