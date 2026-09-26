package cloudformation

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
)

const resTypeRDSDBProxyEndpoint = "AWS::RDS::DBProxyEndpoint"

func (rc *ResourceCreator) createRDSSupplementalResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::RDS::OptionGroup":
		id, err := rc.createRDSOptionGroup(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::RDS::EventSubscription":
		id, err := rc.createRDSEventSubscription(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::RDS::GlobalCluster":
		id, err := rc.createRDSGlobalCluster(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeRDSDBProxyEndpoint:
		id, err := rc.createRDSDBProxyEndpoint(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteRDSSupplementalResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::RDS::OptionGroup":
		return true, rc.deleteRDSOptionGroup(physicalID)
	case "AWS::RDS::EventSubscription":
		return true, rc.deleteRDSEventSubscription(physicalID)
	case "AWS::RDS::GlobalCluster":
		return true, rc.deleteRDSGlobalCluster(physicalID)
	case resTypeRDSDBProxyEndpoint:
		return true, rc.deleteRDSDBProxyEndpoint(physicalID)
	default:
		return false, nil
	}
}

// ---- RDS OptionGroup ----

func (rc *ResourceCreator) createRDSOptionGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.RDS == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "OptionGroupName", params, physicalIDs)
	if name == "" {
		name = strings.ToLower(logicalID)
	}

	og, err := rc.backends.RDS.Backend.CreateOptionGroup(
		name,
		strProp(props, "EngineName", params, physicalIDs),
		strProp(props, "MajorEngineVersion", params, physicalIDs),
		strProp(props, "OptionGroupDescription", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create RDS option group %s: %w", name, err)
	}

	return og.OptionGroupName, nil
}

func (rc *ResourceCreator) deleteRDSOptionGroup(name string) error {
	if rc.backends.RDS == nil {
		return nil
	}

	err := rc.backends.RDS.Backend.DeleteOptionGroup(name)
	if errors.Is(err, rdsbackend.ErrOptionGroupNotFound) {
		return nil
	}

	return err
}

// ---- RDS EventSubscription ----

func (rc *ResourceCreator) createRDSEventSubscription(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.RDS == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "SubscriptionName", params, physicalIDs)
	if name == "" {
		name = strings.ToLower(logicalID)
	}

	sub, err := rc.backends.RDS.Backend.CreateEventSubscription(
		name,
		strProp(props, "SnsTopicArn", params, physicalIDs),
		strProp(props, "SourceType", params, physicalIDs),
		strSliceProp(props["SourceIds"], params, physicalIDs),
		strSliceProp(props["EventCategories"], params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create RDS event subscription %s: %w", name, err)
	}

	return sub.SubscriptionName, nil
}

func (rc *ResourceCreator) deleteRDSEventSubscription(name string) error {
	if rc.backends.RDS == nil {
		return nil
	}

	_, err := rc.backends.RDS.Backend.DeleteEventSubscription(name)
	if errors.Is(err, rdsbackend.ErrEventSubscriptionNotFound) {
		return nil
	}

	return err
}

// ---- RDS GlobalCluster ----

func (rc *ResourceCreator) createRDSGlobalCluster(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.RDS == nil {
		return logicalID + "-stub", nil
	}

	id := strProp(props, "GlobalClusterIdentifier", params, physicalIDs)
	if id == "" {
		id = strings.ToLower(logicalID)
	}

	gc, err := rc.backends.RDS.Backend.CreateGlobalCluster(
		id,
		strProp(props, "Engine", params, physicalIDs),
		strProp(props, "EngineVersion", params, physicalIDs),
		strProp(props, "EngineLifecycleSupport", params, physicalIDs),
		boolProp(props, "StorageEncrypted"),
		boolProp(props, "DeletionProtection"),
	)
	if err != nil {
		return "", fmt.Errorf("create RDS global cluster %s: %w", id, err)
	}

	return gc.GlobalClusterIdentifier, nil
}

func (rc *ResourceCreator) deleteRDSGlobalCluster(id string) error {
	if rc.backends.RDS == nil {
		return nil
	}

	_, err := rc.backends.RDS.Backend.DeleteGlobalCluster(id)
	if errors.Is(err, rdsbackend.ErrGlobalClusterNotFound) {
		return nil
	}

	return err
}

// ---- RDS DBProxyEndpoint ----

func (rc *ResourceCreator) createRDSDBProxyEndpoint(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.RDS == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DBProxyEndpointName", params, physicalIDs)
	if name == "" {
		name = strings.ToLower(logicalID)
	}

	ep, err := rc.backends.RDS.Backend.CreateDBProxyEndpoint(
		strProp(props, "DBProxyName", params, physicalIDs),
		name,
		strProp(props, "TargetRole", params, physicalIDs),
		strSliceProp(props["VpcSubnetIds"], params, physicalIDs),
		strSliceProp(props["VpcSecurityGroupIds"], params, physicalIDs),
		strProp(props, "EndpointNetworkType", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create RDS DB proxy endpoint %s: %w", name, err)
	}

	physicalIDs[logicalID+"/DBProxyEndpointArn"] = ep.DBProxyEndpointARN
	physicalIDs[logicalID+"/Endpoint"] = ep.Endpoint
	physicalIDs[logicalID+"/IsDefault"] = strconv.FormatBool(ep.IsDefault)
	physicalIDs[logicalID+"/VpcId"] = ep.VpcID

	return ep.DBProxyEndpointName, nil
}

func (rc *ResourceCreator) deleteRDSDBProxyEndpoint(name string) error {
	if rc.backends.RDS == nil {
		return nil
	}

	_, err := rc.backends.RDS.Backend.DeleteDBProxyEndpoint(name)
	if errors.Is(err, rdsbackend.ErrDBProxyEndpointNotFound) {
		return nil
	}

	return err
}
