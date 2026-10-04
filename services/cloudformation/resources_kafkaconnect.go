package cloudformation

import (
	"fmt"
	"strconv"

	kafkaconnectbackend "github.com/blackbirdworks/gopherstack/services/kafkaconnect"
)

const (
	resTypeKafkaConnectConnector           = "AWS::KafkaConnect::Connector"
	resTypeKafkaConnectCustomPlugin        = "AWS::KafkaConnect::CustomPlugin"
	resTypeKafkaConnectWorkerConfiguration = "AWS::KafkaConnect::WorkerConfiguration"
)

// createKafkaConnectResource handles the MSK Connect resource types listed above.
func (rc *ResourceCreator) createKafkaConnectResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeKafkaConnectConnector:
		id, err := rc.createKafkaConnectConnector(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeKafkaConnectCustomPlugin:
		id, err := rc.createKafkaConnectCustomPlugin(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeKafkaConnectWorkerConfiguration:
		id, err := rc.createKafkaConnectWorkerConfiguration(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteKafkaConnectResource handles deletion for the types created above.
// All three types delete by their ARN-shaped physicalID directly.
func (rc *ResourceCreator) deleteKafkaConnectResource(resourceType, physicalID string) (bool, error) {
	if rc.backends.KafkaConnect == nil {
		switch resourceType {
		case resTypeKafkaConnectConnector, resTypeKafkaConnectCustomPlugin, resTypeKafkaConnectWorkerConfiguration:
			return true, nil
		default:
			return false, nil
		}
	}

	switch resourceType {
	case resTypeKafkaConnectConnector:
		_, err := rc.backends.KafkaConnect.Backend.DeleteConnector(physicalID, "")

		return true, ignoreNotFound(err, kafkaconnectbackend.ErrConnectorNotFound)
	case resTypeKafkaConnectCustomPlugin:
		_, err := rc.backends.KafkaConnect.Backend.DeleteCustomPlugin(physicalID)

		return true, ignoreNotFound(err, kafkaconnectbackend.ErrCustomPluginNotFound)
	case resTypeKafkaConnectWorkerConfiguration:
		_, err := rc.backends.KafkaConnect.Backend.DeleteWorkerConfiguration(physicalID)

		return true, ignoreNotFound(err, kafkaconnectbackend.ErrWorkerConfigNotFound)
	default:
		return false, nil
	}
}

// ---- AWS::KafkaConnect::Connector ----
// Ref and Fn::GetAtt ConnectorArn both return the connector ARN: confirmed
// against the AWS-published resource-provider schema
// (aws-cloudformation-resource-providers-kafkaconnect,
// primaryIdentifier == /properties/ConnectorArn == its sole readOnlyProperty),
// since the CloudFormation Template Reference page itself leaves Ref
// undocumented and states only the ConnectorArn Fn::GetAtt attribute.

func (rc *ResourceCreator) createKafkaConnectConnector(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.KafkaConnect == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ConnectorName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	capacity, _ := props["Capacity"].(map[string]any)
	kafkaCluster, _ := props["KafkaCluster"].(map[string]any)
	clientAuth, _ := props["KafkaClusterClientAuthentication"].(map[string]any)
	encryption, _ := props["KafkaClusterEncryptionInTransit"].(map[string]any)
	logDelivery, _ := props["LogDelivery"].(map[string]any)
	workerConfig, _ := props["WorkerConfiguration"].(map[string]any)

	spec := kafkaconnectbackend.ConnectorSpec{
		Name:                             name,
		Description:                      strProp(props, "ConnectorDescription", params, physicalIDs),
		ConnectorConfiguration:           stringMapProp(props["ConnectorConfiguration"], params, physicalIDs),
		Capacity:                         kafkaConnectCapacityFromProps(capacity, params, physicalIDs),
		ApacheKafkaCluster:               kafkaConnectApacheKafkaClusterFromProps(kafkaCluster, params, physicalIDs),
		KafkaClusterClientAuthentication: strProp(clientAuth, "AuthenticationType", params, physicalIDs),
		KafkaClusterEncryptionInTransit:  strProp(encryption, "EncryptionType", params, physicalIDs),
		KafkaConnectVersion:              strProp(props, "KafkaConnectVersion", params, physicalIDs),
		ServiceExecutionRoleArn:          strProp(props, "ServiceExecutionRoleArn", params, physicalIDs),
		NetworkType:                      strProp(props, "NetworkType", params, physicalIDs),
		Plugins:                          kafkaConnectPluginsFromProps(props["Plugins"], params, physicalIDs),
		WorkerLogDelivery:                kafkaConnectWorkerLogDeliveryFromProps(logDelivery, params, physicalIDs),
		Tags:                             tagListProp(props, params, physicalIDs),
	}

	if workerConfig != nil {
		spec.WorkerConfiguration = &kafkaconnectbackend.WorkerConfigRef{
			Arn:      strProp(workerConfig, "WorkerConfigurationArn", params, physicalIDs),
			Revision: int64Prop(workerConfig, "Revision", params, physicalIDs),
		}
	}

	c, err := rc.backends.KafkaConnect.Backend.CreateConnector(rc.backends.AccountID, rc.backends.Region, spec)
	if err != nil {
		return "", fmt.Errorf("create MSK Connect connector %s: %w", name, err)
	}

	physicalIDs[logicalID+"/ConnectorArn"] = c.ARN

	return c.ARN, nil
}

func kafkaConnectCapacityFromProps(
	v map[string]any, params, physicalIDs map[string]string,
) kafkaconnectbackend.Capacity {
	var capacity kafkaconnectbackend.Capacity

	if as, ok := v["AutoScaling"].(map[string]any); ok {
		scaleIn, _ := as["ScaleInPolicy"].(map[string]any)
		scaleOut, _ := as["ScaleOutPolicy"].(map[string]any)

		capacity.AutoScaling = &kafkaconnectbackend.AutoScaling{
			MinWorkerCount:          int32Prop(as, "MinWorkerCount", params, physicalIDs),
			MaxWorkerCount:          int32Prop(as, "MaxWorkerCount", params, physicalIDs),
			McuCount:                int32Prop(as, "McuCount", params, physicalIDs),
			MaxAutoscalingTaskCount: int32Prop(as, "MaxAutoscalingTaskCount", params, physicalIDs),
			ScaleInCPUPercent:       int32Prop(scaleIn, "CpuUtilizationPercentage", params, physicalIDs),
			ScaleOutCPUPercent:      int32Prop(scaleOut, "CpuUtilizationPercentage", params, physicalIDs),
		}
	}

	if pc, ok := v["ProvisionedCapacity"].(map[string]any); ok {
		capacity.Provisioned = &kafkaconnectbackend.ProvisionedCapacity{
			McuCount:    int32Prop(pc, "McuCount", params, physicalIDs),
			WorkerCount: int32Prop(pc, "WorkerCount", params, physicalIDs),
		}
	}

	return capacity
}

func kafkaConnectApacheKafkaClusterFromProps(
	v map[string]any, params, physicalIDs map[string]string,
) kafkaconnectbackend.ApacheKafkaCluster {
	akc, _ := v["ApacheKafkaCluster"].(map[string]any)
	vpc, _ := akc["Vpc"].(map[string]any)

	return kafkaconnectbackend.ApacheKafkaCluster{
		BootstrapServers: strProp(akc, "BootstrapServers", params, physicalIDs),
		Vpc: kafkaconnectbackend.Vpc{
			SecurityGroups: strSliceProp(vpc["SecurityGroups"], params, physicalIDs),
			Subnets:        strSliceProp(vpc["Subnets"], params, physicalIDs),
		},
	}
}

func kafkaConnectPluginsFromProps(v any, params, physicalIDs map[string]string) []kafkaconnectbackend.PluginRef {
	list, ok := v.([]any)
	if !ok {
		return nil
	}

	out := make([]kafkaconnectbackend.PluginRef, 0, len(list))

	for _, item := range list {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		cp, _ := m["CustomPlugin"].(map[string]any)
		out = append(out, kafkaconnectbackend.PluginRef{
			CustomPluginArn: strProp(cp, "CustomPluginArn", params, physicalIDs),
			Revision:        int64Prop(cp, "Revision", params, physicalIDs),
		})
	}

	return out
}

func kafkaConnectWorkerLogDeliveryFromProps(
	v map[string]any, params, physicalIDs map[string]string,
) *kafkaconnectbackend.WorkerLogDelivery {
	wld, ok := v["WorkerLogDelivery"].(map[string]any)
	if !ok {
		return nil
	}

	out := &kafkaconnectbackend.WorkerLogDelivery{}

	if cw, isMap := wld["CloudWatchLogs"].(map[string]any); isMap {
		out.CloudWatchLogs = &kafkaconnectbackend.CloudWatchLogsDelivery{
			Enabled:  boolProp(cw, "Enabled"),
			LogGroup: strProp(cw, "LogGroup", params, physicalIDs),
		}
	}

	if fh, isMap := wld["Firehose"].(map[string]any); isMap {
		out.Firehose = &kafkaconnectbackend.FirehoseDelivery{
			DeliveryStream: strProp(fh, "DeliveryStream", params, physicalIDs),
			Enabled:        boolProp(fh, "Enabled"),
		}
	}

	if s3, isMap := wld["S3"].(map[string]any); isMap {
		out.S3 = &kafkaconnectbackend.S3LogDelivery{
			Bucket:  strProp(s3, "Bucket", params, physicalIDs),
			Prefix:  strProp(s3, "Prefix", params, physicalIDs),
			Enabled: boolProp(s3, "Enabled"),
		}
	}

	return out
}

// ---- AWS::KafkaConnect::CustomPlugin ----
// Ref and Fn::GetAtt CustomPluginArn both return the custom plugin ARN (see
// the Connector doc comment above for the Ref-attribution basis; confirmed
// separately for CustomPlugin against its own resource-provider schema).
// Revision is a documented Fn::GetAtt attribute.

func (rc *ResourceCreator) createKafkaConnectCustomPlugin(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.KafkaConnect == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	location, _ := props["Location"].(map[string]any)
	s3Location, _ := location["S3Location"].(map[string]any)

	p, err := rc.backends.KafkaConnect.Backend.CreateCustomPlugin(
		rc.backends.AccountID,
		rc.backends.Region,
		name,
		strProp(props, "Description", params, physicalIDs),
		strProp(props, "ContentType", params, physicalIDs),
		strProp(s3Location, "BucketArn", params, physicalIDs),
		strProp(s3Location, "FileKey", params, physicalIDs),
		strProp(s3Location, "ObjectVersion", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create MSK Connect custom plugin %s: %w", name, err)
	}

	physicalIDs[logicalID+"/CustomPluginArn"] = p.ARN
	physicalIDs[logicalID+"/Revision"] = strconv.FormatInt(p.Revision, 10)

	return p.ARN, nil
}

// ---- AWS::KafkaConnect::WorkerConfiguration ----
// Ref and Fn::GetAtt WorkerConfigurationArn both return the worker
// configuration ARN (see the Connector doc comment above for the
// Ref-attribution basis; confirmed separately for WorkerConfiguration
// against its own resource-provider schema). Revision is a documented
// Fn::GetAtt attribute.

func (rc *ResourceCreator) createKafkaConnectWorkerConfiguration(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.KafkaConnect == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	w, err := rc.backends.KafkaConnect.Backend.CreateWorkerConfiguration(
		rc.backends.AccountID,
		rc.backends.Region,
		name,
		strProp(props, "Description", params, physicalIDs),
		strProp(props, "PropertiesFileContent", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create MSK Connect worker configuration %s: %w", name, err)
	}

	physicalIDs[logicalID+"/WorkerConfigurationArn"] = w.ARN
	physicalIDs[logicalID+"/Revision"] = strconv.FormatInt(w.LatestRevision.Revision, 10)

	return w.ARN, nil
}
