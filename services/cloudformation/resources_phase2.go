package cloudformation

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"

	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	cloudwatchbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
)

// eipAllocIDSuffixLen is the number of UUID hex characters used to generate
// a stub allocation ID when no EC2 backend is configured.
const eipAllocIDSuffixLen = 17

// parseLayerVersionARN parses a Lambda layer version ARN and returns the layer name and version.
// Expected format: arn:aws:lambda:{region}:{account}:layer:{name}:{version}.
func parseLayerVersionARN(arn string) (string, int64) {
	parts := strings.Split(arn, ":")
	const layerARNParts = 8
	if len(parts) != layerARNParts || parts[5] != "layer" {
		return "", 0
	}

	var v int64
	if _, err := fmt.Sscanf(parts[7], "%d", &v); err != nil {
		return "", 0
	}

	return parts[6], v
}

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
		id, engine, instanceClass, dbName, masterUser, paramGroupName, allocatedStorage,
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

// ---- ElastiCache extensions ----

func (rc *ResourceCreator) createElastiCacheReplicationGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ElastiCache == nil {
		return logicalID + "-stub", nil
	}

	id := strProp(props, "ReplicationGroupId", params, physicalIDs)
	if id == "" {
		id = strings.ToLower(logicalID)
	}

	description := strProp(props, "ReplicationGroupDescription", params, physicalIDs)

	rg, err := rc.backends.ElastiCache.Backend.CreateReplicationGroup(id, description)
	if err != nil {
		return "", fmt.Errorf("create ElastiCache replication group %s: %w", id, err)
	}

	return rg.ReplicationGroupID, nil
}

func (rc *ResourceCreator) deleteElastiCacheReplicationGroup(_ context.Context, id string) error {
	if rc.backends.ElastiCache == nil {
		return nil
	}

	return rc.backends.ElastiCache.Backend.DeleteReplicationGroup(id)
}

func (rc *ResourceCreator) createElastiCacheSubnetGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ElastiCache == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "CacheSubnetGroupName", params, physicalIDs)
	if name == "" {
		name = strings.ToLower(logicalID)
	}

	description := strProp(props, "CacheSubnetGroupDescription", params, physicalIDs)

	var subnetIDs []string
	if list, ok := props["SubnetIds"].([]any); ok {
		for _, v := range list {
			if s := resolve(v, params, physicalIDs); s != "" {
				subnetIDs = append(subnetIDs, s)
			}
		}
	}

	grp, err := rc.backends.ElastiCache.Backend.CreateSubnetGroup(name, description, subnetIDs)
	if err != nil {
		return "", fmt.Errorf("create ElastiCache subnet group %s: %w", name, err)
	}

	return grp.Name, nil
}

func (rc *ResourceCreator) deleteElastiCacheSubnetGroup(name string) error {
	if rc.backends.ElastiCache == nil {
		return nil
	}

	return rc.backends.ElastiCache.Backend.DeleteSubnetGroup(name)
}

// ---- Route53 HealthCheck ----

func (rc *ResourceCreator) createRoute53HealthCheck(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Route53 == nil {
		return logicalID + "-stub", nil
	}

	callerRef := strProp(props, "CallerReference", params, physicalIDs)
	if callerRef == "" {
		callerRef = uuid.New().String()
	}

	hcType := route53backend.HealthCheckType(strProp(props, "Type", params, physicalIDs))
	if hcType == "" {
		hcType = route53backend.HealthCheckTypeHTTPS
	}

	cfg := route53backend.HealthCheckConfig{Type: hcType}
	applyHealthCheckConfigProps(props, params, physicalIDs, &cfg)

	hc, err := rc.backends.Route53.Backend.CreateHealthCheck(callerRef, cfg)
	if err != nil {
		return "", fmt.Errorf("create Route53 health check: %w", err)
	}

	return hc.ID, nil
}

// applyHealthCheckConfigProps fills in the HealthCheckConfig from the HealthCheckConfig property bag.
func applyHealthCheckConfigProps(
	props map[string]any,
	params, physicalIDs map[string]string,
	cfg *route53backend.HealthCheckConfig,
) {
	hcc, ok := props["HealthCheckConfig"].(map[string]any)
	if !ok {
		return
	}

	cfg.FullyQualifiedDomainName = resolve(hcc["FullyQualifiedDomainName"], params, physicalIDs)
	cfg.IPAddress = resolve(hcc["IPAddress"], params, physicalIDs)

	if portVal := resolve(hcc["Port"], params, physicalIDs); portVal != "" {
		if p, err := strconv.Atoi(portVal); err == nil {
			cfg.Port = p
		}
	}

	if resourcePath := resolve(hcc["ResourcePath"], params, physicalIDs); resourcePath != "" {
		cfg.ResourcePath = resourcePath
	}

	if hcTypeStr := resolve(hcc["Type"], params, physicalIDs); hcTypeStr != "" {
		cfg.Type = route53backend.HealthCheckType(hcTypeStr)
	}
}

func (rc *ResourceCreator) deleteRoute53HealthCheck(id string) error {
	if rc.backends.Route53 == nil {
		return nil
	}

	return rc.backends.Route53.Backend.DeleteHealthCheck(id)
}

// ---- CloudWatch CompositeAlarm ----

func (rc *ResourceCreator) createCloudWatchCompositeAlarm(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatch == nil {
		return logicalID + "-stub", nil
	}

	alarmName := strProp(props, "AlarmName", params, physicalIDs)
	if alarmName == "" {
		alarmName = logicalID
	}

	alarmRule := strProp(props, "AlarmRule", params, physicalIDs)
	alarmDescription := strProp(props, "AlarmDescription", params, physicalIDs)

	alarm := &cloudwatchbackend.CompositeAlarm{
		AlarmName:        alarmName,
		AlarmRule:        alarmRule,
		AlarmDescription: alarmDescription,
		StateValue:       "INSUFFICIENT_DATA",
		ActionsEnabled:   true,
	}

	if err := rc.backends.CloudWatch.Backend.PutCompositeAlarm(alarm); err != nil {
		return "", fmt.Errorf("create CloudWatch composite alarm %s: %w", alarmName, err)
	}

	return alarmName, nil
}

// ---- ECS ----

func (rc *ResourceCreator) createECSCluster(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ECS == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ClusterName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	cluster, err := rc.backends.ECS.Backend.CreateCluster(ecsbackend.CreateClusterInput{
		ClusterName: name,
	})
	if err != nil {
		return "", fmt.Errorf("create ECS cluster %s: %w", name, err)
	}

	return cluster.ClusterArn, nil
}

func (rc *ResourceCreator) deleteECSCluster(arn string) error {
	if rc.backends.ECS == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	_, err := rc.backends.ECS.Backend.DeleteCluster(name)

	return err
}

func (rc *ResourceCreator) createECSTaskDefinition(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ECS == nil {
		return logicalID + "-stub", nil
	}

	family := strProp(props, "Family", params, physicalIDs)
	if family == "" {
		family = logicalID
	}

	networkMode := strProp(props, "NetworkMode", params, physicalIDs)

	var containerDefs []ecsbackend.ContainerDefinition
	if list, ok := props["ContainerDefinitions"].([]any); ok {
		containerDefs = parseContainerDefinitions(list, params, physicalIDs)
	}

	td, err := rc.backends.ECS.Backend.RegisterTaskDefinition(ecsbackend.RegisterTaskDefinitionInput{
		Family:               family,
		NetworkMode:          networkMode,
		ContainerDefinitions: containerDefs,
	})
	if err != nil {
		return "", fmt.Errorf("register ECS task definition %s: %w", family, err)
	}

	return td.TaskDefinitionArn, nil
}

func (rc *ResourceCreator) deleteECSTaskDefinition(arn string) error {
	if rc.backends.ECS == nil {
		return nil
	}

	_, err := rc.backends.ECS.Backend.DeregisterTaskDefinition(arn)

	return err
}

// parseContainerDefinitions converts CloudFormation container definition property maps
// to ECS ContainerDefinition structs.
func parseContainerDefinitions(
	list []any,
	params, physicalIDs map[string]string,
) []ecsbackend.ContainerDefinition {
	defs := make([]ecsbackend.ContainerDefinition, 0, len(list))

	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		cd := ecsbackend.ContainerDefinition{
			Name:  resolve(m["Name"], params, physicalIDs),
			Image: resolve(m["Image"], params, physicalIDs),
		}

		if cpu, ok2 := m["Cpu"].(float64); ok2 {
			cd.CPU = int(cpu)
		}

		if mem, ok2 := m["Memory"].(float64); ok2 {
			cd.Memory = int(mem)
		}

		defs = append(defs, cd)
	}

	return defs
}

func (rc *ResourceCreator) createECSService(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ECS == nil {
		return logicalID + "-stub", nil
	}

	serviceName := strProp(props, "ServiceName", params, physicalIDs)
	if serviceName == "" {
		serviceName = logicalID
	}

	cluster := strProp(props, "Cluster", params, physicalIDs)
	taskDef := strProp(props, "TaskDefinition", params, physicalIDs)
	launchType := strProp(props, "LaunchType", params, physicalIDs)

	var desiredCount int
	if v, ok := props["DesiredCount"].(float64); ok {
		desiredCount = int(v)
	}

	svc, err := rc.backends.ECS.Backend.CreateService(ecsbackend.CreateServiceInput{
		ServiceName:    serviceName,
		Cluster:        cluster,
		TaskDefinition: taskDef,
		LaunchType:     launchType,
		DesiredCount:   desiredCount,
	})
	if err != nil {
		return "", fmt.Errorf("create ECS service %s: %w", serviceName, err)
	}

	return svc.ServiceArn, nil
}

func (rc *ResourceCreator) deleteECSService(arn string) error {
	if rc.backends.ECS == nil {
		return nil
	}

	// ARN format: arn:aws:ecs:{region}:{account}:service/{cluster}/{service}
	// After splitting on "/" we need at least 3 parts: prefix, cluster name, service name.
	parts := strings.Split(arn, "/")
	if len(parts) < 3 { //nolint:mnd // ARN must have prefix/cluster/service sections
		return nil
	}

	// Last part is the service name, second-to-last is the cluster name.
	serviceName := parts[len(parts)-1]
	clusterName := parts[len(parts)-2]

	_, err := rc.backends.ECS.Backend.DeleteService(clusterName, serviceName)

	return err
}

// ---- ECR ----

func (rc *ResourceCreator) createECRRepository(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ECR == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "RepositoryName", params, physicalIDs)
	if name == "" {
		name = strings.ToLower(logicalID)
	}

	repo, err := rc.backends.ECR.Backend.CreateRepository(name)
	if err != nil {
		return "", fmt.Errorf("create ECR repository %s: %w", name, err)
	}

	return repo.RepositoryARN, nil
}

func (rc *ResourceCreator) deleteECRRepository(arn string) error {
	if rc.backends.ECR == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	_, err := rc.backends.ECR.Backend.DeleteRepository(name)

	return err
}

// ---- Lambda Layer Version ----

func (rc *ResourceCreator) createLambdaLayerVersion(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Lambda == nil {
		return logicalID + "-stub", nil
	}

	imb, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

	layerName := strProp(props, "LayerName", params, physicalIDs)
	if layerName == "" {
		layerName = logicalID
	}

	description := strProp(props, "Description", params, physicalIDs)
	licenseInfo := strProp(props, "LicenseInfo", params, physicalIDs)

	var compatibleRuntimes []string
	if list, ok2 := props["CompatibleRuntimes"].([]any); ok2 {
		for _, v := range list {
			if s := resolve(v, params, physicalIDs); s != "" {
				compatibleRuntimes = append(compatibleRuntimes, s)
			}
		}
	}

	input := &lambdabackend.PublishLayerVersionInput{
		LayerName:          layerName,
		Description:        description,
		LicenseInfo:        licenseInfo,
		CompatibleRuntimes: compatibleRuntimes,
		Content: &lambdabackend.LayerVersionContentInput{
			ZipFile: []byte{},
		},
	}

	out, err := imb.PublishLayerVersion(input)
	if err != nil {
		return "", fmt.Errorf("publish Lambda layer version %s: %w", layerName, err)
	}

	return out.LayerVersionArn, nil
}

func (rc *ResourceCreator) deleteLambdaLayerVersion(layerVersionARN string) error {
	if rc.backends.Lambda == nil {
		return nil
	}

	imb, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return nil
	}

	// Parse ARN: arn:aws:lambda:{region}:{account}:layer:{name}:{version}
	layerName, version := parseLayerVersionARN(layerVersionARN)
	if layerName == "" {
		return nil
	}

	return imb.DeleteLayerVersion(layerName, version)
}

func (rc *ResourceCreator) createLambdaLayerVersionPermission(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Lambda == nil {
		return logicalID + "-stub", nil
	}

	imb, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

	layerVersionARN := strProp(props, "LayerVersionArn", params, physicalIDs)
	statementID := strProp(props, "Id", params, physicalIDs)
	if statementID == "" {
		statementID = logicalID
	}

	action := strProp(props, "Action", params, physicalIDs)
	principal := strProp(props, "Principal", params, physicalIDs)
	orgID := strProp(props, "OrganizationId", params, physicalIDs)

	layerName, version := parseLayerVersionARN(layerVersionARN)
	if layerName == "" {
		return logicalID + "-stub", nil
	}

	_, err := imb.AddLayerVersionPermission(layerName, version, &lambdabackend.AddLayerVersionPermissionInput{
		Action:         action,
		Principal:      principal,
		StatementID:    statementID,
		OrganizationID: orgID,
	})
	if err != nil {
		return "", fmt.Errorf("add Lambda layer version permission: %w", err)
	}

	// Physical ID encodes layer ARN + statement ID.
	return layerVersionARN + ":" + statementID, nil
}

func (rc *ResourceCreator) deleteLambdaLayerVersionPermission(physicalID string) error {
	if rc.backends.Lambda == nil {
		return nil
	}

	imb, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return nil
	}

	// physicalID format: {layerVersionARN}:{statementID}
	lastColon := strings.LastIndex(physicalID, ":")
	if lastColon < 0 {
		return nil
	}

	layerVersionARN := physicalID[:lastColon]
	statementID := physicalID[lastColon+1:]

	layerName, version := parseLayerVersionARN(layerVersionARN)
	if layerName == "" {
		return nil
	}

	return imb.RemoveLayerVersionPermission(layerName, version, statementID)
}

// ---- Redshift ----

func (rc *ResourceCreator) createRedshiftCluster(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Redshift == nil {
		return logicalID + "-stub", nil
	}

	id := strProp(props, "ClusterIdentifier", params, physicalIDs)
	if id == "" {
		id = strings.ToLower(logicalID)
	}

	nodeType := strProp(props, "NodeType", params, physicalIDs)
	dbName := strProp(props, "DBName", params, physicalIDs)
	masterUser := strProp(props, "MasterUsername", params, physicalIDs)

	cluster, err := rc.backends.Redshift.Backend.CreateCluster(id, nodeType, dbName, masterUser)
	if err != nil {
		return "", fmt.Errorf("create Redshift cluster %s: %w", id, err)
	}

	return cluster.ClusterIdentifier, nil
}

func (rc *ResourceCreator) deleteRedshiftCluster(id string) error {
	if rc.backends.Redshift == nil {
		return nil
	}

	_, err := rc.backends.Redshift.Backend.DeleteCluster(id)

	return err
}

// ---- OpenSearch ----

func (rc *ResourceCreator) createOpenSearchDomain(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.OpenSearch == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DomainName", params, physicalIDs)
	if name == "" {
		name = strings.ToLower(logicalID)
	}

	engineVersion := strProp(props, "EngineVersion", params, physicalIDs)

	var clusterConfig opensearchbackend.ClusterConfig
	if cc, ok := props["ClusterConfig"].(map[string]any); ok {
		clusterConfig.InstanceType = resolve(cc["InstanceType"], params, physicalIDs)

		if n, ok2 := cc["InstanceCount"].(float64); ok2 {
			clusterConfig.InstanceCount = int(n)
		}
	}

	domain, err := rc.backends.OpenSearch.Backend.CreateDomain(name, engineVersion, clusterConfig)
	if err != nil {
		return "", fmt.Errorf("create OpenSearch domain %s: %w", name, err)
	}

	return domain.ARN, nil
}

func (rc *ResourceCreator) deleteOpenSearchDomain(arn string) error {
	if rc.backends.OpenSearch == nil {
		return nil
	}

	// OpenSearch domain name can be extracted from ARN: arn:aws:es:{region}:{account}:domain/{name}
	name := resourceNameFromARN(arn)

	_, err := rc.backends.OpenSearch.Backend.DeleteDomain(name)

	return err
}

// ---- Firehose ----

func (rc *ResourceCreator) createFirehoseDeliveryStream(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Firehose == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DeliveryStreamName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	stream, err := rc.backends.Firehose.Backend.CreateDeliveryStream(firehosebackend.CreateDeliveryStreamInput{
		Name: name,
	})
	if err != nil {
		return "", fmt.Errorf("create Firehose delivery stream %s: %w", name, err)
	}

	return stream.ARN, nil
}

func (rc *ResourceCreator) deleteFirehoseDeliveryStream(arn string) error {
	if rc.backends.Firehose == nil {
		return nil
	}

	// Extract stream name from ARN: arn:aws:firehose:{region}:{account}:deliverystream/{name}
	name := resourceNameFromARN(arn)

	return rc.backends.Firehose.Backend.DeleteDeliveryStream(name)
}

// ---- Route53Resolver ----

func (rc *ResourceCreator) createRoute53ResolverEndpoint(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Route53Resolver == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	direction := strProp(props, "Direction", params, physicalIDs)
	if direction == "" {
		direction = "INBOUND"
	}

	ep, err := rc.backends.Route53Resolver.Backend.CreateResolverEndpoint(name, direction, "", nil)
	if err != nil {
		return "", fmt.Errorf("create Route53Resolver endpoint %s: %w", name, err)
	}

	return ep.ID, nil
}

func (rc *ResourceCreator) deleteRoute53ResolverEndpoint(id string) error {
	if rc.backends.Route53Resolver == nil {
		return nil
	}

	return rc.backends.Route53Resolver.Backend.DeleteResolverEndpoint(id)
}

func (rc *ResourceCreator) createRoute53ResolverRule(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Route53Resolver == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	domainName := strProp(props, "DomainName", params, physicalIDs)
	ruleType := strProp(props, "RuleType", params, physicalIDs)
	if ruleType == "" {
		ruleType = "FORWARD"
	}

	endpointID := strProp(props, "ResolverEndpointId", params, physicalIDs)

	rule, err := rc.backends.Route53Resolver.Backend.CreateResolverRule(name, domainName, ruleType, endpointID)
	if err != nil {
		return "", fmt.Errorf("create Route53Resolver rule %s: %w", name, err)
	}

	return rule.ID, nil
}

func (rc *ResourceCreator) deleteRoute53ResolverRule(id string) error {
	if rc.backends.Route53Resolver == nil {
		return nil
	}

	return rc.backends.Route53Resolver.Backend.DeleteResolverRule(id)
}

// ---- SWF ----

func (rc *ResourceCreator) createSWFDomain(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SWF == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	description := strProp(props, "Description", params, physicalIDs)

	if err := rc.backends.SWF.Backend.RegisterDomain(name, description); err != nil {
		return "", fmt.Errorf("create SWF domain %s: %w", name, err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteSWFDomain(name string) error {
	if rc.backends.SWF == nil {
		return nil
	}

	return rc.backends.SWF.Backend.DeprecateDomain(name)
}

// ---- AppSync ----

func (rc *ResourceCreator) createAppSyncGraphQLAPI(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppSync == nil {
		return logicalID + "-stub", nil
	}

	imb, ok := rc.backends.AppSync.Backend.(*appsyncbackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	authType := appsyncbackend.AuthenticationType(strProp(props, "AuthenticationType", params, physicalIDs))
	if authType == "" {
		authType = appsyncbackend.AuthTypeAPIKey
	}

	api, err := imb.CreateGraphqlAPI(name, authType, nil)
	if err != nil {
		return "", fmt.Errorf("create AppSync GraphQL API %s: %w", name, err)
	}

	return api.APIID, nil
}

func (rc *ResourceCreator) deleteAppSyncGraphQLAPI(apiID string) error {
	if rc.backends.AppSync == nil {
		return nil
	}

	imb, ok := rc.backends.AppSync.Backend.(*appsyncbackend.InMemoryBackend)
	if !ok {
		return nil
	}

	return imb.DeleteGraphqlAPI(apiID)
}

// ---- SES ----

func (rc *ResourceCreator) createSESEmailIdentity(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SES == nil {
		return logicalID + "-stub", nil
	}

	emailIdentity := strProp(props, "EmailIdentity", params, physicalIDs)
	if emailIdentity == "" {
		emailIdentity = logicalID
	}

	if err := rc.backends.SES.Backend.VerifyEmailIdentity(emailIdentity); err != nil {
		return "", fmt.Errorf("create SES email identity %s: %w", emailIdentity, err)
	}

	return emailIdentity, nil
}

func (rc *ResourceCreator) deleteSESEmailIdentity(emailIdentity string) error {
	if rc.backends.SES == nil {
		return nil
	}

	return rc.backends.SES.Backend.DeleteIdentity(emailIdentity)
}

// ---- ACM ----

func (rc *ResourceCreator) createACMCertificate(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ACM == nil {
		return logicalID + "-stub", nil
	}

	domainName := strProp(props, "DomainName", params, physicalIDs)
	if domainName == "" {
		domainName = logicalID + ".example.com"
	}

	validationMethod := strProp(props, "ValidationMethod", params, physicalIDs)

	var sans []string
	if list, ok := props["SubjectAlternativeNames"].([]any); ok {
		for _, v := range list {
			if s := resolve(v, params, physicalIDs); s != "" {
				sans = append(sans, s)
			}
		}
	}

	cert, err := rc.backends.ACM.Backend.RequestCertificate(domainName, "AMAZON_ISSUED", validationMethod, sans)
	if err != nil {
		return "", fmt.Errorf("create ACM certificate for %s: %w", domainName, err)
	}

	return cert.ARN, nil
}

func (rc *ResourceCreator) deleteACMCertificate(arn string) error {
	if rc.backends.ACM == nil {
		return nil
	}

	return rc.backends.ACM.Backend.DeleteCertificate(arn)
}

// ---- Cognito ----

func (rc *ResourceCreator) createCognitoUserPool(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CognitoIDP == nil {
		return logicalID + "-stub", nil
	}

	poolName := strProp(props, "PoolName", params, physicalIDs)
	if poolName == "" {
		poolName = logicalID
	}

	pool, err := rc.backends.CognitoIDP.Backend.CreateUserPool(poolName)
	if err != nil {
		return "", fmt.Errorf("create Cognito user pool %s: %w", poolName, err)
	}

	return pool.ID, nil
}

func (rc *ResourceCreator) deleteCognitoUserPool(poolID string) error {
	if rc.backends.CognitoIDP == nil {
		return nil
	}

	return rc.backends.CognitoIDP.Backend.DeleteUserPool(poolID)
}

func (rc *ResourceCreator) createCognitoUserPoolClient(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CognitoIDP == nil {
		return logicalID + "-stub", nil
	}

	userPoolID := strProp(props, "UserPoolId", params, physicalIDs)
	clientName := strProp(props, "ClientName", params, physicalIDs)
	if clientName == "" {
		clientName = logicalID
	}

	client, err := rc.backends.CognitoIDP.Backend.CreateUserPoolClient(userPoolID, clientName)
	if err != nil {
		return "", fmt.Errorf("create Cognito user pool client %s: %w", clientName, err)
	}

	return client.ClientID, nil
}

func (rc *ResourceCreator) deleteCognitoUserPoolClient(clientID string) error {
	if rc.backends.CognitoIDP == nil {
		return nil
	}

	// physicalID for CognitoUserPoolClient is the clientID, which is enough to delete.
	// We use empty string for userPoolID since our implementation doesn't strictly need it.
	return rc.backends.CognitoIDP.Backend.DeleteUserPoolClient("", clientID)
}

// ---- EC2 NatGateway and EIP ----

func (rc *ResourceCreator) createEC2NatGateway(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	subnetID := strProp(props, "SubnetId", params, physicalIDs)
	allocationID := strProp(props, "AllocationId", params, physicalIDs)

	ngw, err := rc.backends.EC2.Backend.CreateNatGateway(subnetID, allocationID)
	if err != nil {
		return "", fmt.Errorf("create EC2 NAT gateway: %w", err)
	}

	return ngw.ID, nil
}

func (rc *ResourceCreator) deleteEC2NatGateway(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteNatGateway(id)
}

func (rc *ResourceCreator) createEC2EIP(_ string) (string, error) {
	if rc.backends.EC2 == nil {
		return "eipalloc-" + uuid.New().String()[:eipAllocIDSuffixLen], nil
	}

	addr, err := rc.backends.EC2.Backend.AllocateAddress()
	if err != nil {
		return "", fmt.Errorf("allocate EC2 EIP: %w", err)
	}

	return addr.AllocationID, nil
}

func (rc *ResourceCreator) deleteEC2EIP(allocationID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.ReleaseAddress(allocationID)
}
