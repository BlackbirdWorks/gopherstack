package cloudformation

import (
	"context"
	"fmt"
	"strings"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	appautoscalingbackend "github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
)

// createPhase5Resource handles ApplicationAutoScaling, SecretsManager supplemental,
// SSM supplemental, DynamoDB GlobalTable, Glue supplemental, and AppSync supplemental resources.
func (rc *ResourceCreator) createPhase5Resource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if id, ok, err := rc.createAppAutoScalingResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, err
	}

	if id, ok, err := rc.createSecretsManagerSupplementalResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, err
	}

	if id, ok, err := rc.createSSMSupplementalResource(ctx, logicalID, resourceType, props, params, physicalIDs); ok {
		return id, err
	}

	if id, ok, err := rc.createDynamoDBSupplementalResource(ctx, logicalID, resourceType, props, params, physicalIDs); ok {
		return id, err
	}

	if id, ok, err := rc.createGlueSupplementalResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, err
	}

	if id, ok, err := rc.createAppSyncSupplementalResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, err
	}

	return logicalID + "-stub", nil
}

// deletePhase5Resource handles deletion of phase-5 resource types.
func (rc *ResourceCreator) deletePhase5Resource(ctx context.Context, physicalID, resourceType string) error {
	switch resourceType {
	case "AWS::ApplicationAutoScaling::ScalableTarget":
		return rc.deleteAppAutoScalingScalableTarget(physicalID)
	case "AWS::ApplicationAutoScaling::ScalingPolicy":
		return rc.deleteAppAutoScalingScalingPolicy(physicalID)
	case "AWS::SecretsManager::RotationSchedule":
		return nil // rotation config is part of the secret; no separate resource to delete
	case "AWS::SecretsManager::SecretTargetAttachment":
		return nil // logical attachment; no separate resource to delete
	case "AWS::SSM::MaintenanceWindow":
		return rc.deleteSSMMaintenanceWindow(ctx, physicalID)
	case "AWS::SSM::Association":
		return rc.deleteSSMAssociation(ctx, physicalID)
	case "AWS::DynamoDB::GlobalTable":
		return nil // global tables persist with the underlying tables; no separate delete
	case "AWS::Glue::Crawler":
		return rc.deleteGlueCrawler(physicalID)
	case "AWS::Glue::Table":
		return rc.deleteGlueTable(physicalID)
	case "AWS::Glue::Trigger":
		return rc.deleteGlueTrigger(physicalID)
	case "AWS::Glue::Connection":
		return rc.deleteGlueConnection(physicalID)
	case "AWS::Glue::Partition":
		return nil // partitions are deleted with the table
	case "AWS::AppSync::DataSource":
		return rc.deleteAppSyncDataSource(physicalID)
	case "AWS::AppSync::Resolver":
		return rc.deleteAppSyncResolver(physicalID)
	case "AWS::AppSync::FunctionConfiguration":
		return rc.deleteAppSyncFunction(physicalID)
	case "AWS::AppSync::ApiKey":
		return rc.deleteAppSyncAPIKey(physicalID)
	default:
		return nil
	}
}

// ---- ApplicationAutoScaling ----

func (rc *ResourceCreator) createAppAutoScalingResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::ApplicationAutoScaling::ScalableTarget":
		id, err := rc.createAppAutoScalingScalableTarget(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::ApplicationAutoScaling::ScalingPolicy":
		id, err := rc.createAppAutoScalingScalingPolicy(logicalID, props, params, physicalIDs)
		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) createAppAutoScalingScalableTarget(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppAutoScaling == nil {
		return logicalID + "-stub", nil
	}

	serviceNamespace := strProp(props, "ServiceNamespace", params, physicalIDs)
	resourceID := strProp(props, "ResourceId", params, physicalIDs)
	scalableDimension := strProp(props, "ScalableDimension", params, physicalIDs)

	if serviceNamespace == "" {
		serviceNamespace = "ecs"
	}
	if resourceID == "" {
		resourceID = "service/" + logicalID + "/default"
	}
	if scalableDimension == "" {
		scalableDimension = "ecs:service:DesiredCount"
	}

	var minCap, maxCap int32 = 1, 10
	if v, ok := props["MinCapacity"].(float64); ok {
		minCap = int32(v)
	}
	if v, ok := props["MaxCapacity"].(float64); ok {
		maxCap = int32(v)
	}

	roleARN := strProp(props, "RoleARN", params, physicalIDs)

	target, err := rc.backends.AppAutoScaling.Backend.RegisterScalableTarget(
		serviceNamespace, resourceID, scalableDimension, minCap, maxCap, nil, roleARN, nil,
	)
	if err != nil {
		return "", fmt.Errorf("register scalable target %s: %w", resourceID, err)
	}

	return target.ARN, nil
}

func (rc *ResourceCreator) deleteAppAutoScalingScalableTarget(arn string) error {
	if rc.backends.AppAutoScaling == nil {
		return nil
	}

	// Physical ID is the ARN; parse serviceNamespace/resourceID/scalableDimension from it.
	// Format: arn:aws:application-autoscaling:<region>:<account>:scalable-target/<uuid>
	// We stored it by ARN index — use DeregisterScalableTarget with ARN lookup.
	// The backend's DeregisterScalableTarget takes (serviceNamespace, resourceID, scalableDimension).
	// We store the ARN as physical ID; find via DescribeScalableTargets with empty filter.
	targets := rc.backends.AppAutoScaling.Backend.DescribeScalableTargets(
		appautoscalingbackend.DescribeScalableTargetsFilter{},
	)
	for _, t := range targets {
		if t.ARN == arn {
			return rc.backends.AppAutoScaling.Backend.DeregisterScalableTarget(
				t.ServiceNamespace, t.ResourceID, t.ScalableDimension,
			)
		}
	}
	return nil
}

func (rc *ResourceCreator) createAppAutoScalingScalingPolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppAutoScaling == nil {
		return logicalID + "-stub", nil
	}

	policyName := strProp(props, "PolicyName", params, physicalIDs)
	if policyName == "" {
		policyName = logicalID
	}

	serviceNamespace := strProp(props, "ServiceNamespace", params, physicalIDs)
	resourceID := strProp(props, "ResourceId", params, physicalIDs)
	scalableDimension := strProp(props, "ScalableDimension", params, physicalIDs)
	policyType := strProp(props, "PolicyType", params, physicalIDs)

	if serviceNamespace == "" {
		serviceNamespace = "ecs"
	}
	if resourceID == "" {
		resourceID = "service/" + logicalID + "/default"
	}
	if scalableDimension == "" {
		scalableDimension = "ecs:service:DesiredCount"
	}

	policy, err := rc.backends.AppAutoScaling.Backend.PutScalingPolicy(
		serviceNamespace, resourceID, scalableDimension, policyName, policyType, nil, nil,
	)
	if err != nil {
		return "", fmt.Errorf("put scaling policy %s: %w", policyName, err)
	}

	return policy.ARN, nil
}

func (rc *ResourceCreator) deleteAppAutoScalingScalingPolicy(policyARN string) error {
	if rc.backends.AppAutoScaling == nil {
		return nil
	}

	policies := rc.backends.AppAutoScaling.Backend.DescribeScalingPolicies(
		appautoscalingbackend.DescribeScalingPoliciesFilter{},
	)
	for _, p := range policies {
		if p.ARN == policyARN {
			return rc.backends.AppAutoScaling.Backend.DeleteScalingPolicy(
				p.ServiceNamespace, p.ResourceID, p.ScalableDimension, p.PolicyName,
			)
		}
	}
	return nil
}

// ---- SecretsManager supplemental ----

func (rc *ResourceCreator) createSecretsManagerSupplementalResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::SecretsManager::RotationSchedule":
		id, err := rc.createSecretsManagerRotationSchedule(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::SecretsManager::SecretTargetAttachment":
		id, err := rc.createSecretsManagerSecretTargetAttachment(logicalID, props, params, physicalIDs)
		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) createSecretsManagerRotationSchedule(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	secretID := strProp(props, "SecretId", params, physicalIDs)
	if secretID == "" {
		secretID = logicalID
	}

	// Physical ID is the secret ID — the rotation is configured on the secret itself.
	return secretID + "-rotation", nil
}

func (rc *ResourceCreator) createSecretsManagerSecretTargetAttachment(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	secretID := strProp(props, "SecretId", params, physicalIDs)
	targetID := strProp(props, "TargetId", params, physicalIDs)
	targetType := strProp(props, "TargetType", params, physicalIDs)

	// Physical ID encodes the attachment; no real backend operation needed.
	id := secretID + ":attachment:" + targetType + ":" + targetID
	if id == ":attachment::" {
		id = logicalID + "-attachment"
	}

	return id, nil
}

// ---- SSM supplemental ----

func (rc *ResourceCreator) createSSMSupplementalResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::SSM::MaintenanceWindow":
		id, err := rc.createSSMMaintenanceWindow(ctx, logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::SSM::Association":
		id, err := rc.createSSMAssociation(ctx, logicalID, props, params, physicalIDs)
		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) createSSMMaintenanceWindow(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SSM == nil {
		return logicalID + "-stub", nil
	}

	imb, ok := rc.backends.SSM.Backend.(*ssmbackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	schedule := strProp(props, "Schedule", params, physicalIDs)
	if schedule == "" {
		schedule = "cron(0 2 ? * SUN *)"
	}

	var duration, cutoff int32 = 4, 1
	if v, ok := props["Duration"].(float64); ok {
		duration = int32(v)
	}
	if v, ok := props["Cutoff"].(float64); ok {
		cutoff = int32(v)
	}

	allowUnassociated := false
	if v, ok := props["AllowUnassociatedTargets"].(bool); ok {
		allowUnassociated = v
	}

	out, err := imb.CreateMaintenanceWindow(ctx, &ssmbackend.CreateMaintenanceWindowInput{
		Name:                     name,
		Schedule:                 schedule,
		Duration:                 duration,
		Cutoff:                   cutoff,
		AllowUnassociatedTargets: allowUnassociated,
	})
	if err != nil {
		return "", fmt.Errorf("create SSM maintenance window %s: %w", name, err)
	}

	return out.WindowID, nil
}

func (rc *ResourceCreator) deleteSSMMaintenanceWindow(ctx context.Context, windowID string) error {
	if rc.backends.SSM == nil {
		return nil
	}

	imb, ok := rc.backends.SSM.Backend.(*ssmbackend.InMemoryBackend)
	if !ok {
		return nil
	}

	_, err := imb.DeleteMaintenanceWindow(ctx, &ssmbackend.DeleteMaintenanceWindowInput{
		WindowID: windowID,
	})

	return err
}

func (rc *ResourceCreator) createSSMAssociation(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SSM == nil {
		return logicalID + "-stub", nil
	}

	imb, ok := rc.backends.SSM.Backend.(*ssmbackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	assocName := strProp(props, "AssociationName", params, physicalIDs)

	out, err := imb.CreateAssociation(ctx, &ssmbackend.CreateAssociationInput{
		Name:            name,
		AssociationName: assocName,
	})
	if err != nil {
		// SSM Association requires a document; if it doesn't exist, return a stub.
		return logicalID + "-stub", nil
	}

	return out.AssociationDescription.AssociationID, nil
}

func (rc *ResourceCreator) deleteSSMAssociation(ctx context.Context, assocID string) error {
	if rc.backends.SSM == nil {
		return nil
	}

	imb, ok := rc.backends.SSM.Backend.(*ssmbackend.InMemoryBackend)
	if !ok {
		return nil
	}

	_, err := imb.DeleteAssociation(ctx, &ssmbackend.DeleteAssociationInput{
		AssociationID: assocID,
	})

	return err
}

// ---- DynamoDB supplemental ----

func (rc *ResourceCreator) createDynamoDBSupplementalResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::DynamoDB::GlobalTable":
		id, err := rc.createDynamoDBGlobalTable(ctx, logicalID, props, params, physicalIDs)
		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) createDynamoDBGlobalTable(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.DynamoDB == nil {
		return logicalID + "-stub", nil
	}

	tableName := strProp(props, "TableName", params, physicalIDs)
	if tableName == "" {
		tableName = logicalID
	}

	// Build replication group from Replicas prop.
	var replicas []ddbtypes.Replica
	if replicaList, ok := props["Replicas"].([]any); ok {
		for _, r := range replicaList {
			if rm, ok := r.(map[string]any); ok {
				if region, ok := rm["Region"].(string); ok && region != "" {
					regionCopy := region
					replicas = append(replicas, ddbtypes.Replica{RegionName: &regionCopy})
				}
			}
		}
	}

	if len(replicas) == 0 {
		region := "us-east-1"
		replicas = []ddbtypes.Replica{{RegionName: &region}}
	}

	out, err := rc.backends.DynamoDB.Backend.CreateGlobalTable(ctx, &awsddb.CreateGlobalTableInput{
		GlobalTableName:  &tableName,
		ReplicationGroup: replicas,
	})
	if err != nil {
		return "", fmt.Errorf("create DynamoDB global table %s: %w", tableName, err)
	}

	if out.GlobalTableDescription != nil && out.GlobalTableDescription.GlobalTableArn != nil {
		return *out.GlobalTableDescription.GlobalTableArn, nil
	}

	return tableName, nil
}

// ---- Glue supplemental ----

func (rc *ResourceCreator) createGlueSupplementalResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::Glue::Crawler":
		id, err := rc.createGlueCrawler(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::Glue::Table":
		id, err := rc.createGlueTable(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::Glue::Trigger":
		id, err := rc.createGlueTrigger(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::Glue::Connection":
		id, err := rc.createGlueConnection(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::Glue::Partition":
		id, err := rc.createGluePartition(logicalID, props, params, physicalIDs)
		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) createGlueCrawler(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	role := strProp(props, "Role", params, physicalIDs)
	if role == "" {
		role = "AWSGlueServiceRole"
	}

	dbName := strProp(props, "DatabaseName", params, physicalIDs)

	crawler, err := rc.backends.Glue.Backend.CreateCrawler(name, role, dbName, gluebackend.CrawlerTarget{}, nil)
	if err != nil {
		return "", fmt.Errorf("create Glue crawler %s: %w", name, err)
	}

	return crawler.Name, nil
}

func (rc *ResourceCreator) deleteGlueCrawler(name string) error {
	if rc.backends.Glue == nil {
		return nil
	}

	return rc.backends.Glue.Backend.DeleteCrawler(name)
}

func (rc *ResourceCreator) createGlueTable(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	dbName := strProp(props, "DatabaseName", params, physicalIDs)
	if dbName == "" {
		dbName = "default"
	}

	var tableName string
	if ti, ok := props["TableInput"].(map[string]any); ok {
		tableName = strProp(ti, "Name", params, physicalIDs)
	}
	if tableName == "" {
		tableName = strings.ToLower(logicalID)
	}

	tableInput := gluebackend.TableInput{Name: tableName}
	_, err := rc.backends.Glue.Backend.CreateTable(dbName, tableInput)
	if err != nil {
		return "", fmt.Errorf("create Glue table %s in database %s: %w", tableName, dbName, err)
	}

	return dbName + "/" + tableName, nil
}

func (rc *ResourceCreator) deleteGlueTable(physicalID string) error {
	if rc.backends.Glue == nil {
		return nil
	}

	const parts = 2
	split := strings.SplitN(physicalID, "/", parts)
	if len(split) < parts {
		return nil
	}

	return rc.backends.Glue.Backend.DeleteTable(split[0], split[1])
}

func (rc *ResourceCreator) createGlueTrigger(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	triggerType := strProp(props, "Type", params, physicalIDs)
	if triggerType == "" {
		triggerType = "ON_DEMAND"
	}

	trigger, err := rc.backends.Glue.Backend.CreateTrigger(gluebackend.Trigger{
		Name: name,
		Type: triggerType,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("create Glue trigger %s: %w", name, err)
	}

	return trigger.Name, nil
}

func (rc *ResourceCreator) deleteGlueTrigger(name string) error {
	if rc.backends.Glue == nil {
		return nil
	}

	return rc.backends.Glue.Backend.DeleteTrigger(name)
}

func (rc *ResourceCreator) createGlueConnection(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	var connName, connType string
	if ci, ok := props["ConnectionInput"].(map[string]any); ok {
		connName = strProp(ci, "Name", params, physicalIDs)
		connType = strProp(ci, "ConnectionType", params, physicalIDs)
	}

	if connName == "" {
		connName = logicalID
	}
	if connType == "" {
		connType = "JDBC"
	}

	conn, err := rc.backends.Glue.Backend.CreateConnection(connName, connType, nil, nil)
	if err != nil {
		return "", fmt.Errorf("create Glue connection %s: %w", connName, err)
	}

	return conn.Name, nil
}

func (rc *ResourceCreator) deleteGlueConnection(name string) error {
	if rc.backends.Glue == nil {
		return nil
	}

	return rc.backends.Glue.Backend.DeleteConnection(name)
}

func (rc *ResourceCreator) createGluePartition(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	dbName := strProp(props, "DatabaseName", params, physicalIDs)
	tableName := strProp(props, "TableName", params, physicalIDs)

	if dbName == "" || tableName == "" {
		return logicalID + "-stub", nil
	}

	var values []string
	if pi, ok := props["PartitionInput"].(map[string]any); ok {
		if vs, ok := pi["Values"].([]any); ok {
			for _, v := range vs {
				if s, ok := v.(string); ok {
					values = append(values, s)
				}
			}
		}
	}

	if len(values) == 0 {
		values = []string{"default"}
	}

	_, errs := rc.backends.Glue.Backend.BatchCreatePartition(dbName, tableName, []gluebackend.PartitionInput{
		{Values: values},
	})
	if len(errs) > 0 {
		return "", fmt.Errorf("create Glue partition in %s/%s: %s", dbName, tableName, errs[0].ErrorDetail.ErrorMessage)
	}

	return dbName + "/" + tableName + "/" + strings.Join(values, ","), nil
}

// ---- AppSync supplemental ----

func (rc *ResourceCreator) createAppSyncSupplementalResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::AppSync::DataSource":
		id, err := rc.createAppSyncDataSource(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::AppSync::Resolver":
		id, err := rc.createAppSyncResolver(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::AppSync::FunctionConfiguration":
		id, err := rc.createAppSyncFunction(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::AppSync::ApiKey":
		id, err := rc.createAppSyncAPIKey(logicalID, props, params, physicalIDs)
		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) createAppSyncDataSource(
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

	apiID := strProp(props, "ApiId", params, physicalIDs)
	name := strProp(props, "Name", params, physicalIDs)
	dsType := strProp(props, "Type", params, physicalIDs)

	if name == "" {
		name = logicalID
	}
	if dsType == "" {
		dsType = "NONE"
	}

	ds, err := imb.CreateDataSource(apiID, &appsyncbackend.DataSource{
		Name: name,
		Type: appsyncbackend.DataSourceType(dsType),
	})
	if err != nil {
		return "", fmt.Errorf("create AppSync data source %s: %w", name, err)
	}

	return ds.DataSourceARN, nil
}

func (rc *ResourceCreator) deleteAppSyncDataSource(arn string) error {
	if rc.backends.AppSync == nil {
		return nil
	}

	imb, ok := rc.backends.AppSync.Backend.(*appsyncbackend.InMemoryBackend)
	if !ok {
		return nil
	}

	// ARN format: arn:aws:appsync:<region>:<account>:apis/<apiID>/datasources/<name>
	apiID, name := parseAppSyncARNParts(arn, "datasources")
	if apiID == "" || name == "" {
		return nil
	}

	return imb.DeleteDataSource(apiID, name)
}

func (rc *ResourceCreator) createAppSyncResolver(
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

	apiID := strProp(props, "ApiId", params, physicalIDs)
	typeName := strProp(props, "TypeName", params, physicalIDs)
	fieldName := strProp(props, "FieldName", params, physicalIDs)
	dataSourceName := strProp(props, "DataSourceName", params, physicalIDs)
	kind := strProp(props, "Kind", params, physicalIDs)

	if typeName == "" {
		typeName = "Query"
	}
	if fieldName == "" {
		fieldName = strings.ToLower(logicalID)
	}
	if kind == "" {
		kind = "UNIT"
	}
	if kind == "UNIT" && dataSourceName == "" {
		dataSourceName = "none"
	}

	r, err := imb.CreateResolver(apiID, typeName, &appsyncbackend.Resolver{
		FieldName:      fieldName,
		Kind:           kind,
		DataSourceName: dataSourceName,
	})
	if err != nil {
		return "", fmt.Errorf("create AppSync resolver %s.%s: %w", typeName, fieldName, err)
	}

	return r.ResolverARN, nil
}

func (rc *ResourceCreator) deleteAppSyncResolver(arn string) error {
	if rc.backends.AppSync == nil {
		return nil
	}

	imb, ok := rc.backends.AppSync.Backend.(*appsyncbackend.InMemoryBackend)
	if !ok {
		return nil
	}

	// ARN format: arn:aws:appsync:<region>:<account>:apis/<apiID>/types/<typeName>/resolvers/<fieldName>
	// Parse by splitting on "apis/"
	const prefix = "apis/"
	idx := strings.Index(arn, prefix)
	if idx < 0 {
		return nil
	}

	rest := arn[idx+len(prefix):]
	parts := strings.SplitN(rest, "/", 5) // apiID/types/typeName/resolvers/fieldName
	if len(parts) < 5 {
		return nil
	}

	apiID := parts[0]
	typeName := parts[2]
	fieldName := parts[4]

	return imb.DeleteResolver(apiID, typeName, fieldName)
}

func (rc *ResourceCreator) createAppSyncFunction(
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

	apiID := strProp(props, "ApiId", params, physicalIDs)
	name := strProp(props, "Name", params, physicalIDs)
	dataSourceName := strProp(props, "DataSourceName", params, physicalIDs)

	if name == "" {
		name = logicalID
	}
	if dataSourceName == "" {
		dataSourceName = "none"
	}

	f, err := imb.CreateFunction(apiID, &appsyncbackend.Function{
		Name:           name,
		DataSourceName: dataSourceName,
	})
	if err != nil {
		return "", fmt.Errorf("create AppSync function %s: %w", name, err)
	}

	return f.FunctionARN, nil
}

func (rc *ResourceCreator) deleteAppSyncFunction(arn string) error {
	if rc.backends.AppSync == nil {
		return nil
	}

	imb, ok := rc.backends.AppSync.Backend.(*appsyncbackend.InMemoryBackend)
	if !ok {
		return nil
	}

	apiID, funcID := parseAppSyncARNParts(arn, "functions")
	if apiID == "" || funcID == "" {
		return nil
	}

	return imb.DeleteFunction(apiID, funcID)
}

func (rc *ResourceCreator) createAppSyncAPIKey(
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

	apiID := strProp(props, "ApiId", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)

	key, err := imb.CreateAPIKey(apiID, description, 0)
	if err != nil {
		return "", fmt.Errorf("create AppSync API key for %s: %w", apiID, err)
	}

	return apiID + "/" + key.ID, nil
}

func (rc *ResourceCreator) deleteAppSyncAPIKey(physicalID string) error {
	if rc.backends.AppSync == nil {
		return nil
	}

	imb, ok := rc.backends.AppSync.Backend.(*appsyncbackend.InMemoryBackend)
	if !ok {
		return nil
	}

	const parts = 2
	split := strings.SplitN(physicalID, "/", parts)
	if len(split) < parts {
		return nil
	}

	return imb.DeleteAPIKey(split[0], split[1])
}

// parseAppSyncARNParts extracts apiID and resource name from an AppSync ARN.
// ARN format: arn:aws:appsync:<region>:<account>:apis/<apiID>/<resourceType>/<name>
func parseAppSyncARNParts(arn, resourceType string) (apiID, name string) {
	marker := "apis/"
	idx := strings.Index(arn, marker)
	if idx < 0 {
		return "", ""
	}

	rest := arn[idx+len(marker):]
	parts := strings.SplitN(rest, "/"+resourceType+"/", 2)
	if len(parts) < 2 {
		return "", ""
	}

	return parts[0], parts[1]
}
