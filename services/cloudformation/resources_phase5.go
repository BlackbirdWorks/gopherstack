package cloudformation

import (
	"context"
<<<<<<< HEAD
	"errors"
	"fmt"
	"strings"

	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	appautoscalingbackend "github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
)

const defaultGlueDB = "default"

var errGluePartition = errors.New("glue partition create failed")

// createPhase5Resource handles ApplicationAutoScaling, SecretsManager supplemental,
// SSM supplemental, DynamoDB GlobalTable, Glue supplemental, and AppSync supplemental resources.
=======
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
)

const (
	resTypeLogsLogStream         = "AWS::Logs::LogStream"
	resTypeLogsMetricFilter      = "AWS::Logs::MetricFilter"
	resTypeLogsSubscriptionFltr  = "AWS::Logs::SubscriptionFilter"
	resTypeEC2Volume             = "AWS::EC2::Volume"
	resTypeEC2NetworkInterface   = "AWS::EC2::NetworkInterface"
	resTypeEventsConnection      = "AWS::Events::Connection"
	resTypeStepFunctionsActivity = "AWS::StepFunctions::Activity"
	resTypeKMSAlias              = "AWS::KMS::Alias"
)

// createPhase5Resource handles phase-5 resource types added for §K CloudFormation
// resource-type coverage. It returns handled=false when resourceType is not a phase-5 type
// so the caller can fall through to the remaining dispatch chain.
>>>>>>> ef905acf (feat(cloudformation): §K pass-1 — 22 new CFN resource types)
func (rc *ResourceCreator) createPhase5Resource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
<<<<<<< HEAD
) (string, error) {
	if id, ok, err := rc.createAppAutoScalingResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, err
	}

	if id, ok := rc.createSecretsManagerSupplementalResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, nil
	}

	if id, ok, err := rc.createSSMSupplementalResource(ctx, logicalID, resourceType, props, params, physicalIDs); ok {
		return id, err
	}

	if id, ok, err := rc.createDynamoDBSupplementalResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
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
	case "AWS::SecretsManager::RotationSchedule",
		"AWS::SecretsManager::SecretTargetAttachment",
		"AWS::DynamoDB::GlobalTable",
		"AWS::Glue::Partition":
		// config-only or logical resources; nothing to delete
		return nil
	case "AWS::SSM::MaintenanceWindow":
		return rc.deleteSSMMaintenanceWindow(ctx, physicalID)
	case "AWS::SSM::Association":
		return rc.deleteSSMAssociation(ctx, physicalID)
	case "AWS::Glue::Crawler":
		return rc.deleteGlueCrawler(physicalID)
	case "AWS::Glue::Table":
		return rc.deleteGlueTable(physicalID)
	case "AWS::Glue::Trigger":
		return rc.deleteGlueTrigger(physicalID)
	case "AWS::Glue::Connection":
		return rc.deleteGlueConnection(physicalID)
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
=======
) (string, bool, error) {
	if physID, handled, err := rc.createPhase5LogsResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); handled {
		return physID, true, err
	}

	if physID, handled, err := rc.createPhase5NetworkResource(
		logicalID, resourceType, props, params, physicalIDs,
	); handled {
		return physID, true, err
	}

	return rc.createPhase5PlatformResource(ctx, logicalID, resourceType, props, params, physicalIDs)
}

// deletePhase5Resource handles deletion for phase-5 resource types.
func (rc *ResourceCreator) deletePhase5Resource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	if handled, err := rc.deletePhase5LogsResource(ctx, resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deletePhase5NetworkResource(resourceType, physicalID); handled {
		return true, err
	}

	return rc.deletePhase5PlatformResource(ctx, resourceType, physicalID)
}

// ---- CloudWatch Logs (LogStream, MetricFilter, SubscriptionFilter, ResourcePolicy, QueryDefinition) ----

func (rc *ResourceCreator) createPhase5LogsResource(
	ctx context.Context,
>>>>>>> ef905acf (feat(cloudformation): §K pass-1 — 22 new CFN resource types)
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
<<<<<<< HEAD
	case "AWS::ApplicationAutoScaling::ScalableTarget":
		id, err := rc.createAppAutoScalingScalableTarget(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApplicationAutoScaling::ScalingPolicy":
		id, err := rc.createAppAutoScalingScalingPolicy(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
=======
	case resTypeLogsLogStream:
		id, err := rc.createLogsLogStream(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeLogsMetricFilter:
		id, err := rc.createLogsMetricFilter(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeLogsSubscriptionFltr:
		id, err := rc.createLogsSubscriptionFilter(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::Logs::ResourcePolicy":
		id, err := rc.createLogsResourcePolicy(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::Logs::QueryDefinition":
		id, err := rc.createLogsQueryDefinition(logicalID, props, params, physicalIDs)

		return id, true, err
	default:

>>>>>>> ef905acf (feat(cloudformation): §K pass-1 — 22 new CFN resource types)
		return "", false, nil
	}
}

<<<<<<< HEAD
func (rc *ResourceCreator) createAppAutoScalingScalableTarget(
=======
func (rc *ResourceCreator) deletePhase5LogsResource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case resTypeLogsLogStream:

		return true, rc.deleteLogsLogStream(ctx, physicalID)
	case resTypeLogsMetricFilter:

		return true, rc.deleteLogsMetricFilter(ctx, physicalID)
	case resTypeLogsSubscriptionFltr:

		return true, rc.deleteLogsSubscriptionFilter(ctx, physicalID)
	case "AWS::Logs::ResourcePolicy":

		return true, rc.deleteLogsResourcePolicy(physicalID)
	case "AWS::Logs::QueryDefinition":

		return true, rc.deleteLogsQueryDefinition(physicalID)
	default:

		return false, nil
	}
}

// physID encodes "<logGroupName>|<childName>" so delete can address the parent group.
const logsPhysIDSep = "|"

func (rc *ResourceCreator) createLogsLogStream(
	ctx context.Context,
>>>>>>> ef905acf (feat(cloudformation): §K pass-1 — 22 new CFN resource types)
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
<<<<<<< HEAD
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
=======
	if rc.backends.CloudWatchLogs == nil {
		return logicalID + "-stub", nil
	}

	groupName := strProp(props, "LogGroupName", params, physicalIDs)
	streamName := strProp(props, "LogStreamName", params, physicalIDs)
	if streamName == "" {
		streamName = logicalID
	}

	if _, err := rc.backends.CloudWatchLogs.Backend.CreateLogStream(ctx, groupName, streamName); err != nil {
		return "", fmt.Errorf("create CloudWatch Logs log stream %s: %w", streamName, err)
	}

	return groupName + logsPhysIDSep + streamName, nil
}

func (rc *ResourceCreator) deleteLogsLogStream(ctx context.Context, physicalID string) error {
	if rc.backends.CloudWatchLogs == nil {
		return nil
	}

	groupName, streamName, ok := splitLogsPhysID(physicalID)
	if !ok {
		return nil
	}

	return rc.backends.CloudWatchLogs.Backend.DeleteLogStream(ctx, groupName, streamName)
}

func (rc *ResourceCreator) createLogsMetricFilter(
	ctx context.Context,
>>>>>>> ef905acf (feat(cloudformation): §K pass-1 — 22 new CFN resource types)
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
<<<<<<< HEAD
	if rc.backends.AppAutoScaling == nil {
=======
	if rc.backends.CloudWatchLogs == nil {
		return logicalID + "-stub", nil
	}

	groupName := strProp(props, "LogGroupName", params, physicalIDs)
	filterName := strProp(props, "FilterName", params, physicalIDs)
	if filterName == "" {
		filterName = logicalID
	}

	pattern := strProp(props, "FilterPattern", params, physicalIDs)
	transforms := parseMetricTransformations(props, params, physicalIDs)

	if err := rc.backends.CloudWatchLogs.Backend.PutMetricFilter(
		ctx, groupName, filterName, pattern, transforms,
	); err != nil {
		return "", fmt.Errorf("create CloudWatch Logs metric filter %s: %w", filterName, err)
	}

	return groupName + logsPhysIDSep + filterName, nil
}

func parseMetricTransformations(
	props map[string]any,
	params, physicalIDs map[string]string,
) []cwlogsbackend.MetricTransformation {
	rawList, ok := props["MetricTransformations"].([]any)
	if !ok || len(rawList) == 0 {
		// AWS requires at least one transformation; synthesize a minimal valid one.
		return []cwlogsbackend.MetricTransformation{
			{MetricName: "Events", MetricNamespace: "CFN", MetricValue: "1"},
		}
	}

	out := make([]cwlogsbackend.MetricTransformation, 0, len(rawList))
	for _, raw := range rawList {
		m, mOK := raw.(map[string]any)
		if !mOK {
			continue
		}
		out = append(out, cwlogsbackend.MetricTransformation{
			MetricName:      resolve(m["MetricName"], params, physicalIDs),
			MetricNamespace: resolve(m["MetricNamespace"], params, physicalIDs),
			MetricValue:     resolve(m["MetricValue"], params, physicalIDs),
		})
	}

	if len(out) == 0 {
		return []cwlogsbackend.MetricTransformation{
			{MetricName: "Events", MetricNamespace: "CFN", MetricValue: "1"},
		}
	}

	return out
}

func (rc *ResourceCreator) deleteLogsMetricFilter(ctx context.Context, physicalID string) error {
	if rc.backends.CloudWatchLogs == nil {
		return nil
	}

	groupName, filterName, ok := splitLogsPhysID(physicalID)
	if !ok {
		return nil
	}

	return rc.backends.CloudWatchLogs.Backend.DeleteMetricFilter(ctx, groupName, filterName)
}

func (rc *ResourceCreator) createLogsSubscriptionFilter(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatchLogs == nil {
		return logicalID + "-stub", nil
	}

	groupName := strProp(props, "LogGroupName", params, physicalIDs)
	filterName := strProp(props, "FilterName", params, physicalIDs)
	if filterName == "" {
		filterName = logicalID
	}

	pattern := strProp(props, "FilterPattern", params, physicalIDs)
	destinationArn := strProp(props, "DestinationArn", params, physicalIDs)
	roleArn := strProp(props, "RoleArn", params, physicalIDs)
	distribution := strProp(props, "Distribution", params, physicalIDs)

	if err := rc.backends.CloudWatchLogs.Backend.PutSubscriptionFilter(
		ctx, groupName, filterName, pattern, destinationArn, roleArn, distribution,
	); err != nil {
		return "", fmt.Errorf("create CloudWatch Logs subscription filter %s: %w", filterName, err)
	}

	return groupName + logsPhysIDSep + filterName, nil
}

func (rc *ResourceCreator) deleteLogsSubscriptionFilter(ctx context.Context, physicalID string) error {
	if rc.backends.CloudWatchLogs == nil {
		return nil
	}

	groupName, filterName, ok := splitLogsPhysID(physicalID)
	if !ok {
		return nil
	}

	return rc.backends.CloudWatchLogs.Backend.DeleteSubscriptionFilter(ctx, groupName, filterName)
}

func (rc *ResourceCreator) createLogsResourcePolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatchLogs == nil {
>>>>>>> ef905acf (feat(cloudformation): §K pass-1 — 22 new CFN resource types)
		return logicalID + "-stub", nil
	}

	policyName := strProp(props, "PolicyName", params, physicalIDs)
	if policyName == "" {
		policyName = logicalID
	}

<<<<<<< HEAD
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
=======
	policyDoc := strProp(props, "PolicyDocument", params, physicalIDs)

	mem, ok := rc.backends.CloudWatchLogs.Backend.(*cwlogsbackend.InMemoryBackend)
	if !ok {
		return policyName, nil
	}

	if _, err := mem.PutResourcePolicy(policyName, policyDoc); err != nil {
		return "", fmt.Errorf("create CloudWatch Logs resource policy %s: %w", policyName, err)
	}

	return policyName, nil
}

func (rc *ResourceCreator) deleteLogsResourcePolicy(policyName string) error {
	if rc.backends.CloudWatchLogs == nil {
		return nil
	}

	mem, ok := rc.backends.CloudWatchLogs.Backend.(*cwlogsbackend.InMemoryBackend)
	if !ok {
		return nil
	}

	return mem.DeleteResourcePolicy(policyName)
}

func (rc *ResourceCreator) createLogsQueryDefinition(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatchLogs == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	queryString := strProp(props, "QueryString", params, physicalIDs)
	groupNames := strSliceProp(props["LogGroupNames"], params, physicalIDs)

	id, err := rc.backends.CloudWatchLogs.Backend.PutQueryDefinition(name, queryString, "", groupNames)
	if err != nil {
		return "", fmt.Errorf("create CloudWatch Logs query definition %s: %w", name, err)
	}

	return id, nil
}

func (rc *ResourceCreator) deleteLogsQueryDefinition(id string) error {
	if rc.backends.CloudWatchLogs == nil {
		return nil
	}

	return rc.backends.CloudWatchLogs.Backend.DeleteQueryDefinition(id)
}

func splitLogsPhysID(physicalID string) (string, string, bool) {
	const parts = 2
	split := strings.SplitN(physicalID, logsPhysIDSep, parts)
	if len(split) < parts {
		return "", "", false
	}

	return split[0], split[1], true
}

// ---- EC2 (Volume, VolumeAttachment, NetworkInterface) ----

func (rc *ResourceCreator) createPhase5NetworkResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeEC2Volume:
		id, err := rc.createEC2Volume(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::EC2::VolumeAttachment":
		id, err := rc.createEC2VolumeAttachment(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2NetworkInterface:
		id, err := rc.createEC2NetworkInterface(logicalID, props, params, physicalIDs)

		return id, true, err
	default:

		return "", false, nil
	}
}

func (rc *ResourceCreator) deletePhase5NetworkResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeEC2Volume:

		return true, rc.deleteEC2Volume(physicalID)
	case "AWS::EC2::VolumeAttachment":

		return true, rc.deleteEC2VolumeAttachment(physicalID)
	case resTypeEC2NetworkInterface:

		return true, rc.deleteEC2NetworkInterface(physicalID)
	default:

		return false, nil
	}
}

const defaultVolumeSizeGiB = 8

func (rc *ResourceCreator) createEC2Volume(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	az := strProp(props, "AvailabilityZone", params, physicalIDs)
	volType := strProp(props, "VolumeType", params, physicalIDs)
	if volType == "" {
		volType = "gp2"
	}

	size := intProp(props, "Size")
	if size == 0 {
		size = defaultVolumeSizeGiB
	}

	vol, err := rc.backends.EC2.Backend.CreateVolume(az, volType, size)
	if err != nil {
		return "", fmt.Errorf("create EC2 volume: %w", err)
	}

	return vol.ID, nil
}

func (rc *ResourceCreator) deleteEC2Volume(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteVolume(id)
}

func (rc *ResourceCreator) createEC2VolumeAttachment(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	volumeID := strProp(props, "VolumeId", params, physicalIDs)
	instanceID := strProp(props, "InstanceId", params, physicalIDs)
	device := strProp(props, "Device", params, physicalIDs)
	if device == "" {
		device = "/dev/sdf"
	}

	if _, err := rc.backends.EC2.Backend.AttachVolume(volumeID, instanceID, device); err != nil {
		return "", fmt.Errorf("attach EC2 volume %s to %s: %w", volumeID, instanceID, err)
	}

	return volumeID, nil
}

func (rc *ResourceCreator) deleteEC2VolumeAttachment(volumeID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	_, err := rc.backends.EC2.Backend.DetachVolume(volumeID, true)

	return err
}

func (rc *ResourceCreator) createEC2NetworkInterface(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	subnetID := strProp(props, "SubnetId", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)

	eni, err := rc.backends.EC2.Backend.CreateNetworkInterface(subnetID, description)
	if err != nil {
		return "", fmt.Errorf("create EC2 network interface in %s: %w", subnetID, err)
	}

	return eni.ID, nil
}

func (rc *ResourceCreator) deleteEC2NetworkInterface(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteNetworkInterface(id)
}

// ---- Platform: APIGatewayV2, KMS, SNS, Events, StepFunctions, SSM, SecretsManager, CloudFront ----

func (rc *ResourceCreator) createPhase5PlatformResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if physID, handled, err := rc.createPhase5APIGatewayV2Resource(
		logicalID, resourceType, props, params, physicalIDs,
	); handled {
		return physID, true, err
	}

	if physID, handled, err := rc.createPhase5MessagingResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); handled {
		return physID, true, err
	}

	return rc.createPhase5ManagedResource(ctx, logicalID, resourceType, props, params, physicalIDs)
}

func (rc *ResourceCreator) deletePhase5PlatformResource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	if handled, err := rc.deletePhase5APIGatewayV2Resource(resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deletePhase5MessagingResource(ctx, resourceType, physicalID); handled {
		return true, err
	}

	return rc.deletePhase5ManagedResource(ctx, resourceType, physicalID)
}

// physID for apigwv2 children encodes "<apiID>|<childID>".
const apigwv2PhysIDSep = "|"

func (rc *ResourceCreator) createPhase5APIGatewayV2Resource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::ApiGatewayV2::Integration":
		id, err := rc.createAPIGatewayV2Integration(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApiGatewayV2::Route":
		id, err := rc.createAPIGatewayV2Route(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApiGatewayV2::Authorizer":
		id, err := rc.createAPIGatewayV2Authorizer(logicalID, props, params, physicalIDs)

		return id, true, err
	default:

		return "", false, nil
	}
}

func (rc *ResourceCreator) deletePhase5APIGatewayV2Resource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::ApiGatewayV2::Integration":

		return true, rc.deleteAPIGatewayV2Integration(physicalID)
	case "AWS::ApiGatewayV2::Route":

		return true, rc.deleteAPIGatewayV2Route(physicalID)
	case "AWS::ApiGatewayV2::Authorizer":

		return true, rc.deleteAPIGatewayV2Authorizer(physicalID)
	default:

		return false, nil
	}
}

func (rc *ResourceCreator) createAPIGatewayV2Integration(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGatewayV2 == nil {
		return logicalID + "-stub", nil
	}

	apiID := strProp(props, "ApiId", params, physicalIDs)
	integrationType := strProp(props, "IntegrationType", params, physicalIDs)
	if integrationType == "" {
		integrationType = "AWS_PROXY"
	}

	integ, err := rc.backends.APIGatewayV2.Backend.CreateIntegration(apiID, apigatewayv2backend.CreateIntegrationInput{
		IntegrationType:      integrationType,
		IntegrationURI:       strProp(props, "IntegrationUri", params, physicalIDs),
		IntegrationMethod:    strProp(props, "IntegrationMethod", params, physicalIDs),
		PayloadFormatVersion: strProp(props, "PayloadFormatVersion", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway V2 integration: %w", err)
	}

	return apiID + apigwv2PhysIDSep + integ.IntegrationID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayV2Integration(physicalID string) error {
	if rc.backends.APIGatewayV2 == nil {
		return nil
	}

	apiID, integID, ok := splitAPIGatewayV2PhysID(physicalID)
	if !ok {
		return nil
	}

	return rc.backends.APIGatewayV2.Backend.DeleteIntegration(apiID, integID)
}

func (rc *ResourceCreator) createAPIGatewayV2Route(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGatewayV2 == nil {
		return logicalID + "-stub", nil
	}

	apiID := strProp(props, "ApiId", params, physicalIDs)
	routeKey := strProp(props, "RouteKey", params, physicalIDs)
	if routeKey == "" {
		routeKey = "$default"
	}

	route, err := rc.backends.APIGatewayV2.Backend.CreateRoute(apiID, apigatewayv2backend.CreateRouteInput{
		RouteKey:          routeKey,
		Target:            strProp(props, "Target", params, physicalIDs),
		AuthorizationType: strProp(props, "AuthorizationType", params, physicalIDs),
		AuthorizerID:      strProp(props, "AuthorizerId", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway V2 route %s: %w", routeKey, err)
	}

	return apiID + apigwv2PhysIDSep + route.RouteID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayV2Route(physicalID string) error {
	if rc.backends.APIGatewayV2 == nil {
		return nil
	}

	apiID, routeID, ok := splitAPIGatewayV2PhysID(physicalID)
	if !ok {
		return nil
	}

	return rc.backends.APIGatewayV2.Backend.DeleteRoute(apiID, routeID)
}

func (rc *ResourceCreator) createAPIGatewayV2Authorizer(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGatewayV2 == nil {
		return logicalID + "-stub", nil
	}

	apiID := strProp(props, "ApiId", params, physicalIDs)
	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	authType := strProp(props, "AuthorizerType", params, physicalIDs)
	if authType == "" {
		authType = "REQUEST"
	}

	auth, err := rc.backends.APIGatewayV2.Backend.CreateAuthorizer(apiID, apigatewayv2backend.CreateAuthorizerInput{
		Name:           name,
		AuthorizerType: authType,
		AuthorizerURI:  strProp(props, "AuthorizerUri", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway V2 authorizer %s: %w", name, err)
	}

	return apiID + apigwv2PhysIDSep + auth.AuthorizerID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayV2Authorizer(physicalID string) error {
	if rc.backends.APIGatewayV2 == nil {
		return nil
	}

	apiID, authID, ok := splitAPIGatewayV2PhysID(physicalID)
	if !ok {
		return nil
	}

	return rc.backends.APIGatewayV2.Backend.DeleteAuthorizer(apiID, authID)
}

func splitAPIGatewayV2PhysID(physicalID string) (string, string, bool) {
	const parts = 2
	split := strings.SplitN(physicalID, apigwv2PhysIDSep, parts)
	if len(split) < parts {
		return "", "", false
	}

	return split[0], split[1], true
}

func (rc *ResourceCreator) createPhase5MessagingResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::SNS::TopicPolicy":
		id, err := rc.createSNSTopicPolicy(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEventsConnection:
		id, err := rc.createEventsConnection(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::Events::Archive":
		id, err := rc.createEventsArchive(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeStepFunctionsActivity:
		id, err := rc.createStepFunctionsActivity(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:

		return "", false, nil
	}
}

func (rc *ResourceCreator) deletePhase5MessagingResource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case "AWS::SNS::TopicPolicy":

		return true, nil // topic policy is an attribute on the topic; removed with the topic
	case resTypeEventsConnection:

		return true, rc.deleteEventsConnection(ctx, physicalID)
	case "AWS::Events::Archive":

		return true, rc.deleteEventsArchive(ctx, physicalID)
	case resTypeStepFunctionsActivity:

		return true, rc.deleteStepFunctionsActivity(physicalID)
	default:

		return false, nil
	}
}

func (rc *ResourceCreator) createSNSTopicPolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SNS == nil {
		return logicalID + "-stub", nil
	}

	policyDoc := strProp(props, "PolicyDocument", params, physicalIDs)
	topicArns := strSliceProp(props["Topics"], params, physicalIDs)

	for _, topicArn := range topicArns {
		if topicArn == "" {
			continue
		}
		if err := rc.backends.SNS.Backend.SetTopicAttributes(topicArn, "Policy", policyDoc); err != nil {
			return "", fmt.Errorf("set SNS topic policy on %s: %w", topicArn, err)
		}
	}

	return logicalID, nil
}

func (rc *ResourceCreator) createEventsConnection(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EventBridge == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	authType := strProp(props, "AuthorizationType", params, physicalIDs)
	if authType == "" {
		authType = "API_KEY"
	}

	conn, err := rc.backends.EventBridge.Backend.CreateConnection(ctx, ebbackend.CreateConnectionInput{
		Name:              name,
		AuthorizationType: authType,
		Description:       strProp(props, "Description", params, physicalIDs),
		AuthParameters:    defaultConnectionAuthParameters(authType),
	})
	if err != nil {
		return "", fmt.Errorf("create EventBridge connection %s: %w", name, err)
	}

	return conn.Name, nil
}

func defaultConnectionAuthParameters(authType string) *ebbackend.ConnectionAuthParameters {
	if authType == "API_KEY" {
		return &ebbackend.ConnectionAuthParameters{
			APIKeyAuthParameters: &ebbackend.ConnectionAPIKeyAuthParameters{
				APIKeyName:  "x-api-key",
				APIKeyValue: "cfn-managed",
			},
>>>>>>> ef905acf (feat(cloudformation): §K pass-1 — 22 new CFN resource types)
		}
	}

	return nil
}

<<<<<<< HEAD
// ---- SecretsManager supplemental ----

func (rc *ResourceCreator) createSecretsManagerSupplementalResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool) {
	switch resourceType {
	case "AWS::SecretsManager::RotationSchedule":
		id := rc.createSecretsManagerRotationSchedule(logicalID, props, params, physicalIDs)

		return id, true
	case "AWS::SecretsManager::SecretTargetAttachment":
		id := rc.createSecretsManagerSecretTargetAttachment(logicalID, props, params, physicalIDs)

		return id, true
	default:
		return "", false
	}
}

func (rc *ResourceCreator) createSecretsManagerRotationSchedule(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) string {
	secretID := strProp(props, "SecretId", params, physicalIDs)
	if secretID == "" {
		secretID = logicalID
	}

	// Physical ID is the secret ID — the rotation is configured on the secret itself.
	return secretID + "-rotation"
}

func (rc *ResourceCreator) createSecretsManagerSecretTargetAttachment(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) string {
	secretID := strProp(props, "SecretId", params, physicalIDs)
	targetID := strProp(props, "TargetId", params, physicalIDs)
	targetType := strProp(props, "TargetType", params, physicalIDs)

	// Physical ID encodes the attachment; no real backend operation needed.
	id := secretID + ":attachment:" + targetType + ":" + targetID
	if id == ":attachment::" {
		id = logicalID + "-attachment"
	}

	return id
}

// ---- SSM supplemental ----

func (rc *ResourceCreator) createSSMSupplementalResource(
=======
func (rc *ResourceCreator) deleteEventsConnection(ctx context.Context, name string) error {
	if rc.backends.EventBridge == nil {
		return nil
	}

	return rc.backends.EventBridge.Backend.DeleteConnection(ctx, name)
}

func (rc *ResourceCreator) createEventsArchive(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EventBridge == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ArchiveName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	arch, err := rc.backends.EventBridge.Backend.CreateArchive(ctx, ebbackend.CreateArchiveInput{
		ArchiveName:    name,
		EventSourceArn: strProp(props, "SourceArn", params, physicalIDs),
		Description:    strProp(props, "Description", params, physicalIDs),
		EventPattern:   strProp(props, "EventPattern", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create EventBridge archive %s: %w", name, err)
	}

	return arch.ArchiveName, nil
}

func (rc *ResourceCreator) deleteEventsArchive(ctx context.Context, name string) error {
	if rc.backends.EventBridge == nil {
		return nil
	}

	return rc.backends.EventBridge.Backend.DeleteArchive(ctx, name)
}

func (rc *ResourceCreator) createStepFunctionsActivity(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.StepFunctions == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	act, err := rc.backends.StepFunctions.Backend.CreateActivity(ctx, name)
	if err != nil {
		return "", fmt.Errorf("create Step Functions activity %s: %w", name, err)
	}

	return act.ActivityArn, nil
}

func (rc *ResourceCreator) deleteStepFunctionsActivity(activityArn string) error {
	if rc.backends.StepFunctions == nil {
		return nil
	}

	return rc.backends.StepFunctions.Backend.DeleteActivity(activityArn)
}

func (rc *ResourceCreator) createPhase5ManagedResource(
>>>>>>> ef905acf (feat(cloudformation): §K pass-1 — 22 new CFN resource types)
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
<<<<<<< HEAD
	case "AWS::SSM::MaintenanceWindow":
		id, err := rc.createSSMMaintenanceWindow(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::SSM::Association":
		id := rc.createSSMAssociation(ctx, logicalID, props, params, physicalIDs)

		return id, true, nil
	default:
=======
	case resTypeKMSAlias:
		id, err := rc.createKMSAlias(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::SSM::Document":
		id, err := rc.createSSMDocument(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::SecretsManager::ResourcePolicy":
		id, err := rc.createSecretsManagerResourcePolicy(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::CloudFront::Function":
		id, err := rc.createCloudFrontFunction(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::CloudFront::CachePolicy":
		id, err := rc.createCloudFrontCachePolicy(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::CloudFront::OriginAccessControl":
		id, err := rc.createCloudFrontOriginAccessControl(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::CloudFront::ResponseHeadersPolicy":
		id, err := rc.createCloudFrontResponseHeadersPolicy(logicalID, props, params, physicalIDs)

		return id, true, err
	default:

>>>>>>> ef905acf (feat(cloudformation): §K pass-1 — 22 new CFN resource types)
		return "", false, nil
	}
}

<<<<<<< HEAD
func (rc *ResourceCreator) createSSMMaintenanceWindow(
=======
func (rc *ResourceCreator) deletePhase5ManagedResource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case resTypeKMSAlias:

		return true, rc.deleteKMSAlias(physicalID)
	case "AWS::SSM::Document":

		return true, rc.deleteSSMDocument(ctx, physicalID)
	case "AWS::SecretsManager::ResourcePolicy":

		return true, rc.deleteSecretsManagerResourcePolicy(physicalID)
	case "AWS::CloudFront::Function":

		return true, rc.deleteCloudFrontFunction(physicalID)
	case "AWS::CloudFront::CachePolicy":

		return true, rc.deleteCloudFrontCachePolicy(physicalID)
	case "AWS::CloudFront::OriginAccessControl":

		return true, rc.deleteCloudFrontOriginAccessControl(physicalID)
	case "AWS::CloudFront::ResponseHeadersPolicy":

		return true, rc.deleteCloudFrontResponseHeadersPolicy(physicalID)
	default:

		return false, nil
	}
}

func (rc *ResourceCreator) createKMSAlias(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.KMS == nil {
		return logicalID + "-stub", nil
	}

	aliasName := strProp(props, "AliasName", params, physicalIDs)
	if aliasName == "" {
		aliasName = "alias/" + logicalID
	}
	if !strings.HasPrefix(aliasName, "alias/") {
		aliasName = "alias/" + aliasName
	}

	targetKeyID := strProp(props, "TargetKeyId", params, physicalIDs)

	if err := rc.backends.KMS.Backend.CreateAlias(&kmsbackend.CreateAliasInput{
		AliasName:   aliasName,
		TargetKeyID: targetKeyID,
	}); err != nil {
		return "", fmt.Errorf("create KMS alias %s: %w", aliasName, err)
	}

	return aliasName, nil
}

func (rc *ResourceCreator) deleteKMSAlias(aliasName string) error {
	if rc.backends.KMS == nil {
		return nil
	}

	return rc.backends.KMS.Backend.DeleteAlias(&kmsbackend.DeleteAliasInput{AliasName: aliasName})
}

func (rc *ResourceCreator) createSSMDocument(
>>>>>>> ef905acf (feat(cloudformation): §K pass-1 — 22 new CFN resource types)
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SSM == nil {
		return logicalID + "-stub", nil
	}

<<<<<<< HEAD
	imb, ok := rc.backends.SSM.Backend.(*ssmbackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

=======
>>>>>>> ef905acf (feat(cloudformation): §K pass-1 — 22 new CFN resource types)
	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

<<<<<<< HEAD
	schedule := strProp(props, "Schedule", params, physicalIDs)
	if schedule == "" {
		schedule = "cron(0 2 ? * SUN *)"
	}

	var duration, cutoff int32 = 4, 1
	if v, hasDuration := props["Duration"].(float64); hasDuration {
		duration = int32(v)
	}
	if v, hasCutoff := props["Cutoff"].(float64); hasCutoff {
		cutoff = int32(v)
	}

	allowUnassociated := false
	if v, hasAllow := props["AllowUnassociatedTargets"].(bool); hasAllow {
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
) string {
	if rc.backends.SSM == nil {
		return logicalID + "-stub"
	}

	imb, ok := rc.backends.SSM.Backend.(*ssmbackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub"
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	assocName := strProp(props, "AssociationName", params, physicalIDs)

	// SSM Association requires a document; if it doesn't exist, CreateAssociation errors.
	// Treat errors as a stub to avoid propagating document-not-found failures.
	out, _ := imb.CreateAssociation(ctx, &ssmbackend.CreateAssociationInput{
		Name:            name,
		AssociationName: assocName,
	})
	if out == nil || out.AssociationDescription.AssociationID == "" {
		return logicalID + "-stub"
	}

	return out.AssociationDescription.AssociationID
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
	if replicaList, hasReplicas := props["Replicas"].([]any); hasReplicas {
		for _, r := range replicaList {
			if rm, isMap := r.(map[string]any); isMap {
				if region, hasRegion := rm["Region"].(string); hasRegion && region != "" {
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
		dbName = defaultGlueDB
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
	if pi, hasPartition := props["PartitionInput"].(map[string]any); hasPartition {
		if vs, hasValues := pi["Values"].([]any); hasValues {
			for _, v := range vs {
				if s, isStr := v.(string); isStr {
					values = append(values, s)
				}
			}
		}
	}

	if len(values) == 0 {
		values = []string{defaultGlueDB}
	}

	_, errs := rc.backends.Glue.Backend.BatchCreatePartition(dbName, tableName, []gluebackend.PartitionInput{
		{Values: values},
	})
	if len(errs) > 0 {
		return "", fmt.Errorf("%w in %s/%s: %s", errGluePartition, dbName, tableName, errs[0].ErrorDetail.ErrorMessage)
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
	_, afterAPIs, hasAPIs := strings.Cut(arn, "apis/")
	if !hasAPIs {
		return nil
	}

	apiID, rest1, hasTypes := strings.Cut(afterAPIs, "/types/")
	if !hasTypes {
		return nil
	}

	typeName, fieldName, hasResolvers := strings.Cut(rest1, "/resolvers/")
	if !hasResolvers {
		return nil
	}

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
=======
	content := documentContent(props, params, physicalIDs)
	docType := strProp(props, "DocumentType", params, physicalIDs)
	if docType == "" {
		docType = "Command"
	}

	docFormat := strProp(props, "DocumentFormat", params, physicalIDs)

	out, err := rc.backends.SSM.Backend.CreateDocument(ctx, &ssmbackend.CreateDocumentInput{
		Name:           name,
		Content:        content,
		DocumentType:   docType,
		DocumentFormat: docFormat,
	})
	if err != nil {
		return "", fmt.Errorf("create SSM document %s: %w", name, err)
	}

	return out.DocumentDescription.Name, nil
}

func documentContent(props map[string]any, params, physicalIDs map[string]string) string {
	switch c := props["Content"].(type) {
	case string:
		return c
	case map[string]any:
		if b, err := marshalJSON(c); err == nil {
			return string(b)
		}
	}

	return strProp(props, "Content", params, physicalIDs)
}

func (rc *ResourceCreator) deleteSSMDocument(ctx context.Context, name string) error {
	if rc.backends.SSM == nil {
		return nil
	}

	_, err := rc.backends.SSM.Backend.DeleteDocument(ctx, &ssmbackend.DeleteDocumentInput{Name: name})

	return err
}

func (rc *ResourceCreator) createSecretsManagerResourcePolicy(
>>>>>>> ef905acf (feat(cloudformation): §K pass-1 — 22 new CFN resource types)
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
<<<<<<< HEAD
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
=======
	if rc.backends.SecretsManager == nil {
		return logicalID + "-stub", nil
	}

	secretID := strProp(props, "SecretId", params, physicalIDs)
	policy := strProp(props, "ResourcePolicy", params, physicalIDs)

	if _, err := rc.backends.SecretsManager.Backend.PutResourcePolicy(&secretsmanagerbackend.PutResourcePolicyInput{
		SecretID:       secretID,
		ResourcePolicy: policy,
	}); err != nil {
		return "", fmt.Errorf("create Secrets Manager resource policy for %s: %w", secretID, err)
	}

	return secretID, nil
}

func (rc *ResourceCreator) deleteSecretsManagerResourcePolicy(secretID string) error {
	if rc.backends.SecretsManager == nil {
		return nil
	}

	_, err := rc.backends.SecretsManager.Backend.DeleteResourcePolicy(&secretsmanagerbackend.DeleteResourcePolicyInput{
		SecretID: secretID,
	})

	return err
}

func (rc *ResourceCreator) createCloudFrontFunction(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	code := strProp(props, "FunctionCode", params, physicalIDs)
	if code == "" {
		code = "function handler(event) { return event.request; }"
	}

	runtime := functionRuntime(props, params, physicalIDs)

	fn, err := rc.backends.CloudFront.Backend.CreateFunction(name, "", runtime, code)
	if err != nil {
		return "", fmt.Errorf("create CloudFront function %s: %w", name, err)
	}

	return fn.Name, nil
}

func functionRuntime(props map[string]any, params, physicalIDs map[string]string) string {
	if cfg, ok := props["FunctionConfig"].(map[string]any); ok {
		if rt := resolve(cfg["Runtime"], params, physicalIDs); rt != "" {
			return rt
		}
	}
	if rt := strProp(props, "Runtime", params, physicalIDs); rt != "" {
		return rt
	}

	return "cloudfront-js-2.0"
}

func (rc *ResourceCreator) deleteCloudFrontFunction(name string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	return rc.backends.CloudFront.Backend.DeleteFunction(name)
}

func (rc *ResourceCreator) createCloudFrontCachePolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	cfg := cachePolicyConfig(logicalID, props, params, physicalIDs)

	policy, err := rc.backends.CloudFront.Backend.CreateCachePolicy(
		cfg.name, "", cfg.defaultTTL, cfg.maxTTL, cfg.minTTL,
	)
	if err != nil {
		return "", fmt.Errorf("create CloudFront cache policy %s: %w", cfg.name, err)
	}

	return policy.ID, nil
}

type cachePolicySettings struct {
	name                       string
	defaultTTL, maxTTL, minTTL int64
}

func cachePolicyConfig(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) cachePolicySettings {
	const (
		fallbackDefaultTTL = 86400
		fallbackMaxTTL     = 31536000
	)
	settings := cachePolicySettings{name: logicalID, defaultTTL: fallbackDefaultTTL, maxTTL: fallbackMaxTTL}

	cfg, ok := props["CachePolicyConfig"].(map[string]any)
	if !ok {
		return settings
	}
	if n := resolve(cfg["Name"], params, physicalIDs); n != "" {
		settings.name = n
	}
	if v := int64Val(cfg["DefaultTTL"]); v != 0 {
		settings.defaultTTL = v
	}
	if v := int64Val(cfg["MaxTTL"]); v != 0 {
		settings.maxTTL = v
	}
	settings.minTTL = int64Val(cfg["MinTTL"])

	return settings
}

func (rc *ResourceCreator) deleteCloudFrontCachePolicy(id string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	return rc.backends.CloudFront.Backend.DeleteCachePolicy(id)
}

func (rc *ResourceCreator) createCloudFrontOriginAccessControl(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	cfg := oacConfig(logicalID, props, params, physicalIDs)

	oac, err := rc.backends.CloudFront.Backend.CreateOriginAccessControl(
		cfg.name, "", cfg.originType, cfg.signingBehavior, cfg.signingProtocol,
	)
	if err != nil {
		return "", fmt.Errorf("create CloudFront origin access control %s: %w", cfg.name, err)
	}

	return oac.ID, nil
}

type oacSettings struct {
	name            string
	originType      string
	signingBehavior string
	signingProtocol string
}

func oacConfig(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) oacSettings {
	settings := oacSettings{name: logicalID, originType: "s3", signingBehavior: "always", signingProtocol: "sigv4"}

	cfg, ok := props["OriginAccessControlConfig"].(map[string]any)
	if !ok {
		return settings
	}
	if n := resolve(cfg["Name"], params, physicalIDs); n != "" {
		settings.name = n
	}
	if v := resolve(cfg["OriginAccessControlOriginType"], params, physicalIDs); v != "" {
		settings.originType = v
	}
	if v := resolve(cfg["SigningBehavior"], params, physicalIDs); v != "" {
		settings.signingBehavior = v
	}
	if v := resolve(cfg["SigningProtocol"], params, physicalIDs); v != "" {
		settings.signingProtocol = v
	}

	return settings
}

func (rc *ResourceCreator) deleteCloudFrontOriginAccessControl(id string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	return rc.backends.CloudFront.Backend.DeleteOriginAccessControl(id)
}

func (rc *ResourceCreator) createCloudFrontResponseHeadersPolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	name := logicalID
	if cfg, ok := props["ResponseHeadersPolicyConfig"].(map[string]any); ok {
		if n := resolve(cfg["Name"], params, physicalIDs); n != "" {
			name = n
		}
	}

	policy, err := rc.backends.CloudFront.Backend.CreateResponseHeadersPolicy(name, "")
	if err != nil {
		return "", fmt.Errorf("create CloudFront response headers policy %s: %w", name, err)
	}

	return policy.ID, nil
}

func (rc *ResourceCreator) deleteCloudFrontResponseHeadersPolicy(id string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	return rc.backends.CloudFront.Backend.DeleteResponseHeadersPolicy(id)
}

// ---- phase-5 property helpers ----

// intProp reads an integer-valued property, accepting JSON numbers (float64) and ints.
func intProp(props map[string]any, key string) int {
	return int(int64Val(props[key]))
}

// int64Val converts a JSON-decoded numeric value to int64. CloudFormation templates may carry
// numbers as float64 (JSON), int, or string. Returns 0 when the value is absent or unparseable.
func int64Val(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int:
		return int64(n)
	case int64:
		return n
	case json.Number:
		i, err := n.Int64()
		if err == nil {
			return i
		}
	}

	return 0
}

// strSliceProp resolves a property that is expected to be a list of strings (or refs).
func strSliceProp(v any, params, physicalIDs map[string]string) []string {
	list, ok := v.([]any)
>>>>>>> ef905acf (feat(cloudformation): §K pass-1 — 22 new CFN resource types)
	if !ok {
		return nil
	}

<<<<<<< HEAD
	const parts = 2
	split := strings.SplitN(physicalID, "/", parts)
	if len(split) < parts {
		return nil
	}

	return imb.DeleteAPIKey(split[0], split[1])
}

// parseAppSyncARNParts extracts apiID and resource name from an AppSync ARN.
// ARN format: arn:aws:appsync:<region>:<account>:apis/<apiID>/<resourceType>/<name>.
func parseAppSyncARNParts(arn, resourceType string) (string, string) {
	_, afterAPIs, hasAPIs := strings.Cut(arn, "apis/")
	if !hasAPIs {
		return "", ""
	}

	apiID, name, found := strings.Cut(afterAPIs, "/"+resourceType+"/")
	if !found {
		return "", ""
	}

	return apiID, name
=======
	out := make([]string, 0, len(list))
	for _, item := range list {
		if s := resolve(item, params, physicalIDs); s != "" {
			out = append(out, s)
		}
	}

	return out
}

// marshalJSON serializes a value to compact JSON bytes.
func marshalJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}

// getPhase5ResourceAttribute derives Fn::GetAtt attribute values for phase-5 resource types.
// It returns ok=false when resType is not a phase-5 type so the caller can fall back to physID.
func getPhase5ResourceAttribute(resType, physID, attrName, accountID, region string) (string, bool) {
	switch resType {
	case resTypeEC2Volume, resTypeEC2NetworkInterface:
		return physID, true
	case resTypeKMSAlias:
		if attrName == attrNameArn {
			return arn.Build("kms", region, accountID, physID), true
		}

		return physID, true
	case resTypeStepFunctionsActivity:
		if attrName == "Name" {
			return arnResourceTail(physID), true
		}

		return physID, true
	case resTypeEventsConnection:
		if attrName == attrNameArn {
			return arn.Build("events", region, accountID, "connection/"+physID), true
		}

		return physID, true
	case resTypeLogsLogStream, resTypeLogsMetricFilter, resTypeLogsSubscriptionFltr:
		// physID is "<group>|<child>"; GetAtt returns the child name.
		if _, child, ok := splitLogsPhysID(physID); ok {
			return child, true
		}

		return physID, true
	}

	return "", false
}

// arnResourceTail returns the final colon-delimited segment of an ARN (the resource name).
func arnResourceTail(s string) string {
	parts := strings.Split(s, ":")
	if len(parts) == 0 {
		return s
	}

	return parts[len(parts)-1]
>>>>>>> ef905acf (feat(cloudformation): §K pass-1 — 22 new CFN resource types)
}
