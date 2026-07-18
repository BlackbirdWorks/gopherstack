package cloudformation

import (
	"context"
	"fmt"
	"strings"

	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
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
	resTypeAPIGatewayV2Integ     = "AWS::ApiGatewayV2::Integration"
	resTypeAPIGatewayV2Route     = "AWS::ApiGatewayV2::Route"
)

func (rc *ResourceCreator) createExtraLogsResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
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

		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteExtraLogsResource(
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
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
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
		return logicalID + "-stub", nil
	}

	policyName := strProp(props, "PolicyName", params, physicalIDs)
	if policyName == "" {
		policyName = logicalID
	}

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
